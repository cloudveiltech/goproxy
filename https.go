package goproxy

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	tls "github.com/refraction-networking/utls"
)

type ConnectActionLiteral int

const (
	ConnectAccept = iota
	ConnectReject
	ConnectMitm
	ConnectHijack
	ConnectHTTPMitm
	ConnectProxyAuthHijack
)

var (
	OkConnect       = &ConnectAction{Action: ConnectAccept, TLSConfig: TLSConfigFromCA(&GoproxyCa)}
	MitmConnect     = &ConnectAction{Action: ConnectMitm, TLSConfig: TLSConfigFromCA(&GoproxyCa)}
	HTTPMitmConnect = &ConnectAction{Action: ConnectHTTPMitm, TLSConfig: TLSConfigFromCA(&GoproxyCa)}
	RejectConnect   = &ConnectAction{Action: ConnectReject, TLSConfig: TLSConfigFromCA(&GoproxyCa)}
	httpsRegexp     = regexp.MustCompile(`^https:\/\/`)
)

type ConnectAction struct {
	Action    ConnectActionLiteral
	Hijack    func(req *http.Request, client net.Conn, ctx *ProxyCtx)
	TLSConfig func(host string, ctx *ProxyCtx) (*tls.Config, error)
}

func stripPort(s string) string {
	ix := strings.IndexRune(s, ':')
	if ix == -1 {
		return s
	}
	return s[:ix]
}

func (proxy *ProxyHttpServer) dial(network, addr string) (c net.Conn, err error) {
	if proxy.Tr.Dial != nil {
		return proxy.Tr.Dial(network, addr)
	}
	return net.Dial(network, addr)
}

func (proxy *ProxyHttpServer) connectDial(network, addr string) (c net.Conn, err error) {
	if proxy.ConnectDial == nil {
		return proxy.dial(network, addr)
	}
	return proxy.ConnectDial(network, addr)
}

type halfClosable interface {
	net.Conn
	CloseWrite() error
	CloseRead() error
}

var _ halfClosable = (*net.TCPConn)(nil)

func (proxy *ProxyHttpServer) handleHttps(w http.ResponseWriter, r *http.Request) {
	ctx := &ProxyCtx{Req: r, Session: atomic.AddInt64(&proxy.sess, 1), proxy: proxy, certStore: proxy.CertStore}

	hij, ok := w.(http.Hijacker)
	if !ok {
		panic("httpserver does not support hijacking")
	}

	proxyClient, _, e := hij.Hijack()
	if e != nil {
		panic("Cannot hijack connection " + e.Error())
	}

	ctx.Logf("Running %d CONNECT handlers", len(proxy.httpsHandlers))
	todo, host := OkConnect, r.URL.Host
	for i, h := range proxy.httpsHandlers {
		newtodo, newhost := h.HandleConnect(host, ctx)

		// If found a result, break the loop immediately
		if newtodo != nil {
			todo, host = newtodo, newhost
			ctx.Logf("on %dth handler: %v %s", i, todo, host)
			break
		}
	}
	switch todo.Action {
	case ConnectAccept:
		if !hasPort.MatchString(host) {
			host += ":80"
		}
		targetSiteCon, err := proxy.connectDial("tcp", host)
		if err != nil {
			httpError(proxyClient, ctx, err)
			return
		}
		ctx.Logf("Accepting CONNECT to %s", host)
		proxyClient.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))

		targetTCP, targetOK := targetSiteCon.(halfClosable)
		proxyClientTCP, clientOK := proxyClient.(halfClosable)
		if targetOK && clientOK {
			go copyAndClose(ctx, targetTCP, proxyClientTCP)
			go copyAndClose(ctx, proxyClientTCP, targetTCP)
		} else {
			go func() {
				var wg sync.WaitGroup
				wg.Add(2)
				go copyOrWarn(ctx, targetSiteCon, proxyClient, &wg)
				go copyOrWarn(ctx, proxyClient, targetSiteCon, &wg)
				wg.Wait()
				proxyClient.Close()
				targetSiteCon.Close()

			}()
		}

	case ConnectHijack:
		ctx.Logf("Hijacking CONNECT to %s", host)
		proxyClient.Write([]byte("HTTP/1.0 200 OK\r\n\r\n"))
		todo.Hijack(r, proxyClient, ctx)
	case ConnectHTTPMitm:
		proxyClient.Write([]byte("HTTP/1.0 200 OK\r\n\r\n"))
		ctx.Logf("Assuming CONNECT is plain HTTP tunneling, mitm proxying it")
		targetSiteCon, err := proxy.connectDial("tcp", host)
		if err != nil {
			ctx.Warnf("Error dialing to %s: %s", host, err.Error())
			return
		}
		for {
			client := bufio.NewReader(proxyClient)
			remote := bufio.NewReader(targetSiteCon)
			req, err := http.ReadRequest(client)
			if err != nil && err != io.EOF {
				ctx.Warnf("cannot read request of MITM HTTP client: %+#v", err)
			}
			if err != nil {
				return
			}
			req, resp := proxy.filterRequest(req, ctx)
			if resp == nil {
				if err := req.Write(targetSiteCon); err != nil {
					httpError(proxyClient, ctx, err)
					return
				}
				resp, err = http.ReadResponse(remote, req)
				if err != nil {
					httpError(proxyClient, ctx, err)
					return
				}
				defer resp.Body.Close()
			}
			resp = proxy.filterResponse(resp, ctx)
			if err := resp.Write(proxyClient); err != nil {
				httpError(proxyClient, ctx, err)
				return
			}
		}
	case ConnectMitm:
		proxyClient.Write([]byte("HTTP/1.0 200 OK\r\n\r\n"))
		ctx.Logf("Assuming CONNECT is TLS, mitm proxying it")
		// this goes in a separate goroutine, so that the net/http server won't think we're
		// still handling the request even after hijacking the connection. Those HTTP CONNECT
		// request can take forever, and the server will be stuck when "closed".
		// TODO: Allow Server.Close() mechanism to shut down this connection as nicely as possible
		tlsConfig := defaultTLSConfig
		if todo.TLSConfig != nil {
			var err error
			tlsConfig, err = todo.TLSConfig(host, ctx)
			if err != nil {
				httpError(proxyClient, ctx, err)
				return
			}
		}

		go func() {
			//TODO: cache connections to the remote website
			tlsConfig.Renegotiation = tls.RenegotiateFreelyAsClient
			tlsConfig.NextProtos = []string{"h2", "http/1.1"}
			var err error
			var proxyURL *url.URL
			var roundTripper http.RoundTripper
			host := r.Host

			if proxy.Tr.Proxy != nil {
				proxyURL, _ = proxy.Tr.Proxy(r)
				if proxyURL != nil {
					host = proxyURL.Host
				}
			}
			if host != r.Host {
				tlsConfig.NextProtos = []string{"http/1.1"}
			}

			remote := dialTls(host, r, ctx, tlsConfig)

			if remote == nil {
				tlsConfig.NextProtos = []string{"http/1.1"}
				rawClientTls := tls.Server(proxyClient, tlsConfig)
				rawClientTls.Handshake()
				rawClientTls.ConnectionState()
				httpError(rawClientTls, ctx, fmt.Errorf("Can't dial remote"))
				return
			}

			rawClientTls := tls.Server(proxyClient, tlsConfig)
			if err := rawClientTls.Handshake(); err != nil {
				ctx.Warnf("Cannot handshake Server %v %v", r.Host, err)
				return
			}

			clientHttpProtocol := rawClientTls.ConnectionState().NegotiatedProtocol
			if clientHttpProtocol != remote.(*tls.UConn).ConnectionState().NegotiatedProtocol {
				remote.Close()
				tlsConfig.NextProtos = []string{clientHttpProtocol}
				remote = dialTls(host, r, ctx, tlsConfig)
				if remote == nil {
					tlsConfig.NextProtos = []string{"http/1.1"}
					rawClientTls := tls.Server(proxyClient, tlsConfig)
					rawClientTls.Handshake()
					rawClientTls.ConnectionState()
					httpError(rawClientTls, ctx, fmt.Errorf("Can't dial remote"))
					return
				}
			}

			if rawClientTls.ConnectionState().NegotiatedProtocol != "h2" {
				tlsConfig.NextProtos = []string{"http/1.1"}
			} else {
				tlsConfig.NextProtos = []string{"h2", "http/1.1"}
			}

			if rawClientTls.ConnectionState().NegotiatedProtocol == "h2" {
				if proxy.Http2Handler != nil {
					if proxy.Http2Handler(r, rawClientTls, remote.(*tls.UConn)) {
						return
					} else {
						ctx.Warnf("Fail negotiate http2, switching to http/1.1")
					}
				}
			} else {
				ctx.Warnf("Fail negotiate http2, switching to http/1.1")
				tlsConfig.NextProtos = []string{"http/1.1", "h2"}
				tlsConfig.MinVersion = tls.VersionTLS12
				tlsConfig.InsecureSkipVerify = true
				roundTripper, err = NewUTLSRoundTripper("hellorandomizednoalpn_maxtls", tlsConfig, proxyURL)
				if err != nil {
					log.Printf("Cannot connect: %s %v", r.Host, err)
					httpError(rawClientTls, ctx, err)
					return
				}
			}

			//	defer remote.Close()
			//	defer rawClientTls.Close()
			clientTlsReader := bufio.NewReader(rawClientTls)
			for !isEof(clientTlsReader) {
				req, err := http.ReadRequest(clientTlsReader)
				if err != nil {
					ctx.Warnf("error read request %v", err)
					return
				}
				if strings.Contains(req.Method, "RDG") { //remote desktop gateway
					cp := func(dst io.Writer, src io.Reader) {
						io.Copy(dst, src)
					}

					req.Write(remote)
					// Start proxying websocket data
					go cp(rawClientTls, remote)
					cp(remote, rawClientTls)
					return
				}
				var ctx = &ProxyCtx{Req: req, Session: atomic.AddInt64(&proxy.sess, 1), proxy: proxy, UserData: ctx.UserData}
				if err != nil && err != io.EOF {
					return
				}
				if err != nil {
					ctx.Warnf("Cannot read TLS request from mitm'd client %v %v", r.Host, err)
					return
				}

				req.RemoteAddr = r.RemoteAddr // since we're converting the request, need to carry over the original connecting IP as well
				ctx.Logf("req %v", r.Host)

				if !httpsRegexp.MatchString(req.URL.String()) {
					req.URL, err = url.Parse("https://" + r.Host + req.URL.String())
				}
				if isWebSocketRequest(req) {
					ctx.Logf("Request looks like websocket upgrade.")
					err := req.Write(remote)
					if err != nil {
						httpError(rawClientTls, ctx, err)
						return
					}
					go func() {
						io.Copy(remote, rawClientTls)
					}()
					io.Copy(rawClientTls, remote)
					return
				}
				// Bug fix which goproxy fails to provide request
				// information URL in the context when does HTTPS MITM
				ctx.Req = req

				req, resp := proxy.filterRequest(req, ctx)
				if resp == nil {
					if err != nil {
						ctx.Warnf("Illegal URL %s", "https://"+r.Host+req.URL.Path)
						return
					}
					//	removeProxyHeaders(ctx, req)
					if roundTripper == nil {
						resp, err = ctx.RoundTrip(req)
					} else {
						resp, err = roundTripper.RoundTrip(req)
					}
					if err != nil {
						ctx.Warnf("Cannot read TLS response from mitm'd server %v", err)

						httpError(rawClientTls, ctx, err)
						rawClientTls.Close()
						remote.Close()
						return
					}
					ctx.Logf("resp %v", resp.Status)
				}
				state := remote.(*tls.UConn).ConnectionState()
				ctx.ConnectionState = &state
				resp = proxy.filterResponse(resp, ctx)
				defer resp.Body.Close()

				text := resp.Status

				statusCode := strconv.Itoa(resp.StatusCode) + " "
				if strings.HasPrefix(text, statusCode) {
					text = text[len(statusCode):]
				}
				// always use 1.1 to support chunked encoding
				if _, err := io.WriteString(rawClientTls, "HTTP/1.1"+" "+statusCode+text+"\r\n"); err != nil {
					ctx.Warnf("Cannot write TLS response HTTP status from mitm'd client: %v", err)
					return
				}
				// Since we don't know the length of resp, return chunked encoded response
				// TODO: use a more reasonable scheme
				resp.Header.Del("Content-Length")
				resp.Header.Set("Transfer-Encoding", "chunked")
				// Force connection close otherwise chrome will keep CONNECT tunnel open forever
				//	resp.Header.Set("Connection", "close")
				if err := resp.Header.Write(rawClientTls); err != nil {
					ctx.Warnf("Cannot write TLS response header from mitm'd client: %v", err)
					return
				}
				if _, err = io.WriteString(rawClientTls, "\r\n"); err != nil {
					ctx.Warnf("Cannot write TLS response header end from mitm'd client: %v", err)
					return
				}
				chunked := newChunkedWriter(rawClientTls)
				var written int64 = 1
				for written > 0 {
					written, _ = io.Copy(chunked, resp.Body)
					//ctx.Warnf("Cannot write TLS response body from mitm'd client: %v", err)
					//return
				}
				if err := chunked.Close(); err != nil {
					ctx.Warnf("Cannot write TLS chunked EOF from mitm'd client: %v", err)
					return
				}
				if _, err = io.WriteString(rawClientTls, "\r\n"); err != nil {
					ctx.Warnf("Cannot write TLS response chunked trailer from mitm'd client: %v", err)
					return
				}
			}
			ctx.Logf("Exiting on EOF")
		}()
	case ConnectProxyAuthHijack:
		proxyClient.Write([]byte("HTTP/1.1 407 Proxy Authentication Required\r\n"))
		todo.Hijack(r, proxyClient, ctx)
	case ConnectReject:
		if ctx.Resp != nil {
			if err := ctx.Resp.Write(proxyClient); err != nil {
				ctx.Warnf("Cannot write response that reject http CONNECT: %v", err)
			}
		}
		proxyClient.Close()
	}
}

func dialTls(host string, r *http.Request, ctx *ProxyCtx, tlsConfig *tls.Config) io.ReadWriteCloser {
	tcpConn, err := net.Dial("tcp", host)
	if err != nil {
		log.Printf("Cannot dial: %s %v", r.Host, err)
		return nil
	}

	var remote io.ReadWriteCloser = tcpConn
	if host == r.Host {
		clientHelloId := tls.HelloChrome_Auto
		invalidProtos := false
		for _, proto := range tlsConfig.NextProtos {
			if len(proto) == 0 || []rune(proto)[0] != 'h' {
				invalidProtos = true
				break
			}
		}
		if invalidProtos {
			log.Printf("Invalid NextProtos detected for host %s", host)
			tlsConfig.NextProtos = []string{"h2", "http/1.1"}
		}

		if len(tlsConfig.NextProtos) > 0 && tlsConfig.NextProtos[0] != "h2" {
			clientHelloId = tls.HelloRandomizedNoALPN
		}

		remoteTls := tls.UClient(tcpConn, tlsConfig, clientHelloId)
		err = remoteTls.Handshake()
		if err != nil {
			log.Printf("Cannot handshake: %s %v", r.Host, err)
			return nil
		}

		if remoteTls.ConnectionState().NegotiatedProtocol != "h2" {
			tlsConfig.NextProtos = []string{"http/1.1"}
		} else {
			tlsConfig.NextProtos = []string{"h2", "http/1.1"}
		}
		remote = remoteTls
	}
	return remote
}

func httpError(w io.WriteCloser, ctx *ProxyCtx, err error) {
	msg := fmt.Sprintf("HTTP/1.1 500 Server error\r\n\r\n%v\r\n", err)
	if _, err := io.WriteString(w, msg); err != nil {
		ctx.Warnf("Error responding to client: %s", err)
	}
	if err := w.Close(); err != nil {
		ctx.Warnf("Error closing client connection: %s", err)
	}
}

func copyOrWarn(ctx *ProxyCtx, dst io.Writer, src io.Reader, wg *sync.WaitGroup) {
	if _, err := io.Copy(dst, src); err != nil {
		ctx.Warnf("Error copying to client: %s", err)
	}
	wg.Done()
}

func copyAndClose(ctx *ProxyCtx, dst, src halfClosable) {
	if _, err := io.Copy(dst, src); err != nil {
		ctx.Warnf("Error copying to client: %s", err)
	}

	dst.CloseWrite()
	src.CloseRead()
}

func dialerFromEnv(proxy *ProxyHttpServer) func(network, addr string) (net.Conn, error) {
	https_proxy := os.Getenv("HTTPS_PROXY")
	if https_proxy == "" {
		https_proxy = os.Getenv("https_proxy")
	}
	if https_proxy == "" {
		return nil
	}
	return proxy.NewConnectDialToProxy(https_proxy)
}

func (proxy *ProxyHttpServer) NewConnectDialToProxy(https_proxy string) func(network, addr string) (net.Conn, error) {
	return proxy.NewConnectDialToProxyWithHandler(https_proxy, nil)
}

func (proxy *ProxyHttpServer) NewConnectDialToProxyWithHandler(https_proxy string, connectReqHandler func(req *http.Request)) func(network, addr string) (net.Conn, error) {
	u, err := url.Parse(https_proxy)
	if err != nil {
		return nil
	}
	if u.Scheme == "" || u.Scheme == "http" {
		if strings.IndexRune(u.Host, ':') == -1 {
			u.Host += ":80"
		}
		return func(network, addr string) (net.Conn, error) {
			connectReq := &http.Request{
				Method: "CONNECT",
				URL:    &url.URL{Opaque: addr},
				Host:   addr,
				Header: make(http.Header),
			}
			if connectReqHandler != nil {
				connectReqHandler(connectReq)
			}
			c, err := proxy.dial(network, u.Host)
			if err != nil {
				return nil, err
			}
			connectReq.Write(c)
			// Read response.
			// Okay to use and discard buffered reader here, because
			// TLS server will not speak until spoken to.
			br := bufio.NewReader(c)
			resp, err := http.ReadResponse(br, connectReq)
			if err != nil {
				c.Close()
				return nil, err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				resp, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return nil, err
				}
				c.Close()
				return nil, errors.New("proxy refused connection" + string(resp))
			}
			return c, nil
		}
	}
	if u.Scheme == "https" || u.Scheme == "wss" {
		if strings.IndexRune(u.Host, ':') == -1 {
			u.Host += ":443"
		}
		return func(network, addr string) (net.Conn, error) {
			c, err := proxy.dial(network, u.Host)
			if err != nil {
				return nil, err
			}
			c = tls.Client(c, defaultTLSConfig)
			connectReq := &http.Request{
				Method: "CONNECT",
				URL:    &url.URL{Opaque: addr},
				Host:   addr,
				Header: make(http.Header),
			}
			if connectReqHandler != nil {
				connectReqHandler(connectReq)
			}
			connectReq.Write(c)
			// Read response.
			// Okay to use and discard buffered reader here, because
			// TLS server will not speak until spoken to.
			br := bufio.NewReader(c)
			resp, err := http.ReadResponse(br, connectReq)
			if err != nil {
				c.Close()
				return nil, err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				body, err := ioutil.ReadAll(io.LimitReader(resp.Body, 500))
				if err != nil {
					return nil, err
				}
				c.Close()
				return nil, errors.New("proxy refused connection" + string(body))
			}
			return c, nil
		}
	}
	return nil
}

func TLSConfigFromCA(ca *tls.Certificate) func(host string, ctx *ProxyCtx) (*tls.Config, error) {
	return func(host string, ctx *ProxyCtx) (*tls.Config, error) {
		var err error
		var cert *tls.Certificate

		hostname := stripPort(host)
		config := *defaultTLSConfig
		ctx.Logf("signing for %s", stripPort(host))

		genCert := func() (*tls.Certificate, error) {
			return signHost(*ca, []string{hostname})
		}
		if ctx.certStore != nil {
			cert, err = ctx.certStore.Fetch(hostname, genCert)
		} else {
			cert, err = genCert()
		}

		if err != nil {
			ctx.Warnf("Cannot sign host certificate, provided CA: %s", err)
			return nil, err
		}

		config.ServerName = hostname
		config.Certificates = append(config.Certificates, *cert)
		return &config, nil
	}
}
