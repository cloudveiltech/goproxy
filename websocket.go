package goproxy

import (
	"bufio"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	tls "github.com/refraction-networking/utls"
)

func headerContains(header http.Header, name string, value string) bool {
	for _, v := range header[name] {
		for _, s := range strings.Split(v, ",") {
			if strings.EqualFold(value, strings.TrimSpace(s)) {
				return true
			}
		}
	}
	return false
}

func isWebSocketRequest(r *http.Request) bool {
	if strings.Contains(r.Method, "RDG") {
		return false
	}
	return headerContains(r.Header, "Connection", "upgrade") &&
		headerContains(r.Header, "Upgrade", "websocket")
}

func (proxy *ProxyHttpServer) serveWebsocketTLS(ctx *ProxyCtx, w http.ResponseWriter, req *http.Request, tlsConfig *tls.Config, clientConn *tls.Conn) {
	targetURL := url.URL{Scheme: "wss", Host: req.URL.Host, Path: req.URL.Path}

	// Connect to upstream
	targetConn, err := tls.Dial("tcp", targetURL.Host, tlsConfig)
	if err != nil {
		ctx.Warnf("Error dialing target site: %v", err)
		return
	}
	defer targetConn.Close()

	// Perform handshake
	if err := proxy.websocketHandshake(ctx, req, targetConn, clientConn); err != nil {
		ctx.Warnf("Websocket handshake error: %v", err)
		return
	}

	// Proxy wss connection
	proxy.proxyWebsocket(ctx, targetConn, clientConn)
}

func (proxy *ProxyHttpServer) serveWebsocket(ctx *ProxyCtx, w http.ResponseWriter, req *http.Request) {
	h, ok := w.(http.Hijacker)
	if !ok {
		return
	}

	client, _, err := h.Hijack()
	if err != nil {
		log.Printf("Websocket error Hijack %s", err)
		return
	}

	remote := dialRemote(req)
	if remote == nil {
		return
	}
	defer remote.Close()
	defer client.Close()

	log.Printf("Got websocket request %s %s", req.Host, req.URL)

	req.Write(remote)
	go func() {
		for {
			n, err := io.Copy(remote, client)
			if err != nil {
				log.Printf("Websocket error request %s", err)
				return
			}
			if n == 0 {
				log.Printf("Websocket nothing requested close")
				return
			}
			time.Sleep(time.Millisecond) //reduce CPU usage due to infinite nonblocking loop
		}
	}()

	for {
		n, err := io.Copy(client, remote)
		if err != nil {
			log.Printf("Websocket error response %s", err)
			return
		}
		if n == 0 {
			log.Printf("Websocket nothing responded close")
			return
		}
		time.Sleep(time.Millisecond) //reduce CPU usage due to infinite nonblocking loop
	}
}

func dialRemote(req *http.Request) net.Conn {
	port := ""
	if !strings.Contains(req.URL.Host, ":") {
		if req.URL.Scheme == "https" {
			port = ":443"
		} else {
			port = ":80"
		}
	}

	if req.URL.Scheme == "https" {
		conf := tls.Config{
			//InsecureSkipVerify: true,
		}
		remote, err := tls.Dial("tcp", req.URL.Host+port, &conf)
		if err != nil {
			log.Printf("Websocket error connect %s", err)
			return nil
		}
		return remote
	} else {
		remote, err := net.Dial("tcp", req.URL.Host+port)
		if err != nil {
			log.Printf("Websocket error connect %s", err)
			return nil
		}
		return remote
	}
}

func (proxy *ProxyHttpServer) websocketHandshake(ctx *ProxyCtx, req *http.Request, targetSiteConn io.ReadWriter, clientConn io.ReadWriter) error {
	// write handshake request to target
	err := req.Write(targetSiteConn)
	if err != nil {
		ctx.Warnf("Error writing upgrade request: %v", err)
		return err
	}

	targetTLSReader := bufio.NewReader(targetSiteConn)

	// Read handshake response from target
	resp, err := http.ReadResponse(targetTLSReader, req)
	if err != nil {
		ctx.Warnf("Error reading handhsake response  %v", err)
		return err
	}

	// Run response through handlers
	//resp = proxy.filterResponse(resp, ctx)

	// Proxy handshake back to client
	err = resp.Write(clientConn)
	if err != nil {
		ctx.Warnf("Error writing handshake response: %v", err)
		return err
	}
	return nil
}

func (proxy *ProxyHttpServer) proxyWebsocket(ctx *ProxyCtx, dest io.ReadWriter, source io.ReadWriter) {
	errChan := make(chan error, 2)
	cp := func(dst io.Writer, src io.Reader) {
		_, err := io.Copy(dst, src)
		ctx.Warnf("Websocket error: %v", err)
		errChan <- err
	}

	// Start proxying websocket data
	go cp(dest, source)
	go cp(source, dest)
	<-errChan
}
