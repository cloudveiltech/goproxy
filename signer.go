package goproxy

import (
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"runtime"
	"sort"
	"time"
)

func hashSorted(lst []string) []byte {
	c := make([]string, len(lst))
	copy(c, lst)
	sort.Strings(c)
	h := sha1.New()
	for _, s := range c {
		h.Write([]byte(s + ","))
	}
	return h.Sum(nil)
}

func hashSortedBigInt(lst []string) *big.Int {
	rv := new(big.Int)
	rv.SetBytes(hashSorted(lst))
	return rv
}

var goproxySignerVersion = ":goroxy1"
var hostMap map[string]tls.Certificate

func signHost(ca tls.Certificate, hosts []string) (cert tls.Certificate, err error) {
	var x509ca *x509.Certificate

	// FIXME: There is a bug here. If hosts[] is ever more than one element long, there will be potential for host mismatches.
	if hostMap != nil {
		if len(hosts) == 0 {
			return
		}

		cachedCert, ok := hostMap[hosts[0]]

		if ok {
			cert = cachedCert
			return
		}
	}

	// Use the provided ca and not the global GoproxyCa for certificate generation.
	if x509ca, err = x509.ParseCertificate(ca.Certificate[0]); err != nil {
		return
	}
	start := time.Unix(0, 0)
	end, err := time.Parse("2006-01-02", "2049-12-31")
	if err != nil {
		panic(err)
	}
	hash := hashSorted(append(hosts, goproxySignerVersion, ":"+runtime.Version()))
	serial := new(big.Int)
	serial.SetBytes(hash)
	template := x509.Certificate{
		// TODO(elazar): instead of this ugly hack, just encode the certificate and hash the binary form.
		SerialNumber: serial,
		Issuer:       x509ca.Subject,
		Subject: pkix.Name{
			Organization: []string{"GoProxy untrusted MITM proxy Inc"},
		},
		NotBefore: start,
		NotAfter:  end,

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
			template.Subject.CommonName = h
		}
	}
	var csprng CounterEncryptorRand
	if csprng, err = NewCounterEncryptorRandFromKey(ca.PrivateKey, hash); err != nil {
		return
	}
	var certpriv *rsa.PrivateKey
	if certpriv, err = rsa.GenerateKey(&csprng, 2048); err != nil {
		return
	}
	var derBytes []byte
	if derBytes, err = x509.CreateCertificate(&csprng, &template, x509ca, &certpriv.PublicKey, ca.PrivateKey); err != nil {
		return
	}
	tlsCert := tls.Certificate{
		Certificate: [][]byte{derBytes, ca.Certificate[0]},
		PrivateKey:  certpriv,
	}

	// Cache the certificate for later.
	if hostMap == nil {
		hostMap = make(map[string]tls.Certificate)
	}

	if hostMap != nil {
		for _, h := range hosts {
			hostMap[h] = tlsCert
		}
	}

	return tlsCert, nil
}
