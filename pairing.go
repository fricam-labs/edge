package main

import (
	"crypto/rand"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/skip2/go-qrcode"
)

//go:embed app-icon.svg
var appIconSVG []byte

const pairingCodeTTL = 5 * time.Minute

var pairingRandomRead = rand.Read
var pairingQREncode = qrcode.Encode

type pairingPayload struct {
	Version  int    `json:"v"`
	Type     string `json:"type"`
	Origin   string `json:"origin"`
	Code     string `json:"code"`
	DeviceID string `json:"deviceId"`
}

type pairingManager struct {
	identity edgeIdentity
	now      func() time.Time
	mu       sync.Mutex
	codes    map[string]time.Time
}

func newPairingManager(identity edgeIdentity) *pairingManager {
	return &pairingManager{identity: identity, now: time.Now, codes: make(map[string]time.Time)}
}

func (p *pairingManager) issue(origin string) (pairingPayload, error) {
	random := make([]byte, 24)
	if _, err := pairingRandomRead(random); err != nil {
		return pairingPayload{}, err
	}
	code := base64.RawURLEncoding.EncodeToString(random)
	now := p.now()
	p.mu.Lock()
	for existing, expires := range p.codes {
		if !expires.After(now) || len(p.codes) >= 64 {
			delete(p.codes, existing)
		}
	}
	p.codes[code] = now.Add(pairingCodeTTL)
	p.mu.Unlock()
	return pairingPayload{1, "fricam-edge-pair", origin, code, p.identity.DeviceID}, nil
}

func (p *pairingManager) claim(code string) bool {
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	expires, ok := p.codes[code]
	if !ok || !expires.After(now) {
		delete(p.codes, code)
		return false
	}
	delete(p.codes, code)
	return true
}

func pairingOrigin(request *http.Request) (string, bool) {
	host := request.Host
	hostname, _, err := net.SplitHostPort(host)
	if err != nil {
		hostname = host
	}
	if !isPrivateAuthHost(strings.Trim(hostname, "[]")) {
		return "", false
	}
	scheme := "http"
	if request.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + host, true
}

func (p *pairingManager) servePage(w http.ResponseWriter, request *http.Request) {
	origin, ok := pairingOrigin(request)
	if !ok {
		http.Error(w, "pairing is available only on the private network", http.StatusForbidden)
		return
	}
	payload, err := p.issue(origin)
	if err != nil {
		http.Error(w, "could not create pairing code", http.StatusInternalServerError)
		return
	}
	raw, _ := json.Marshal(payload)
	png, err := pairingQREncode(string(raw), qrcode.Medium, 384)
	if err != nil {
		http.Error(w, "could not render pairing code", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; img-src data:; style-src 'unsafe-inline'")
	_, _ = fmt.Fprintf(w, `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="dark"><title>Connect Fricam Edge</title>
<style>
:root{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#f3f1ec;background:#0c0d0e}
*{box-sizing:border-box}body{margin:0;min-height:100vh;background:#0c0d0e}
main{width:min(100%%,980px);min-height:100vh;margin:auto;padding:56px 40px;display:grid;grid-template-columns:minmax(0,.85fr) minmax(320px,1fr);gap:72px;align-items:center}
.brand{display:flex;align-items:center;gap:12px;font-size:14px;font-weight:700;letter-spacing:.01em;color:#f3f1ec}.brand-logo{width:36px;height:36px;border-radius:9px;flex:none}
h1{font-size:clamp(38px,6vw,64px);line-height:.98;letter-spacing:-.035em;margin:44px 0 20px;max-width:8ch}p{font-size:18px;line-height:1.55;color:#aaa8a2;margin:0;max-width:36ch}
.path{margin-top:36px;padding-top:24px;border-top:1px solid #2c2e30;color:#f3f1ec;font-size:15px;line-height:1.6}.path strong{color:#ffb800}.privacy{display:flex;gap:10px;align-items:flex-start;margin-top:24px;color:#898985;font-size:14px;line-height:1.45}.dot{width:8px;height:8px;border-radius:50%%;margin-top:6px;background:#70c59b;flex:none}
.code{margin:0}.qr{background:#fff;border-radius:16px;padding:18px;box-shadow:0 24px 64px rgba(0,0,0,.28)}img{display:block;width:100%%;height:auto}.expiry{text-align:center;margin-top:16px;color:#aaa8a2;font-size:14px}.expiry strong{color:#f3f1ec}
@media(max-width:700px){main{padding:28px 20px 40px;grid-template-columns:1fr;gap:36px;align-content:start}.brand{margin-top:8px}h1{margin-top:32px;font-size:44px;max-width:none}.copy p{font-size:16px}.code{width:min(100%%,420px);margin:auto}.path{margin-top:24px}}
</style></head><body><main><section class="copy"><div class="brand"><img class="brand-logo" alt="" src="data:image/svg+xml;base64,%s">Fricam Edge</div><h1>Scan to connect</h1><p>Link this Frigate to Fricam for faster, more reliable remote live video.</p><div class="path">In Fricam, open <strong>Settings → Fricam Edge</strong>, choose the QR option, then scan this code.</div><div class="privacy"><span class="dot"></span><span>Local, one-time pairing. Your Frigate password is never included.</span></div></section><figure class="code"><div class="qr"><img alt="Fricam Edge pairing QR code" src="data:image/png;base64,%s"></div><figcaption class="expiry"><strong>Ready to scan</strong> · Code expires in 5 minutes</figcaption></figure></main></body></html>`, base64.StdEncoding.EncodeToString(appIconSVG), base64.StdEncoding.EncodeToString(png))
}
