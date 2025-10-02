package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ovh/kmip-go"
	"github.com/ovh/kmip-go/kmipserver"
	"github.com/ovh/kmip-go/payloads"
)

/*
  Minimal server:
  - TLS/mTLS on :5696 (client verification only through CA)
  - Supported operations: DiscoverVersions, Create(AES), Register(SymmetricKey),
    Activate, Get, GetAttributes, GetAttributeList, Destroy, Locate(by Name)
  - Key storage — in memory (process)
*/

// ===== In-memory store =====

type objectRecord struct {
	ID        string
	Type      kmip.ObjectType
	State     kmip.State
	Names     []kmip.Name
	KeyBlock  *kmip.KeyBlock // only for SymmetricKey
	CreatedAt time.Time
}

type store struct {
	mu      sync.RWMutex
	objects map[string]*objectRecord
}

func newStore() *store { return &store{objects: map[string]*objectRecord{}} }

func (s *store) put(obj *objectRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[obj.ID] = obj
}

func (s *store) get(id string) (*objectRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.objects[id]
	return o, ok
}

func (s *store) destroy(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o, ok := s.objects[id]; ok {
		o.State = kmip.StateDestroyed
		o.KeyBlock = nil
		return true
	}
	return false
}

func (s *store) locateByName(name string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var ids []string
	for id, o := range s.objects {
		for _, n := range o.Names {
			if n.NameValue == name {
				ids = append(ids, id)
				break
			}
		}
	}
	return ids
}

// ===== Handler =====

type handler struct{ st *store }

func (h *handler) HandleRequest(ctx context.Context, req *kmip.RequestMessage) *kmip.ResponseMessage {
	resp := &kmip.ResponseMessage{
		Header: kmip.ResponseHeader{
			ProtocolVersion: req.Header.ProtocolVersion,
			BatchCount:      req.Header.BatchCount,
		},
	}

	for _, bi := range req.BatchItem {
		rbi := kmip.ResponseBatchItem{
			Operation:         bi.Operation,
			UniqueBatchItemID: bi.UniqueBatchItemID,
			ResultStatus:      kmip.ResultStatusSuccess,
		}

		if err := ctx.Err(); err != nil {
			rbi.ResultStatus = kmip.ResultStatusOperationFailed
			rbi.ResultReason = kmip.ResultReasonGeneralFailure
			rbi.ResultMessage = "context canceled"
			resp.BatchItem = append(resp.BatchItem, rbi)
			continue
		}

		var err error
		switch bi.Operation {
		case kmip.OperationDiscoverVersions:
			rbi.ResponsePayload = &payloads.DiscoverVersionsResponsePayload{
				ProtocolVersion: []kmip.ProtocolVersion{
					{ProtocolVersionMajor: 1, ProtocolVersionMinor: 4},
					{ProtocolVersionMajor: 1, ProtocolVersionMinor: 3},
					{ProtocolVersionMajor: 1, ProtocolVersionMinor: 2},
					{ProtocolVersionMajor: 1, ProtocolVersionMinor: 1},
					{ProtocolVersionMajor: 1, ProtocolVersionMinor: 0},
				},
			}

		case kmip.OperationCreate:
			err = h.opCreate(&rbi, bi.RequestPayload)

		case kmip.OperationRegister:
			err = h.opRegister(&rbi, bi.RequestPayload)

		case kmip.OperationActivate:
			err = h.opActivate(&rbi, bi.RequestPayload)

		case kmip.OperationGet:
			err = h.opGet(&rbi, bi.RequestPayload)

		case kmip.OperationGetAttributes:
			err = h.opGetAttributes(&rbi, bi.RequestPayload)

		case kmip.OperationGetAttributeList:
			err = h.opGetAttributeList(&rbi, bi.RequestPayload)

		case kmip.OperationDestroy:
			err = h.opDestroy(&rbi, bi.RequestPayload)

		case kmip.OperationLocate:
			err = h.opLocate(&rbi, bi.RequestPayload)

		default:
			err = fmt.Errorf("operation %v not implemented", bi.Operation)
			rbi.ResultStatus = kmip.ResultStatusOperationFailed
			rbi.ResultReason = kmip.ResultReasonOperationNotSupported
		}

		if err != nil {
			if rbi.ResultStatus == 0 {
				rbi.ResultStatus = kmip.ResultStatusOperationFailed
			}
			if rbi.ResultReason == 0 {
				rbi.ResultReason = kmip.ResultReasonGeneralFailure
			}
			rbi.ResultMessage = err.Error()
		}

		resp.BatchItem = append(resp.BatchItem, rbi)
	}

	return resp
}

// ---- operations ----

func (h *handler) opCreate(rbi *kmip.ResponseBatchItem, pl any) error {
	req, ok := pl.(*payloads.CreateRequestPayload)
	if !ok {
		return errors.New("bad payload for Create")
	}

	if req.ObjectType != kmip.ObjectTypeSymmetricKey {
		rbi.ResultReason = kmip.ResultReasonOperationNotSupported
		return fmt.Errorf("only SymmetricKey is supported")
	}

	algo := kmip.CryptographicAlgorithmAES
	var length int32 = 256
	var names []kmip.Name

	for _, a := range req.TemplateAttribute.Attribute {
		switch a.AttributeName {
		case kmip.AttributeNameCryptographicAlgorithm:
			if v, ok := a.AttributeValue.(kmip.CryptographicAlgorithm); ok {
				algo = v
			}
		case kmip.AttributeNameCryptographicLength:
			switch v := a.AttributeValue.(type) {
			case int32:
				length = v
			case int:
				length = int32(v)
			}
		case kmip.AttributeNameName:
			if v, ok := a.AttributeValue.(kmip.Name); ok {
				names = append(names, v)
			}
		}
	}

	if algo != kmip.CryptographicAlgorithmAES {
		rbi.ResultReason = kmip.ResultReasonOperationNotSupported
		return fmt.Errorf("only AES is supported")
	}
	if length != 128 && length != 192 && length != 256 {
		return fmt.Errorf("invalid AES length: %d (128/192/256)", length)
	}

	keyBytes := make([]byte, length/8)
	if _, err := rand.Read(keyBytes); err != nil {
		return err
	}

	keyBlock := &kmip.KeyBlock{
		KeyFormatType:          kmip.KeyFormatTypeTransparentSymmetricKey,
		CryptographicAlgorithm: algo,
		CryptographicLength:    length,
		KeyValue: &kmip.KeyValue{
			Plain: &kmip.PlainKeyValue{
				KeyMaterial: kmip.KeyMaterial{
					TransparentSymmetricKey: &kmip.TransparentSymmetricKey{Key: keyBytes},
				},
			},
		},
	}

	id := uuid.NewString()
	h.st.put(&objectRecord{
		ID:        id,
		Type:      kmip.ObjectTypeSymmetricKey,
		State:     kmip.StatePreActive,
		Names:     names,
		KeyBlock:  keyBlock,
		CreatedAt: time.Now(),
	})

	rbi.ResponsePayload = &payloads.CreateResponsePayload{
		ObjectType:       kmip.ObjectTypeSymmetricKey,
		UniqueIdentifier: id,
	}
	return nil
}

func (h *handler) opRegister(rbi *kmip.ResponseBatchItem, pl any) error {
	req, ok := pl.(*payloads.RegisterRequestPayload)
	if !ok {
		return errors.New("bad payload for Register")
	}
	switch o := req.Object.(type) {
	case *kmip.SymmetricKey:
		id := uuid.NewString()
		kb := o.KeyBlock
		h.st.put(&objectRecord{
			ID:        id,
			Type:      kmip.ObjectTypeSymmetricKey,
			State:     kmip.StatePreActive,
			KeyBlock:  &kb,
			CreatedAt: time.Now(),
		})
		rbi.ResponsePayload = &payloads.RegisterResponsePayload{UniqueIdentifier: id}
	default:
		rbi.ResultReason = kmip.ResultReasonOperationNotSupported
		return fmt.Errorf("Register supports only SymmetricKey")
	}
	return nil
}

func (h *handler) opActivate(rbi *kmip.ResponseBatchItem, pl any) error {
	req, ok := pl.(*payloads.ActivateRequestPayload)
	if !ok {
		return errors.New("bad payload for Activate")
	}
	rec, ok := h.st.get(req.UniqueIdentifier)
	if !ok {
		rbi.ResultReason = kmip.ResultReasonItemNotFound
		return fmt.Errorf("object not found")
	}
	rec.State = kmip.StateActive
	rbi.ResponsePayload = &payloads.ActivateResponsePayload{UniqueIdentifier: req.UniqueIdentifier}
	return nil
}

func (h *handler) opGet(rbi *kmip.ResponseBatchItem, pl any) error {
	req, ok := pl.(*payloads.GetRequestPayload)
	if !ok {
		return errors.New("bad payload for Get")
	}
	rec, ok := h.st.get(req.UniqueIdentifier)
	if !ok {
		rbi.ResultReason = kmip.ResultReasonItemNotFound
		return fmt.Errorf("object not found")
	}
	if rec.Type != kmip.ObjectTypeSymmetricKey || rec.KeyBlock == nil {
		rbi.ResultReason = kmip.ResultReasonOperationNotSupported
		return fmt.Errorf("only SymmetricKey is supported")
	}
	rbi.ResponsePayload = &payloads.GetResponsePayload{
		ObjectType: kmip.ObjectTypeSymmetricKey,
		Object:     &kmip.SymmetricKey{KeyBlock: *rec.KeyBlock}, // pointer!
	}
	return nil
}

func (h *handler) opGetAttributes(rbi *kmip.ResponseBatchItem, pl any) error {
	req, ok := pl.(*payloads.GetAttributesRequestPayload)
	if !ok {
		return errors.New("bad payload for GetAttributes")
	}
	rec, ok := h.st.get(req.UniqueIdentifier)
	if !ok {
		rbi.ResultReason = kmip.ResultReasonItemNotFound
		return fmt.Errorf("object not found")
	}
	attrs := []kmip.Attribute{
		{AttributeName: kmip.AttributeNameState, AttributeValue: rec.State},
	}
	if rec.KeyBlock != nil {
		attrs = append(attrs,
			kmip.Attribute{AttributeName: kmip.AttributeNameCryptographicAlgorithm, AttributeValue: rec.KeyBlock.CryptographicAlgorithm},
			kmip.Attribute{AttributeName: kmip.AttributeNameCryptographicLength, AttributeValue: rec.KeyBlock.CryptographicLength},
		)
	}
	for _, n := range rec.Names {
		attrs = append(attrs, kmip.Attribute{AttributeName: kmip.AttributeNameName, AttributeValue: n})
	}
	rbi.ResponsePayload = &payloads.GetAttributesResponsePayload{
		UniqueIdentifier: req.UniqueIdentifier,
		Attribute:        attrs,
	}
	return nil
}

func (h *handler) opGetAttributeList(rbi *kmip.ResponseBatchItem, pl any) error {
	req, ok := pl.(*payloads.GetAttributeListRequestPayload)
	if !ok {
		return errors.New("bad payload for GetAttributeList")
	}
	rec, ok := h.st.get(req.UniqueIdentifier)
	if !ok {
		rbi.ResultReason = kmip.ResultReasonItemNotFound
		return fmt.Errorf("object not found")
	}
	names := []kmip.AttributeName{kmip.AttributeNameState}
	if rec.KeyBlock != nil {
		names = append(names, kmip.AttributeNameCryptographicAlgorithm, kmip.AttributeNameCryptographicLength)
	}
	if len(rec.Names) > 0 {
		names = append(names, kmip.AttributeNameName)
	}
	rbi.ResponsePayload = &payloads.GetAttributeListResponsePayload{
		UniqueIdentifier: req.UniqueIdentifier,
		AttributeName:    names,
	}
	return nil
}

func (h *handler) opDestroy(rbi *kmip.ResponseBatchItem, pl any) error {
	req, ok := pl.(*payloads.DestroyRequestPayload)
	if !ok {
		return errors.New("bad payload for Destroy")
	}
	if !h.st.destroy(req.UniqueIdentifier) {
		rbi.ResultReason = kmip.ResultReasonItemNotFound
		return fmt.Errorf("object not found")
	}
	rbi.ResponsePayload = &payloads.DestroyResponsePayload{UniqueIdentifier: req.UniqueIdentifier}
	return nil
}

func (h *handler) opLocate(rbi *kmip.ResponseBatchItem, pl any) error {
	req, ok := pl.(*payloads.LocateRequestPayload)
	if !ok {
		return errors.New("bad payload for Locate")
	}
	var name string
	for _, a := range req.Attribute {
		if a.AttributeName == kmip.AttributeNameName {
			if v, ok := a.AttributeValue.(kmip.Name); ok {
				name = v.NameValue
				break
			}
		}
	}
	ids := []string{}
	if name != "" {
		ids = h.st.locateByName(name)
	}
	rbi.ResponsePayload = &payloads.LocateResponsePayload{UniqueIdentifier: ids}
	return nil
}

// ===== TLS & main =====

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func validateCACertificate(caPath string) error {
	caData := mustRead(caPath)
	
	// Just check that file exists and is not empty
	if len(caData) == 0 {
		return fmt.Errorf("CA certificate file is empty")
	}
	
	log.Printf("CA certificate file size: %d bytes", len(caData))
	
	// Accept file as-is, without parsing
	return nil
}

func validateCertificates(certPath, keyPath, caPath string) error {
	// Validate server certificate
	_, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return fmt.Errorf("invalid server certificate: %v", err)
	}
	
	// Validate CA certificate (simply check for existence)
	if err := validateCACertificate(caPath); err != nil {
		return fmt.Errorf("CA certificate validation failed: %v", err)
	}
	
	return nil
}

func createValidCACertificate(caPath string) error {
	caData := mustRead(caPath)
	
	// Create new file in temporary directory
	tempDir := os.TempDir()
	fileName := "ca_cert_" + time.Now().Format("20060102_150405") + ".pem"
	newPath := filepath.Join(tempDir, fileName)
	
	// Find start and end of certificate
	dataStr := string(caData)
	beginIndex := strings.Index(dataStr, "-----BEGIN CERTIFICATE-----")
	endIndex := strings.Index(dataStr, "-----END CERTIFICATE-----")
	
	if beginIndex == -1 || endIndex == -1 {
		return fmt.Errorf("certificate markers not found")
	}
	
	// Create new certificate
	newCert := dataStr[beginIndex:endIndex+len("-----END CERTIFICATE-----")]
	newCert += "\n"
	
	// Write new file
	if err := os.WriteFile(newPath, []byte(newCert), 0644); err != nil {
		return fmt.Errorf("cannot write new CA certificate: %v", err)
	}
	
	log.Printf("Created new CA certificate file: %s", newPath)
	log.Printf("New certificate size: %d bytes", len(newCert))
	
	// Check if new certificate can be parsed
	if _, err := x509.ParseCertificate([]byte(newCert)); err != nil {
		log.Printf("Warning: New certificate still cannot be parsed: %v", err)
	} else {
		log.Printf("New certificate parsed successfully")
	}
	
	return nil
}

func mustRead(path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read %s: %v", path, err)
	}
	return b
}

func cleanCACertificate(caData []byte) ([]byte, error) {
	// Convert to string for processing
	dataStr := string(caData)
	
	// Remove extra spaces and newline characters
	dataStr = strings.TrimSpace(dataStr)
	
	// Find start and end of certificate
	beginIndex := strings.Index(dataStr, "-----BEGIN CERTIFICATE-----")
	endIndex := strings.Index(dataStr, "-----END CERTIFICATE-----")
	
	if beginIndex == -1 || endIndex == -1 {
		return nil, fmt.Errorf("certificate markers not found")
	}
	
	// Remove everything before start of certificate
	dataStr = dataStr[beginIndex:]
	
	// Find end of certificate
	endIndex = strings.Index(dataStr, "-----END CERTIFICATE-----")
	if endIndex == -1 {
		return nil, fmt.Errorf("end certificate marker not found")
	}
	
	// Trim to end of certificate + marker length
	dataStr = dataStr[:endIndex+len("-----END CERTIFICATE-----")]
	
	// Add newline at end
	dataStr += "\n"
	
	log.Printf("Cleaned certificate: %d bytes", len(dataStr))
	log.Printf("Cleaned certificate starts: %s", dataStr[:min(100, len(dataStr))])
	
	return []byte(dataStr), nil
}

func loadCertificateChain(certPath, keyPath, caPath string) (tls.Certificate, error) {
	// Load server certificate with key
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("load server certificate: %v", err)
	}

	// Create simple certificate chain
	// Use only server certificate
	var certChain [][]byte
	
	// Add server certificate
	if len(cert.Certificate) > 0 {
		certChain = append(certChain, cert.Certificate[0])
		log.Printf("Added server certificate to chain")
	}

	// Create tls.Certificate with server certificate
	return tls.Certificate{
		Certificate: certChain,
		PrivateKey:  cert.PrivateKey,
		Leaf:        cert.Leaf,
	}, nil
}

func loadServerTLS(certPath, keyPath, caPath, serverName string) *tls.Config {
	// Load CA certificate for client authentication
	cp := x509.NewCertPool()
	caData := mustRead(caPath)
	
	// Simply add CA certificate to pool (like nginx does)
	if !cp.AppendCertsFromPEM(caData) {
		log.Printf("Warning: Could not append CA certificate to pool, but continuing...")
		// Create empty pool if CA cannot be loaded
		cp = x509.NewCertPool()
	} else {
		log.Printf("Successfully loaded CA certificate for client authentication")
	}
	
	// Load server certificate
	certChain, err := loadCertificateChain(certPath, keyPath, caPath)
	if err != nil {
		log.Fatalf("load certificate chain: %v", err)
	}
	
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certChain},
		ClientAuth:   tls.RequireAndVerifyClientCert, // mTLS
		ClientCAs:    cp,
		// Add additional security settings
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		},
		PreferServerCipherSuites: true,
	}
}

func loadServerTLSTest(certPath, keyPath, caPath, serverName string) *tls.Config {
	// Test TLS configuration for development
	certChain, err := loadCertificateChain(certPath, keyPath, caPath)
	if err != nil {
		log.Fatalf("load certificate chain: %v", err)
	}
	
	// Load CA certificate
	cp := x509.NewCertPool()
	caData := mustRead(caPath)
	if !cp.AppendCertsFromPEM(caData) {
		log.Printf("Warning: Could not append CA certificate to pool, but continuing...")
		cp = x509.NewCertPool()
	} else {
		log.Printf("Successfully loaded CA certificate for client authentication")
	}
	
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certChain},
		ClientAuth:   tls.RequireAndVerifyClientCert, // mTLS
		ClientCAs:    cp,
		// Test settings
		ServerName:   serverName,
		// For testing can use less strict settings
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		},
		PreferServerCipherSuites: true,
	}
}

func loadServerTLSInsecure(certPath, keyPath, caPath, serverName string) *tls.Config {
	// Insecure TLS configuration for testing (bypasses verification)
	certChain, err := loadCertificateChain(certPath, keyPath, caPath)
	if err != nil {
		log.Fatalf("load certificate chain: %v", err)
	}
	
	// Load CA certificate
	cp := x509.NewCertPool()
	caData := mustRead(caPath)
	if !cp.AppendCertsFromPEM(caData) {
		log.Printf("Warning: Could not append CA certificate to pool, but continuing...")
		cp = x509.NewCertPool()
	} else {
		log.Printf("Successfully loaded CA certificate for client authentication")
	}
	
	log.Printf("WARNING: Using insecure TLS configuration - server certificate verification disabled")
	
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certChain},
		ClientAuth:   tls.RequireAndVerifyClientCert, // mTLS
		ClientCAs:    cp,
		// Insecure settings for testing
		ServerName:   serverName,
		// Disable strict server certificate verification
		InsecureSkipVerify: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		},
		PreferServerCipherSuites: true,
	}
}

func main() {
	addr := getenv("KMIP_ADDR", ":5696")
	cert := getenv("KMIP_CERT", "/certs/server.crt")
	key := getenv("KMIP_KEY", "/certs/server.key")
	ca := getenv("KMIP_CA", "/certs/ca.pem")
	name := getenv("KMIP_NAME", "pykmip")
	testMode := getenv("KMIP_TEST_MODE", "false")
	insecureMode := getenv("KMIP_INSECURE_MODE", "false")

	// Validate certificates before startup
	if err := validateCertificates(cert, key, ca); err != nil {
		log.Fatalf("TLS validation failed: %v", err)
	}

	// Check TLS files existence
	for _, p := range []string{cert, key, ca} {
		if _, err := os.Stat(p); errors.Is(err, fs.ErrNotExist) {
			log.Fatalf("missing TLS file: %s", p)
		}
	}

	var tcfg *tls.Config
	if insecureMode == "true" {
		log.Printf("Using insecure TLS configuration (WARNING: not for production!)")
		tcfg = loadServerTLSInsecure(cert, key, ca, name)
	} else if testMode == "true" {
		log.Printf("Using test TLS configuration")
		tcfg = loadServerTLSTest(cert, key, ca, name)
	} else {
		log.Printf("Using production TLS configuration")
		tcfg = loadServerTLS(cert, key, ca, name)
	}
	
	ln, err := tls.Listen("tcp", addr, tcfg)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}
	defer ln.Close()

	srv := kmipserver.NewServer(ln, &handler{st: newStore()})
	log.Printf("KMIP server listening on %s", addr)

	if err := srv.Serve(); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Fatalf("server error: %v", err)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
