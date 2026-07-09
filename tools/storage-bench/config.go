package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type pbmStorageConfig struct {
	Storage struct {
		Type string         `yaml:"type"`
		S3   *s3StorageYAML `yaml:"s3"`
		OCI  *ociStorageYAML `yaml:"oci"`
	} `yaml:"storage"`
	S3  *s3StorageYAML `yaml:"s3"`
	OCI *ociStorageYAML `yaml:"oci"`
}

type s3StorageYAML struct {
	Region               string            `yaml:"region"`
	EndpointURL          string            `yaml:"endpointUrl"`
	ForcePathStyle       *bool             `yaml:"forcePathStyle"`
	Bucket               string            `yaml:"bucket"`
	Prefix               string            `yaml:"prefix"`
	Credentials          s3CredentialsYAML `yaml:"credentials"`
	InsecureSkipTLSVerify bool             `yaml:"insecureSkipTLSVerify"`
}

type s3CredentialsYAML struct {
	AccessKeyID     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
	SessionToken    string `yaml:"session-token"`
}

type ociStorageYAML struct {
	Region      string              `yaml:"region"`
	Namespace   string              `yaml:"namespace"`
	Bucket      string              `yaml:"bucket"`
	Prefix      string              `yaml:"prefix"`
	Credentials ociCredentialsYAML  `yaml:"credentials"`
}

type ociCredentialsYAML struct {
	Type          string                    `yaml:"type"`
	UserPrincipal *ociUserPrincipalYAML     `yaml:"userPrincipal"`
}

type ociUserPrincipalYAML struct {
	Tenancy              string `yaml:"tenancy"`
	User                 string `yaml:"user"`
	Fingerprint          string `yaml:"fingerprint"`
	PrivateKey           string `yaml:"privateKey"`
	PrivateKeyPassphrase string `yaml:"privateKeyPassphrase"`
}

func loadConfig(path string) (pbmStorageConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return pbmStorageConfig{}, fmt.Errorf("read config: %w", err)
	}

	var cfg pbmStorageConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return pbmStorageConfig{}, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

func (c pbmStorageConfig) s3Config() (*s3StorageYAML, error) {
	switch {
	case c.Storage.S3 != nil:
		return c.Storage.S3, nil
	case c.S3 != nil:
		return c.S3, nil
	default:
		return nil, fmt.Errorf("config has no s3 section")
	}
}

func (c pbmStorageConfig) ociConfig() (*ociStorageYAML, error) {
	switch {
	case c.Storage.OCI != nil:
		return c.Storage.OCI, nil
	case c.OCI != nil:
		return c.OCI, nil
	default:
		return nil, fmt.Errorf("config has no oci section")
	}
}

func joinPrefix(base, suffix string) string {
	base = strings.Trim(base, "/")
	suffix = strings.Trim(suffix, "/")
	switch {
	case base == "":
		return suffix
	case suffix == "":
		return base
	default:
		return base + "/" + suffix
	}
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}

	multiplier := int64(1)
	switch {
	case strings.HasSuffix(s, "TB"):
		multiplier = 1024 * 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "TB")
	case strings.HasSuffix(s, "GB"), strings.HasSuffix(s, "G"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(strings.TrimSuffix(s, "GB"), "G")
	case strings.HasSuffix(s, "MB"), strings.HasSuffix(s, "M"):
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(strings.TrimSuffix(s, "MB"), "M")
	case strings.HasSuffix(s, "KB"), strings.HasSuffix(s, "K"):
		multiplier = 1024
		s = strings.TrimSuffix(strings.TrimSuffix(s, "KB"), "K")
	case strings.HasSuffix(s, "B"):
		s = strings.TrimSuffix(s, "B")
	}

	var value int64
	if _, err := fmt.Sscanf(s, "%d", &value); err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", s, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("size must be positive")
	}
	return value * multiplier, nil
}
