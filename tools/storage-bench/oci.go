package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/common/auth"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/oracle/oci-go-sdk/v65/objectstorage/transfer"
)

type ociBench struct {
	client    *objectstorage.ObjectStorageClient
	namespace string
	bucket    string
	key       string
}

func newOCIBench(cfg *ociStorageYAML, objectKey string) (*ociBench, error) {
	if cfg == nil {
		return nil, fmt.Errorf("oci config is nil")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("oci region is required")
	}
	if cfg.Namespace == "" {
		return nil, fmt.Errorf("oci namespace is required")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("oci bucket is required")
	}

	provider, err := ociConfigurationProvider(cfg)
	if err != nil {
		return nil, err
	}

	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		return nil, fmt.Errorf("create oci client: %w", err)
	}
	client.HTTPClient = &http.Client{}

	return &ociBench{
		client:    &client,
		namespace: cfg.Namespace,
		bucket:    cfg.Bucket,
		key:       objectKey,
	}, nil
}

func ociConfigurationProvider(cfg *ociStorageYAML) (common.ConfigurationProvider, error) {
	authType := cfg.Credentials.Type
	if authType == "" {
		authType = "userPrincipal"
	}

	switch authType {
	case "userPrincipal":
		creds := cfg.Credentials.UserPrincipal
		if creds == nil {
			return nil, fmt.Errorf("credentials.userPrincipal is required")
		}
		if creds.Tenancy == "" {
			return nil, fmt.Errorf("credentials.userPrincipal.tenancy is required")
		}
		if creds.User == "" {
			return nil, fmt.Errorf("credentials.userPrincipal.user is required")
		}
		if creds.Fingerprint == "" {
			return nil, fmt.Errorf("credentials.userPrincipal.fingerprint is required")
		}
		if creds.PrivateKey == "" {
			return nil, fmt.Errorf("credentials.userPrincipal.privateKey is required")
		}
		var passphrase *string
		if creds.PrivateKeyPassphrase != "" {
			passphrase = common.String(creds.PrivateKeyPassphrase)
		}
		// NewRawConfigurationProvider(tenancy, user, region, fingerprint, privateKey, passphrase)
		return common.NewRawConfigurationProvider(
			creds.Tenancy,
			creds.User,
			cfg.Region,
			creds.Fingerprint,
			creds.PrivateKey,
			passphrase,
		), nil
	case "instancePrincipal":
		return auth.InstancePrincipalConfigurationProviderForRegion(common.StringToRegion(cfg.Region))
	case "okeWorkloadIdentity":
		return auth.OkeWorkloadIdentityConfigurationProvider()
	default:
		return nil, fmt.Errorf("unsupported oci credentials type %q", authType)
	}
}

func (b *ociBench) upload(size int64) error {
	_, err := transfer.NewUploadManager().UploadStream(
		context.Background(),
		transfer.UploadStreamRequest{
			UploadRequest: transfer.UploadRequest{
				ObjectStorageClient:   b.client,
				NamespaceName:         common.String(b.namespace),
				BucketName:            common.String(b.bucket),
				ObjectName:            common.String(b.key),
				PartSize:              common.Int64(10 * 1024 * 1024),
				AllowMultipartUploads: common.Bool(true),
				AllowParrallelUploads: common.Bool(true),
				NumberOfGoroutines:    common.Int(5),
			},
			StreamReader: newZeroReader(size),
		},
	)
	return err
}

func (b *ociBench) download() error {
	resp, err := b.client.GetObject(context.Background(), objectstorage.GetObjectRequest{
		NamespaceName: common.String(b.namespace),
		BucketName:    common.String(b.bucket),
		ObjectName:    common.String(b.key),
	})
	if err != nil {
		return err
	}
	defer resp.Content.Close()

	_, err = io.Copy(io.Discard, resp.Content)
	return err
}

func (b *ociBench) delete() error {
	_, err := b.client.DeleteObject(context.Background(), objectstorage.DeleteObjectRequest{
		NamespaceName: common.String(b.namespace),
		BucketName:    common.String(b.bucket),
		ObjectName:    common.String(b.key),
	})
	return err
}

func runOCIBenchmark(cfg *ociStorageYAML, prefix string, size int64, keep bool) error {
	objectKey := joinPrefix(prefix, fmt.Sprintf("storage-bench-%d.bin", time.Now().UnixNano()))
	fmt.Printf("OCI bucket: %s (namespace %s, region %s)\n", cfg.Bucket, cfg.Namespace, cfg.Region)
	fmt.Printf("Object key: %s\n", objectKey)
	fmt.Printf("Payload size: %d bytes (%0.2f MiB)\n\n", size, float64(size)/(1024*1024))

	bench, err := newOCIBench(cfg, objectKey)
	if err != nil {
		return err
	}

	upload, err := runBench("upload", size, func() error { return bench.upload(size) })
	if err != nil {
		return err
	}
	printResult("Upload", upload)

	download, err := runBench("download", size, bench.download)
	if err != nil {
		return err
	}
	printResult("Download", download)

	if !keep {
		if err := bench.delete(); err != nil {
			return fmt.Errorf("cleanup: %w", err)
		}
		fmt.Printf("\nDeleted test object %s\n", objectKey)
	} else {
		fmt.Printf("\nKept test object %s\n", objectKey)
	}

	return nil
}

func ociObjectPrefix(cfg *ociStorageYAML, override string) string {
	if override != "" {
		return strings.Trim(override, "/")
	}
	return joinPrefix(cfg.Prefix, "storage-bench")
}
