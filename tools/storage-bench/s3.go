package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3Bench struct {
	client *s3.Client
	bucket string
	key    string
}

func newS3Bench(cfg *s3StorageYAML, objectKey string) (*s3Bench, error) {
	if cfg == nil {
		return nil, fmt.Errorf("s3 config is nil")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 bucket is required")
	}

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	loadOpts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}
	if cfg.Credentials.AccessKeyID != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.Credentials.AccessKeyID,
			cfg.Credentials.SecretAccessKey,
			cfg.Credentials.SessionToken,
		)))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	s3Opts := func(o *s3.Options) {
		if cfg.EndpointURL != "" {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		}
		if cfg.ForcePathStyle != nil {
			o.UsePathStyle = *cfg.ForcePathStyle
		} else if cfg.EndpointURL != "" {
			o.UsePathStyle = true
		}
		if cfg.InsecureSkipTLSVerify {
			o.HTTPClient = &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
				},
			}
		}
	}

	return &s3Bench{
		client: s3.NewFromConfig(awsCfg, s3Opts),
		bucket: cfg.Bucket,
		key:    objectKey,
	}, nil
}

func (b *s3Bench) upload(size int64) error {
	uploader := manager.NewUploader(b.client, func(u *manager.Uploader) {
		u.PartSize = 10 * 1024 * 1024
		u.Concurrency = 5
	})

	_, err := uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.key),
		Body:   newZeroReader(size),
	})
	return err
}

func (b *s3Bench) download() error {
	out, err := b.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.key),
	})
	if err != nil {
		return err
	}
	defer out.Body.Close()

	_, err = io.Copy(io.Discard, out.Body)
	return err
}

func (b *s3Bench) delete() error {
	_, err := b.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.key),
	})
	return err
}

func runS3Benchmark(cfg *s3StorageYAML, prefix string, size int64, keep bool) error {
	objectKey := joinPrefix(prefix, fmt.Sprintf("storage-bench-%d.bin", time.Now().UnixNano()))
	fmt.Printf("S3 bucket: %s\n", cfg.Bucket)
	fmt.Printf("Object key: %s\n", objectKey)
	fmt.Printf("Payload size: %d bytes (%0.2f MiB)\n\n", size, float64(size)/(1024*1024))

	bench, err := newS3Bench(cfg, objectKey)
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

func s3ObjectPrefix(cfg *s3StorageYAML, override string) string {
	if override != "" {
		return strings.Trim(override, "/")
	}
	return joinPrefix(cfg.Prefix, "storage-bench")
}
