package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	provider := flag.String("provider", "", "Storage provider: aws or oci")
	configPath := flag.String("config", "", "Path to PBM storage config YAML")
	sizeFlag := flag.String("size", "100M", "Payload size to upload/download (e.g. 100M, 1G)")
	prefix := flag.String("prefix", "", "Object key prefix override (default: <config prefix>/storage-bench)")
	keep := flag.Bool("keep", false, "Keep the uploaded test object instead of deleting it")
	flag.Parse()

	if *provider == "" || *configPath == "" {
		flag.Usage()
		os.Exit(2)
	}

	size, err := parseSize(*sizeFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -size: %v\n", err)
		os.Exit(1)
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	switch strings.ToLower(*provider) {
	case "aws", "s3":
		s3Cfg, err := cfg.s3Config()
		if err != nil {
			fmt.Fprintf(os.Stderr, "aws config error: %v\n", err)
			os.Exit(1)
		}
		if err := runS3Benchmark(s3Cfg, s3ObjectPrefix(s3Cfg, *prefix), size, *keep); err != nil {
			fmt.Fprintf(os.Stderr, "benchmark failed: %v\n", err)
			os.Exit(1)
		}
	case "oci":
		ociCfg, err := cfg.ociConfig()
		if err != nil {
			fmt.Fprintf(os.Stderr, "oci config error: %v\n", err)
			os.Exit(1)
		}
		if err := runOCIBenchmark(ociCfg, ociObjectPrefix(ociCfg, *prefix), size, *keep); err != nil {
			fmt.Fprintf(os.Stderr, "benchmark failed: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unsupported provider %q (use aws or oci)\n", *provider)
		os.Exit(1)
	}
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Measure upload and download speed to object storage.\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n  storage-bench -provider aws|oci -config /path/to/pbm-storage.yaml [options]\n\n")
		fmt.Fprintf(os.Stderr, "The config file can be a full PBM storage config or just the s3/oci section.\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  storage-bench -provider aws -config /tmp/pbm-agent-storage-aws.yaml -size 500M\n")
		fmt.Fprintf(os.Stderr, "  storage-bench -provider oci -config /tmp/pbm-agent-storage-oci.yaml -size 1G\n\n")
		flag.PrintDefaults()
	}
}
