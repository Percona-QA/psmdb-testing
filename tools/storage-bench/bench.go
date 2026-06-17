package main

import (
	"fmt"
	"io"
	"time"
)

type benchResult struct {
	Bytes     int64
	Duration  time.Duration
	MBPerSec  float64
	MibPerSec float64
}

func runBench(label string, size int64, fn func() error) (benchResult, error) {
	start := time.Now()
	if err := fn(); err != nil {
		return benchResult{}, fmt.Errorf("%s: %w", label, err)
	}
	duration := time.Since(start)

	sec := duration.Seconds()
	if sec == 0 {
		sec = 1e-9
	}

	return benchResult{
		Bytes:     size,
		Duration:  duration,
		MBPerSec:  float64(size) / (1024 * 1024) / sec,
		MibPerSec: float64(size) / (1024 * 1024) / sec,
	}, nil
}

func printResult(label string, r benchResult) {
	fmt.Printf("%s: %s (%0.2f MiB/s)\n",
		label,
		r.Duration.Round(time.Millisecond),
		r.MibPerSec,
	)
}

type zeroReader struct {
	remaining int64
}

func (r *zeroReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > r.remaining {
		n = r.remaining
	}
	for i := int64(0); i < n; i++ {
		p[i] = 0
	}
	r.remaining -= n
	return int(n), nil
}

func newZeroReader(size int64) io.Reader {
	return &zeroReader{remaining: size}
}
