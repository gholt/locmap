package valuelocmap

import (
	"os"
	"runtime"
	"strconv"
)

type config struct {
	workers         int
	roots           int
	pageSize        int
	splitMultiplier float64
}

func resolveConfig(opts ...func(*config)) *config {
	cfg := &config{}
	if env := os.Getenv("VALUELOCMAP_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.workers = val
		}
	} else if env = os.Getenv("VALUESTORE_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.workers = val
		}
	}
	if cfg.workers <= 0 {
		cfg.workers = runtime.GOMAXPROCS(0)
	}
	if env := os.Getenv("VALUELOCMAP_ROOTS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.roots = val
		}
	}
	if cfg.roots <= 0 {
		cfg.roots = cfg.workers * cfg.workers
	}
	if env := os.Getenv("VALUELOCMAP_PAGESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.pageSize = val
		}
	}
	if cfg.pageSize <= 0 {
		cfg.pageSize = 1048576
	}
	if env := os.Getenv("VALUELOCMAP_SPLITMULTIPLIER"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			cfg.splitMultiplier = val
		}
	}
	if cfg.splitMultiplier <= 0 {
		cfg.splitMultiplier = 3.0
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.workers < 1 {
		cfg.workers = 1
	}
	if cfg.roots < 2 {
		cfg.roots = 2
	}
	if cfg.pageSize < 1 {
		cfg.pageSize = 1
	}
	if cfg.splitMultiplier <= 0 {
		cfg.splitMultiplier = 0.01
	}
	return cfg
}

// OptList returns a slice with the opts given; useful if you want to possibly
// append more options to the list before using it with New(list...).
func OptList(opts ...func(*config)) []func(*config) {
	return opts
}

// OptWorkers indicates how many workers may be in use (for calculating the
// number of locks to create, for example). Note that the valuelocmap itself
// does not create any goroutines itself, but is written to allow concurrent
// access. Defaults to env VALUELOCMAP_WORKERS, VALUESTORE_WORKERS, or
// GOMAXPROCS.
func OptWorkers(count int) func(*config) {
	return func(cfg *config) {
		cfg.workers = count
	}
}

// OptRoots indicates how many top level nodes the map should have. More top
// level nodes means less contention but a bit more memory. Defaults to
// VALUELOCMAP_ROOTS or the OptWorkers value squared. This will be rounded up
// to the next power of two.
func OptRoots(roots int) func(*config) {
	return func(cfg *config) {
		cfg.roots = roots
	}
}

// OptPageSize controls the size of each chunk of memory allocated. Defaults to
// env VALUELOCMAP_PAGESIZE or 524,288.
func OptPageSize(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.pageSize = bytes
	}
}

// OptSplitMultiplier indicates how full a memory page can get before being
// split into two pages. Defaults to env VALUELOCMAP_SPLITMULTIPLIER or 3.0.
func OptSplitMultiplier(multiplier float64) func(*config) {
	return func(cfg *config) {
		cfg.splitMultiplier = multiplier
	}
}
