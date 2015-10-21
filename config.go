package valuelocmap

import (
	"os"
	"runtime"
	"strconv"
)

// Config represents the set of values for configuring a ValueLocMap. Note that
// changing the values in this structure will have no effect on existing
// ValueLocMaps; they are copied on instance creation.
type Config struct {
	// Workers indicates how many workers may be in use (for calculating the
	// number of locks to create, for example). Note that the ValueLocMap
	// does not create any goroutines itself, but is written to allow
	// concurrent access. Defaults to GOMAXPROCS.
	Workers int
	// Roots indicates how many top level nodes the map should have. More top
	// level nodes means less contention but a bit more memory. Defaults to the
	// Workers value squared. This will be rounded up to the next power of two.
	// The floor for this setting is 2.
	Roots int
	// PageSize controls the size in bytes of each chunk of memory allocated.
	// Defaults to 1,048,576 bytes. The floor for this setting is four times
	// the Sizeof an internal entry (4 * 40 = 160 bytes).
	PageSize int
	// SplitMultiplier indicates how full a memory page can get before being
	// split into two pages. Defaults to 3.0, which means 3 times as many
	// entries as the page alone has slots (overflow subpages are used on
	// collisions).
	SplitMultiplier float64
}

func resolveConfig(c *Config) *Config {
	cfg := &Config{}
	if c != nil {
		*cfg = *c
	}
	if env := os.Getenv("VALUELOCMAP_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.Workers = val
		}
	} else if env = os.Getenv("VALUESTORE_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.Workers = val
		}
	}
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	if cfg.Workers < 1 { // GOMAXPROCS should always give >= 1, but in case
		cfg.Workers = 1
	}
	if env := os.Getenv("VALUELOCMAP_ROOTS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.Roots = val
		}
	}
	if cfg.Roots <= 0 {
		cfg.Roots = cfg.Workers * cfg.Workers
	}
	// Because the Roots logic needs at least a bit to work with.
	if cfg.Roots < 2 {
		cfg.Roots = 2
	}
	if env := os.Getenv("VALUELOCMAP_PAGESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.PageSize = val
		}
	}
	if cfg.PageSize <= 0 {
		cfg.PageSize = 1048576
	}
	if env := os.Getenv("VALUELOCMAP_SPLITMULTIPLIER"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			cfg.SplitMultiplier = val
		}
	}
	if cfg.SplitMultiplier <= 0 {
		cfg.SplitMultiplier = 3.0
	}
	return cfg
}
