package redis

import (
	"bytes"
	"runtime"
	"testing"
)

func checkMallocs(t *testing.T, key string, count int, fn func(t *testing.T)) {
	memstats := new(runtime.MemStats)
	runtime.ReadMemStats(memstats)
	mallocs := 0 - memstats.Mallocs

	for i := 0; i < count; i++ {
		fn(t)
	}

	runtime.ReadMemStats(memstats)
	mallocs += memstats.Mallocs
	t.Logf("mallocs per %s: %d\n", key, mallocs/uint64(count))

}

func TestCommands(t *testing.T) {
	// Use a non-standard port to avoid accidently clobbering
	c := NewClient("tcp", "127.0.0.1:6999")
	if err := c.Ping(); err != nil {
		t.Fatalf("Ping failed with %+v", err)
	}

	if err := c.Select(0); err != nil {
		t.Fatalf("Select failed with %+v", err)
	}

	by := []byte{1, 2}

	checkMallocs(t, "PING", 100, func(t *testing.T) {
		if err := c.Ping(); err != nil {
			t.Fatalf("PING failed with %+v", err)
		}
	})
	if err := c.Set("test", []byte("0"), 0); err != nil {
		t.Fatalf("SET failed with %+v", err)
	}
	checkMallocs(t, "INCR", 100, func(t *testing.T) {
		if _, err := c.Incr("test"); err != nil {
			t.Fatalf("INCR failed with %+v", err)
		}
	})
	checkMallocs(t, "SET", 100, func(t *testing.T) {
		if err := c.Set("test", by, 0); err != nil {
			t.Fatalf("Set failed with %+v", err)
		}
	})
	checkMallocs(t, "GET", 100, func(t *testing.T) {
		if v, err := c.Get("test"); err != nil {
			t.Fatalf("Get failed with %+v", err)
		} else if !bytes.Equal(v, by) {
			t.Fatal("Get returned unequal response")
		}
	})

	if ok, err := c.SetNX("test", by, -1); err != nil {
		t.Fatalf("SetNX failed with %+v", err)
	} else if ok {
		t.Fatal("SetNX should have returned !ok")
	}
}
