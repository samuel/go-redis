package redis

import (
	"bytes"
	"testing"
)

func TestPipeline(t *testing.T) {
	// Use a non-standard port to avoid accidently clobbering
	c := NewClient("tcp", "127.0.0.1:6999")
	if err := c.Ping(); err != nil {
		t.Fatalf("Ping failed with %+v", err)
	}

	if err := c.Select(0); err != nil {
		t.Fatalf("Select failed with %+v", err)
	}

	const count = 100

	by := []byte{1, 2}

	checkMallocs2(t, "Pipeline.GET", count, func(t *testing.T, count int) {
		pipe, err := c.Pipeline()
		if err != nil {
			t.Fatalf("Pipeline failed with %+v", err)
		}
		r := pipe.Get("test")
		for i := 0; i < count; i++ {
			pipe.Get("test")
		}
		results, err := pipe.Flush()
		if err != nil {
			t.Fatalf("Pipeline.Flush failed with %+v", err)
		}
		if len(results) != count+1 {
			t.Fatalf("Pipeline results len expected to be %d but is instead %d", count+1, len(results))
		}
		if r.Value() == nil || !bytes.Equal(r.Value(), by) {
			t.Fatalf("Pipeline.Get returned %+v instead of %+v as expected", r.Value(), by)
		}
		for _, rr := range results {
			r := rr.(*BulkReply)
			if r.Value() == nil || !bytes.Equal(r.Value(), by) {
				t.Fatalf("Pipeline.Get returned %+v instead of %+v as expected", r.Value(), by)
			}
		}
	})
}
