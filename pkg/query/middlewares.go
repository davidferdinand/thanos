package query

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql"
)

type RangeCachedMiddleware struct {
	wrapped *API

	cache cache.Cache
}

func WrapWithRangeCacheMiddleware(api *API, cache cache.Cache) *RangeCachedMiddleware {
	return &RangeCachedMiddleware{wrapped: api, cache: cache}
}

const millisecondPerDay = int64(24 * time.Hour / time.Millisecond)

func (a *RangeCachedMiddleware) QueryRange(qs string, start, end time.Time, interval time.Duration, opts Options) (promql.Query, error) {
	span, ctx := tracing.StartSpan(q.ctx, "querier_cached_query_range")
	defer span.Finish()
	var (
		day      = start.Day()
		key      = fmt.Sprintf("%s:%s:%d:%d", userID, r.Query, r.Step, day)
		extents  []Extent
		response *APIResponse
	)

	return a.wrapped.QueryRange(qs, start, end, interval, opts)
}

type SplitByDayMiddleware struct {
	wrapped Range

	concurrency int
}

func WrapWithSplitByDayMiddleware(api *API) *SplitByDayMiddleware {
	return &SplitByDayMiddleware{wrapped: api}
}

type timeRange struct {
	start, end time.Time
}

func (m *SplitByDayMiddleware) QueryRange(ctx context.Context, qs string, start, end time.Time, interval time.Duration, opts Options) (promql.Value, []error, error) {
	span, ctx := tracing.StartSpan(ctx, "querier_cached_query_range")
	defer span.Finish()

	rngs := splitByDay(timestamp.FromTime(start), timestamp.FromTime(end), int64(interval/time.Millisecond))

	var (
		rngCh   = make(chan timeRange)
		g, gctx = errgroup.WithContext(ctx)
		mtx     = sync.Mutex{}
		vals    []promql.Value
		warns   []error
	)

	for i := 0; i < m.concurrency; i++ {
		g.Go(func() error {
			// TODO(bwplotka): Support partial response WARN?
			for rng := range rngCh {
				val, warns, err := m.wrapped.QueryRange(gctx, qs, rng.start, rng.end, interval, opts)
				if err != nil {
					return err
				}
				mtx.Lock()
				vals = append(vals, val)
				warns = append(warns, warns...)
				mtx.Unlock()
			}
			return nil
		})
	}

rngLoop:
	for _, rng := range rngs {
		select {
		case <-gctx.Done():
			break rngLoop
		case rngCh <- rng:
		}
	}
	close(rngCh)

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	if gctx.Err() != nil {
		return nil, nil, gctx.Err()
	}

	/*
		func matrixMerge(resps []*APIResponse) []SampleStream {
			output := map[string]*SampleStream{}
			for _, resp := range resps {
				for _, stream := range resp.Data.Result {
					metric := client.FromLabelAdaptersToLabels(stream.Labels).String()
					existing, ok := output[metric]
					if !ok {
						existing = &SampleStream{
							Labels: stream.Labels,
						}
					}
					existing.Samples = append(existing.Samples, stream.Samples...)
					output[metric] = existing
				}
			}

			keys := make([]string, 0, len(output))
			for key := range output {
				keys = append(keys, key)
			}
			sort.Strings(keys)

			result := make([]SampleStream, 0, len(output))
			for _, key := range keys {
				result = append(result, *output[key])
			}

			return result
		}
	*/

}

func splitByDay(start, end int64, interval int64) []timeRange {
	var rngs []timeRange

	for s := start; s < end; s = nextDayBoundary(s, interval) + interval {
		e := nextDayBoundary(s, interval)
		if e+interval >= end {
			e = end
		}

		rngs = append(rngs, timeRange{
			start: timestamp.Time(s),
			end:   timestamp.Time(e),
		})
	}
	return rngs
}

// Round up to the step before the next day boundary.
func nextDayBoundary(t, step int64) int64 {
	startOfNextDay := ((t / millisecondPerDay) + 1) * millisecondPerDay
	// ensure that target is a multiple of steps away from the start time
	target := startOfNextDay - ((startOfNextDay - t) % step)
	if target == startOfNextDay {
		target -= step
	}
	return target
}
