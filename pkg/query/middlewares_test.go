package query

import (
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/stretchr/testify/require"
)

const seconds = 1e3 // 1e3 milliseconds per second.

func TestNextDayBoundary(t *testing.T) {
	for i, tc := range []struct {
		in, step, out int64
	}{
		// Smallest possible period is 1 millisecond
		{0, 1, millisecondPerDay - 1},
		// A more standard example
		{0, 15 * seconds, millisecondPerDay - 15*seconds},
		// Move start time forward 1 second; end time moves the same
		{1 * seconds, 15 * seconds, millisecondPerDay - (15-1)*seconds},
		// Move start time forward 14 seconds; end time moves the same
		{14 * seconds, 15 * seconds, millisecondPerDay - (15-14)*seconds},
		// Now some examples where the period does not divide evenly into a day:
		// 1 day modulus 35 seconds = 20 seconds
		{0, 35 * seconds, millisecondPerDay - 20*seconds},
		// Move start time forward 1 second; end time moves the same
		{1 * seconds, 35 * seconds, millisecondPerDay - (20-1)*seconds},
		// If the end time lands exactly on midnight we stop one period before that
		{20 * seconds, 35 * seconds, millisecondPerDay - 35*seconds},
		// This example starts 35 seconds after the 5th one ends
		{millisecondPerDay + 15*seconds, 35 * seconds, 2*millisecondPerDay - 5*seconds},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require.Equal(t, tc.out, nextDayBoundary(tc.in, tc.step))
		})
	}
}

func TestSplitByDay(t *testing.T) {
	for i, tc := range []struct {
		start, end int64
		interval   int64
		expected   []timeRange
	}{
		{
			start:    0,
			end:      60 * 60 * seconds,
			interval: 15 * seconds,
			expected: []timeRange{
				{
					start: timestamp.Time(0),
					end:   timestamp.Time(60 * 60 * seconds),
				},
			},
		},
		{
			start:    0,
			end:      24 * 3600 * seconds,
			interval: 15 * seconds,
			expected: []timeRange{
				{
					start: timestamp.Time(0),
					end:   timestamp.Time(24 * 3600 * seconds),
				},
			},
		},
		{
			start:    0,
			end:      2 * 24 * 3600 * seconds,
			interval: 15 * seconds,
			expected: []timeRange{
				{
					start: timestamp.Time(0),
					end:   timestamp.Time((24 * 3600 * seconds) - (15 * seconds)),
				},
				{
					start: timestamp.Time(0),
					end:   timestamp.Time(2 * 24 * 3600 * seconds),
				},
			},
		},
		{
			start:    3 * 3600 * seconds,
			end:      3 * 24 * 3600 * seconds,
			interval: 15 * seconds,
			expected: []timeRange{
				{
					start: timestamp.Time(3 * 3600 * seconds),
					end:   timestamp.Time((24 * 3600 * seconds) - (15 * seconds)),
				},
				{
					start: timestamp.Time(24 * 3600 * seconds),
					end:   timestamp.Time((2 * 24 * 3600 * seconds) - (15 * seconds)),
				},
				{
					start: timestamp.Time(2 * 24 * 3600 * seconds),
					end:   timestamp.Time(3 * 24 * 3600 * seconds),
				},
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			days := splitByDay(tc.start, tc.end, tc.interval)
			require.Equal(t, tc.expected, days)
		})
	}
}
