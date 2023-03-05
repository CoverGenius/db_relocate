// Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package aws

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	startOfTheDayTestTimestamp = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	ensOfTheDayTestTimestamp   = time.Date(2000, 1, 1, 23, 59, 0, 0, time.UTC)
)

func TestIsInServiceWindow(t *testing.T) {
	c := &Controller{}
	tests := []struct {
		name               string
		now                time.Time
		serviceWindowStart time.Time
		serviceWindowEnd   time.Time
		expected           bool
	}{
		{
			name:               "Current time is within service time window.",
			now:                time.Now().UTC(),
			serviceWindowStart: time.Now().UTC().Add(-time.Minute * 15),
			serviceWindowEnd:   time.Now().UTC().Add(time.Minute * 15),
			expected:           true,
		},
		{
			name:               "Current time is too close to service time window end.",
			now:                time.Now().UTC(),
			serviceWindowStart: time.Now().UTC().Add(-time.Minute * 60),
			serviceWindowEnd:   time.Now().UTC().Add(-time.Minute * time.Duration(SERVICE_WINDOW_HIGH_THRESHOLD-5)),
			expected:           true,
		},
		{
			name:               "Current time is too close to service time window start.",
			now:                time.Now().UTC(),
			serviceWindowStart: time.Now().UTC().Add(time.Hour * time.Duration(SERVICE_WINDOW_LOW_THRESHOLD-1)),
			serviceWindowEnd:   time.Now().UTC().Add(time.Hour * time.Duration(SERVICE_WINDOW_LOW_THRESHOLD+1)),
			expected:           true,
		},
		{
			name:               "Current time is on a safe distance after service time window end.",
			now:                time.Now().UTC(),
			serviceWindowStart: time.Now().UTC().Add(-time.Minute * 60),
			serviceWindowEnd:   time.Now().UTC().Add(-time.Minute * time.Duration(SERVICE_WINDOW_HIGH_THRESHOLD+5)),
			expected:           false,
		},
		{
			name:               "Current time is on a safe distance before service time window start.",
			now:                time.Now().UTC(),
			serviceWindowStart: time.Now().UTC().Add(time.Hour * time.Duration(SERVICE_WINDOW_LOW_THRESHOLD+1)),
			serviceWindowEnd:   time.Now().UTC().Add(time.Hour * time.Duration(SERVICE_WINDOW_LOW_THRESHOLD+2)),
			expected:           false,
		},
		{
			name:               "Current time is within service time window but falls on a previous day.",
			now:                startOfTheDayTestTimestamp.Add(time.Minute * 5).Add(-time.Hour * 24),
			serviceWindowStart: startOfTheDayTestTimestamp,
			serviceWindowEnd:   startOfTheDayTestTimestamp.Add(time.Minute * 15),
			expected:           false,
		},
		{
			name:               "Current time is too close to service time window end but falls on a previous day.",
			now:                startOfTheDayTestTimestamp.Add(time.Minute * 20).Add(-time.Hour * 24),
			serviceWindowStart: startOfTheDayTestTimestamp,
			serviceWindowEnd:   startOfTheDayTestTimestamp.Add(time.Minute * 15),
			expected:           false,
		},
		{
			name:               "Current time is too close to service time window start but falls on a previous day.",
			now:                startOfTheDayTestTimestamp.Add(-time.Hour * 25),
			serviceWindowStart: startOfTheDayTestTimestamp,
			serviceWindowEnd:   startOfTheDayTestTimestamp.Add(time.Minute * 15),
			expected:           false,
		},
		{
			name:               "Current time is on a safe distance after service time window end but falls on a previous day.",
			now:                startOfTheDayTestTimestamp.Add(time.Hour * 1),
			serviceWindowStart: startOfTheDayTestTimestamp,
			serviceWindowEnd:   startOfTheDayTestTimestamp.Add(time.Minute * 30),
			expected:           false,
		},
		{
			name:               "Current time is on a safe distance before service time window start but falls on a previous day.",
			now:                startOfTheDayTestTimestamp.Add(-time.Hour * 30),
			serviceWindowStart: startOfTheDayTestTimestamp,
			serviceWindowEnd:   startOfTheDayTestTimestamp.Add(time.Minute * 15),
			expected:           false,
		},
		{
			name:               "Current time is within service time window but falls on a next day.",
			now:                ensOfTheDayTestTimestamp.Add(time.Minute * 5).Add(time.Hour * 24),
			serviceWindowStart: ensOfTheDayTestTimestamp,
			serviceWindowEnd:   ensOfTheDayTestTimestamp.Add(time.Minute * 15),
			expected:           false,
		},
		{
			name:               "Current time is too close to service time window end but falls on a next day.",
			now:                ensOfTheDayTestTimestamp.Add(time.Minute * 20).Add(time.Hour * 24),
			serviceWindowStart: ensOfTheDayTestTimestamp,
			serviceWindowEnd:   ensOfTheDayTestTimestamp.Add(time.Minute * 15),
			expected:           false,
		},
		{
			name:               "Current time is too close to service time window start but falls on a next day.",
			now:                ensOfTheDayTestTimestamp.Add(-time.Hour * 1).Add(time.Hour * 24),
			serviceWindowStart: ensOfTheDayTestTimestamp,
			serviceWindowEnd:   ensOfTheDayTestTimestamp.Add(time.Minute * 15),
			expected:           false,
		},
		{
			name:               "Current time is on a safe distance after service time window end but falls on a next day.",
			now:                ensOfTheDayTestTimestamp.Add(time.Hour * 25),
			serviceWindowStart: ensOfTheDayTestTimestamp,
			serviceWindowEnd:   ensOfTheDayTestTimestamp.Add(time.Minute * 30),
			expected:           false,
		},
		{
			name:               "Current time is on a safe distance before service time window start but falls on a next day.",
			now:                ensOfTheDayTestTimestamp.Add(-time.Hour * 3).Add(time.Hour * 24),
			serviceWindowStart: ensOfTheDayTestTimestamp,
			serviceWindowEnd:   ensOfTheDayTestTimestamp.Add(time.Minute * 15),
			expected:           false,
		},
	}

	for _, test := range tests {
		testFunction := func(t *testing.T) {
			result := c.isInServiceWindow(&test.now, &test.serviceWindowStart, &test.serviceWindowEnd)
			assert.Equal(t, test.expected, result, "received result must match expected result")
		}
		t.Run(test.name, testFunction)
	}
}
