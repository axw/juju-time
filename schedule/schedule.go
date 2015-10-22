// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package schedule

import (
	"time"

	"github.com/axw/juju-time/clock"
	"github.com/axw/juju-time/timequeue"
)

// Schedule provides a schedule of operations, with the following properties:
//  - operations are associated with a unique key, and a time
//  - operations define a delay, which will be added to the current
//    time when enqueuing. The delay need not be constant; for example,
//    exponential backoff can be implemented by having the delay
//    multiplied each time the operation is re-enqueued
//  - operations are popped off in order of time
//  - fast to add and remove operations by key: O(log(n)); n is the total number of operations
//  - fast to identify the next queued operation: O(log(n))
//  - fast to remove arbitrary operations: O(log(n))
type Schedule struct {
	time clock.Clock
	q    *timequeue.Queue
}

// Operation is the interface for schedule operations.
type Operation interface {
	// Key uniquely identifies the schedule operation.
	Key() interface{}

	// Delay is the duration to add to the current time
	// when enqueuing the operation, to determine the
	// time at which the operation will be "ready".
	Delay() time.Duration
}

// NewSchedule constructs a new schedule, using the given Clock for the Next
// and Add methods.
func NewSchedule(clock clock.Clock) *Schedule {
	return &Schedule{time: clock, q: timequeue.New(clock)}
}

// Next returns a channel which will send after the next scheduled operation's
// time has been reached. If there are no scheduled operations, nil is returned.
func (s *Schedule) Next() <-chan time.Time {
	return s.q.Next()
}

// Ready returns the parameters for operations that are scheduled at or before
// "now", and removes them from the schedule. The resulting slices are in
// order of time; operations scheduled for the same time have no defined relative
// order.
func (s *Schedule) Ready(now time.Time) []Operation {
	readyItems := s.q.Ready(now)
	ready := make([]Operation, len(readyItems))
	for i, item := range readyItems {
		ready[i] = item.(Operation)
	}
	return ready
}

// Add adds an operation with the specified value, with the corresponding key
// and time to the schedule. Add will panic if there already exists an operation
// with the same key.
func (s *Schedule) Add(op Operation) {
	key, delay := op.Key(), op.Delay()
	s.q.Add(key, op, s.time.Now().Add(delay))
}

// Remove removes the operation corresponding to the specified key from the
// schedule. If no operation with the specified key exists, this is a no-op.
func (s *Schedule) Remove(key interface{}) {
	s.q.Remove(key)
}
