// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package timequeue

import (
	"container/heap"
	"time"

	"github.com/axw/juju-time/clock"
	"github.com/juju/errors"
)

// Queue provides a queue, with the following properties:
//  - items are associated with a unique key, and a time
//  - items are popped off in order of time
//  - fast to add and remove items by key: O(log(n)); n is the total number of items
//  - fast to identify the next queued item: O(log(n))
//  - fast to remove arbitrary items: O(log(n))
type Queue struct {
	time  clock.Clock
	items queueItems
	m     map[interface{}]*queueItem
}

// New constructs a new queue, using the given Clock for the Next
// method.
func New(clock clock.Clock) *Queue {
	return &Queue{
		time: clock,
		m:    make(map[interface{}]*queueItem),
	}
}

// Next returns a channel which will send after the next queued item's time
// has been reached. If there are no queued items, nil is returned.
func (s *Queue) Next() <-chan time.Time {
	if len(s.items) > 0 {
		return s.time.After(s.items[0].t.Sub(s.time.Now()))
	}
	return nil
}

// Ready returns the parameters for items that are queued at or before
// "now", and removes them from the queue. The resulting slices are in
// order of time; items queued for the same time have no defined relative
// order.
func (s *Queue) Ready(now time.Time) []interface{} {
	var ready []interface{}
	for len(s.items) > 0 && !s.items[0].t.After(now) {
		item := heap.Pop(&s.items).(*queueItem)
		delete(s.m, item.key)
		ready = append(ready, item.value)
	}
	return ready
}

// Add adds an item with the specified value, with the corresponding key
// and time to the queue. Add will panic if there already exists an item
// with the same key.
func (s *Queue) Add(key, value interface{}, t time.Time) {
	if _, ok := s.m[key]; ok {
		panic(errors.Errorf("duplicate key %v", key))
	}
	item := &queueItem{key: key, value: value, t: t}
	s.m[key] = item
	heap.Push(&s.items, item)
}

// Remove removes the item corresponding to the specified key from the
// queue. If no item with the specified key exists, this is a no-op.
func (s *Queue) Remove(key interface{}) {
	if item, ok := s.m[key]; ok {
		heap.Remove(&s.items, item.i)
		delete(s.m, key)
	}
}

type queueItems []*queueItem

type queueItem struct {
	i     int
	key   interface{}
	value interface{}
	t     time.Time
}

func (s queueItems) Len() int {
	return len(s)
}

func (s queueItems) Less(i, j int) bool {
	return s[i].t.Before(s[j].t)
}

func (s queueItems) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
	s[i].i = i
	s[j].i = j
}

func (s *queueItems) Push(x interface{}) {
	item := x.(*queueItem)
	item.i = len(*s)
	*s = append(*s, item)
}

func (s *queueItems) Pop() interface{} {
	n := len(*s) - 1
	x := (*s)[n]
	*s = (*s)[:n]
	return x
}
