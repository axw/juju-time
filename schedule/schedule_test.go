// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package schedule_test

import (
	"time"

	"github.com/axw/juju-time/schedule"
	coretesting "github.com/juju/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
)

type scheduleSuite struct {
	coretesting.BaseSuite
}

var _ = gc.Suite(&scheduleSuite{})

func (*scheduleSuite) TestNextNoEvents(c *gc.C) {
	s := schedule.NewSchedule(coretesting.NewClock(time.Time{}))
	next := s.Next()
	c.Assert(next, gc.IsNil)
}

func (*scheduleSuite) TestNext(c *gc.C) {
	clock := coretesting.NewClock(time.Time{})
	s := schedule.NewSchedule(clock)

	op0 := operation{"k0", "v0", 3 * time.Second}
	op1 := operation{"k1", "v1", 1500 * time.Millisecond}
	op2 := operation{"k2", "v2", 2 * time.Second}
	op3 := operation{"k3", "v3", 2500 * time.Millisecond}

	s.Add(op0)
	s.Add(op1)
	s.Add(op2)
	s.Add(op3)

	assertNextOp(c, s, clock, 1500*time.Millisecond)
	clock.Advance(1500 * time.Millisecond)
	assertReady(c, s, clock, op1)

	clock.Advance(500 * time.Millisecond)
	assertNextOp(c, s, clock, 0)
	assertReady(c, s, clock, op2)

	s.Remove("k3")

	clock.Advance(2 * time.Second) // T+4
	assertNextOp(c, s, clock, 0)
	assertReady(c, s, clock, op0)
}

func (*scheduleSuite) TestReadyNoEvents(c *gc.C) {
	s := schedule.NewSchedule(coretesting.NewClock(time.Time{}))
	ready := s.Ready(time.Now())
	c.Assert(ready, gc.HasLen, 0)
}

func (*scheduleSuite) TestAdd(c *gc.C) {
	clock := coretesting.NewClock(time.Time{})
	s := schedule.NewSchedule(clock)

	op0 := operation{"k0", "v0", 3 * time.Second}
	op1 := operation{"k1", "v1", 1500 * time.Millisecond}
	op2 := operation{"k2", "v2", 2 * time.Second}

	s.Add(op0)
	s.Add(op1)
	s.Add(op2)

	clock.Advance(time.Second) // T+1
	assertReady(c, s, clock /* nothing */)

	clock.Advance(time.Second) // T+2
	assertReady(c, s, clock, op1, op2)
	assertReady(c, s, clock /* nothing */)

	clock.Advance(500 * time.Millisecond) // T+2.5
	assertReady(c, s, clock /* nothing */)

	clock.Advance(time.Second) // T+3.5
	assertReady(c, s, clock, op0)
}

func (*scheduleSuite) TestRemove(c *gc.C) {
	clock := coretesting.NewClock(time.Time{})
	s := schedule.NewSchedule(clock)

	op0 := operation{"k0", "v0", 3 * time.Second}
	op1 := operation{"k1", "v1", 2 * time.Second}
	s.Add(op0)
	s.Add(op1)
	s.Remove("k0")
	assertReady(c, s, clock /* nothing */)

	clock.Advance(3 * time.Second)
	assertReady(c, s, clock, op1)
}

func (*scheduleSuite) TestRemoveKeyNotFound(c *gc.C) {
	s := schedule.NewSchedule(coretesting.NewClock(time.Time{}))
	s.Remove("0") // does not explode
}

func (*scheduleSuite) TestExponentialBackoff(c *gc.C) {
	clock := coretesting.NewClock(time.Time{})
	now := clock.Now()
	s := schedule.NewSchedule(clock)
	op := &exponentialBackoffOperation{key: "key"}

	expectedTimes := []time.Time{
		now,
		now.Add(30 * time.Second),
		now.Add(1 * time.Minute),
		now.Add(2 * time.Minute),
		now.Add(4 * time.Minute),
		now.Add(8 * time.Minute),
		now.Add(16 * time.Minute),
		now.Add(30 * time.Minute), // truncated
		now.Add(30 * time.Minute),
	}
	for i, expected := range expectedTimes {
		c.Logf("%d: expect %s", i, expected)
		t := s.Add(op)
		c.Assert(t, gc.DeepEquals, expected)
		s.Remove(op.Key())
	}
}

type operation struct {
	key   string
	value string
	delay time.Duration
}

func (o operation) Key() interface{} {
	return o.key
}

func (o operation) Delay() time.Duration {
	return o.delay
}

type exponentialBackoffOperation struct {
	schedule.ExponentialBackoff
	key string
}

func (o *exponentialBackoffOperation) Key() interface{} {
	return o.key
}

func assertNextOp(c *gc.C, s *schedule.Schedule, clock *coretesting.Clock, d time.Duration) {
	next := s.Next()
	c.Assert(next, gc.NotNil)
	if d > 0 {
		select {
		case <-next:
			c.Fatal("Next channel signalled too soon")
		default:
		}
	}

	// temporarily move time forward
	clock.Advance(d)
	defer clock.Advance(-d)

	select {
	case _, ok := <-next:
		c.Assert(ok, jc.IsTrue)
		// the time value is unimportant to us
	default:
		c.Fatal("Next channel not signalled")
	}
}

func assertReady(c *gc.C, s *schedule.Schedule, clock *coretesting.Clock, expect ...schedule.Operation) {
	ready := s.Ready(clock.Now())
	c.Assert(ready, jc.DeepEquals, expect)
}
