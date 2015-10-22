// Copyright 2015 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package schedule

import "time"

// minRetryDelay is the minimum delay to apply
// to operation retries; this does not apply to
// the first attempt for operations.
const minRetryDelay = 30 * time.Second

// maxRetryDelay is the maximum delay to apply
// to operation retries. Retry delays will backoff
// up to this ceiling.
const maxRetryDelay = 30 * time.Minute

// ExponentialBackoff is a type that can be embedded in an Operation to
// implement the Delay() method, providing truncated binary exponential
// backoff for operations that may be rescheduled.
type ExponentialBackoff time.Duration

func (e *ExponentialBackoff) Delay() time.Duration {
	current := time.Duration(*e)
	if time.Duration(*e) < minRetryDelay {
		*e = ExponentialBackoff(minRetryDelay)
	} else {
		*e *= 2
		if time.Duration(*e) > maxRetryDelay {
			*e = ExponentialBackoff(maxRetryDelay)
		}
	}
	return current
}
