/*
 * @Author: jinde.zgm
 * @Date: 2020-07-27 21:39:56
 * @Descripttion:
 */

package gmap

import "errors"

var (
	// ErrStopped gmap is stopped
	ErrStopped = errors.New("gmap: server stopped")
	// ErrCanceled request is canceled
	ErrCanceled = errors.New("gmap: request cancelled")
	// ErrTimeout request is timeout
	ErrTimeout = errors.New("gmap: request timed out")
)
