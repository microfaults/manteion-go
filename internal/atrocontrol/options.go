package atrocontrol

import (
	"time"
)

type controllerOpts struct {
	timeout     time.Duration
	concurrency int
	filter      InstanceFilter
	logger      logger
}

type callOpts struct {
	timeout     time.Duration
	concurrency int
	filter      InstanceFilter
}

type CallOption func(*callOpts)

func WithTimeout(d time.Duration) CallOption {
	return func(o *callOpts) { o.timeout = d }
}

func WithInstanceFilter(f InstanceFilter) CallOption {
	return func(o *callOpts) { o.filter = f }
}

// Controller-level options set via New().

type ControllerOption func(*controllerOpts)

func WithDefaultTimeout(d time.Duration) ControllerOption {
	return func(o *controllerOpts) { o.timeout = d }
}

func WithDefaultConcurrency(n int) ControllerOption {
	return func(o *controllerOpts) { o.concurrency = n }
}

func WithLogger(l logger) ControllerOption {
	return func(o *controllerOpts) { o.logger = l }
}
