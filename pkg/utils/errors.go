package utils

import (
	"errors"

	ngxstorage "github.com/ngxstorage/ngxstorage-sdk-go/pkg/ngxstorage"
)

type ErrorClass int

const (
	ErrorInternal ErrorClass = iota
	ErrorNotFound
	ErrorAlreadyExists
	ErrorUnavailable
	ErrorInvalidArgument
)

// ClassifyError maps confirmed NGX response semantics to a storage-level
// class. CSI gRPC mapping remains in BackendStatusError.
func ClassifyError(err error) ErrorClass {
	if err == nil {
		return ErrorInternal
	}
	// Placement failures retain their storage meaning even when request
	// wraps them with transport-refresh context.
	if errors.Is(err, ngxstorage.ErrPoolNotFound) {
		return ErrorNotFound
	}
	if errors.Is(err, ngxstorage.ErrClusterNotReady) {
		return ErrorUnavailable
	}

	// A transport failure means no controller completed the request. CSI may
	// retry the operation after the driver's one bounded placement refresh.
	var transportErr *ngxstorage.TransportError
	if errors.As(err, &transportErr) {
		return ErrorUnavailable
	}

	// Semantic API responses are classified only from their canonical NGX code
	// and preserved HTTP status.
	var apiErr *ngxstorage.APIError
	if !errors.As(err, &apiErr) {
		return ErrorInternal
	}
	switch {
	case apiErr.Code == 5011 || apiErr.StatusCode == 404:
		return ErrorNotFound
	case apiErr.StatusCode == 409:
		return ErrorAlreadyExists
	case apiErr.Code == 725 || apiErr.StatusCode == 429 || apiErr.StatusCode >= 500:
		return ErrorUnavailable
	case apiErr.StatusCode == 400:
		return ErrorInvalidArgument
	default:
		return ErrorInternal
	}
}
