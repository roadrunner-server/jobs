// Package protocol handles the internal worker response protocol for the
// RoadRunner jobs plugin.
//
// When a PHP worker finishes processing a job, it sends a typed response
// (NoError, Error, ACK, NACK, or REQUEUE) encoded as JSON. RespHandler
// unmarshals these responses and performs the appropriate action on the job:
// acknowledging, negatively acknowledging, or requeueing with optional delay
// and updated headers.
package protocol
