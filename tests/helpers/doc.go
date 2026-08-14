// Package helpers boots RoadRunner containers for the jobs plugin end-to-end
// tests and drives them over rpc. It holds the container lifecycle (Start and
// its error-path variants), readiness probes, the jobs rpc calls used by the
// tests, metrics scraping and the broker reachability guard.
package helpers
