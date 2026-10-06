package api

import "time"

// EventType identifies a store mutation.
type EventType string

const (
	EventCreated       EventType = "Created"
	EventUpdated       EventType = "Updated"
	EventStatusUpdated EventType = "StatusUpdated"
	EventDeleted       EventType = "Deleted"
)

// Event is a single store mutation delivered to watchers.
type Event struct {
	Type     EventType
	Resource *Resource
}

// Now is overridable in tests; production code leaves it as time.Now.
var Now = time.Now
