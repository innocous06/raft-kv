package kv

// ClientRecord stores the last executed sequence number and result for client request deduplication.
type ClientRecord struct {
	LastSeq    uint64   `json:"lastSeq"`
	LastResult OpResult `json:"lastResult"`
}
