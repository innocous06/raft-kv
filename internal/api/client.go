package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Client is a smart Raft KV client with transparent leader redirection, retries, and request deduplication.
type Client struct {
	mu         sync.Mutex
	clientID   string
	seqNum     uint64
	endpoints  []string // e.g. ["http://127.0.0.1:8001", "http://127.0.0.1:8002"]
	leaderIdx  int
	httpClient *http.Client
}

// NewClient initializes a client with cluster endpoints.
func NewClient(endpoints []string) *Client {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &Client{
		clientID:   fmt.Sprintf("client-%d-%d", time.Now().UnixNano(), rnd.Int63n(100000)),
		endpoints:  endpoints,
		leaderIdx:  0,
		httpClient: &http.Client{Timeout: 2 * time.Second},
	}
}

func (c *Client) nextSeq() uint64 {
	return atomic.AddUint64(&c.seqNum, 1)
}

// Put writes key and value to the cluster with linearizable semantics.
func (c *Client) Put(ctx context.Context, key, value string) error {
	seq := c.nextSeq()
	reqBody := PutRequest{
		Key:      key,
		Value:    value,
		ClientID: c.clientID,
		SeqNum:   seq,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	return c.retryLoop(ctx, func(endpoint string) (bool, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/api/v1/kv/put", bytes.NewReader(data))
		if err != nil {
			return false, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return false, err // try next endpoint
		}
		defer resp.Body.Close()

		var apiResp APIResponse
		if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
			return false, err
		}

		if resp.StatusCode == http.StatusOK && apiResp.Success {
			return true, nil
		}

		return false, fmt.Errorf("request rejected: %s", apiResp.Error)
	})
}

// Get reads key value from the cluster.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	var result string
	err := c.retryLoop(ctx, func(endpoint string) (bool, error) {
		url := fmt.Sprintf("%s/api/v1/kv/get?key=%s", endpoint, key)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return false, err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()

		var apiResp APIResponse
		if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
			return false, err
		}

		if resp.StatusCode == http.StatusOK && apiResp.Success {
			result = apiResp.Value
			return true, nil
		}

		if resp.StatusCode == http.StatusNotFound {
			result = ""
			return true, fmt.Errorf("key not found")
		}

		return false, fmt.Errorf("get error: %s", apiResp.Error)
	})

	return result, err
}

// Delete removes key from the cluster.
func (c *Client) Delete(ctx context.Context, key string) error {
	seq := c.nextSeq()
	reqBody := DeleteRequest{
		Key:      key,
		ClientID: c.clientID,
		SeqNum:   seq,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	return c.retryLoop(ctx, func(endpoint string) (bool, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/api/v1/kv/delete", bytes.NewReader(data))
		if err != nil {
			return false, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()

		var apiResp APIResponse
		if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
			return false, err
		}

		if resp.StatusCode == http.StatusOK && apiResp.Success {
			return true, nil
		}

		return false, fmt.Errorf("delete error: %s", apiResp.Error)
	})
}

func (c *Client) retryLoop(ctx context.Context, fn func(endpoint string) (bool, error)) error {
	c.mu.Lock()
	idx := c.leaderIdx
	c.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		endpoint := c.endpoints[idx]
		success, err := fn(endpoint)
		if success {
			c.mu.Lock()
			c.leaderIdx = idx
			c.mu.Unlock()
			return nil
		}

		// If error is key not found, don't keep retrying across nodes
		if err != nil && err.Error() == "key not found" {
			return err
		}

		// Try next node
		idx = (idx + 1) % len(c.endpoints)
		time.Sleep(50 * time.Millisecond)
	}
}
