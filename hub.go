package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Timeouts control connection liveness. Tests shorten them.
type Timeouts struct {
	// Write bounds every write to a client.
	Write time.Duration
	// Pong is how long a connection may stay silent before it is reaped.
	Pong time.Duration
	// Ping is how often the server sends a WS ping frame. Must be less than Pong.
	Ping time.Duration
}

var DefaultTimeouts = Timeouts{
	Write: 10 * time.Second,
	Pong:  60 * time.Second,
	Ping:  30 * time.Second,
}

const (
	// maxMessageSize caps what a client may send us. Clients only send small JSON requests.
	maxMessageSize = 64 * 1024
	// sendBufferSize is how many messages may queue for one client before it is
	// treated as too slow and disconnected.
	sendBufferSize = 256
)

// Request is the JSON shape of both client requests and broadcast messages.
type Request struct {
	ClientID string                 `json:"client_id"`
	Channel  string                 `json:"channel"`
	Request  string                 `json:"request"`
	Data     map[string]interface{} `json:"data"`
}

// pong is the reply to a client's {"request":"ping"}.
type pong struct {
	Request string `json:"request"`
	Channel string `json:"channel"`
}

// Hub tracks which connections listen on which channel and fans messages out to them.
type Hub struct {
	mu sync.RWMutex
	// channels maps a channel to the set of clients registered on it.
	channels map[string]map[*Client]struct{}
	// memberships maps a client to the channels it registered on, so it can be
	// removed from all of them when it goes away.
	memberships map[*Client]map[string]struct{}
}

func NewHub() *Hub {
	return &Hub{
		channels:    make(map[string]map[*Client]struct{}),
		memberships: make(map[*Client]map[string]struct{}),
	}
}

// Register adds c to channel. It returns false if c is already registered on
// that channel or has already been closed.
func (h *Hub) Register(c *Client, channel string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	// A client closes itself before unregistering, so checking under the lock
	// guarantees a closed client is never added back after its cleanup ran.
	if c.isClosed() {
		return false
	}

	if _, ok := h.memberships[c][channel]; ok {
		return false
	}

	if h.channels[channel] == nil {
		h.channels[channel] = make(map[*Client]struct{})
	}
	h.channels[channel][c] = struct{}{}

	if h.memberships[c] == nil {
		h.memberships[c] = make(map[string]struct{})
	}
	h.memberships[c][channel] = struct{}{}

	return true
}

// Unregister removes c from every channel it registered on.
func (h *Hub) Unregister(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for channel := range h.memberships[c] {
		delete(h.channels[channel], c)
		if len(h.channels[channel]) == 0 {
			delete(h.channels, channel)
		}
	}
	delete(h.memberships, c)
}

// Broadcast parses a raw pub/sub payload and queues it for every client on its
// channel. It returns an error for a payload that isn't valid JSON, and never
// blocks on a client.
func (h *Hub) Broadcast(payload []byte) error {
	req := &Request{}
	if err := json.Unmarshal(payload, req); err != nil {
		return fmt.Errorf("invalid broadcast payload: %w", err)
	}

	message, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("could not encode broadcast message: %w", err)
	}

	clients := h.clientsOn(req.Channel)
	logDebug(fmt.Sprintf("Broadcasting '%s' to %d connection(s) on channel '%s'", req.Request, len(clients), req.Channel))

	for _, c := range clients {
		c.Send(message)
	}

	return nil
}

// clientsOn returns a snapshot of the clients registered on channel, so
// broadcasting never happens while holding the lock.
func (h *Hub) clientsOn(channel string) []*Client {
	h.mu.RLock()
	defer h.mu.RUnlock()

	clients := make([]*Client, 0, len(h.channels[channel]))
	for c := range h.channels[channel] {
		clients = append(clients, c)
	}
	return clients
}

// ConnectionCount returns how many clients are registered on channel.
func (h *Hub) ConnectionCount(channel string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.channels[channel])
}

// ChannelCount returns how many channels have at least one client.
func (h *Hub) ChannelCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.channels)
}

// Client is one websocket connection. Only its write pump writes to the
// connection, which keeps gorilla's one-writer rule.
type Client struct {
	hub       *Hub
	conn      *websocket.Conn
	timeouts  Timeouts
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

func NewClient(hub *Hub, conn *websocket.Conn, timeouts Timeouts) *Client {
	return &Client{
		hub:      hub,
		conn:     conn,
		timeouts: timeouts,
		send:     make(chan []byte, sendBufferSize),
		done:     make(chan struct{}),
	}
}

// Run serves the connection until it fails or goes silent, then cleans up.
func (c *Client) Run() {
	go c.writePump()
	c.readPump()
}

// Send queues a message for the client without blocking. A client whose queue
// is full is too slow to keep up and gets disconnected.
func (c *Client) Send(message []byte) {
	select {
	case <-c.done:
		return
	default:
	}

	select {
	case c.send <- message:
	default:
		log.Println("Client send buffer full, closing connection")
		c.Close()
	}
}

// Close closes the connection and removes the client from every channel. Safe
// to call more than once and from any goroutine.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
		c.conn.Close()
		c.hub.Unregister(c)
	})
}

func (c *Client) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *Client) readPump() {
	defer c.Close()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(c.timeouts.Pong))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(c.timeouts.Pong))
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			logDebug(fmt.Sprintf("Read error: %s", err))
			return
		}
		// Any traffic from the client also proves it is alive.
		c.conn.SetReadDeadline(time.Now().Add(c.timeouts.Pong))
		logDebug(fmt.Sprintf("recv: '%s'", message))

		c.handleMessage(message)
	}
}

func (c *Client) handleMessage(message []byte) {
	req := &Request{}
	if err := json.Unmarshal(message, req); err != nil {
		log.Println("Could not parse websocket request JSON, ignoring")
		return
	}

	switch req.Request {
	case "ping":
		reply, _ := json.Marshal(pong{Request: "pong", Channel: req.Channel})
		c.Send(reply)
	case "register_listener":
		if req.Channel == "" {
			log.Println("Ignoring register_listener without a channel")
			return
		}
		if c.hub.Register(c, req.Channel) {
			logDebug(fmt.Sprintf("Channel '%s' adding connection #%d", req.Channel, c.hub.ConnectionCount(req.Channel)))
		}
	default:
		logDebug(fmt.Sprintf("Ignoring unknown request '%s'", req.Request))
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(c.timeouts.Ping)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case message := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(c.timeouts.Write))
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				logDebug(fmt.Sprintf("Error writing to websocket: %s", err))
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(c.timeouts.Write))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				logDebug(fmt.Sprintf("Error sending ping: %s", err))
				return
			}
		case <-c.done:
			return
		}
	}
}
