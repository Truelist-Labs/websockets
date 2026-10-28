package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/gorilla/websocket"
)

func newTestServer(t *testing.T, timeouts Timeouts) (*Server, string) {
	t.Helper()

	s := NewServer(NewOriginAllowlist(DefaultAllowedOrigins, false))
	s.Timeouts = timeouts
	ts := httptest.NewServer(http.HandlerFunc(s.Serve))
	t.Cleanup(ts.Close)

	return s, "ws" + strings.TrimPrefix(ts.URL, "http")
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func send(t *testing.T, conn *websocket.Conn, request, channel string) {
	t.Helper()

	msg := fmt.Sprintf(`{"client_id":"frontend","channel":%q,"request":%q}`, channel, request)
	if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func read(t *testing.T, conn *websocket.Conn) string {
	t.Helper()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(msg)
}

// expectNothing asserts nothing arrives on conn for a short while.
func expectNothing(t *testing.T, conn *websocket.Conn) {
	t.Helper()

	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, msg, err := conn.ReadMessage()
	if err == nil {
		t.Fatalf("expected no message, got %s", msg)
	}
}

// syncConn sends a ping and waits for the pong. The server handles a connection's
// messages in order, so everything sent before it has been processed.
func syncConn(t *testing.T, conn *websocket.Conn) {
	t.Helper()

	send(t, conn, "ping", "sync")
	if got := read(t, conn); got != `{"request":"pong","channel":"sync"}` {
		t.Fatalf("expected pong, got %s", got)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestPingGetsPongOnSameChannel(t *testing.T) {
	_, url := newTestServer(t, DefaultTimeouts)
	conn := dial(t, url)

	send(t, conn, "ping", "u1")

	if got := read(t, conn); got != `{"request":"pong","channel":"u1"}` {
		t.Fatalf("got %s", got)
	}
}

func TestBroadcastReachesOnlyClientsOnItsChannel(t *testing.T) {
	s, url := newTestServer(t, DefaultTimeouts)
	u1 := dial(t, url)
	u2 := dial(t, url)
	send(t, u1, "register_listener", "u1")
	send(t, u2, "register_listener", "u2")
	syncConn(t, u1)
	syncConn(t, u2)

	payload := `{"client_id":"backend","channel":"u1","request":"batch_updated","data":{"uuid":"b1","percentComplete":50}}`
	if err := s.Hub.Broadcast([]byte(payload)); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	// Same shape the server has always sent: the Request struct re-encoded.
	want := `{"client_id":"backend","channel":"u1","request":"batch_updated","data":{"percentComplete":50,"uuid":"b1"}}`
	if got := read(t, u1); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
	expectNothing(t, u2)
}

func TestOnlyRegisterListenerRegisters(t *testing.T) {
	s, url := newTestServer(t, DefaultTimeouts)
	conn := dial(t, url)

	send(t, conn, "", "u1")
	send(t, conn, "something_else", "u1")
	send(t, conn, "ping", "u1")
	read(t, conn) // pong
	conn.WriteMessage(websocket.TextMessage, []byte("not json"))
	syncConn(t, conn)

	if n := s.Hub.ConnectionCount("u1"); n != 0 {
		t.Fatalf("expected no registrations, got %d", n)
	}
}

func TestDuplicateRegistrationReceivesOnce(t *testing.T) {
	s, url := newTestServer(t, DefaultTimeouts)
	conn := dial(t, url)
	send(t, conn, "register_listener", "u1")
	send(t, conn, "register_listener", "u1")
	syncConn(t, conn)

	if n := s.Hub.ConnectionCount("u1"); n != 1 {
		t.Fatalf("expected 1 registration, got %d", n)
	}

	s.Hub.Broadcast([]byte(`{"channel":"u1","request":"batch_updated"}`))
	read(t, conn)
	expectNothing(t, conn)
}

func TestClosedConnectionIsRemovedFromEveryChannel(t *testing.T) {
	s, url := newTestServer(t, DefaultTimeouts)
	conn := dial(t, url)
	other := dial(t, url)
	for _, channel := range []string{"u1", "u2", "u3"} {
		send(t, conn, "register_listener", channel)
	}
	send(t, other, "register_listener", "u1")
	syncConn(t, conn)
	syncConn(t, other)

	conn.Close()

	eventually(t, "closed connection to be removed", func() bool {
		return s.Hub.ConnectionCount("u1") == 1 && s.Hub.ChannelCount() == 1
	})

	s.Hub.Broadcast([]byte(`{"channel":"u1","request":"batch_updated"}`))
	read(t, other)
}

func TestMalformedRedisPayloadDoesNotKillBroadcaster(t *testing.T) {
	s, url := newTestServer(t, DefaultTimeouts)
	conn := dial(t, url)
	send(t, conn, "register_listener", "u1")
	syncConn(t, conn)

	messages := make(chan *redis.Message)
	done := make(chan struct{})
	go func() {
		s.listen(messages)
		close(done)
	}()

	messages <- &redis.Message{Channel: redisChannel, Payload: "garbage"}
	messages <- &redis.Message{Channel: redisChannel, Payload: `{"channel":"u1","request":"batch_updated","data":"not an object"}`}
	messages <- &redis.Message{Channel: redisChannel, Payload: `{"channel":"u1","request":"batch_updated"}`}

	if got := read(t, conn); !strings.Contains(got, `"request":"batch_updated"`) {
		t.Fatalf("got %s", got)
	}

	close(messages)
	<-done
}

func TestBroadcastRejectsInvalidJSON(t *testing.T) {
	hub := NewHub()
	if err := hub.Broadcast([]byte("garbage")); err == nil {
		t.Fatal("expected an error")
	}
}

func TestSilentConnectionIsReaped(t *testing.T) {
	s, url := newTestServer(t, Timeouts{Write: time.Second, Pong: 300 * time.Millisecond, Ping: 100 * time.Millisecond})
	conn := dial(t, url)
	send(t, conn, "register_listener", "u1")

	eventually(t, "registration", func() bool { return s.Hub.ConnectionCount("u1") == 1 })

	// The test client never reads, so it never answers the server's ping
	// frames, like a laptop that went to sleep.
	eventually(t, "silent connection to be reaped", func() bool { return s.Hub.ChannelCount() == 0 })
}

func TestConnectionAnsweringPingsStaysRegistered(t *testing.T) {
	s, url := newTestServer(t, Timeouts{Write: time.Second, Pong: 500 * time.Millisecond, Ping: 100 * time.Millisecond})
	conn := dial(t, url)
	send(t, conn, "register_listener", "u1")

	// Reading makes gorilla answer ping frames with pongs, as browsers do.
	pings := 0
	conn.SetPingHandler(func(data string) error {
		pings++
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
	})
	// Stay connected for several Pong windows.
	conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
	_, _, err := conn.ReadMessage()
	if err == nil {
		t.Fatal("expected no data messages")
	}

	if pings < 5 {
		t.Fatalf("expected ping frames every 100ms, got %d", pings)
	}
	if n := s.Hub.ConnectionCount("u1"); n != 1 {
		t.Fatalf("expected connection to stay registered, got %d", n)
	}
}

func TestSlowClientDoesNotBlockBroadcast(t *testing.T) {
	// A long write deadline: if Broadcast ever waited on the slow client's
	// write, a single call would take about a minute.
	s, url := newTestServer(t, Timeouts{Write: time.Minute, Pong: time.Minute, Ping: time.Minute})
	slow := dial(t, url)
	send(t, slow, "register_listener", "u1")
	syncConn(t, slow)

	// The slow client never reads again. Big messages fill its TCP buffers,
	// which blocks its write pump, then its send queue fills up.
	big := strings.Repeat("x", 64*1024)
	payload := []byte(fmt.Sprintf(`{"channel":"u1","request":"batch_updated","data":{"blob":%q}}`, big))

	var slowest time.Duration
	for i := 0; i < 2*sendBufferSize; i++ {
		start := time.Now()
		if err := s.Hub.Broadcast(payload); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed > slowest {
			slowest = elapsed
		}
	}
	if slowest > 5*time.Second {
		t.Fatalf("a broadcast blocked for %s", slowest)
	}

	// Dropped because its send queue overflowed, long before the write deadline.
	eventually(t, "slow client to be dropped", func() bool { return s.Hub.ChannelCount() == 0 })
}

func TestConcurrentRegisterUnregisterBroadcast(t *testing.T) {
	s, url := newTestServer(t, DefaultTimeouts)
	channels := []string{"u1", "u2", "u3"}

	stop := make(chan struct{})
	var broadcasters sync.WaitGroup
	for i := 0; i < 3; i++ {
		broadcasters.Add(1)
		go func(i int) {
			defer broadcasters.Done()
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				channel := channels[(i+j)%len(channels)]
				s.Hub.Broadcast([]byte(`{"channel":"` + channel + `","request":"batch_updated"}`))
				s.Hub.ConnectionCount(channel)
			}
		}(i)
	}

	var clients sync.WaitGroup
	for i := 0; i < 30; i++ {
		clients.Add(1)
		go func(i int) {
			defer clients.Done()
			conn, _, err := websocket.DefaultDialer.Dial(url, nil)
			if err != nil {
				t.Errorf("dial: %v", err)
				return
			}
			defer conn.Close()

			for j := 0; j < 5; j++ {
				msg, _ := json.Marshal(Request{Channel: channels[(i+j)%len(channels)], Request: "register_listener"})
				conn.WriteMessage(websocket.TextMessage, msg)
				conn.WriteMessage(websocket.TextMessage, []byte(`{"request":"ping","channel":"x"}`))
			}
			// Drain a few messages so writes and reads overlap, then hang up.
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			for k := 0; k < 10; k++ {
				if _, _, err := conn.ReadMessage(); err != nil {
					break
				}
			}
		}(i)
	}

	clients.Wait()
	close(stop)
	broadcasters.Wait()

	eventually(t, "all connections to be removed", func() bool { return s.Hub.ChannelCount() == 0 })
}
