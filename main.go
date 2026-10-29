package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/go-redis/redis"
	"github.com/gorilla/websocket"
)

const redisChannel = "clearlist_realtime"

// Server is server stuff
type Server struct {
	Hub      *Hub
	Timeouts Timeouts
	upgrader websocket.Upgrader
}

func NewServer(origins *OriginAllowlist) *Server {
	return &Server{
		Hub:      NewHub(),
		Timeouts: DefaultTimeouts,
		upgrader: websocket.Upgrader{CheckOrigin: origins.CheckOrigin},
	}
}

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)
	flag.Parse()

	origins := OriginAllowlistFromEnv()
	log.Println("Allowed origins: ", origins)

	server := NewServer(origins)
	server.SetupRedisListener()

	http.HandleFunc("/up", HandleHeartbeat)
	http.HandleFunc("/ws", server.Serve)

	port := os.Getenv("PORT")
	if len(port) == 0 {
		port = "8080"
	}

	url := fmt.Sprintf(":%s", port)
	log.Println("Listening at ", url)

	log.Fatal(http.ListenAndServe(url, nil))
}

func HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"alive": true}`))
}

func (s *Server) Serve(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade has already written an HTTP error response.
		log.Print("upgrade error: ", err)
		return
	}

	NewClient(s.Hub, conn, s.Timeouts).Run()
}

func (s *Server) SetupRedisListener() {
	log.Println("Connecting to redis...")

	url := os.Getenv("REDIS_URL")
	if len(url) == 0 {
		url = "redis://127.0.0.1:6379"
	}

	opt, err := redis.ParseURL(url)
	if err != nil {
		fmt.Println("Error connecting to REDIS_URL:")
		panic(err)
	}
	redisConn := redis.NewClient(opt)

	_, err = redisConn.Ping().Result()
	if err != nil {
		log.Println("Error connecting to redis: ", err)
		panic(err)
	}

	pubsub := redisConn.Subscribe(redisChannel)
	go func() {
		s.listen(pubsub.Channel())
		log.Fatal("Redis subscription closed")
	}()
}

// listen broadcasts every pub/sub message until messages is closed. A bad
// payload is logged and skipped.
func (s *Server) listen(messages <-chan *redis.Message) {
	for message := range messages {
		logDebug(fmt.Sprintf("redis message is %s", message.Payload))

		if err := s.Hub.Broadcast([]byte(message.Payload)); err != nil {
			log.Println("Skipping redis message: ", err)
		}
	}
}

func logDebug(str string) {
	if len(os.Getenv("DEBUG")) != 0 {
		log.Println(str)
	}
}
