package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"
)

// Single message format from sender
type IncomingMessage struct {
	DeviceId string `json:"deviceId"`
	Payload  interface{} `json:"payload"`
}

// Single message format for receiver
type OutgoingMessage struct {
	Sender   string `json:"sender"`
	Payload  interface{} `json:"encPayload"`
	SeqID    uint64 `json:"seqID"`
}

// Internal message format for moving between sender and notification handler
type Message struct {
	To       string
	Outgoing  OutgoingMessage
}

type Event interface {}
type Notification interface {}

type MessageEvent struct {
	Messages []*Message
	SeqID    uint64
}

type NeedsOneTimeKeyEvent struct {
	DeviceId string `json:"deviceId"`
	Needs    uint `json:"needs"`
}

type ClientChan struct {
	DeviceId string
	Channel  chan Notification
}

type MessageStorage struct {
	lock  sync.Mutex
	db    *pebble.DB
}

type Server struct {
	MessageStorage *MessageStorage

	// Events are pushed to this channel by the main events-gathering routine
	Notifier chan Event

	// New client connections
	newClients chan ClientChan

	// Closed client connections
	closingClients chan string

	// Client connections registry
	clients map[string]chan Notification
}

type Shards struct {
	// Shards know they are part of a large server with other shards
	Shards map[string]Server
}



func NewServer(db *pebble.DB) (server *Server) {
	// Instantiate a server
	server = &Server{
		MessageStorage: new(MessageStorage),
		Notifier:       make(chan Event),
		newClients:     make(chan ClientChan),
		closingClients: make(chan string),
		clients:        make(map[string]chan Notification),
	}
	server.MessageStorage.db = db

	// Set it running - listening and broadcasting events
	go server.listen()

	return
}

func (server *Server) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	rw.Header().Set("Access-Control-Allow-Headers", "*")
	rw.Header().Set("Access-Control-Allow-Methods", "OPTIONS,GET,POST,DELETE")

	if req.Method == "OPTIONS" {
		return
	}

	switch req.URL.Path {
	case "/events":
		if req.Method != "GET" {
			http.Error(rw, "Not Found", http.StatusNotFound)
			return
		}
		// Subscribe to server-sent events
		server.serveEvents(rw, req)
	case "/message":
		if req.Method != "POST" {
			http.Error(rw, "Not Found", http.StatusNotFound)
			return
		}
		// Post a message
		server.postMessage(rw, req)
	case "/self/messages":
		if req.Method == "GET" {
			// Retrieve outstanding messages
			server.getMessages(rw, req)
		} else if req.Method == "DELETE" {
			// Delete processed messages
			server.deleteMessages(rw, req)
		} else {
			http.Error(rw, "Not Found", http.StatusNotFound)
			return
		}
	case "/devices/otkey":
		if req.Method != "GET" {
			http.Error(rw, "Not Found", http.StatusNotFound)
			return
		}
		// Get one-time key for a device
		server.getOneTimeKey(rw, req)
	case "/self/otkeys":
		if req.Method != "POST" {
			http.Error(rw, "Not Found", http.StatusNotFound)
			return
		}
		// Add new one-time keys for this device
		server.addOneTimeKeys(rw, req)
	default:
		http.Error(rw, "Not Found", http.StatusNotFound)
	}
}

// Expects a query parameter `device_id` containing the URL-encoded device id to get a one time key for
//
// Returns the one-time-key's public key in the dictionary:
// ```
// {
//   "otkey": "somepublickey"
// }
// ```
func (server *Server) getOneTimeKey(rw http.ResponseWriter, req *http.Request) {
	deviceId, e := url.QueryUnescape(req.URL.Query().Get("device_id"))

	if e != nil {
		http.Error(rw, fmt.Sprintf("%s", e), http.StatusInternalServerError)
		return
	}

	server.MessageStorage.lock.Lock()
	defer server.MessageStorage.lock.Unlock()

	prefix := append(append([]byte("otkeys/"), []byte(deviceId)...), 0)
	batch := server.MessageStorage.db.NewIndexedBatch()
	iter := batch.NewIter(&pebble.IterOptions {
		LowerBound: prefix,
		UpperBound: append(append([]byte("otkeys/"), []byte(deviceId)...), 1),
	})
	iter.First()
	if iter.Valid() {
		iter.Value()
		type OneTimeKey struct {
			Otkey string `json:"otkey"`
		}
		otkey := OneTimeKey {
			Otkey: string(iter.Value()),
		}
		json.NewEncoder(rw).Encode(&otkey)
		batch.Delete(iter.Key(), pebble.Sync)
		count := 0
		for ; iter.Next(); iter.Valid() {
			count += 1
			if count >= 10 {
				break
			}
		}
		if count < 10 {
			server.Notifier <- &NeedsOneTimeKeyEvent {
				DeviceId: deviceId,
				Needs: 20,
			}
		}
	} else {
		server.Notifier <- &NeedsOneTimeKeyEvent {
			DeviceId: deviceId,
			Needs: 20,
		}
		http.Error(rw, "Not Found", http.StatusNotFound)
	}
	batch.Commit(pebble.Sync)

}

// Expects a JSON dictionary of keyIds to public keys
//
// ```
// { "somekeyidentifier": "thepublickey"}
// ```
//
// Returns the added keys in the same format
func (server *Server) addOneTimeKeys(rw http.ResponseWriter, req *http.Request) {
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" || len(authHeader) < 8 {
		http.Error(rw, "Not authorized", http.StatusNotFound)
		return
	}
	deviceId := strings.TrimSpace(authHeader[7:])

	var keys map[string]string = make(map[string]string)
	json.NewDecoder(req.Body).Decode(&keys)

	batch := server.MessageStorage.db.NewBatch()

	prefix := append(append([]byte("otkeys/"), []byte(deviceId)...), 0)
	for keyId, publicKey := range keys {
		batch.Set(append(prefix, []byte(keyId)...), []byte(publicKey), pebble.Sync)
	}

	batch.Commit(pebble.Sync)
	json.NewEncoder(rw).Encode(keys)
}

// Expects a query parameter `seqId` with highest sequence id to delete
//
// Returns whatever
func (server *Server) deleteMessages(rw http.ResponseWriter, req *http.Request) {
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" || len(authHeader) < 8 {
		http.Error(rw, "Not authorized", http.StatusNotFound)
		return
	}
	deviceId := strings.TrimSpace(authHeader[7:])

	type ToDelete struct {
		SeqID uint64 `json:"seqID"`
	}
	var td ToDelete;
	e := json.NewDecoder(req.Body).Decode(&td);
	if e != nil {
		http.Error(rw, fmt.Sprintf("%s", e), http.StatusInternalServerError)
		return
	}

	td.SeqID += 1 // DeleteRange is not inclusive, so bound by a seqID one higher

	seqBin := make([]byte, 8)
	binary.LittleEndian.PutUint64(seqBin, td.SeqID)
	lowerBound := append([]byte(deviceId), 0)
	upperBound := append(lowerBound, seqBin...)
	e = server.MessageStorage.db.DeleteRange(lowerBound, upperBound, pebble.Sync)
	if e != nil {
		http.Error(rw, fmt.Sprintf("%s", e), http.StatusInternalServerError)
		return
	}
	rw.Write([]byte("{}"))
}

// Doesn't expect a body or any parameters
//
// Returns the current device's mailbox as a JSON list of messages with sequentIDs and senders:
//
// ```
// [ {
//   "seqID": 1234,
//   "sender": "someDeviceId",
//   "encPayload": "youcan'treadme!"
// } ]
// ```
func (server *Server) getMessages(rw http.ResponseWriter, req *http.Request) {
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" || len(authHeader) < 8 {
		http.Error(rw, "Not authorized", http.StatusNotFound)
		return
	}
	deviceId := strings.TrimSpace(authHeader[7:])

	snapshot := server.MessageStorage.db.NewSnapshot()
	iter := snapshot.NewIter(&pebble.IterOptions {
		LowerBound: append([]byte(deviceId), 0),
		UpperBound: append([]byte(deviceId), 1),
	})

	var msgs []*OutgoingMessage = []*OutgoingMessage{}
	for iter.First(); iter.Valid(); iter.Next() {
		om := new(OutgoingMessage)
		json.Unmarshal(iter.Value(), om)
		msgs = append(msgs, om)
	}
	iter.Close()

	json.NewEncoder(rw).Encode(msgs)
}

// Expects the body of the function to be a JSON object of the format:
//
// ```
// {
//   "batch": [
//     { "deviceId": "someid", "payload": "encryptedBlob" }
//   ]
// }
// ```
//
// Returns whatever
func (server *Server) postMessage(rw http.ResponseWriter, req *http.Request) {
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" || len(authHeader) < 8 {
		http.Error(rw, "Not authorized", http.StatusNotFound)
		return
	}
	senderDeviceId := strings.TrimSpace(authHeader[7:])

	type Batch struct {
		Batch []*IncomingMessage `json:"batch"`
	}
	var msgs Batch
	e := json.NewDecoder(req.Body).Decode(&msgs)
	if e != nil {
		http.Error(rw, fmt.Sprintf("%s", e), http.StatusInternalServerError)
		return
	}

	server.MessageStorage.lock.Lock()
	batch := server.MessageStorage.db.NewIndexedBatch()
	var seqID uint64 = 0
	var newSeqCount []byte
	seqCount, closer, e := batch.Get([]byte{0})
	if e == nil {
		seqID = binary.LittleEndian.Uint64(seqCount)
		closer.Close()
		newSeqCount = make([]byte, 8)
		binary.LittleEndian.PutUint64(newSeqCount, seqID + 1)
	} else {
		seqCount = make([]byte, 8)
		binary.LittleEndian.PutUint64(seqCount, seqID)

		newSeqCount = make([]byte, 8)
		binary.LittleEndian.PutUint64(newSeqCount, seqID + 1)
	}
	batch.Set([]byte{0}, newSeqCount, pebble.Sync)
	var tmsgs []*Message
	for _, msg := range msgs.Batch {
		tmsg := Message{}

		tmsg.To = msg.DeviceId
		tmsg.Outgoing.Payload = msg.Payload
		tmsg.Outgoing.Sender = senderDeviceId
		tmsg.Outgoing.SeqID = seqID

		tmsgs = append(tmsgs, &tmsg)

		k := append(append([]byte(msg.DeviceId), 0), seqCount...)
		msgStorage, _ := json.Marshal(&tmsg.Outgoing)
		batch.Set(k, msgStorage, pebble.Sync)
	}
	batch.Commit(pebble.Sync)
	event := MessageEvent{Messages: tmsgs, SeqID: seqID}

	server.Notifier <- &event
	server.MessageStorage.lock.Unlock()
	rw.Write([]byte("{}"))
}

func (server *Server) serveEvents(rw http.ResponseWriter, req *http.Request) {

	// Make sure that the writer supports flushing.
	//
	flusher, ok := rw.(http.Flusher)

	if !ok {
		http.Error(rw, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")

	authHeader := req.Header.Get("Authorization")
	if authHeader == "" || len(authHeader) < 8 {
		http.Error(rw, "Not authorized", http.StatusUnauthorized)
		return
	}
	deviceId := strings.TrimSpace(authHeader[7:])

	messageChan := ClientChan{
		DeviceId: deviceId,
		Channel:  make(chan Notification, 10),
	}

	server.newClients <- messageChan

	// Remove this client from the map of connected clients
	// when this handler exits.
	defer func() {
		server.closingClients <- deviceId
	}()

	notify := req.Context().Done()

	go func() {
		<-notify
		server.closingClients <- deviceId
	}()


	for {
		// Write to the ResponseWriter
		// Server Sent Events compatible
		msg := <-messageChan.Channel

		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.Encode(msg)

		switch msg.(type) {
		case *OutgoingMessage:
			fmt.Fprintf(rw, "event: msg\ndata: %v\n\n", buf.String())
			fmt.Printf("data: %v\n", buf.String())
		case *NeedsOneTimeKeyEvent:
			fmt.Fprintf(rw, "event: otkey\ndata: %v\n\n", buf.String())
			fmt.Printf("event: otkey\ndata: %v\n", buf.String())
		}
		// Flush the data immediatly instead of buffering it for later.
		flusher.Flush()
	}

}

func (server *Server) listen() {
	for {
		select {
		case s := <-server.newClients:

			// A new client has connected.
			// Register their message channel
			server.clients[s.DeviceId] = s.Channel

			// Check if there are some one time keys
			func() {
				prefix := append(append([]byte("otkeys/"), []byte(s.DeviceId)...), 0)
				batch := server.MessageStorage.db.NewIndexedBatch()
				iter := batch.NewIter(&pebble.IterOptions {
					LowerBound: prefix,
					UpperBound: append(append([]byte("otkeys/"), []byte(s.DeviceId)...), 1),
				})
				count := 0
				for iter.First(); iter.Next(); iter.Valid() {
					count += 1
					if count >= 10 {
						break
					}
				}
				batch.Commit(pebble.NoSync)

				if count < 10 {
					s.Channel <- &NeedsOneTimeKeyEvent {
						DeviceId: s.DeviceId,
						Needs: 20,
					}
				}
			}()

			// Check if there are some messages
			func() {
				prefix := append([]byte(s.DeviceId), 0)
				batch := server.MessageStorage.db.NewIndexedBatch()
				iter := batch.NewIter(&pebble.IterOptions {
					LowerBound: prefix,
					UpperBound: append([]byte(s.DeviceId), 1),
				})
				for iter.First(); iter.Next(); iter.Valid() {
					om := new(OutgoingMessage)
					json.Unmarshal(iter.Value(), om)
					s.Channel <- om
				}
				batch.Commit(pebble.NoSync)
			}()
			log.Printf("Client added. %d registered clients", len(server.clients))
		case s := <-server.closingClients:

			// A client has dettached and we want to
			// stop sending them messages.
			delete(server.clients, s)
			log.Printf("Removed client. %d registered clients", len(server.clients))
		case event := <-server.Notifier:

			switch event.(type) {
			case *MessageEvent:
				// Send event to all connected clients
				e := event.(*MessageEvent)
				for _, msg := range e.Messages {
					c, ok := server.clients[msg.To]
					var outgoing *OutgoingMessage = &msg.Outgoing
					if ok {
						c <- outgoing
					}
				}
			case *NeedsOneTimeKeyEvent:
				e := event.(*NeedsOneTimeKeyEvent)
				c, ok := server.clients[e.DeviceId]
				if ok {
					c <- e
				}
			}
		}
	}
}

func main() {

	db, err := pebble.Open("storage", &pebble.Options{})
	if err != nil {
		log.Panic(err)
	}
	server := NewServer(db)

	log.Fatal("HTTP server error: ", http.ListenAndServe("localhost:8080", server))

}
