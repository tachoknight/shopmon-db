package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

const (
	host     = ""
	port     = 5432
	user     = ""
	password = ""
	dbname   = ""
)

// Our connection to the postgresql database
var db *sql.DB

type StatusMessage struct {
	spaceStatus string
}

// Our channel that accepts StatusMessages from the MQTT server
var statusChannel chan StatusMessage

// The channel we are going to use to send the new messages to
// the MQTT server
var fullStatusChannel chan string

// The map and guard that we use to keep track of what
// sensors we've seen and their timestamps
var sensorMap map[string]time.Time
var mutex = &sync.Mutex{}

// The time, in seconds, of how long an 'active' status message
// can live before it's expired
const expiry = 10

func buildTimeline() {
	for {
		time.Sleep(1 * time.Second)
		if len(sensorMap) > 0 {
			mutex.Lock()
			for k, v := range sensorMap {
				fmt.Println("k:", k, "v:", v)
				// Okay, first we need to split the key out into its
				// separate fields...
				keyParts := strings.Split(k, ":")

				// Now let's insert the record into the database
				sqlStatement := "INSERT INTO shopmon (datetime, sensor, area) VALUES ($1, $2, $3)"
				_, err := db.Exec(sqlStatement, v, keyParts[0], keyParts[1])
				if err != nil {
					panic(err)
				}
			}
			mutex.Unlock()
		}
	}
}

func main() {

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	var err error
	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	// The channel we're going to receive messages on
	statusChannel = make(chan StatusMessage)
	// The channel we're going to send the full data on
	fullStatusChannel = make(chan string)

	// Set us up to listen to the topics on the MQTT server...
	go listenOnTopic()

	// Create our map that will hold the key of sensor name
	// to its timestamp
	sensorMap = make(map[string]time.Time)
	// This goroutine reads from the map
	go buildTimeline()

	// And here we go!
	for {
		// Get our message from the MQTT topic
		statusMsg := <-statusChannel

		// And split it up into its various parts
		lineParts := strings.Split(statusMsg.spaceStatus, ",")
		i, err := strconv.ParseInt(lineParts[0], 10, 64)
		if err != nil {
			panic(err)
		}

		// We want to preserve the area the sensor is in, but what is being
		// sent onward doesn't have access to the original message but for what
		// is in the map so we are gonna do a little trick and append the area name
		// to the sensor id so the key is effectively "HotMetals-2:Hot Metals", so
		// that downstream they'll have both parts and can split on the colon
		fullKey := lineParts[1] + ":" + lineParts[2]

		// Convert the unix timestamp to a time object for the map
		tm := time.Unix(i, 0)
		mutex.Lock()
		// Check to see if there's already an entry for this sensor in the map
		if _, testIfExists := sensorMap[fullKey]; testIfExists {
			// There is an entry, but it has an older timestamp
			// so we are going to simply delete it, because we know that
			// the timestamp we have right now is newer than this one (even if
			// by a second), and that's all we care about
			delete(sensorMap, fullKey)
		}
		// Now we add our entry into the map so the buildTimeLine() function
		// can evaulate it
		sensorMap[fullKey] = tm
		mutex.Unlock()
	}

	// Should never get here
	fmt.Println("Hello world!")
}
