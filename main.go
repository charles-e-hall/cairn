package main


import (
	"github.com/labstack/echo/v4"
	"os"
	"net/http"
	// "fmt"
	"encoding/binary"
)


// Log File contains message blocks, each of which contains message index, message length in bytes, message
// Position file contains message index, position in log file of message block start
// Pub/Sub endpoints. Return client_id and save client_id for tracking what offset they are on for consumers
// Topics endpoint to list topics and create a new topic
// Log File:
//	index		uint32 (4 bytes)
//	message_length	uint16 (2 bytes)
//	message		variable (max 65,535 bytes)
// Position File:
//	index		uint32 (4 bytes)
//	file_position	uint64 (8 bytes)
//	block_size	uint32 (4 bytes)

// Once the max index has been reached, I will need to create a new set of log & position files and the 
// system will somehow need to know to use the most recent one. But then there will need to be a mechanism 
// to know if the data sought is in one of the previous files.
// Every topic will have at least one set of log and position files.

// PUBLISHING & CONSUMING
// Communication is done over http so that everything is easy - no special protocols
// /topics
// POST - creates new topic
// GET - lists topics
// /topics/<topic>/publish
// POST - publish new message to topic with "message" key being json object that is serialized as message
// /topics/<topic>/consume?client_id=<client_id>&[index=<index>]
// index is optional when consuming. The broker will track the most recently consumed index and return the next index
// This is done so that consumers can remain stateless if the designer prefers that.

// Insert algorithm
// 1. Open index partition file, seek to X bytes from the end and read the next X bytes. Check that last index in range is not at limit for file.
//	a. If at limit for file, create new key partition, create new postiion file, create new log file
//	b. Generate new index and add it to the file
// 2. Open position file and seek to 16 bytes from the end. Read 16 bytes.
// 3. Get add value of 8 bytes position to value of next 4 bytes (message block size). Write new position block to position file (16 bytes)
// 4. Open log file and seek to the end. Write new message block

// Read algorithm
// 1. Open index partition file and iteratively read until find index partition?
// 2. Calculate seek offset for position file as offset = index * 16 (bytes). Open position file and seek to offset from beginning of file. Read 16 bytes.
// 3. Confirm that first 4 bytes match index (if not?). Next 8 bytes are position in log file. Save position.
// 4. Open log file and seek to position. Read 4 bytes and check against index. Read 2 bytes as message length. Read message length. Return message.
 

// GOLANG DESIGN
// Service that handles all file writes and reads
// That service takes requests from a channel? But then how would it return results for reads?
// Should I pass around a context so that it can add its stuff to the context?

type Message struct {
	Value		string
	BytesValue	[]byte
	Length		int
}

func logWrite(m *Message) error {
	f, err := os.OpenFile("/home/charlie/Documents/projects/cairn/log.bin", os.O_APPEND | os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	err = binary.Write(f, binary.LittleEndian, uint16(1)
	if err != nil {
		panic(err)
	}
	err = binary.Write(f, binary.LittleEndian, uint16(m.Length))
	if err != nil {
		panic(err)	
	}
	err = binary.Write(f, binary.LittleEndian, m.BytesValue)

	if err != nil {
		panic(err)	
	}
	f.Close()
	return nil
}

func handleProduce(c echo.Context) error {
	m := c.QueryParam("msg")
	msg := Message{
		Value: m,
		BytesValue: []byte(m),
		Length: len([]byte(m)),
	}
	err := logWrite(&msg)
	if err != nil {
		panic(err)
	}
	return c.JSON(http.StatusOK, echo.Map{"status": "OK"})
}

func main() {
	e := echo.New()

	e.GET("/produce", handleProduce)

	e.Logger.Fatal(e.Start(":9092"))
}
