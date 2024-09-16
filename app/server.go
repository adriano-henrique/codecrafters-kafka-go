package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

var _ = net.Listen
var _ = os.Exit

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()
	fmt.Println("Server is listening on port 9092...")

	for {
        // Accept an incoming connection
        conn, err := l.Accept()
        if err != nil {
            fmt.Println("Error accepting connection:", err)
            continue
        }

        fmt.Println("Accepted new connection from:", conn.RemoteAddr())

        go func(c net.Conn) {
            defer c.Close()

			_, headerBytes, err := parseMessage(c)

            if err != nil {
                fmt.Println("Error parsing message:", err)
            }
			response := buildResponse(headerBytes)
			c.Write(response)
        }(conn)
    }
}

func parseMessage(conn net.Conn) ([]byte, []byte, error) {
	buff := make([]byte, 1024)
	_, err := conn.Read(buff)
	if err != nil {
		fmt.Println("Error reading from connection:", err)
		return nil, nil, err
	}

	lengthBytes := buff[0:4]
	headerBytes := buff[4:12]

	return lengthBytes, headerBytes, nil
}

func buildResponse(headerBytes []byte) []byte {
	responseLength := uint32(4)
	response := make([]byte, 8)
	var correlationID = binary.BigEndian.Uint32(headerBytes[4:8])

	binary.BigEndian.PutUint32(response[0:4], responseLength)
	binary.BigEndian.PutUint32(response[4:8], correlationID)
	return response
}