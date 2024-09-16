package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

var _ = net.Listen
var _ = os.Exit
var validAPIVersions = []uint16{0, 1, 2, 3, 4}

type ResponseType uint8

const (
	ResponseTypeError ResponseType = iota
	ResponseTypeCorrelationID
)

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

			apiVersion := headerBytes[2:4]
			if isValidAPIVersion(apiVersion) {
				response := buildResponse(headerBytes, 0)
				c.Write(response)
			} else {
				response := buildResponse(headerBytes, 35)
				c.Write(response)
			}

            if err != nil {
                fmt.Println("Error parsing message:", err)
            }
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

func buildResponse(headerBytes []byte, errorCode int16) []byte {
	responseLength := uint32(4)
	response := make([]byte, 10)
	var correlationID = binary.BigEndian.Uint32(headerBytes[4:8])
	binary.BigEndian.PutUint32(response[0:4], responseLength)
	binary.BigEndian.PutUint32(response[4:8], correlationID)
	binary.BigEndian.PutUint16(response[8:10], uint16(errorCode))
	return response
}

func isValidAPIVersion(apiVersion []byte) bool {
	for _, validAPIVersion := range validAPIVersions {
		if binary.BigEndian.Uint16(apiVersion) == validAPIVersion {
			return true
		}
	}
	return false
}