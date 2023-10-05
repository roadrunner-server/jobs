package main

import (
	"log"
	rand2 "math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api/v4/build/jobs/v1"
	goridgeRpc "github.com/roadrunner-server/goridge/v3/pkg/rpc"
)

const (
	push string = "jobs.Push"
)

func main() {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	conn, err := net.Dial("tcp", "127.0.0.1:6001")
	if err != nil {
		log.Fatal(err)
	}

	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	for j := 0; j < 100000; j++ {
		push100(client, "test-1")
		time.Sleep(time.Millisecond)
	}
	wg.Done()

	wg.Wait()
}

func push100(client *rpc.Client, pipe string) {
	for j := 0; j < 100; j++ {
		payloads := &jobsProto.PushRequest{
			Job: &jobsProto.Job{
				Job:     "Some/Super/PHP/Class",
				Id:      uuid.NewString(),
				Payload: []byte("Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularized in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."),
				Headers: map[string]*jobsProto.HeaderValue{"test": {Value: []string{"hello"}}},
				Options: &jobsProto.Options{
					Priority: int64(rand2.Intn(100) + 1), //nolint:gosec
					Pipeline: pipe,
				},
			},
		}

		resp := jobsProto.Empty{}
		err := client.Call(push, payloads, &resp)
		if err != nil {
			log.Println(err)
		}
	}
}
