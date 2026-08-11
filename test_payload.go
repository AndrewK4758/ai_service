package main

import (
	"fmt"
	"github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/proto"
)

func main() {
	req := &shared_protos.ExecuteTaskRequest{
		TaskSubject: "worker.ai.inference",
	}
	data, _ := proto.Marshal(req)
	
	req2 := &shared_protos.ExecuteTaskRequest{}
	proto.Unmarshal(data, req2)
	
	fmt.Printf("Payload is nil? %v\n", req2.Payload == nil)
}
