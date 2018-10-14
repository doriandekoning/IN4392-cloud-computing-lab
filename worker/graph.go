package main

import "fmt"

type Message struct {
	From    int
	To      int
	Message int
	Step    int
}

type Edge struct {
	Start    int
	End      int
	Weight   int
	Messages []*Message
}

type Node struct {
	Id            int
	IncomingEdges []*Edge
	OutgoingEdges []*Edge
	SuperStep     int
}

type Graph struct {
	Nodes []*Node
}

func (n Node) ReceiveMessage(message Message) {
	for _, edge := range n.IncomingEdges {
		if edge.Start == message.From {
			edge.Messages = append(edge.Messages, &message)
			break
		}
	}
	// Check if all messages for current superstep are received
	for _, edge := range n.IncomingEdges {
		for _, message := range edge.Messages {
			if message.Step != n.SuperStep {
				return
			}
		}
	}
	//Handle superstep (all messages are here)
	//TODO impelment step behaviour

	fmt.Println("Did superstep", n.SuperStep, "For node", n.Id)
	n.SuperStep++

	//Remove old messages

}
