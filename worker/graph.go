package main

type Message struct {
	From    int
	To      int
	Message float64
	Step    int
}

type Edge struct {
	Start    int
	End      int
	Weight   float64
	Messages []*Message
}

type Node struct {
	Id            int
	IncomingEdges []*Edge
	OutgoingEdges []*Edge
	SuperStep     int
	graph         *Graph
	VoteToHalt    bool
	//TODO (Not sure if this is only used for pagerank or all algorithms)
	//therefore we might not want to store this here but in a seperate splice based on node id
	Value float64
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
	n.VoteToHalt = false
}

func (n *Node) DoStep() {
	//For now pagerank according to https://kowshik.github.io/JPregel/pregel_paper.pdf
	if n.SuperStep >= 1 {
		sum := 0.0
		for _, incomingEdge := range n.IncomingEdges {
			if incomingEdge.Messages[0] != nil && incomingEdge.Messages[0].Step == n.SuperStep {
				sum += float64(incomingEdge.Messages[0].Message)
				//Remove first edge
				incomingEdge.Messages = incomingEdge.Messages[1:]
			}

		}

		n.Value = (0.15 / float64(len(n.graph.Nodes))) + (0.85 * sum)
	}
	if n.SuperStep < 3 {
		// Send out sum of tentative pagerank divided by number of outgoing edges
		for _, outgoingEdge := range n.OutgoingEdges {
			m := Message{
				From:    n.Id,
				To:      outgoingEdge.End,
				Message: n.Value / float64(len(n.OutgoingEdges)),
				Step:    n.SuperStep + 1,
			}
			n.graph.Nodes[outgoingEdge.End].ReceiveMessage(m)
		}
	} else {
		n.VoteToHalt = true
	}

	n.SuperStep++
}
