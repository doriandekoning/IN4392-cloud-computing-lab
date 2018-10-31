package graphs

import (
	"math"
)

type AlgorithmInterface interface {
	Initialize()
	Step(n *Node, step int)
}

type PagerankInstance struct {
	//Implement AlgorithmInterface
	AlgorithmInterface
	MaxSteps int
	Graph    *Graph
}

func (instance *PagerankInstance) Initialize() {
	for _, node := range instance.Graph.Nodes {
		node.Active = true
	}
}

//TODO check if we can pass the step variable to this function
func (instance *PagerankInstance) Step(n *Node, step int) {
	//For now pagerank according to https://kowshik.github.io/JPregel/pregel_paper.pdf
	if step >= 1 {
		sum := 0.0
		for _, incomingEdge := range n.IncomingEdges {
			if len(incomingEdge.Messages) > 0 && incomingEdge.Messages[0] != nil && incomingEdge.Messages[0].Step == step {
				sum += float64(incomingEdge.Messages[0].Message)
				//Remove first message
				incomingEdge.Messages = incomingEdge.Messages[1:]
			}

		}

		n.Value = (0.15 / float64(len(instance.Graph.Nodes))) + (0.85 * sum)
	}
	if step < instance.MaxSteps {
		// Send out sum of tentative pagerank divided by number of outgoing edges
		for _, outgoingEdge := range n.OutgoingEdges {
			m := Message{
				From:    n.Id,
				To:      outgoingEdge.End,
				Message: n.Value / float64(len(n.OutgoingEdges)),
				Step:    step + 1,
			}
			instance.Graph.Nodes[outgoingEdge.End].ReceiveMessage(m)
		}
	} else {
		n.VoteToHalt()
	}

}

type ShortestPathInstance struct {
	//implement AlgorithmInterface
	AlgorithmInterface
	Graph    *Graph
	SourceID int
}

func (instance *ShortestPathInstance) Initialize() {
	for _, n := range instance.Graph.Nodes {
		n.Value = math.MaxFloat64
		n.Active = true
	}
}

func (instance *ShortestPathInstance) Step(n *Node, step int) {
	// Set mindist to max int (int with all bits 1 except the first)
	minDist := math.MaxFloat64
	if n.Id == instance.SourceID {
		minDist = 0
	}
	for _, message := range n.GetMessages(step) {
		minDist = math.Min(minDist, message.Message)
		//Remove first message
		// incomingEdge.Messages = incomingEdge.Messages[1:]
	}

	if minDist < n.Value {
		n.Value = minDist
		for _, outgoingEdge := range n.OutgoingEdges {
			//TODO make function that builds message and stuff
			m := Message{
				From:    n.Id,
				To:      outgoingEdge.End,
				Message: minDist + float64(outgoingEdge.Weight),
				Step:    step + 1,
			}
			instance.Graph.Nodes[outgoingEdge.End].ReceiveMessage(m)
		}
	}
	n.VoteToHalt()

}
