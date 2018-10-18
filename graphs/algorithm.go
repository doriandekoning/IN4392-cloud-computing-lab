package graphs

type AlgorithmInterface interface {
	Step(n *Node)
}

type Pagerank struct {
	//Implement AlgorithmInterface
	AlgorithmInterface
}

//TODO check if we can pass the step variable to this function
func (v *Pagerank) Step(n *Node) {
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

		n.Value = (0.15 / float64(len(n.Graph.Nodes))) + (0.85 * sum)
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
			n.Graph.Nodes[outgoingEdge.End].ReceiveMessage(m)
		}
	} else {
		n.VoteToHalt = true
	}

	n.SuperStep++
}

//TODO impelement maxval
