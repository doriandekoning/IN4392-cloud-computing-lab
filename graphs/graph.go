package graphs

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
	Graph         *Graph
	Active        bool
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
	n.VoteToHalt()
}

func (graph *Graph) ProcessGraph(alg AlgorithmInterface) {

}

func (g Graph) AddEdge(e Edge) {
	fromNode := g.Nodes[e.Start]
	g.Nodes[e.Start] = fromNode
	fromNode.OutgoingEdges = append(fromNode.OutgoingEdges, &e)

	toNode := g.Nodes[e.End]
	g.Nodes[e.End] = toNode
	toNode.IncomingEdges = append(toNode.IncomingEdges, &e)
}

func (n *Node) VoteToHalt() {
	for _, incomingEdge := range n.IncomingEdges {
		if len(incomingEdge.Messages) > 0 {
			n.Active = true
			return
		}
	}
	n.Active = false
}

func (n Node) GetMessages(step int) []*Message {
	messages := make([]*Message, 0)
	for _, incomingEdge := range n.IncomingEdges {
		for index, message := range incomingEdge.Messages {
			if message.Step == step {
				messages = append(messages, message)
				incomingEdge.Messages[index] = incomingEdge.Messages[len(incomingEdge.Messages)-1]
				incomingEdge.Messages = incomingEdge.Messages[:len(incomingEdge.Messages)-1]
				break
			}
		}
	}
	return messages
}
