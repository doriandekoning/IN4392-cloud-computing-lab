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
	Weight   float32
	Messages []*Message
}

type Node struct {
	Id            int
	IncomingEdges []*Edge
	OutgoingEdges []*Edge
	SuperStep     int
	Graph         *Graph
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
