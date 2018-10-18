package main

type Edge struct {
	Start  int
	End    int
	Weight int
}

type Node struct {
	Id            int
	IncomingEdges []*Edge
	OutgoingEdges []*Edge
	Value         float64
}

type Graph struct {
	Nodes []*Node
}

func (g Graph) addEdge(e Edge) {
	fromNode := g.Nodes[e.Start]
	g.Nodes[e.Start] = fromNode
	fromNode.OutgoingEdges = append(fromNode.OutgoingEdges, &e)

	toNode := g.Nodes[e.End]
	g.Nodes[e.End] = toNode
	toNode.IncomingEdges = append(toNode.IncomingEdges, &e)
}
