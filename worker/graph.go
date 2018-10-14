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
}

type Graph struct {
	Nodes []*Node
}
