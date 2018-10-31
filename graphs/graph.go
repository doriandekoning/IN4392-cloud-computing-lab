package graphs

import (
	"encoding/binary"
	"math"

	uuid "github.com/satori/go.uuid"
)

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
	Graph         *Graph
	Active        bool
	//TODO (Not sure if this is only used for pagerank or all algorithms)
	//therefore we might not want to store this here but in a seperate splice based on node id
	Value float64
}

type Graph struct {
	Id    uuid.UUID
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

func FromBytes(binaryGraph []byte, size int, initialNodeValue float64) Graph {
	graph := Graph{Nodes: make([]*Node, size)}
	for i := 0; i < size; i++ {
		graph.Nodes[i] = &Node{Id: i, IncomingEdges: make([]*Edge, 0), OutgoingEdges: make([]*Edge, 0), Graph: &graph, Active: true, Value: initialNodeValue}
	}

	padding := 8 - (size * size % 8)
	if padding == 8 {
		padding = 0
	}
	weightsOffset := ((size * size) + padding) / 8

	for i := 0; i < size; i++ {
		for j := 0; j < size; j++ {
			bitIndex := (i * size) + j
			bitValue := binaryGraph[bitIndex/8]&(255&(1<<(8-1-uint(bitIndex%8)))) > 0
			if bitValue {
				//Get edge weight
				weight := math.Float32frombits(binary.LittleEndian.Uint32(binaryGraph[weightsOffset : weightsOffset+4]))
				weightsOffset += 4
				//Edge goes from i to j
				var from, to int
				from = j
				to = i
				edge := Edge{Start: from, End: to, Weight: weight, Messages: make([]*Message, 0)}
				graph.Nodes[from].OutgoingEdges = append(graph.Nodes[from].OutgoingEdges, &edge)
				graph.Nodes[to].IncomingEdges = append(graph.Nodes[to].IncomingEdges, &edge)

			}
		}
	}
	return graph
}
