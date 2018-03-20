package main

import (
	"container/heap"
	"container/list"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
)

const IdLength = 20

type NodeID [IdLength]byte

func NewNodeID(data string) (ret NodeID) {
	decoded, _ := hex.DecodeString(data)
	for i := 0; i < IdLength; i++ {
		ret[i] = decoded[i]
	}
	return
}

func NewRandomNodeID() (ret NodeID) {
	for i := 0; i < IdLength; i++ {
		ret[i] = uint8(rand.Intn(256))
	}
	return
}

func (node NodeID) String() string {
	return hex.EncodeToString(node[0:IdLength])
}

func (node NodeID) Equals(other NodeID) bool {
	for i := 0; i < IdLength; i++ {
		if node[i] != other[i] {
			return false
		}
	}
	return true
}

func (node NodeID) Less(other interface{}) bool {
	for i := 0; i < IdLength; i++ {
		if node[i] != other.(NodeID)[i] {
			return node[i] < other.(NodeID)[i]
		}
	}
	return false
}

func (node NodeID) Xor(other NodeID) (ret NodeID) {
	for i := 0; i < IdLength; i++ {
		ret[i] = node[i] ^ other[i]
	}
	return
}

func (node NodeID) PrefixLen() (ret int) {
	for i := 0; i < IdLength; i++ {
		for j := 0; j < 8; j++ {
			if (node[i]>>uint8(7-j))&0x1 != 0 {
				return i*8 + j
			}
		}
	}
	return IdLength*8 - 1
}

const BucketSize = 20

type Contact struct {
	id      NodeID
	address string
}

type RoutingTable struct {
	node    Contact
	buckets [IdLength * 8]*list.List
}

func NewRoutingTable(node *Contact) (ret *RoutingTable) {
	for i := 0; i < IdLength*8; i++ {
		ret.buckets[i] = list.New()
	}
	ret.node = *node
	return
}

func Filter(vs *list.List, f func(interface{}) bool) *list.Element {

	for v := vs.Front(); v != nil; v = v.Next() {
		if f(v) {
			return v
		}
	}
	return nil
}

func (table *RoutingTable) Update(contact *Contact) {
	prefixLength := contact.id.Xor(table.node.id).PrefixLen()
	bucket := table.buckets[prefixLength]

	element := Filter(bucket, func(x interface{}) bool {
		return x.(*Contact).id.Equals(table.node.id)
	})

	if element == nil {
		if bucket.Len() <= BucketSize {
			bucket.PushFront(contact)
		}
		// TODO: Handle insertion when the list is full by evicting old elements if
		// they don't respond to a ping.
	} else {
		bucket.MoveToFront(element)
	}
}

type ContactRecord struct {
	node    *Contact
	sortKey NodeID
}

func (rec *ContactRecord) Less(other interface{}) bool {
	return rec.sortKey.Less(other.(*ContactRecord).sortKey)
}

func copyToVector(start *list.Element, end *list.Element, vec *[]ContactRecord, target NodeID) {
	for elt := start; elt != end; elt = elt.Next() {
		contact := elt.Value.(*Contact)
		*vec = append(*vec, (ContactRecord{contact, contact.id.Xor(target)}))
	}
}

func (table *RoutingTable) FindClosest(target NodeID, count int) []ContactRecord {
	var ret []ContactRecord

	bucketNum := target.Xor(table.node.id).PrefixLen()
	bucket := table.buckets[bucketNum]
	copyToVector(bucket.Front(), nil, &ret, target)

	lenRet := len(ret)
	for i := 1; (bucketNum-i >= 0 || bucketNum+i < IdLength*8) && lenRet < count; i++ {
		if bucketNum-i >= 0 {
			bucket = table.buckets[bucketNum-i]
			copyToVector(bucket.Front(), nil, &ret, target)
		}
		if bucketNum+i < IdLength*8 {
			bucket = table.buckets[bucketNum+i]
			copyToVector(bucket.Front(), nil, &ret, target)
		}
	}

	// sort.Sort(ret)
	// if ret.Len() > count {
	// 	ret.Cut(count, ret.Len())
	//}
	return ret
}

type Kademlia struct {
	routes    *RoutingTable
	NetworkId string
}

func NewKademlia(self *Contact, networkId string) (ret *Kademlia) {
	ret = new(Kademlia)
	ret.routes = NewRoutingTable(self)
	ret.NetworkId = networkId
	return
}

type RPCHeader struct {
	Sender    *Contact
	NetworkId string
}

func (k *Kademlia) HandleRPC(request, response *RPCHeader) error {
	if request.NetworkId != k.NetworkId {
		return errors.New(fmt.Sprintf("Expected network ID %s, got %s",
			k.NetworkId, request.NetworkId))
	}
	if request.Sender != nil {
		k.routes.Update(request.Sender)
	}
	response.Sender = &k.routes.node
	return nil
}

type KademliaCore struct {
	kad *Kademlia
}

type PingRequest struct {
	RPCHeader
}

type PingResponse struct {
	RPCHeader
}

func (kc *KademliaCore) Ping(args *PingRequest, response *PingResponse) (err error) {
	if err = kc.kad.HandleRPC(&args.RPCHeader, &response.RPCHeader); err == nil {
		fmt.Printf("Ping from %s\n", args.RPCHeader)
	}
	return
}

func (k *Kademlia) Serve() (err error) {
	rpc.Register(&KademliaCore{k})

	rpc.HandleHTTP()
	if l, err := net.Listen("tcp", k.routes.node.address); err == nil {
		go http.Serve(l, nil)
	}
	return
}

type FindNodeRequest struct {
	RPCHeader
	target NodeID
}

type FindNodeResponse struct {
	RPCHeader
	contacts []Contact
}

func (kc *KademliaCore) FindNode(args *FindNodeRequest, response *FindNodeResponse) (err error) {
	if err = kc.kad.HandleRPC(&args.RPCHeader, &response.RPCHeader); err == nil {
		contacts := kc.kad.routes.FindClosest(args.target, BucketSize)
		response.contacts = make([]Contact, len(contacts))

		for i := 0; i < len(contacts); i++ {
			response.contacts[i] = *contacts[i].node
		}
	}
	return
}

func (k *Kademlia) Call(contact *Contact, method string, args, reply interface{}) (err error) {
	if client, err := rpc.DialHTTP("tcp", contact.address); err == nil {
		err = client.Call(method, args, reply)
		if err == nil {
			k.routes.Update(contact)
		}
	}
	return
}

func (k *Kademlia) sendQuery(node *Contact, target NodeID, done chan []Contact) {
	args := FindNodeRequest{RPCHeader{&k.routes.node, k.NetworkId}, target}
	reply := FindNodeResponse{}

	if err := k.Call(node, "KademliaCore.FindNode", &args, &reply); err == nil {
		done <- reply.contacts
	} else {
		done <- []Contact{}
	}
}
func (k *Kademlia) IterativeFindNode(target NodeID, delta int) (ret *list.List) {
	done := make(chan []Contact)

	// A vector of *ContactRecord structs
	//ret = new(vector.Vector).Resize(0, BucketSize)

	// A heap of not-yet-queried *Contact structs
	var frontier heap.Interface

	// A map of client values we've seen so far
	seen := make(map[string]bool)

	// Initialize the return list, frontier heap, and seen list with local nodes
	for _, node := range k.routes.FindClosest(target, delta) {
		record := node
		ret.PushFront(record)
		//frontier.PushFront(record)
		heap.Push(frontier, record.node)

		seen[record.node.id.String()] = true
	}

	// Start off delta queries
	pending := 0
	for i := 0; i < delta && frontier.Len() > 0; i++ {
		pending++
		go k.sendQuery(frontier.Pop().(*Contact), target, done)
	}
	// Iteratively look for closer nodes
	for pending > 0 {
		nodes := <-done
		pending--
		for _, node := range nodes {
			// If we haven't seen the node before, add it
			if _, ok := seen[node.id.String()]; ok == false {
				ret.PushFront(&ContactRecord{&node, node.id.Xor(target)})
				heap.Push(frontier, node)
				seen[node.id.String()] = true
			}
		}

		for pending < delta && frontier.Len() > 0 {
			go k.sendQuery(frontier.Pop().(*Contact), target, done)
			pending++
		}
	}
	// sort.Sort(ret)
	// if ret.Len() > BucketSize {
	// 	ret.Cut(BucketSize, ret.Len())
	// }

	return
}

func main() {

	nodeID1 := NewRandomNodeID()
	nodeID2 := NewRandomNodeID()
	nodeID3 := NewRandomNodeID()

	fmt.Println(nodeID1)
	fmt.Println(nodeID2)
	fmt.Println(nodeID3)

	var nodeID2Contact Contact
	nodeID2Contact.id = nodeID2
	var nodeID3Contact Contact
	nodeID3Contact.id = nodeID3

	// routingTable := NewRoutingTable(nodeID1)

	// routingTable.Update(&nodeID2Contact)
	// routingTable.Update(&nodeID3Contact)

	// findRes := routingTable.FindClosest(nodeID2, 2)
	// fmt.Println(findRes[0].node.id)
}
