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
	Id      NodeID
	Address string
}

type RoutingTable struct {
	node    Contact
	buckets [IdLength * 8]*list.List
}

func NewRoutingTable(node *Contact) *RoutingTable {

	var ret RoutingTable
	for i := 0; i < IdLength*8; i++ {
		ret.buckets[i] = list.New()
	}
	ret.node = *node
	return &ret
}

func Filter(vs *list.List, f func(interface{}) bool) *list.Element {

	for v := vs.Front(); v != nil; v = v.Next() {
		if f(v.Value) {
			return v
		}
	}
	return nil
}

func (table *RoutingTable) Update(contact *Contact) {
	prefixLength := contact.Id.Xor(table.node.Id).PrefixLen()
	bucket := table.buckets[prefixLength]

	element := Filter(bucket, func(x interface{}) bool {
		return x.(*Contact).Id.Equals(table.node.Id)
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

func (rec ContactRecord) Less(other interface{}) bool {
	return rec.sortKey.Less(other.(ContactRecord).sortKey)
}

func copyToVector(start *list.Element, end *list.Element, vec *[]ContactRecord, target NodeID) {
	for elt := start; elt != end; elt = elt.Next() {
		contact := elt.Value.(*Contact)
		*vec = append(*vec, (ContactRecord{contact, contact.Id.Xor(target)}))
	}
}

func (table *RoutingTable) FindClosest(target NodeID, count int) []ContactRecord {
	var ret []ContactRecord

	bucketNum := target.Xor(table.node.Id).PrefixLen()
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
	// }
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
	if l, err := net.Listen("tcp", k.routes.node.Address); err == nil {
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
	Contacts []Contact
}

func (kc *KademliaCore) FindNode(args *FindNodeRequest, response *FindNodeResponse) (err error) {
	if err = kc.kad.HandleRPC(&args.RPCHeader, &response.RPCHeader); err == nil {
		Contacts := kc.kad.routes.FindClosest(args.target, BucketSize)
		response.Contacts = make([]Contact, len(Contacts))

		for i := 0; i < len(Contacts); i++ {
			response.Contacts[i] = *Contacts[i].node
		}
	}
	return
}

func (k *Kademlia) Call(contact *Contact, method string, args, reply interface{}) (err error) {
	if client, err := rpc.DialHTTP("tcp", contact.Address); err == nil {

		err = client.Call(method, args, reply)
		if err == nil {
			k.routes.Update(contact)
		} else {
			fmt.Print(err)
		}

	}
	return
}

func (k *Kademlia) sendQuery(node *Contact, target NodeID, done chan []Contact) {
	args := FindNodeRequest{RPCHeader{&k.routes.node, k.NetworkId}, target}
	reply := FindNodeResponse{}

	if err := k.Call(node, "KademliaCore.FindNode", &args, &reply); err == nil {
		done <- reply.Contacts
	} else {
		done <- []Contact{}
	}
}

type SliceOfContact []Contact

func (pq SliceOfContact) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].Id.Less(pq[j].Id)
}

func (pq SliceOfContact) Len() int { return len(pq) }

func (pq SliceOfContact) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *SliceOfContact) Push(x interface{}) {

	*pq = append(*pq, x.(Contact))
}

func (pq *SliceOfContact) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	//item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (k *Kademlia) IterativeFindNode(target NodeID, delta int) (ret *list.List) {
	done := make(chan []Contact)

	// A vector of *ContactRecord structs
	//ret = new(vector.Vector).Resize(0, BucketSize)

	// A heap of not-yet-queried *Contact structs

	var frontier SliceOfContact
	//heap.Interface
	heap.Init(&frontier)

	ret = list.New()
	// A map of client values we've seen so far
	seen := make(map[string]bool)

	// Initialize the return list, frontier heap, and seen list with local nodes
	for _, node := range k.routes.FindClosest(target, delta) {
		record := node
		ret.PushFront(record)
		//frontier.PushFront(record)
		heap.Push(&frontier, *record.node)

		seen[record.node.Id.String()] = true
	}

	// Start off delta queries
	pending := 0
	for i := 0; i < delta && frontier.Len() > 0; i++ {
		pending++
		go func() {
			cont := frontier.Pop().(Contact)
			k.sendQuery(&cont, target, done)
		}()
	}
	// Iteratively look for closer nodes
	for pending > 0 {
		nodes := <-done
		pending--
		for _, node := range nodes {
			fmt.Printf("got node %s ip %s .\n", node.Id.String(), node.Address)
			// If we haven't seen the node before, add it
			if _, ok := seen[node.Id.String()]; ok == false {
				ret.PushFront(&ContactRecord{&node, node.Id.Xor(target)})
				heap.Push(&frontier, node)
				seen[node.Id.String()] = true
			}
		}

		for pending < delta && frontier.Len() > 0 {
			go func() {
				con := frontier.Pop().(Contact)
				k.sendQuery(&con, target, done)
			}()
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

	var nodeID1Contact Contact
	nodeID1Contact.Id = nodeID1
	nodeID1Contact.Address = "localhost:12000"
	var nodeID2Contact Contact
	nodeID2Contact.Id = nodeID2
	nodeID2Contact.Address = "localhost:12001"
	var nodeID3Contact Contact
	nodeID3Contact.Id = nodeID3
	nodeID3Contact.Address = "localhost:12002"

	fmt.Println("Press number for server ")
	text := "1"
	fmt.Scan(&text)
	var nodeIdContact Contact
	switch text {
	case "1":
		nodeIdContact = nodeID1Contact
	case "2":
		nodeIdContact = nodeID2Contact
	case "3":
		nodeIdContact = nodeID3Contact
	}

	nodeKad := NewKademlia(&nodeIdContact, "netword123")

	nodeKad.Serve()

	fmt.Println("enter number for client")
	client := "1"
	fmt.Scan(&client)
	var nodeIdClientContact Contact
	switch client {
	case "1":
		nodeIdClientContact = nodeID1Contact
	case "2":
		nodeIdClientContact = nodeID2Contact
	case "3":
		nodeIdClientContact = nodeID3Contact
	}
	nodeKad.routes.Update(&nodeIdClientContact)
	nodeKad.IterativeFindNode(nodeIdClientContact.Id, 1)
	// routingTable := NewRoutingTable(nodeID1)

	// routingTable.Update(&nodeID2Contact)
	// routingTable.Update(&nodeID3Contact)

	// findRes := routingTable.FindClosest(nodeID2, 2)
	// fmt.Println(findRes[0].node.Id)
}
