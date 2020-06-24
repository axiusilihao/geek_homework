package main

import (
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"strconv"
	"sync"
)

const (
	DEFAULT_REPLICAS = 160
	DATA_COUNT       = 100_0000
	NODE_COUNT       = 10
)

type HashRing []uint32

func (c HashRing) Len() int {
	return len(c)
}

func (c HashRing) Less(i, j int) bool {
	return c[i] < c[j]
}

func (c HashRing) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

type Node struct {
	Id       int
	Ip       string
	Port     int
	HostName string
	Weight   int
}

func NewNode(id int, ip string, port int, name string, weight int) *Node {
	return &Node{
		Id:       id,
		Ip:       ip,
		Port:     port,
		HostName: name,
		Weight:   weight,
	}
}

type Consistent struct {
	sync.RWMutex
	Nodes     map[uint32]Node
	resources map[int]bool
	ring      HashRing
	numReps   int
}

func NewConsistent() *Consistent {
	nodes := make(map[uint32]Node)
	resources := make(map[int]bool)

	return &Consistent{
		Nodes:     nodes,
		resources: resources,
		ring:      HashRing{},
		numReps:   DEFAULT_REPLICAS,
	}
}

func (c *Consistent) Add(node *Node) bool {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.resources[node.Id]; ok {
		return false
	}

	count := c.numReps * node.Weight
	for i := 0; i < count; i++ {
		s := c.joinStr(i, node)
		c.Nodes[c.hashStr(s)] = *(node)
	}

	c.resources[node.Id] = true
	c.sortHashRing()
	return true
}

func (c *Consistent) sortHashRing() {
	c.ring = HashRing{}
	for k := range c.Nodes {
		c.ring = append(c.ring, k)
	}

	sort.Sort(c.ring)
}

func (c *Consistent) joinStr(i int, node *Node) string {
	return node.Ip + "*" + strconv.Itoa(node.Weight) + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(node.Id)
}

func (c *Consistent) hashStr(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) Get(key string) Node {
	c.RLock()
	defer c.RUnlock()

	hash := c.hashStr(key)
	i := c.search(hash)

	return c.Nodes[c.ring[i]]
}

func (c *Consistent) search(hash uint32) int {
	i := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i] >= hash
	})

	if i < len(c.ring) {
		if i == len(c.ring)-1 {
			return 0
		} else {
			return i
		}
	}

	return len(c.ring) - 1
}

func (c *Consistent) Remove(node *Node) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.resources[node.Id]; !ok {
		return
	}

	delete(c.resources, node.Id)

	count := c.numReps * node.Weight
	for i := 0; i < count; i++ {
		s := c.joinStr(i, node)
		delete(c.Nodes, c.hashStr(s))
	}

	c.sortHashRing()
}

func Expection(vals []int) float64 {
	len := len(vals)
	sum := 0

	for i := 0; i < len; i++ {
		sum += vals[i]
	}

	expection := float64(sum) / float64(NODE_COUNT)

	return expection
}

func StandardVariance(vals []int) float64 {
	len := len(vals)
	sum := 0

	for i := 0; i < len; i++ {
		sum += vals[i]
	}

	mean := float64(sum) / float64(len)

	variance := 0.0
	for i := 0; i < len; i++ {
		variance += math.Pow(float64(vals[i])-mean, 2)
	}

	return math.Sqrt(variance / float64(len))
}

func main() {
	cHashRing := NewConsistent()

	for i := 0; i < NODE_COUNT; i++ {
		si := fmt.Sprintf("%d", i)
		cHashRing.Add(NewNode(i, "192.168.1."+si, 8080, "host_"+si, 1))
	}

	ipMap := make(map[string]int, 0)
	for i := 0; i < DATA_COUNT; i++ {
		si := fmt.Sprintf("key%d", i)
		k := cHashRing.Get(si)
		if _, ok := ipMap[k.Ip]; ok {
			ipMap[k.Ip] += 1
		} else {
			ipMap[k.Ip] = 1
		}
	}

	values := make([]int, 0, len(ipMap))

	// 数据分布情况
	fmt.Println("数据分布情况: ")
	for k, v := range ipMap {
		values = append(values, v)
		fmt.Println("节点IP:", k, "分布数量:", v)
	}

	fmt.Println("标准差: ")

	standardVariance := StandardVariance(values)

	fmt.Println(standardVariance)
}
