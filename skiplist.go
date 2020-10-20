package dbengine

import (
	"fmt"
	"math/rand"
	"time"
)

type skipList struct {
	head     *node
	height   int     // how many levels are there
	size     int     // how many nodes are there
	prob     float32 // a probability used to determine up to what level should a new node be created
	sentinel *node
}

type node struct {
	key                string
	value              []byte
	forwardNodeAtLevel map[int]*node // tracks the next node of this node at different levels
}

func newSkipList() *skipList {
	sentinel := newNode("", []byte{})
	return &skipList{
		head:     sentinel,
		height:   1,
		size:     0,
		prob:     0.25, // use hardcoded probability for now
		sentinel: sentinel,
	}
}

func newNode(key string, value []byte) *node {
	return &node{
		key:                key,
		value:              value,
		forwardNodeAtLevel: make(map[int]*node),
	}
}

func (s *skipList) randomLevel() int {
	lvl := 0
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	for {
		if r.Float32() <= s.prob {
			break
		}
		lvl++
	}
	return lvl
}

func (s *skipList) search(key string) *node {
	curLevel := s.height - 1
	curNode := s.head

	// empty skip list OR direct match
	if curNode == nil || curNode.key == key {
		return curNode
	}

	for {
		nextNode, found := curNode.forwardNodeAtLevel[curLevel]
		if !found {
			// if it's the last node
			if curLevel == 0 {
				return nil
			}
			// if there's no next node, scan down
			curLevel--
			continue
		}

		if nextNode.key >= key {
			if nextNode.key == key {
				return nextNode
			}
			if curLevel == 0 {
				return nil
			}
			// when next node's key is greater than key to search for
			// going down one level
			curLevel--
		} else {
			// scan forward
			curNode = nextNode
		}
	}
}

func (s *skipList) upsert(key string, value []byte) *node {
	curNode := s.head
	curLevel := s.height - 1
	newNode := newNode(key, value)
	// tracks the last node we search through at each level, since when we add the new node to those levels
	// the anchor nodes will be the one that connects to it
	updateAnchors := make([]*node, s.height, s.height)

	for {
		nextNode, found := curNode.forwardNodeAtLevel[curLevel]
		if !found {
			updateAnchors[curLevel] = curNode
			// if we are at the last node, and the new node so far has the "biggest" key
			// insert the new node at the end
			if curLevel == 0 {
				s.insertNewNode(newNode, updateAnchors)
				return newNode
			}
			// otherwise scan downward
			curLevel--
			continue
		}

		if nextNode.key >= key {
			if nextNode.key == key {
				nextNode.value = value
				return nextNode
			}

			updateAnchors[curLevel] = curNode

			// once we are at level 0, insert the new node
			if curLevel == 0 {
				s.insertNewNode(newNode, updateAnchors)
				return newNode
			}
			// otherwise scan downward
			curLevel--
		} else {
			curNode = nextNode
		}
	}
}

func (s *skipList) insertNewNode(newNode *node, updateAnchors []*node) {
	lvl := s.randomLevel()
	// if the generated level is greater than height, create new levels in between for update anchor
	if lvl >= s.height {
		// grow one level at a time
		lvl = s.height + 1
		for i := s.height; i <= lvl; i++ {
			updateAnchors = append(updateAnchors, s.head)
		}
		// update height
		s.height = lvl + 1
	}
	// add new node at each level from 0 to generated lvl
	for level, anchorNode := range updateAnchors {
		if level > lvl {
			break
		}
		oldNext := anchorNode.forwardNodeAtLevel[level]
		anchorNode.forwardNodeAtLevel[level] = newNode
		if oldNext != nil {
			newNode.forwardNodeAtLevel[level] = oldNext
		}
	}
	s.size++
}

func (s *skipList) prettyPrint() {
	for lvl := s.height; lvl >= 0; lvl-- {
		for curNode := s.head; curNode != nil; curNode = curNode.forwardNodeAtLevel[lvl] {
			if curNode != s.sentinel {
				fmt.Printf("%s - ", curNode.key)
			} else {
				fmt.Printf("sentinel - ")
			}
		}
		fmt.Println()
	}
}
