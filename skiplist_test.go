package dbengine

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func Test_InsertOneWhenListIsEmpty(t *testing.T) {
	s := newSkipList()
	s.upsert("hello", []byte("world"))

	n := s.search("hello")
	if string(n.value) != "world" {
		t.Errorf("got %s instead", string(n.value))
	}
}

func Test_InOrderInsert(t *testing.T) {
	s := newSkipList()

	keyList := makeRange(t, 10, false)
	for _, key := range keyList {
		s.upsert(strconv.Itoa(key), []byte(fmt.Sprintf("hello world %s", strconv.Itoa(key))))
	}

	resultKeys := getSkipListKeys(t, s)
	expected := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}

	if !compareStringSlices(t, resultKeys, expected) {
		t.Errorf("got %v instead", resultKeys)
	}
}

func Test_ReverseOrderInsert(t *testing.T) {
	s := newSkipList()

	keyList := makeRange(t, 10, false)
	for idx := len(keyList) - 1; idx >= 0; idx-- {
		key := keyList[idx]
		s.upsert(strconv.Itoa(key), []byte(fmt.Sprintf("hello world %s", strconv.Itoa(key))))
	}

	resultKeys := getSkipListKeys(t, s)
	expected := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}

	if !compareStringSlices(t, resultKeys, expected) {
		t.Errorf("got %v instead", resultKeys)
	}
}

func Test_RandomInsert(t *testing.T) {
	s := newSkipList()

	keyList := makeRange(t, 10, true)
	for _, key := range keyList {
		s.upsert(strconv.Itoa(key), []byte(fmt.Sprintf("hello world %s", strconv.Itoa(key))))
	}

	resultKeys := getSkipListKeys(t, s)
	expectedKeyList := makeRange(t, 10, false)
	expected := make([]string, 10)
	for idx, key := range expectedKeyList {
		expected[idx] = strconv.Itoa(key)
	}

	if !compareStringSlices(t, resultKeys, expected) {
		t.Errorf("expect: %s, got %v instead", expected, resultKeys)
	}
}

func Test_UpdateShouldUpdateExistingElement(t *testing.T) {
	s := newSkipList()

	keyList := makeRange(t, 10, true)
	for _, key := range keyList {
		s.upsert(strconv.Itoa(key), []byte(fmt.Sprintf("hello world %s", strconv.Itoa(key))))
	}

	oldNode := s.search("5")
	s.upsert("5", []byte("updated"))
	newNode := s.search("5")

	val := string(newNode.value)

	if newNode != oldNode {
		t.Error("new node got created instead of updating old one")
	}
	if val != "updated" {
		t.Errorf("got %s instead", val)
	}
}

func Test_SearchExist(t *testing.T) {
	s := newSkipList()

	keyList := makeRange(t, 10, true)
	for _, key := range keyList {
		s.upsert(strconv.Itoa(key), []byte(fmt.Sprintf("hello world %s", strconv.Itoa(key))))
	}

	for _, key := range keyList {
		expected := fmt.Sprintf("hello world %d", key)
		val := string(s.search(strconv.Itoa(key)).value)
		if val != expected {
			t.Errorf("expected: %s, got %s instead", expected, val)
		}
	}
}

func Test_SearchNonExistInSingleElementList(t *testing.T) {
	s := newSkipList()
	s.upsert("hello", []byte("world"))

	n := s.search("hello not exist")
	if n != nil {
		t.Error("Should be nil")
	}
}

func Test_SearchNonExistInMultiElementsList(t *testing.T) {
	s := newSkipList()

	keyList := makeRange(t, 10, true)
	for _, key := range keyList {
		s.upsert(strconv.Itoa(key), []byte(fmt.Sprintf("hello world %s", strconv.Itoa(key))))
	}

	n := s.search("hello")
	if n != nil {
		t.Error("Should be nil")
	}
}

func Test_skipListShouldTrackSize(t *testing.T) {
	s := newSkipList()

	keyList := makeRange(t, 10, true)
	for _, key := range keyList {
		s.upsert(strconv.Itoa(key), []byte(fmt.Sprintf("hello world %s", strconv.Itoa(key))))
	}

	res := s.size
	if res != 10 {
		t.Errorf("Size is incorrect - got: %d", res)
	}
}

func Benchmark_InsertInOrder(b *testing.B) {
	s := newSkipList()
	for i := 0; i < b.N; i++ {
		s.upsert(strconv.Itoa(i), []byte("hello world"))
	}
}

func Benchmark_InsertRandom(b *testing.B) {
	s := newSkipList()

	keyList := makeRange(b, b.N, true)

	for _, key := range keyList {
		s.upsert(strconv.Itoa(key), []byte("hello world"))
	}
}

func makeRange(b testing.TB, max int, shouldShuffle bool) []int {
	b.Helper()

	lst := make([]int, max, max)
	for i := range lst {
		lst[i] = i
	}

	if shouldShuffle {
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(lst), func(i, j int) { lst[i], lst[j] = lst[j], lst[i] })
	}

	return lst
}

func getSkipListKeys(t *testing.T, s *skipList) []string {
	t.Helper()

	keys := make([]string, 0)

	for n := s.head; n != nil; n = n.forwardNodeAtLevel[0] {
		if n != s.sentinel {
			keys = append(keys, n.key)
		}
	}
	return keys
}

func compareStringSlices(t *testing.T, first, second []string) bool {
	t.Helper()

	if len(first) != len(second) {
		return false
	}

	for idx, firstVal := range first {
		secondVal := second[idx]
		if firstVal != secondVal {
			return false
		}
	}
	return true
}
