package redisq

import "time"

type sleepQueue struct {
	arr  []*sleepPtn
	size int
	head int
	tail int
}

type sleepPtn struct {
	timer *time.Timer
	ptn   []int
}

func newSleepQueue(cap int) *sleepQueue {
	return &sleepQueue{
		arr:  make([]*sleepPtn, cap+1),
		size: cap + 1,
	}
}

func (g *sleepQueue) Push(s *sleepPtn) bool {
	if g.IsFull() {
		return false
	}
	g.arr[g.head] = s
	g.head = (g.head + 1) % g.size
	return true
}

func (g *sleepQueue) Pop() *sleepPtn {
	if g.IsEmpty() {
		return nil
	}
	s := g.arr[g.tail]
	g.arr[g.tail] = nil
	g.tail = (g.tail + 1) % g.size
	return s
}

func (g *sleepQueue) Peek() *sleepPtn {
	return g.arr[g.tail]
}

func (g *sleepQueue) Values() []*sleepPtn {
	if g.IsEmpty() {
		return nil
	}
	if g.head > g.tail {
		return g.arr[g.tail:g.head]
	}
	ptn := make([]*sleepPtn, 0, g.size-g.tail+g.head)
	ptn = append(ptn, g.arr[g.tail:]...)
	ptn = append(ptn, g.arr[:g.head]...)
	return ptn
}

func (g *sleepQueue) IsFull() bool {
	return (g.head+1)%g.size == g.tail
}

func (g *sleepQueue) IsEmpty() bool {
	return g.head == g.tail
}

type sleepPtnPool struct {
	arr []*sleepPtn
}

func (sp *sleepPtnPool) Get(cap int) *sleepPtn {
	l := len(sp.arr)
	if l == 0 {
		return &sleepPtn{
			ptn: make([]int, 0, cap),
		}
	}
	s := sp.arr[l-1]
	sp.arr[l-1] = nil
	sp.arr = sp.arr[:l-1]
	return s
}

func (sp *sleepPtnPool) Put(s *sleepPtn) {
	// The business needs to ensure that the channel of timer is empty
	s.ptn = s.ptn[:0]
	sp.arr = append(sp.arr, s)
}
