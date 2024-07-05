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

func (sq *sleepQueue) init(cap int) {
	sq.arr = make([]*sleepPtn, cap+1)
	sq.size = cap + 1
}

func (sq *sleepQueue) Push(s *sleepPtn) bool {
	if sq.IsFull() {
		return false
	}
	sq.arr[sq.head] = s
	sq.head = (sq.head + 1) % sq.size
	return true
}

func (sq *sleepQueue) Pop() *sleepPtn {
	if sq.IsEmpty() {
		return nil
	}
	s := sq.arr[sq.tail]
	sq.arr[sq.tail] = nil
	sq.tail = (sq.tail + 1) % sq.size
	return s
}

func (sq *sleepQueue) Peek() *sleepPtn {
	return sq.arr[sq.tail]
}

func (sq *sleepQueue) Values() []*sleepPtn {
	if sq.IsEmpty() {
		return nil
	}
	if sq.head > sq.tail {
		return sq.arr[sq.tail:sq.head]
	}
	ptn := make([]*sleepPtn, 0, sq.size-sq.tail+sq.head)
	ptn = append(ptn, sq.arr[sq.tail:]...)
	ptn = append(ptn, sq.arr[:sq.head]...)
	return ptn
}

func (sq *sleepQueue) IsFull() bool {
	return (sq.head+1)%sq.size == sq.tail
}

func (sq *sleepQueue) IsEmpty() bool {
	return sq.head == sq.tail
}

type sleepPtnPool struct {
	arr []*sleepPtn
}

func (sp *sleepPtnPool) GetBy(ptn []int) *sleepPtn {
	s := sp.get(len(ptn))
	s.ptn = append(s.ptn, ptn...)
	return s
}

func (sp *sleepPtnPool) Put(s *sleepPtn) {
	// The business needs to ensure that the channel of timer is empty
	s.ptn = s.ptn[:0]
	sp.arr = append(sp.arr, s)
}

func (sp *sleepPtnPool) get(cap int) *sleepPtn {
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
