package roost

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

type encodeJob struct {
	name string
	recs []arrow.RecordBatch
}

// encodePool runs encode+upload jobs on N workers so Append doesn't block.
type encodePool struct {
	jobs chan encodeJob
	run  func(encodeJob) error
	wg   sync.WaitGroup
	mu   sync.Mutex
	err  error
}

func newEncodePool(n int, run func(encodeJob) error) *encodePool {
	p := &encodePool{jobs: make(chan encodeJob, n*2), run: run}
	for i := 0; i < n; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *encodePool) worker() {
	defer p.wg.Done()
	for j := range p.jobs {
		if err := p.run(j); err != nil {
			p.mu.Lock()
			if p.err == nil {
				p.err = err
			}
			p.mu.Unlock()
		}
	}
}

func (p *encodePool) submit(j encodeJob) { p.jobs <- j }

func (p *encodePool) close() error {
	close(p.jobs)
	p.wg.Wait()
	return p.err
}
