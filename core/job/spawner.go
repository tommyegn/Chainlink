package job

import (
	"fmt"
	"github.com/pkg/errors"
	"sync"

	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/services/eth"
	"github.com/smartcontractkit/chainlink/core/store"
	"github.com/smartcontractkit/chainlink/core/store/models"
)

type Spawner struct {
	jobServiceFactories map[string]JobSpecToJobServiceFunc

	store          *store.Store
	logBroadcaster eth.LogBroadcaster
	chAdd          chan addEntry
	chRemove       chan models.ID
	chConnect      chan *models.Head
	chDisconnect   chan struct{}
	chStop         chan struct{}
	chDone         chan struct{}
}

type JobSpecToJobServiceFunc func(jobSpec JobSpec) JobService

type addEntry struct {
	jobID models.ID
	jobs  []ocr.Oracle
}

type JobSpec interface {
	JobID() *models.ID
	JobType() string
}

type JobService interface {
	Start()
	Stop()
}

func (js *Spawner) Start() error {
	go js.runLoop()

	var wg sync.WaitGroup
	err := js.store.JobsAsInterfaces(func(j JobSpec) bool {
		if j == nil {
			err := errors.New("received nil job")
			logger.Error(err)
			return true
		}
		job := *j

		wg.Add(1)
		go func() {
			defer wg.Done()

			err := js.AddJob(job)
			if err != nil {
				logger.Errorf("error adding %v job: %v", js.jobType, err)
			}
		}()
		return true
	})

	wg.Wait()

	return err
}

func (js *Spawner) Stop() {
	if js.disabled {
		logger.Warn("Spawner disabled: cannot stop")
		return
	}

	close(js.chStop)
	<-js.chDone
}

func (js *Spawner) runLoop() {
	defer close(js.chDone)

	jobMap := map[models.ID][]JobService{}

	for {
		select {
		case entry := <-js.chAdd:
			if _, ok := jobMap[entry.jobID]; ok {
				logger.Errorf("%v job '%s' has already been added", js.jobType, entry.jobID.String())
				continue
			}
			for _, job := range entry.jobs {
				job.Start()
			}
			jobMap[entry.jobID] = entry.jobs

		case jobID := <-js.chRemove:
			jobs, ok := jobMap[jobID]
			if !ok {
				logger.Debugf("%v job '%s' is missing", js.jobType, jobID.String())
				continue
			}
			for _, job := range jobs {
				job.Stop()
			}
			delete(jobMap, jobID)

		case <-js.chStop:
			for _, jobs := range jobMap {
				for _, job := range jobs {
					job.Stop()
				}
			}
			return
		}
	}
}

func (js *Spawner) AddJob(jobSpec JobSpec) error {
	if job.JobID() == nil {
		err := errors.New("Job Spawner received job with nil ID")
		logger.Error(err)
		js.store.UpsertErrorFor(job.ID, "Unable to add job - job has nil ID")
		return err
	}

	factory, exists := js.jobServiceFactories[job.JobType()]
	if !exists {
		return errors.Errorf("Job Spawner got unknown job type '%v'", job)
	}

	services := factory(jobSpec)

	if len(services) == 0 {
		return nil
	}

	js.chAdd <- addEntry{*job.ID, services}
	return nil
}

func (js *Spawner) RemoveJob(id *models.ID) {
	if id == nil {
		logger.Warn("nil job ID passed to Spawner#RemoveJob")
		return
	}
	js.chRemove <- *id
}

func (js *Spawner) RegisterJobType(jobType string, factory JobSpecToJobServiceFunc) {
	js.jobServiceFactories[jobType] = factory
}
