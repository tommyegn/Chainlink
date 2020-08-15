package offchainreporting

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"

	"github.com/smartcontractkit/chainlink/core/job"
	"github.com/smartcontractkit/chainlink/core/store/models"
	ocrcontract "github.com/smartcontractkit/offchain-reporting-design/contract"
	ocr "github.com/smartcontractkit/offchain-reporting-design/prototype/offchainreporting"
	ocrconfig "github.com/smartcontractkit/offchain-reporting-design/prototype/offchainreporting/config"
)

const JobType = "offchainreporting"

type JobSpec struct {
	ID                 models.ID      `json:"id"`
	ContractAddress    common.Address `json:"contractAddress"`
	P2PNodeID          string         `json:"p2pNodeID"`
	ObservationTimeout time.Duration  `json:"observationTimeout"`
	ObservationSource  job.Fetcher    `json:"observationSource"`
}

func (spec JobSpec) JobID() *models.ID {
	return &spec.ID
}

func (spec JobSpec) JobType() string {
	return JobType
}

func RegisterJobTypes(jobSpawner *job.Spawner) {
	jobSpawner.RegisterJobType(
		JobType,
		func(jobSpec job.JobSpec) (job.JobService, error) {
			concreteSpec, ok := jobSpec.(JobSpec)
			if !ok {
				return nil, errors.Errorf("expected an offchainreporting.JobSpec, got %T", jobSpec)
			}

			var config *ocrconfig.Config
			var netEndpoint ocr.NetworkEndpoint
			var datasource ocr.DataSource
			var ulairi ocrcontract.Contract
			return ocr.NewOracle(config, netEndpoint, datasource, ulairi), nil
		},
	)
}

type dataSource job.Fetcher

func (ds dataSource) Fetch() (*big.Int, error) {
	val, err := job.Fetcher(ds).Fetch()
	if err != nil {
		return nil, err
	}
	asDecimal, ok := val.(decimal.Decimal)
	if !ok {
		return nil, errors.Errorf("dataSource received value of type %T, expected decimal.Decimal", val)
	}
	return asDecimal.BigInt(), nil
}
