package job

import (
	"math/big"

	"github.com/shopspring/decimal"
)

func toDecimal(input interface{}) (decimal.Decimal, error) {
	switch v := input.(type) {
	case string:
		return decimal.NewFromString(v), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float64, float32:
		return big.NewInt(int64(v)), nil
	case *big.Int:
		return decimal.NewFromBigInt(v, 0), nil
	default:
		return decimal.Decimal{}, errors.Errorf("type %T cannot be converted to decimal.Decimal", input)
	}
}
