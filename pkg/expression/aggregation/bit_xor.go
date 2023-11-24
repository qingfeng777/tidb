// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregation

import (
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type bitXorFunction struct {
	aggFunction
}

func (bf *bitXorFunction) CreateContext(ctx sessionctx.Context) *AggEvaluateContext {
	evalCtx := bf.aggFunction.CreateContext(ctx)
	evalCtx.Value.SetUint64(0)
	return evalCtx
}

func (*bitXorFunction) ResetContext(ctx sessionctx.Context, evalCtx *AggEvaluateContext) {
	evalCtx.Ctx = ctx
	evalCtx.Value.SetUint64(0)
}

// Update implements Aggregation interface.
func (bf *bitXorFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error {
	a := bf.Args[0]
	value, err := a.Eval(evalCtx.Ctx, row)
	if err != nil {
		return err
	}
	if !value.IsNull() {
		if value.Kind() == types.KindUint64 {
			evalCtx.Value.SetUint64(evalCtx.Value.GetUint64() ^ value.GetUint64())
		} else {
			int64Value, err := value.ToInt64(sc.TypeCtx())
			if err != nil {
				return err
			}
			evalCtx.Value.SetUint64(evalCtx.Value.GetUint64() ^ uint64(int64Value))
		}
	}
	return nil
}

// GetResult implements Aggregation interface.
func (*bitXorFunction) GetResult(evalCtx *AggEvaluateContext) types.Datum {
	return evalCtx.Value
}

// GetPartialResult implements Aggregation interface.
func (bf *bitXorFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{bf.GetResult(evalCtx)}
}
