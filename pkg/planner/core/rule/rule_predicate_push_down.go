// Copyright 2016 PingCAP, Inc.
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

package rule

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/constraint"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// PPDSolver stands for Predicate Push Down.
type PPDSolver struct{}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (*PPDSolver) Optimize(_ context.Context, lp base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	_, p := lp.PredicatePushDown(nil, opt)
	return p, planChanged, nil
}

func addSelection(p base.LogicalPlan, child base.LogicalPlan, conditions []expression.Expression, chIdx int, opt *optimizetrace.LogicalOptimizeOp) {
	if len(conditions) == 0 {
		p.Children()[chIdx] = child
		return
	}
	conditions = expression.PropagateConstant(p.SCtx().GetExprCtx(), conditions)
	// Return table dual when filter is constant false or null.
	dual := logicalop.Conds2TableDual(child, conditions)
	if dual != nil {
		p.Children()[chIdx] = dual
		logicalop.AppendTableDualTraceStep(child, dual, conditions, opt)
		return
	}

	conditions = constraint.DeleteTrueExprs(p, conditions)
	if len(conditions) == 0 {
		p.Children()[chIdx] = child
		return
	}
	selection := logicalop.LogicalSelection{Conditions: conditions}.Init(p.SCtx(), p.QueryBlockOffset())
	selection.SetChildren(child)
	p.Children()[chIdx] = selection
	logicalop.AppendAddSelectionTraceStep(p, child, selection, opt)
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*PPDSolver) Name() string {
	return "predicate_push_down"
}
