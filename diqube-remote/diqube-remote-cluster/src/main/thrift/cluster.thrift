//
// diqube: Distributed Query Base.
//
// Copyright (C) 2015 Bastian Gloeckle
//
// This file is part of diqube.
//
// diqube is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//

namespace java org.diqube.remote.cluster.thrift

include "${diqube.thrift.dependencies}/base.thrift"


union RCol {
  1: string colName
}

union RColOrValue {
  1: optional RCol column,
  2: optional base.RValue value
}

struct RIntermediateAggregationResult {
  1: i64 value1,
  2: optional i64 value2,
  3: optional i64 value3
}

// TODO if we transmit some values often when returning the results, we might think about sending a dict once that 
// contains the values once and then in each row we send only indices in that dict.
union RIntermediateResultColumn {
  1: optional base.RValue value,
  2: optional RIntermediateAggregationResult aggregationResult
}

struct RIntermediateResultRow {
  1: list<RIntermediateResultColumn> columns
}

struct RClusterResultTable {
  1: list<string> columnNames,
  2: map<i64, RIntermediateResultRow> rowsByRowId
}

struct RPartialResultTable {
  1: double completePercent,
  2: RClusterResultTable valueTable
}

// Each type corresponds to one ExecutablePlanStep.
enum RExecutionPlanStepType {
  // find row IDs of columns, comparing either to a Value or another column
  ROW_ID_EQ,
  ROW_ID_LT_EQ,
  ROW_ID_LT,
  ROW_ID_GT_EQ,
  ROW_ID_GT,
  // ROW_ID_BETWEEN, 
  ROW_ID_NOT,
  
  ROW_ID_AND,
  ROW_ID_OR,
  
  ROW_ID_SINK,
  
  // project columns
  PROJECT,
  
  // group by value of left column
  GROUP,
  GROUP_INTERMEDIATE_AGGREGATE,
  
  // limit and order result set
  ORDER,
  
  // return values encoded as column dict IDs
  RESOLVE_COLUMN_DICT_IDS,
  // resolve the column dict IDs to actual values
  RESOLVE_VALUES
}

struct RExecutionPlanStepDetailsOrderCol {
  1: RCol column,
  2: bool sortAscending
}

struct RExecutionPlanStepDetailsOrderLimit {
  1: i64 limit,
  2: optional i64 limitStart
}

struct RExecutionPlanStepDetailsOrder {
  1: list<RExecutionPlanStepDetailsOrderCol> orderColumns, 
  // either 'limit' or 'softLimit' is set, but not both at the same time. None may be set.
  2: optional RExecutionPlanStepDetailsOrderLimit limit,
  3: optional i64 softLimit
}

struct RExecutionPlanStepDetailsRowId {
  1: RCol column,
  2: optional RCol otherColumn,
  3: optional list<base.RValue> sortedValues
}

struct RExecutionPlanStepDetailsResolve {
  1: RCol column
}

struct RExecutionPlanStepDetailsGroup {
  1: list<RCol> groupByColumns
}

struct RExecutionPlanStepDetailsFunction {
  1: string functionNameLowerCase,
  2: RCol resultColumn,
  3: optional list<RColOrValue> functionArguments
}

enum RExecutionPlanStepDataType {
  // each one maps 1:1 to a sub-interface of GenericConsumer, it's the 'data type' that flows between two steps.
  COLUMN_BUILT,
  COLUMN_DICT_ID,
  COLUMN_VALUE,
  GROUP,
  GROUP_DELTA,
  GROUP_FINAL_AGG,
  GROUP_INTERMEDIARY_AGG,
  ORDERED_ROW_ID,
  ROW_ID
}

struct RExecutionPlanStep {
  1: i32 stepId,
  2: RExecutionPlanStepType type,
  3: map<i32, list<RExecutionPlanStepDataType>> provideDataForSteps,
  4: optional RExecutionPlanStepDetailsRowId detailsRowId,          // set on type == ROW_ID_EQ
  5: optional RExecutionPlanStepDetailsResolve detailsResolve,      // set on type == RESOLVE_COLUMN_DICT_IDS
  6: optional RExecutionPlanStepDetailsOrder detailsOrder,          // set on type == ORDER
  7: optional RExecutionPlanStepDetailsGroup detailsGroup,          // set on type == GROUP
  8: optional RExecutionPlanStepDetailsFunction detailsFunction,    // set on type == PROJECT, GROUP_INTERMEDIATE_AGGREGATE and GROUP_FINAL_AGGREGATE
}

struct RExecutionPlan {
  1: base.RUUID executionId,
  2: string table,
  3: list<RExecutionPlanStep> steps,
  4: base.RNodeAddress resultRef,
  5: bool providePartialValueTables
}

exception RExecutionException {
    1: string message;
}

service ClusterNodeService {
  base.RUUID executeOnAllShards(1:RExecutionPlan executionPlan),
  oneway void partialResultAvailable(1: base.RUUID nodeExecutionId, 2: RPartialResultTable partialValueTable)
  oneway void resultTableAvailable(1: base.RUUID nodeExecutionId, 2: RClusterResultTable valueTable) 
  oneway void executionException(1: base.RUUID nodeExecutionId, 2:RExecutionException executionException)
}