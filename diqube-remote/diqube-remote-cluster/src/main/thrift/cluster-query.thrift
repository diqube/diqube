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

enum RColumnType {
  STRING,
  DOUBLE,
  LONG
}

union RIntermediateAggregationResultValue {
  1: optional base.RValue value,
  2: optional binary serialized
}

struct RIntermediateAggregationResult {
  1: optional RColumnType inputColumnType, // can be null in case no column is passed as input (e.g. count()).
  2: string outputColName,
  3: list<RIntermediateAggregationResultValue> values
}

struct ROldNewIntermediateAggregationResult {
  1: optional RIntermediateAggregationResult oldResult,
  2: optional RIntermediateAggregationResult newResult
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
  // project the values of array elements of each single row to a new value in that row.
  REPEATED_PROJECT,
  
  // group by value of left column
  GROUP,
  GROUP_INTERMEDIATE_AGGREGATE,
  
  // aggregate columns of single rows (aggregating on repeated fields)
  COLUMN_AGGREGATE,
  
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
  4: optional RExecutionPlanStepDetailsRowId detailsRowId,          // set on type == ROW_ID_EQ etc
  5: optional RExecutionPlanStepDetailsResolve detailsResolve,      // set on type == RESOLVE_COLUMN_DICT_IDS
  6: optional RExecutionPlanStepDetailsOrder detailsOrder,          // set on type == ORDER
  7: optional RExecutionPlanStepDetailsGroup detailsGroup,          // set on type == GROUP
  8: optional RExecutionPlanStepDetailsFunction detailsFunction,    // set on type == PROJECT, GROUP_INTERMEDIATE_AGGREGATE and COLUMN_AGGREGATE
}

struct RExecutionPlanFromFlattened {
  1: string tableName,
  2: string flattenBy,
  3: base.RUUID flattenId
}

union RExecutionPlanFrom {
  1: optional string plainTableName,
  2: optional RExecutionPlanFromFlattened flattened
}

struct RExecutionPlan {
  1: RExecutionPlanFrom fromSpec,
  2: list<RExecutionPlanStep> steps,
}


struct RClusterQueryStatistics {
  1: string nodeName,
  2: i64 startedUntilDoneMs,
  3: map<i32, i64> stepThreadActiveMs,
  4: i32 numberOfThreads,
  5: i32 numberOfTemporaryColumnShardsCreated,
  6: i32 numberOfTemporaryColumnShardsFromCache,
  7: map<string, i32> numberOfPageAccesses,
  8: map<string, i32> numberOfTemporaryPageAccesses,
  9: i32 numberOfPagesInTable,
  10: i32 numberOfTemporaryPages,
  11: map<string, i32> numberOfTemporaryVersionsPerColName
}


exception RExecutionException {
    1: string message;
}

service ClusterQueryService {
  oneway void executeOnAllLocalShards(
    1:RExecutionPlan executionPlan, 2: base.RUUID queryId, 3: base.RNodeAddress resultAddress),
  oneway void cancelExecution(1: base.RUUID queryUuid),

  // needs to be synchronous, see QueryResultHandler.  
  void groupIntermediateAggregationResultAvailable(
    1: base.RUUID queryId, 2:i64 groupId, 3:string colName, 4: ROldNewIntermediateAggregationResult result, 5:i16 percentDoneDelta),
    
  // needs to be synchronous, see QueryResultHandler.  
  void columnValueAvailable(1: base.RUUID queryId, 2:string colName, 3: map<i64, base.RValue> valuesByRowId, 4:i16 percentDoneDelta),
  
  oneway void executionDone(1: base.RUUID queryId),
  
  // needs to be synchronous, see QueryResultHandler.  
  void executionException(1: base.RUUID queryId, 2:RExecutionException executionException),
  
  oneway void queryStatistics(1: base.RUUID queryUuid, 2: RClusterQueryStatistics stats) 
}
