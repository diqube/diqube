/**
 * diqube: Distributed Query Base.
 *
 * Copyright (C) 2015 Bastian Gloeckle
 *
 * This file is part of diqube.
 *
 * diqube is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.diqube.plan;

import java.util.List;
import java.util.stream.Collectors;

import org.diqube.context.AutoInstatiate;
import org.diqube.diql.request.ComparisonRequest;
import org.diqube.diql.request.ExecutionRequest;
import org.diqube.diql.request.FromRequest;
import org.diqube.diql.request.FunctionRequest;
import org.diqube.diql.request.GroupRequest;
import org.diqube.diql.request.OrderRequest;
import org.diqube.remote.base.thrift.RValue;
import org.diqube.remote.cluster.thrift.RCol;
import org.diqube.remote.cluster.thrift.RColOrValue;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.cluster.thrift.RExecutionPlanFrom;
import org.diqube.remote.cluster.thrift.RExecutionPlanFromFlattened;
import org.diqube.remote.cluster.thrift.RExecutionPlanStep;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepDetailsFunction;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepDetailsGroup;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepDetailsOrder;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepDetailsOrderCol;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepDetailsOrderLimit;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepDetailsResolve;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepDetailsRowId;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepType;
import org.diqube.util.ColumnOrValue;
import org.diqube.util.ColumnOrValue.Type;
import org.diqube.util.Pair;

/**
 * A factory that creates remote execution plan steps from {@link ExecutionRequest} objects etc.
 * 
 * <p>
 * Remote execution plan steps are data objects managed by Thrift which can be used to remotely call cluster nodes to
 * execute something.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class RemoteExecutionPlanFactory {

  public RExecutionPlanStep createGroupIntermediaryAggregateStep(FunctionRequest fnReq, int stepId) {
    return createFunctionStep(fnReq, stepId, RExecutionPlanStepType.GROUP_INTERMEDIATE_AGGREGATE);
  }

  public RExecutionPlanStep createColumnAggregateStep(FunctionRequest fnReq, int stepId) {
    return createFunctionStep(fnReq, stepId, RExecutionPlanStepType.COLUMN_AGGREGATE);
  }

  public RExecutionPlanStep createProjectStep(FunctionRequest fnReq, int stepId) {
    return createFunctionStep(fnReq, stepId, RExecutionPlanStepType.PROJECT);
  }

  public RExecutionPlanStep createRepeatedProjectStep(FunctionRequest fnReq, int stepId) {
    return createFunctionStep(fnReq, stepId, RExecutionPlanStepType.REPEATED_PROJECT);
  }

  private RExecutionPlanStep createFunctionStep(FunctionRequest fnReq, int stepId, RExecutionPlanStepType type) {
    RExecutionPlanStep functionStep = new RExecutionPlanStep();
    functionStep.setStepId(stepId);
    functionStep.setType(type);
    functionStep.setDetailsFunction(new RExecutionPlanStepDetailsFunction());
    functionStep.getDetailsFunction().setFunctionNameLowerCase(fnReq.getFunctionName());
    functionStep.getDetailsFunction().setResultColumn(new RCol(RCol._Fields.COL_NAME, fnReq.getOutputColumn()));
    for (ColumnOrValue param : fnReq.getInputParameters()) {
      if (param.getType().equals(ColumnOrValue.Type.COLUMN)) {
        functionStep.getDetailsFunction().addToFunctionArguments(
            new RColOrValue(RColOrValue._Fields.COLUMN, new RCol(RCol._Fields.COL_NAME, param.getColumnName())));
      } else {
        functionStep.getDetailsFunction()
            .addToFunctionArguments(new RColOrValue(RColOrValue._Fields.VALUE, parseRValue(param.getValue())));
      }
    }
    return functionStep;
  }

  private RValue parseRValue(Object value) {
    RValue res = new RValue();
    if (value instanceof String)
      res.setStrValue((String) value);
    else if (value instanceof Long)
      res.setLongValue((Long) value);
    else if (value instanceof Double)
      res.setDoubleValue((Double) value);
    return res;
  }

  public RExecutionPlanStep createRowIdComparison(ComparisonRequest.Leaf comparisonLeaf, int stepId,
      RExecutionPlanStepType type) {
    RExecutionPlanStep step = new RExecutionPlanStep();
    step.setStepId(stepId);
    step.setType(type);
    step.setDetailsRowId(new RExecutionPlanStepDetailsRowId());
    String leftColName = comparisonLeaf.getLeftColumnName();
    ColumnOrValue right = comparisonLeaf.getRight();

    step.getDetailsRowId().setColumn(new RCol(RCol._Fields.COL_NAME, leftColName));
    if (right.getType().equals(Type.COLUMN))
      step.getDetailsRowId().setOtherColumn(new RCol(RCol._Fields.COL_NAME, right.getColumnName()));
    else
      // TODO #22 support multiple equal values
      step.getDetailsRowId().addToSortedValues(parseRValue(right.getValue()));

    return step;
  }

  public RExecutionPlanStep createRowIdAnd(int stepId) {
    RExecutionPlanStep step = new RExecutionPlanStep();
    step.setStepId(stepId);
    step.setType(RExecutionPlanStepType.ROW_ID_AND);
    return step;
  }

  public RExecutionPlanStep createRowIdOr(int stepId) {
    RExecutionPlanStep step = new RExecutionPlanStep();
    step.setStepId(stepId);
    step.setType(RExecutionPlanStepType.ROW_ID_OR);
    return step;
  }

  public RExecutionPlanStep createRowIdNot(int stepId) {
    RExecutionPlanStep step = new RExecutionPlanStep();
    step.setStepId(stepId);
    step.setType(RExecutionPlanStepType.ROW_ID_NOT);
    return step;
  }

  public RExecutionPlanStep createRowIdSink(int stepId) {
    RExecutionPlanStep step = new RExecutionPlanStep();
    step.setStepId(stepId);
    step.setType(RExecutionPlanStepType.ROW_ID_SINK);
    return step;
  }

  public RExecutionPlanStep createGroup(GroupRequest groupRequest, int stepId) {
    RExecutionPlanStep step = new RExecutionPlanStep();
    step.setStepId(stepId);
    step.setType(RExecutionPlanStepType.GROUP);
    step.setDetailsGroup(new RExecutionPlanStepDetailsGroup());
    step.getDetailsGroup().setGroupByColumns(groupRequest.getGroupColumns().stream()
        .map(colName -> new RCol(RCol._Fields.COL_NAME, colName)).collect(Collectors.toList()));
    return step;
  }

  public RExecutionPlanStep createOrder(OrderRequest orderRequest, int stepId) {
    RExecutionPlanStep step = new RExecutionPlanStep();
    step.setStepId(stepId);
    step.setType(RExecutionPlanStepType.ORDER);
    step.setDetailsOrder(new RExecutionPlanStepDetailsOrder());

    for (Pair<String, Boolean> orderCol : orderRequest.getColumns()) {
      RExecutionPlanStepDetailsOrderCol remoteOrderCol = new RExecutionPlanStepDetailsOrderCol();
      remoteOrderCol.setColumn(new RCol(RCol._Fields.COL_NAME, orderCol.getLeft()));
      remoteOrderCol.setSortAscending(orderCol.getRight());
      step.getDetailsOrder().addToOrderColumns(remoteOrderCol);
    }

    if (orderRequest.getLimit() != null) {
      step.getDetailsOrder().setLimit(new RExecutionPlanStepDetailsOrderLimit());
      step.getDetailsOrder().getLimit().setLimit(orderRequest.getLimit());
      if (orderRequest.getLimitStart() != null) {
        step.getDetailsOrder().getLimit().setLimitStart(orderRequest.getLimitStart());
      }
    } else if (orderRequest.getSoftLimit() != null) {
      step.getDetailsOrder().setSoftLimit(orderRequest.getSoftLimit());
    }

    return step;
  }

  public RExecutionPlanStep createColumnDictId(String colName, int stepId) {
    RExecutionPlanStep step = new RExecutionPlanStep();
    step.setStepId(stepId);
    step.setType(RExecutionPlanStepType.RESOLVE_COLUMN_DICT_IDS);
    step.setDetailsResolve(new RExecutionPlanStepDetailsResolve());
    step.getDetailsResolve().setColumn(new RCol(RCol._Fields.COL_NAME, colName));

    return step;
  }

  public RExecutionPlanStep createResolveColumnDictId(String colName, int stepId) {
    // TODO #19 support resolving constants
    return createColumnDictId(colName, stepId);
  }

  public RExecutionPlanStep createResolveValues(int stepId) {
    RExecutionPlanStep step = new RExecutionPlanStep();
    step.setStepId(stepId);
    step.setType(RExecutionPlanStepType.RESOLVE_VALUES);
    return step;
  }

  public RExecutionPlan createExecutionPlan(List<RExecutionPlanStep> steps, FromRequest fromRequest) {
    RExecutionPlan res = new RExecutionPlan();
    res.setSteps(steps);
    RExecutionPlanFrom from = new RExecutionPlanFrom();
    res.setFromSpec(from);
    if (fromRequest.isFlattened()) {
      RExecutionPlanFromFlattened flattened = new RExecutionPlanFromFlattened();
      from.setFlattened(flattened);

      flattened.setTableName(fromRequest.getTable());
      flattened.setFlattenBy(fromRequest.getFlattenByField());
      // Cannot be called yet: flattened.setFlattenId(). This will be filled in automatically by the
      // ExecuteRemotePlanOnShardsStep.
    } else {
      from.setPlainTableName(fromRequest.getTable());
    }
    return res;
  }
}
