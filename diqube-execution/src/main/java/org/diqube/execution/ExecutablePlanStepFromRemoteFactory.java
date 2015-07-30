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
package org.diqube.execution;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.colshard.ColumnShardFactory;
import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.execution.steps.ColumnAggregationStep;
import org.diqube.execution.steps.GroupIntermediaryAggregationStep;
import org.diqube.execution.steps.GroupStep;
import org.diqube.execution.steps.OrderStep;
import org.diqube.execution.steps.ProjectStep;
import org.diqube.execution.steps.RepeatedProjectStep;
import org.diqube.execution.steps.ResolveColumnDictIdsStep;
import org.diqube.execution.steps.ResolveValuesStep;
import org.diqube.execution.steps.RowIdAndStep;
import org.diqube.execution.steps.RowIdEqualsStep;
import org.diqube.execution.steps.RowIdInequalStep;
import org.diqube.execution.steps.RowIdInequalStep.RowIdComparator;
import org.diqube.execution.steps.RowIdNotStep;
import org.diqube.execution.steps.RowIdOrStep;
import org.diqube.execution.steps.RowIdSinkStep;
import org.diqube.execution.util.ColumnPatternUtil;
import org.diqube.function.FunctionFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.remote.base.thrift.RValue;
import org.diqube.remote.cluster.thrift.RColOrValue;
import org.diqube.remote.cluster.thrift.RExecutionPlanStep;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepDetailsOrderCol;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepDetailsRowId;
import org.diqube.util.ColumnOrValue;
import org.diqube.util.Pair;

/**
 * Factory for {@link ExecutablePlanStep}s from {@link RExecutionPlanStep}s.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ExecutablePlanStepFromRemoteFactory {

  @Inject
  private FunctionFactory functionFactory;

  @Inject
  private ColumnShardBuilderFactory columnShardBuilderManagerFactory;

  @Inject
  private ColumnShardFactory columnShardFactory;

  @Inject
  private ColumnPatternUtil columnPatternUtil;

  @Inject
  private RepeatedColumnNameGenerator repeatedColNameGen;

  /**
   * Creates an {@link ExecutablePlanStep} for the given {@link RExecutionPlanStep}. The resulting step will not be
   * data-wired.
   */
  public ExecutablePlanStep createExecutableStep(ExecutionEnvironment defaultEnv, RExecutionPlanStep remoteStep) {
    switch (remoteStep.getType()) {
    case ROW_ID_EQ:
      return createRowIdEq(defaultEnv, remoteStep);
    case ROW_ID_GT_EQ:
      return createRowIdInequal(defaultEnv, remoteStep, new RowIdInequalStep.GtEqRowIdComparator());
    case ROW_ID_GT:
      return createRowIdInequal(defaultEnv, remoteStep, new RowIdInequalStep.GtRowIdComparator());
    case ROW_ID_LT_EQ:
      return createRowIdInequal(defaultEnv, remoteStep, new RowIdInequalStep.LtEqRowIdComparator());
    case ROW_ID_LT:
      return createRowIdInequal(defaultEnv, remoteStep, new RowIdInequalStep.LtRowIdComparator());
    case ROW_ID_AND:
      return createRowIdAnd(defaultEnv, remoteStep);
    case ROW_ID_OR:
      return createRowIdOr(defaultEnv, remoteStep);
    case ROW_ID_NOT:
      return createRowIdNot(defaultEnv, remoteStep);
    case ROW_ID_SINK:
      return createRowIdSink(defaultEnv, remoteStep);
    case ORDER:
      return createOrder(defaultEnv, remoteStep);
    case GROUP:
      return createGroup(defaultEnv, remoteStep);
    case GROUP_INTERMEDIATE_AGGREGATE:
      return createGroupIntermediaryAggregation(defaultEnv, remoteStep);
    case COLUMN_AGGREGATE:
      return createColumnAggregation(defaultEnv, remoteStep);
    case PROJECT:
      return createProject(defaultEnv, remoteStep);
    case REPEATED_PROJECT:
      return createRepeatedProject(defaultEnv, remoteStep);
    case RESOLVE_COLUMN_DICT_IDS:
      return createResolveColumnDictIds(defaultEnv, remoteStep);
    case RESOLVE_VALUES:
      return createResolveValues(remoteStep);
    }
    throw new ExecutablePlanBuildException(
        "Could not create executable step for remote step '" + remoteStep.toString() + "'");
  }

  private ExecutablePlanStep createRowIdInequal(ExecutionEnvironment defaultEnv, RExecutionPlanStep remoteStep,
      RowIdComparator comparator) {
    RExecutionPlanStepDetailsRowId details = remoteStep.getDetailsRowId();
    String colName = details.getColumn().getColName();

    if (details.isSetOtherColumn()) {
      // Comapre col vs. col
      String otherColName = details.getOtherColumn().getColName();
      return new RowIdInequalStep(remoteStep.getStepId(), defaultEnv, colName, otherColName, comparator, true);
    } else {
      // Compare col vs. constant
      Object value;

      if (details.getSortedValues().size() != 1)
        throw new ExecutablePlanBuildException("There can be only one value in a >=/>/<=/< comparison.");

      RValue remoteValue = details.getSortedValues().get(0);

      if (remoteValue.isSetLongValue())
        value = remoteValue.getLongValue();
      else if (remoteValue.isSetStrValue())
        value = remoteValue.getStrValue();
      else
        value = remoteValue.getDoubleValue();

      return new RowIdInequalStep(remoteStep.getStepId(), defaultEnv, colName, value, comparator);
    }
  }

  private ExecutablePlanStep createRowIdEq(ExecutionEnvironment defaultEnv, RExecutionPlanStep remoteStep) {
    RExecutionPlanStepDetailsRowId details = remoteStep.getDetailsRowId();
    String colName = details.getColumn().getColName();

    if (details.isSetOtherColumn()) {
      // Comapre col vs. col
      String otherColName = details.getOtherColumn().getColName();
      return new RowIdEqualsStep(remoteStep.getStepId(), defaultEnv, colName, otherColName);
    } else {
      // Compare col vs. constants
      Object[] sortedValues;

      if (details.getSortedValues().stream().anyMatch(v -> v.isSetLongValue())) {
        if (!details.getSortedValues().stream().allMatch(v -> v.isSetLongValue()))
          throw new ExecutablePlanBuildException("Values compared to column " + colName + " are not of the same type.");

        sortedValues = details.getSortedValues().stream().map(v -> v.getLongValue()).toArray(l -> new Long[l]);
      } else if (details.getSortedValues().stream().anyMatch(v -> v.isSetStrValue())) {
        if (!details.getSortedValues().stream().allMatch(v -> v.isSetStrValue()))
          throw new ExecutablePlanBuildException("Values compared to column " + colName + " are not of the same type.");

        sortedValues = details.getSortedValues().stream().map(v -> v.getStrValue()).toArray(l -> new String[l]);
      } else {
        if (!details.getSortedValues().stream().allMatch(v -> v.isSetDoubleValue()))
          throw new ExecutablePlanBuildException(
              "Values compared to column " + colName + " are not of the same or a valid type.");

        sortedValues = details.getSortedValues().stream().map(v -> v.getDoubleValue()).toArray(l -> new Double[l]);
      }

      return new RowIdEqualsStep(remoteStep.getStepId(), defaultEnv, colName, sortedValues);
    }
  }

  private ExecutablePlanStep createRowIdOr(ExecutionEnvironment defaultEnv, RExecutionPlanStep remoteStep) {
    return new RowIdOrStep(remoteStep.getStepId());
  }

  private ExecutablePlanStep createRowIdAnd(ExecutionEnvironment defaultEnv, RExecutionPlanStep remoteStep) {
    return new RowIdAndStep(remoteStep.getStepId());
  }

  private ExecutablePlanStep createRowIdNot(ExecutionEnvironment defaultEnv, RExecutionPlanStep remoteStep) {
    return new RowIdNotStep(remoteStep.getStepId(), defaultEnv);
  }

  private ExecutablePlanStep createRowIdSink(ExecutionEnvironment defaultEnv, RExecutionPlanStep remoteStep) {
    return new RowIdSinkStep(remoteStep.getStepId(), defaultEnv);
  }

  private ExecutablePlanStep createResolveColumnDictIds(ExecutionEnvironment defaultEnv,
      RExecutionPlanStep remoteStep) {
    String colName = remoteStep.getDetailsResolve().getColumn().getColName();
    return new ResolveColumnDictIdsStep(remoteStep.getStepId(), defaultEnv, colName);
  }

  private ExecutablePlanStep createResolveValues(RExecutionPlanStep remoteStep) {
    return new ResolveValuesStep(remoteStep.getStepId());
  }

  private ExecutablePlanStep createOrder(ExecutionEnvironment defaultEnv, RExecutionPlanStep remoteStep) {
    List<Pair<String, Boolean>> orderColumns = remoteStep.getDetailsOrder().getOrderColumns().stream()
        .map(new Function<RExecutionPlanStepDetailsOrderCol, Pair<String, Boolean>>() {
          @Override
          public Pair<String, Boolean> apply(RExecutionPlanStepDetailsOrderCol t) {
            return new Pair<>(t.getColumn().getColName(), t.isSortAscending());
          }
        }).collect(Collectors.toList());

    Long limit = null;
    Long limitStart = null;

    if (remoteStep.getDetailsOrder().isSetLimit()) {
      limit = remoteStep.getDetailsOrder().getLimit().getLimit();
      if (remoteStep.getDetailsOrder().getLimit().isSetLimitStart())
        limitStart = remoteStep.getDetailsOrder().getLimit().getLimitStart();
    }

    return new OrderStep(remoteStep.getStepId(), defaultEnv, orderColumns, limit, limitStart,
        (remoteStep.getDetailsOrder().isSetSoftLimit()) ? remoteStep.getDetailsOrder().getSoftLimit() : null);
  }

  private ExecutablePlanStep createGroup(ExecutionEnvironment defaultEnv, RExecutionPlanStep remoteStep) {
    List<String> colsToGroupBy = remoteStep.getDetailsGroup().getGroupByColumns().stream().map(col -> col.getColName())
        .collect(Collectors.toList());

    return new GroupStep(remoteStep.getStepId(), defaultEnv, colsToGroupBy);
  }

  private ExecutablePlanStep createProject(ExecutionEnvironment defaultEnv, RExecutionPlanStep remoteStep) {
    String functionName = remoteStep.getDetailsFunction().getFunctionNameLowerCase();
    String outputColName = remoteStep.getDetailsFunction().getResultColumn().getColName();

    ColumnOrValue[] functionParameters = new ColumnOrValue[remoteStep.getDetailsFunction().getFunctionArgumentsSize()];
    for (int i = 0; i < functionParameters.length; i++) {
      RColOrValue remoteArg = remoteStep.getDetailsFunction().getFunctionArguments().get(i);

      ColumnOrValue.Type colType = remoteArg.isSetColumn() ? ColumnOrValue.Type.COLUMN : ColumnOrValue.Type.LITERAL;
      Object value = null;
      if (colType.equals(ColumnOrValue.Type.COLUMN))
        value = remoteArg.getColumn().getColName();
      else {
        if (remoteArg.getValue().isSetStrValue())
          value = remoteArg.getValue().getStrValue();
        else if (remoteArg.getValue().isSetLongValue())
          value = remoteArg.getValue().getLongValue();
        else if (remoteArg.getValue().isSetDoubleValue())
          value = remoteArg.getValue().getDoubleValue();
      }

      ColumnOrValue newArg = new ColumnOrValue(colType, value);

      functionParameters[i] = newArg;
    }

    return new ProjectStep(remoteStep.getStepId(), defaultEnv, functionFactory, functionName, functionParameters,
        outputColName, columnShardBuilderManagerFactory, columnShardFactory, null /* no column versions on remotes. */);
  }

  private ExecutablePlanStep createRepeatedProject(ExecutionEnvironment defaultEnv, RExecutionPlanStep remoteStep) {
    String functionName = remoteStep.getDetailsFunction().getFunctionNameLowerCase();
    String outputColName = remoteStep.getDetailsFunction().getResultColumn().getColName();

    ColumnOrValue[] functionParameters = new ColumnOrValue[remoteStep.getDetailsFunction().getFunctionArgumentsSize()];
    for (int i = 0; i < functionParameters.length; i++) {
      RColOrValue remoteArg = remoteStep.getDetailsFunction().getFunctionArguments().get(i);

      ColumnOrValue.Type colType = remoteArg.isSetColumn() ? ColumnOrValue.Type.COLUMN : ColumnOrValue.Type.LITERAL;
      Object value = null;
      if (colType.equals(ColumnOrValue.Type.COLUMN))
        value = remoteArg.getColumn().getColName();
      else {
        if (remoteArg.getValue().isSetStrValue())
          value = remoteArg.getValue().getStrValue();
        else if (remoteArg.getValue().isSetLongValue())
          value = remoteArg.getValue().getLongValue();
        else if (remoteArg.getValue().isSetDoubleValue())
          value = remoteArg.getValue().getDoubleValue();
      }

      ColumnOrValue newArg = new ColumnOrValue(colType, value);

      functionParameters[i] = newArg;
    }

    return new RepeatedProjectStep(remoteStep.getStepId(), defaultEnv, functionFactory,
        columnShardBuilderManagerFactory, repeatedColNameGen, columnPatternUtil, functionName, functionParameters,
        outputColName);
  }

  private ExecutablePlanStep createGroupIntermediaryAggregation(ExecutionEnvironment defaultEnv,
      RExecutionPlanStep remoteStep) {
    String functionName = remoteStep.getDetailsFunction().getFunctionNameLowerCase();
    String outputColName = remoteStep.getDetailsFunction().getResultColumn().getColName();

    String inputColName = null;
    if (remoteStep.getDetailsFunction().getFunctionArgumentsSize() > 0) {
      inputColName = remoteStep.getDetailsFunction().getFunctionArguments().get(0).getColumn().getColName();
    }

    return new GroupIntermediaryAggregationStep(remoteStep.getStepId(), defaultEnv, functionFactory, functionName,
        outputColName, inputColName);
  }

  private ExecutablePlanStep createColumnAggregation(ExecutionEnvironment defaultEnv, RExecutionPlanStep remoteStep) {
    String functionName = remoteStep.getDetailsFunction().getFunctionNameLowerCase();
    String outputColName = remoteStep.getDetailsFunction().getResultColumn().getColName();

    String inputColName = null;
    inputColName = remoteStep.getDetailsFunction().getFunctionArguments().get(0).getColumn().getColName();

    return new ColumnAggregationStep(remoteStep.getStepId(), defaultEnv, columnPatternUtil,
        columnShardBuilderManagerFactory, functionFactory, functionName, outputColName, inputColName);
  }

}
