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

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

import org.diqube.plan.exception.ValidationException;
import org.diqube.plan.request.ComparisonRequest.Leaf;
import org.diqube.plan.request.ExecutionRequest;
import org.diqube.plan.request.FunctionRequest;
import org.diqube.util.ColumnOrValue.Type;
import org.diqube.util.Pair;

/**
 * Validates an {@link ExecutionRequest}.
 *
 * @author Bastian Gloeckle
 */
public class ExecutionPlanValidator {
  public void validate(ExecutionRequest executionRequest, Map<String, PlannerColumnInfo> colInfos)
      throws ValidationException {

    validateWhere(executionRequest, colInfos);
    validateHaving(executionRequest, colInfos);

    noAggregationOnAggregation(colInfos);

    rowAggregationNeedsGroup(executionRequest, colInfos);

    havingNeedsGroupBy(executionRequest);

    validateLimit(executionRequest);

    anyResultColumn(executionRequest);

    orderByColumnsOnly(executionRequest, colInfos);

    validateGroupBy(executionRequest, colInfos);

    // TODO #23 validate if functions are used correctly (= correct number of params, correct types)

  }

  /**
   * Order By is not allowed when the parameters are effectively only literals.
   */
  private void orderByColumnsOnly(ExecutionRequest executionRequest, Map<String, PlannerColumnInfo> colInfos)
      throws ValidationException {
    if (executionRequest.getOrder() != null) {
      for (Pair<String, Boolean> orderPair : executionRequest.getOrder().getColumns()) {
        String colName = orderPair.getLeft();
        if (colInfos.get(colName) != null && colInfos.get(colName).isTransitivelyDependsOnLiteralsOnly())
          throw new ValidationException(
              "ORDER clause with function '" + colInfos.get(colName).getProvidedByFunctionRequest().getFunctionName()
                  + "' depending on literals only, please use columnar values.");
      }
    }
  }

  private void anyResultColumn(ExecutionRequest executionRequest) throws ValidationException {
    if (executionRequest.getResolveValues() == null || executionRequest.getResolveValues().size() == 0)
      throw new ValidationException("No result columns speicified.");
  }

  private void validateLimit(ExecutionRequest executionRequest) throws ValidationException {
    if (executionRequest.getOrder() != null && executionRequest.getOrder().getLimit() != null) {
      if (executionRequest.getOrder().getLimit() < 1)
        throw new ValidationException("LIMIT needs to be at least 1.");
      if (executionRequest.getOrder().getLimitStart() != null && executionRequest.getOrder().getLimitStart() < 0)
        throw new ValidationException("LIMIT START needs to be at least 0.");
    }
  }

  /**
   * A having clause is only valid if there is a group by clause.
   */
  private void havingNeedsGroupBy(ExecutionRequest executionRequest) throws ValidationException {
    if (executionRequest.getHaving() != null && executionRequest.getGroup() == null)
      // should never happen because of ANTLR grammar, but to be sure...
      throw new ValidationException("HAVING clause only supported when there is a GROUP BY.");
  }

  /**
   * When using eaggregation functions, there needs to be a Group By clause.
   */
  private void rowAggregationNeedsGroup(ExecutionRequest executionRequest, Map<String, PlannerColumnInfo> colInfos)
      throws ValidationException {
    long numberOfAggregationFunctions = colInfos.values().stream()
        .filter(colInfo -> colInfo.getType().equals(FunctionRequest.Type.AGGREGATION_ROW)).count();
    if (numberOfAggregationFunctions > 0 && executionRequest.getGroup() == null)
      throw new ValidationException("There are " + numberOfAggregationFunctions
          + " aggregation functions used, but there is no GROUP BY clause.");
  }

  /**
   * There must be no row aggregation function be applied on an already row aggregated column. The same is true for col
   * aggregated columns. In addition to that it is not valid to have a col aggreation based on a row aggregation (only
   * the other way round!).
   */
  private void noAggregationOnAggregation(Map<String, PlannerColumnInfo> colInfos) throws ValidationException {
    for (PlannerColumnInfo colInfo : colInfos.values()) {
      if (colInfo.getType().equals(FunctionRequest.Type.AGGREGATION_ROW)
          && colInfo.isTransitivelyDependsOnRowAggregation())
        throw new ValidationException(
            "Use of row aggregation function '" + colInfo.getProvidedByFunctionRequest().getFunctionName()
                + "' is based on the result of at least one other row aggregation function. This is invalid.");
      if (colInfo.getType().equals(FunctionRequest.Type.AGGREGATION_COL)
          && colInfo.isTransitivelyDependsOnColAggregation())
        throw new ValidationException(
            "Use of columns aggregation function '" + colInfo.getProvidedByFunctionRequest().getFunctionName()
                + "' is based on the result of at least one other column aggregation function. This is invalid.");
      if (colInfo.getType().equals(FunctionRequest.Type.AGGREGATION_COL)
          && colInfo.isTransitivelyDependsOnRowAggregation())
        throw new ValidationException(
            "Use of columns aggregation function '" + colInfo.getProvidedByFunctionRequest().getFunctionName()
                + "' is based on the result of at least one row aggregation function. This is invalid.");
    }
  }

  private void validateWhere(ExecutionRequest executionRequest, Map<String, PlannerColumnInfo> colInfos) {
    if (executionRequest.getWhere() != null) {
      Collection<Leaf> leafs = executionRequest.getWhere().findRecursivelyAllOfType(Leaf.class);
      Consumer<String> validateCol = colName -> {
        if (colInfos.containsKey(colName) // could be that there is no colInfo if it's no generated
                                          // column.
            && (colInfos.get(colName).isTransitivelyDependsOnRowAggregation()
                || colInfos.get(colName).getType().equals(FunctionRequest.Type.AGGREGATION_ROW)))
          // note: col aggregations are executed on the query remotes, therefore they are fine in WHERE.
          throw new ValidationException(
              "Function '" + colInfos.get(colName).getProvidedByFunctionRequest().getFunctionName()
                  + "' is in WHERE clause and either is a row aggregation function or relies on the "
                  + "result of a row aggregation function. Aggregation functions can only be used in a HAVING clause.");
      };

      for (Leaf leaf : leafs) {
        validateCol.accept(leaf.getLeftColumnName());
        if (leaf.getRight().getType().equals(Type.COLUMN))
          validateCol.accept(leaf.getRight().getColumnName());
      }
    }
  }

  private void validateHaving(ExecutionRequest executionRequest, Map<String, PlannerColumnInfo> colInfos) {
    if (executionRequest.getHaving() != null) {
      Collection<Leaf> leafs = executionRequest.getHaving().findRecursivelyAllOfType(Leaf.class);
      Consumer<String> validateCol = colName -> {
        if (!colInfos.containsKey(colName) // could be that there is no colInfo if it's no generated
                                           // column.
            || (!colInfos.get(colName).isTransitivelyDependsOnRowAggregation()
                && !colInfos.get(colName).getType().equals(FunctionRequest.Type.AGGREGATION_ROW)))
          // note: col aggregations are executed on the query remotes, therefore they need to be in WHERE.
          throw new ValidationException(
              "Function '" + colInfos.get(colName).getProvidedByFunctionRequest().getFunctionName()
                  + "' is in HAVING clause but it is not depending on the result of a row aggregation. For performance "
                  + "reasons, this restriction has to be used in a WHERE clause.");
      };

      for (Leaf leaf : leafs) {
        validateCol.accept(leaf.getLeftColumnName());
        if (leaf.getRight().getType().equals(Type.COLUMN))
          validateCol.accept(leaf.getRight().getColumnName());
      }
    }
  }

  private void validateGroupBy(ExecutionRequest executionRequest, Map<String, PlannerColumnInfo> colInfos) {
    if (executionRequest.getGroup() != null) {
      for (String groupByCol : executionRequest.getGroup().getGroupColumns()) {
        if (!colInfos.containsKey(groupByCol))
          continue;

        if (colInfos.get(groupByCol).isTransitivelyDependsOnRowAggregation()
            || colInfos.get(groupByCol).getType().equals(FunctionRequest.Type.AGGREGATION_ROW))
          // we can aggregate on col aggregation functions, as these are calculated on the query remotes!
          throw new ValidationException("Cannot group on row aggregation functions.");

        if (colInfos.get(groupByCol).isTransitivelyDependsOnLiteralsOnly())
          throw new ValidationException("Cannot group on projections that are based on constants only.");
      }
    }
  }
}
