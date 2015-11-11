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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.diqube.diql.request.ExecutionRequest;
import org.diqube.diql.request.FunctionRequest;
import org.diqube.diql.request.ComparisonRequest.Leaf;
import org.diqube.diql.request.FunctionRequest.Type;
import org.diqube.util.ColumnOrValue;

/**
 * Builds a map containing {@link PlannerColumnInfo}s, for each created column (by projection or aggregation functions)
 * one.
 * 
 * <p>
 * Please note that the resulting Map will of course contain only /one/ {@link PlannerColumnInfo} per column, although
 * it might happen that there are multiple {@link FunctionRequest}s that are equal to each other - only the last will be
 * effectively included in the result map. This is fine, though, because the output column names are unique to the
 * operation the function executes - that means if there are two {@link FunctionRequest}s available that execute the
 * same logic, the output column name will be equal - in the end we do not want to execute the logic twice, though, but
 * only once, so we would need to remove one call anyway.
 *
 * @author Bastian Gloeckle
 */
public class PlannerColumnInfoBuilder {
  private ExecutionRequest executionRequest;

  public PlannerColumnInfoBuilder withExecutionRequest(ExecutionRequest executionRequest) {
    this.executionRequest = executionRequest;
    return this;
  }

  /**
   * See class comment of {@link PlannerColumnInfoBuilder}.
   */
  public Map<String, PlannerColumnInfo> build() {
    Map<String, PlannerColumnInfo> res = new HashMap<>();

    Set<String> columnNamesUsedInHaving = new HashSet<>();
    if (executionRequest.getHaving() != null) {
      Collection<Leaf> leafs = executionRequest.getHaving().findRecursivelyAllOfType(Leaf.class);
      for (Leaf leaf : leafs) {
        columnNamesUsedInHaving.add(leaf.getLeftColumnName());
        if (leaf.getRight().getType().equals(ColumnOrValue.Type.COLUMN))
          columnNamesUsedInHaving.add(leaf.getRight().getColumnName());
      }
    }

    List<PlannerColumnInfo> rowAggregationFunctions = new ArrayList<>();
    List<PlannerColumnInfo> colAggregationFunctions = new ArrayList<>();
    Deque<PlannerColumnInfo> literalOnlyFunctions = new LinkedList<>();

    // go through all FunctionRequest. If there are multiple requests with the same output col name, the latter will
    // overwrite the earlier -> We end up creating each column only once.
    for (FunctionRequest func : executionRequest.getProjectAndAggregate()) {
      PlannerColumnInfo info = new PlannerColumnInfo(func.getOutputColumn());
      info.setType(func.getType());
      info.setProvidedByFunctionRequest(func);
      info.setUsedInHaving(columnNamesUsedInHaving.contains(func.getOutputColumn()));

      if (func.getType().equals(Type.AGGREGATION_ROW)) {
        rowAggregationFunctions.add(info);
        info.setTransitivelyDependsOnLiteralsOnly(false);
      } else if (func.getType().equals(Type.AGGREGATION_COL)) {
        colAggregationFunctions.add(info);
        info.setTransitivelyDependsOnLiteralsOnly(false);
      }

      boolean foundColumn = false;
      for (ColumnOrValue param : func.getInputParameters()) {
        if (param.getType().equals(ColumnOrValue.Type.COLUMN)) {
          foundColumn = true;
          // add backward dependency
          info.getDependsOnColumns().add(param.getColumnName());
        }
      }

      if (func.getType().equals(Type.PROJECTION)) {
        info.setTransitivelyDependsOnLiteralsOnly(!foundColumn);
        if (!foundColumn)
          literalOnlyFunctions.add(info);
      }

      if (func.getType().equals(Type.REPEATED_PROJECTION)) {
        info.setTransitivelyDependsOnLiteralsOnly(false); // cannot be, as there is at least one repeated col as param.
        info.setArrayResult(true);
      } else
        info.setArrayResult(false);

      res.put(info.getName(), info);
    }

    for (PlannerColumnInfo colInfo : res.values()) {
      // create forward dependencies
      Iterator<String> otherColIt = colInfo.getDependsOnColumns().iterator();
      while (otherColIt.hasNext()) {
        String otherCol = otherColIt.next();
        if (!res.containsKey(otherCol))
          // we do not build that column in the ExecutionRequest - expect it to be a column in the TableShard directly.
          otherColIt.remove();
        else
          res.get(otherCol).getColumnsDependingOnThis().add(colInfo.getName());
      }
    }

    // resolve transitive row aggregation functions
    Deque<PlannerColumnInfo> transitiveRowAggregationFunctions = new LinkedList<>();
    // find children of 'top level row agg functions' - if the do not (transitively) depend on other agg functions, they
    // are NOT 'transitive row agg functions'!
    for (PlannerColumnInfo aggregationFunction : rowAggregationFunctions)
      for (String dependingColName : aggregationFunction.getColumnsDependingOnThis())
        transitiveRowAggregationFunctions.add(res.get(dependingColName));

    while (!transitiveRowAggregationFunctions.isEmpty()) {
      PlannerColumnInfo transitiveAggFunction = transitiveRowAggregationFunctions.poll();
      transitiveAggFunction.setTransitivelyDependsOnRowAggregation(true);
      for (String dependingColName : transitiveAggFunction.getColumnsDependingOnThis())
        transitiveRowAggregationFunctions.add(res.get(dependingColName));
    }

    // resolve transitive col aggregation functions
    Deque<PlannerColumnInfo> transitiveColAggregationFunctions = new LinkedList<>();
    // find children of 'top level row agg functions' - if the do not (transitively) depend on other agg functions, they
    // are NOT 'transitive row agg functions'!
    for (PlannerColumnInfo aggregationFunction : colAggregationFunctions)
      for (String dependingColName : aggregationFunction.getColumnsDependingOnThis())
        transitiveColAggregationFunctions.add(res.get(dependingColName));

    while (!transitiveColAggregationFunctions.isEmpty()) {
      PlannerColumnInfo transitiveAggFunction = transitiveColAggregationFunctions.poll();
      transitiveAggFunction.setTransitivelyDependsOnColAggregation(true);
      for (String dependingColName : transitiveAggFunction.getColumnsDependingOnThis())
        transitiveColAggregationFunctions.add(res.get(dependingColName));
    }

    // resolve transitive literal functions
    Map<String, Set<String>> functionDependsOn = new HashMap<>();
    for (PlannerColumnInfo colInfo : res.values())
      functionDependsOn.put(colInfo.getName(), new HashSet<String>(colInfo.getDependsOnColumns()));

    while (!literalOnlyFunctions.isEmpty()) {
      PlannerColumnInfo literalFunction = literalOnlyFunctions.poll();
      literalFunction.setTransitivelyDependsOnLiteralsOnly(true);

      for (String dependingColName : literalFunction.getColumnsDependingOnThis()) {
        functionDependsOn.get(dependingColName).remove(literalFunction.getName());

        if (functionDependsOn.get(dependingColName).isEmpty())
          literalOnlyFunctions.add(res.get(dependingColName));
      }
    }

    return res;
  }
}
