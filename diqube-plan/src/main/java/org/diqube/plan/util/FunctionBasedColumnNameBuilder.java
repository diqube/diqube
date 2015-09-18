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
package org.diqube.plan.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.diqube.data.ColumnType;
import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.function.FunctionFactory;

import com.google.common.collect.Iterables;

/**
 * Calculates the name of the result column when executing a function on some input data.
 * 
 * <p>
 * Each function (aggregation or projection) that is executed on some set of data (constants or columns) creates a
 * column which will hold the result values of applying that function to the input data. This class can calculate the
 * name of this output column in a way that it is unique for the executed operation within the execution of one query.
 *
 * @author Bastian Gloeckle
 */
public class FunctionBasedColumnNameBuilder {
  private List<String> parameterNames = new ArrayList<>();

  private String functionName;

  private RepeatedColumnNameGenerator repeatedCols;

  private FunctionFactory functionFactory;

  private ConcurrentMap<String, List<List<Integer>>> exchangeableIndicesCache;

  /* package */ FunctionBasedColumnNameBuilder(RepeatedColumnNameGenerator repeatedCols,
      FunctionFactory functionFactory, ConcurrentMap<String, List<List<Integer>>> exchangeableIndicesCache) {
    this.repeatedCols = repeatedCols;
    this.functionFactory = functionFactory;
    this.exchangeableIndicesCache = exchangeableIndicesCache;
  }

  /**
   * Transforms all [*] occurrences in the string with [a], therefore the resulting column name represents the finally
   * built column and not the pattern that is used in a query.
   */
  public FunctionBasedColumnNameBuilder addParameterColumnName(String parameterColumnName) {
    if (parameterColumnName.contains(repeatedCols.allEntriesIdentifyingSubstr()))
      parameterNames.add("col%" + parameterColumnName.replace(repeatedCols.allEntriesIdentifyingSubstr(),
          repeatedCols.allEntriesManifestedSubstr()));
    else
      parameterNames.add("col%" + parameterColumnName);
    return this;
  }

  public FunctionBasedColumnNameBuilder addParameterLiteralString(String parameterLiteralString) {
    parameterNames.add("lits%" + parameterLiteralString);
    return this;
  }

  public FunctionBasedColumnNameBuilder addParameterLiteralLong(long parameterLiteralLong) {
    parameterNames.add("litl%" + parameterLiteralLong);
    return this;
  }

  public FunctionBasedColumnNameBuilder addParameterLiteralDouble(double parameterLiteralDouble) {
    parameterNames.add("litd%" + parameterLiteralDouble);
    return this;
  }

  public FunctionBasedColumnNameBuilder withFunctionName(String functionName) {
    this.functionName = functionName;
    return this;
  }

  public String build() {
    // Try to exchange parameters in a deterministic way. This is only done for projection functions.
    // We do this in order to not calculate the same result twice.
    //
    // For example: add(1, a) and add(a, 1) are two distinct columns which though have the exactly same values -> It
    // would be nice to calculate that result only once. We can achieve this by giving those two function requests the
    // same output column (see FunctionColumnInfoBuilder).
    //
    // In addition to calculating the same results only once, we facilitate caching of those columns - even if later
    // queries might specify the params of the function in a different order.
    if (parameterNames.size() > 1) {
      if (!exchangeableIndicesCache.containsKey(functionName)) {
        Collection<ColumnType> inputColTypes =
            functionFactory.getPossibleInputDataTypesForProjectionFunction(functionName);
        if (inputColTypes != null) {
          List<List<Set<Integer>>> exchangeableParamIdxDistinctList =
              inputColTypes.stream().map(inColType -> functionFactory.createProjectionFunction(functionName, inColType)
                  .exchangeableParameterIndices()).distinct().collect(Collectors.toList());
          if (exchangeableParamIdxDistinctList.size() == 1) {
            // all functions with the same functionName have the same exchangeable parameter indices. So we can exchange
            // them! Put them in the cache and process below.

            List<List<Integer>> exchangeableSortedIndices = Iterables.getOnlyElement(exchangeableParamIdxDistinctList)
                .stream().map(set -> set.stream().sorted().collect(Collectors.toList())).collect(Collectors.toList());

            exchangeableIndicesCache.put(functionName, exchangeableSortedIndices);
          } else
            exchangeableIndicesCache.put(functionName, new ArrayList<>()); // no exchangeable indices.
        } else
          exchangeableIndicesCache.put(functionName, new ArrayList<>()); // no exchangeable indices.
      }

      if (exchangeableIndicesCache.containsKey(functionName)) {
        List<List<Integer>> exchangeIdxList = exchangeableIndicesCache.get(functionName);
        for (List<Integer> exchangeIdxSortedList : exchangeIdxList) {

          List<String> paramValues = new ArrayList<>(exchangeIdxSortedList.size());
          exchangeIdxSortedList.forEach(idx -> paramValues.add(parameterNames.get(idx)));

          paramValues.sort(Comparator.naturalOrder());

          Iterator<Integer> idxIt = exchangeIdxSortedList.iterator();
          for (int i = 0; i < paramValues.size(); i++)
            parameterNames.set(idxIt.next(), paramValues.get(i));
        }
      }
    }

    StringBuilder sb = new StringBuilder();
    sb.append(functionName);
    sb.append("{");
    for (String paramName : parameterNames) {
      sb.append(paramName);
      sb.append(",");
    }
    sb.append("}");
    return sb.toString();
  }
}
