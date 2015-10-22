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
package org.diqube.function.aggregate;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.stream.Stream;

import org.diqube.data.column.ColumnType;
import org.diqube.function.AggregationFunction;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;
import org.diqube.function.IntermediaryResult;

/**
 * MAX function for long cols.
 *
 * @author Bastian Gloeckle
 */
@Function(name = MaxLongFunction.NAME)
public class MaxLongFunction implements AggregationFunction<Long, IntermediaryResult<Long, Object, Object>, Long> {

  public static final String NAME = "max";

  private PriorityQueue<Long> maxQueue = new PriorityQueue<>(MaxLongFunction::max);
  private Map<Long, Integer> valueCount = new HashMap<>();

  public static int max(Long a, Long b) {
    return -(a.compareTo(b));
  }

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void addIntermediary(IntermediaryResult<Long, Object, Object> intermediary) {
    long max = intermediary.getLeft();
    int count = valueCount.merge(max, 1, (a, b) -> a + b);

    if (count == 1)
      maxQueue.add(max);
  }

  @Override
  public void removeIntermediary(IntermediaryResult<Long, Object, Object> intermediary) {
    long max = intermediary.getLeft();
    Integer count = valueCount.compute(max, (k, v) -> (v == null || v <= 1) ? null : (v - 1));

    if (count == null || count == 0)
      maxQueue.remove(max);
  }

  @Override
  public void addValues(ValueProvider<Long> valueProvider) {
    Long[] values = valueProvider.getValues();
    OptionalLong max = Stream.of(values).mapToLong(Long::longValue).max();
    // no need to maintain valueCount when addValues is called.
    if (max.isPresent()) {
      if (maxQueue.isEmpty() || max.getAsLong() > maxQueue.peek())
        maxQueue.add(max.getAsLong());
    }
  }

  @Override
  public IntermediaryResult<Long, Object, Object> calculateIntermediary() throws FunctionException {
    return new IntermediaryResult<Long, Object, Object>(calculate(), null, null, ColumnType.LONG);
  }

  @Override
  public Long calculate() throws FunctionException {
    Long res = maxQueue.peek();
    if (res == null)
      return Long.MIN_VALUE;
    return res;
  }

  @Override
  public ColumnType getOutputType() {
    return ColumnType.LONG;
  }

  @Override
  public ColumnType getInputType() {
    return ColumnType.LONG;
  }

  @Override
  public void provideConstantParameter(int idx, Long value) {
    // noop.
  }

  @Override
  public boolean needsActualValues() {
    return true;
  }

}
