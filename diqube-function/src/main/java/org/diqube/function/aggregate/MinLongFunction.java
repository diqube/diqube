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
import org.diqube.function.aggregate.result.IntermediaryResultValueIterator;
import org.diqube.function.aggregate.result.IntermediaryResultValueSink;

/**
 * MIN function for long cols.
 *
 * @author Bastian Gloeckle
 */
@Function(name = MinLongFunction.NAME)
public class MinLongFunction implements AggregationFunction<Long, Long> {

  public static final String NAME = "min";

  private PriorityQueue<Long> minQueue = new PriorityQueue<>();
  private Map<Long, Integer> valueCount = new HashMap<>();

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void addIntermediary(IntermediaryResultValueIterator intermediary) {
    long max = (Long) intermediary.next();
    int count = valueCount.merge(max, 1, (a, b) -> a + b);

    if (count == 1)
      minQueue.add(max);
  }

  @Override
  public void removeIntermediary(IntermediaryResultValueIterator intermediary) {
    long max = (Long) intermediary.next();
    Integer count = valueCount.compute(max, (k, v) -> (v == null || v <= 1) ? null : (v - 1));

    if (count == null || count == 0)
      minQueue.remove(max);
  }

  @Override
  public void addValues(ValueProvider<Long> valueProvider) {
    Long[] values = valueProvider.getValues();
    OptionalLong min = Stream.of(values).mapToLong(Long::longValue).min();
    // no need to maintain valueCount when addValues is called.
    if (min.isPresent()) {
      if (minQueue.isEmpty() || min.getAsLong() < minQueue.peek())
        minQueue.add(min.getAsLong());
    }
  }

  @Override
  public void populateIntermediary(IntermediaryResultValueSink res) throws FunctionException {
    res.pushValue(calculate());
  }

  @Override
  public Long calculate() throws FunctionException {
    Long res = minQueue.peek();
    if (res == null)
      return Long.MAX_VALUE;
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
