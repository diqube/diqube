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
import java.util.OptionalDouble;
import java.util.PriorityQueue;
import java.util.stream.Stream;

import org.diqube.data.column.ColumnType;
import org.diqube.function.AggregationFunction;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;
import org.diqube.function.aggregate.result.IntermediaryResultValueIterator;
import org.diqube.function.aggregate.result.IntermediaryResultValueSink;

/**
 * MAX function for double cols.
 *
 * @author Bastian Gloeckle
 */
@Function(name = MaxDoubleFunction.NAME)
public class MaxDoubleFunction implements AggregationFunction<Double, Double> {

  public static final String NAME = "max";

  private PriorityQueue<Double> maxQueue = new PriorityQueue<>(MaxDoubleFunction::max);
  private Map<Double, Integer> valueCount = new HashMap<>();

  public static int max(Double a, Double b) {
    return -(a.compareTo(b));
  }

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void addIntermediary(IntermediaryResultValueIterator intermediary) {
    double max = (Double) intermediary.next();
    int count = valueCount.merge(max, 1, (a, b) -> a + b);

    if (count == 1)
      maxQueue.add(max);
  }

  @Override
  public void removeIntermediary(IntermediaryResultValueIterator intermediary) {
    double max = (Double) intermediary.next();
    Integer count = valueCount.compute(max, (k, v) -> (v == null || v <= 1) ? null : (v - 1));

    if (count == null || count == 0)
      maxQueue.remove(max);
  }

  @Override
  public void addValues(ValueProvider<Double> valueProvider) {
    Double[] values = valueProvider.getValues();
    OptionalDouble max = Stream.of(values).mapToDouble(Double::doubleValue).max();
    // no need to maintain valueCount when addValues is called.
    if (max.isPresent()) {
      if (maxQueue.isEmpty() || max.getAsDouble() > maxQueue.peek())
        maxQueue.add(max.getAsDouble());
    }
  }

  @Override
  public void populateIntermediary(IntermediaryResultValueSink res) throws FunctionException {
    res.pushValue(calculate());
  }

  @Override
  public Double calculate() throws FunctionException {
    Double res = maxQueue.peek();
    if (res == null)
      return Double.MIN_VALUE;
    return res;
  }

  @Override
  public ColumnType getOutputType() {
    return ColumnType.DOUBLE;
  }

  @Override
  public ColumnType getInputType() {
    return ColumnType.DOUBLE;
  }

  @Override
  public void provideConstantParameter(int idx, Object value) throws FunctionException {
    // noop.
  }

  @Override
  public boolean needsActualValues() {
    return true;
  }

}
