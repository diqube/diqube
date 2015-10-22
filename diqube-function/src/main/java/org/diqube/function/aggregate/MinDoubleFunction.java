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
import org.diqube.function.IntermediaryResult;

/**
 * MIN function for long cols.
 *
 * @author Bastian Gloeckle
 */
@Function(name = MinDoubleFunction.NAME)
public class MinDoubleFunction
    implements AggregationFunction<Double, IntermediaryResult<Double, Object, Object>, Double> {

  public static final String NAME = "min";

  private PriorityQueue<Double> minQueue = new PriorityQueue<>();
  private Map<Double, Integer> valueCount = new HashMap<>();

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void addIntermediary(IntermediaryResult<Double, Object, Object> intermediary) {
    double min = intermediary.getLeft();
    int count = valueCount.merge(min, 1, (a, b) -> a + b);

    if (count == 1)
      minQueue.add(min);
  }

  @Override
  public void removeIntermediary(IntermediaryResult<Double, Object, Object> intermediary) {
    double min = intermediary.getLeft();
    Integer count = valueCount.compute(min, (k, v) -> (v == null || v <= 1) ? null : (v - 1));

    if (count == null || count == 0)
      minQueue.remove(min);
  }

  @Override
  public void addValues(ValueProvider<Double> valueProvider) {
    Double[] values = valueProvider.getValues();
    OptionalDouble min = Stream.of(values).mapToDouble(Double::doubleValue).min();
    // no need to maintain valueCount when addValues is called.
    if (min.isPresent()) {
      if (minQueue.isEmpty() || min.getAsDouble() < minQueue.peek())
        minQueue.add(min.getAsDouble());
    }
  }

  @Override
  public IntermediaryResult<Double, Object, Object> calculateIntermediary() throws FunctionException {
    return new IntermediaryResult<Double, Object, Object>(calculate(), null, null, ColumnType.DOUBLE);
  }

  @Override
  public Double calculate() throws FunctionException {
    Double res = minQueue.peek();
    if (res == null)
      return Double.MAX_VALUE;
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
  public void provideConstantParameter(int idx, Double value) {
    // noop.
  }

  @Override
  public boolean needsActualValues() {
    return true;
  }

}
