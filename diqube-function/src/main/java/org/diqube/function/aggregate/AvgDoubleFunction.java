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

import java.util.stream.Stream;

import org.diqube.data.ColumnType;
import org.diqube.function.AggregationFunction;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;
import org.diqube.function.IntermediaryResult;

/**
 * Average function that takes Doubles as input.
 *
 * @author Bastian Gloeckle
 */
@Function(name = AvgDoubleFunction.NAME)
public class AvgDoubleFunction implements AggregationFunction<Double, IntermediaryResult<Double, Long, Object>, Double> {

  public static final String NAME = "avg";

  private double sum = .0;
  private long count = 0L;

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void addIntermediary(IntermediaryResult<Double, Long, Object> intermediary) {
    sum += intermediary.getLeft();
    count += intermediary.getMiddle();
  }

  @Override
  public void removeIntermediary(IntermediaryResult<Double, Long, Object> intermediary) {
    sum -= intermediary.getLeft();
    count -= intermediary.getMiddle();
  }

  @Override
  public void addValues(ValueProvider<Double> valueProvider) {
    Double[] values = valueProvider.getValues();

    sum += Stream.of(values).mapToDouble(Double::doubleValue).sum();
    count += values.length;
  }

  @Override
  public IntermediaryResult<Double, Long, Object> calculateIntermediary() throws FunctionException {
    return new IntermediaryResult<Double, Long, Object>(sum, count, null, ColumnType.DOUBLE);
  }

  @Override
  public Double calculate() throws FunctionException {
    return sum / count;
  }

  @Override
  public ColumnType getOutputType() {
    return ColumnType.LONG;
  }

  @Override
  public ColumnType getInputType() {
    return ColumnType.DOUBLE;
  }

}
