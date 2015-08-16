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

import org.diqube.data.ColumnType;
import org.diqube.function.AggregationFunction;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;
import org.diqube.function.IntermediaryResult;

/**
 * Aggregation function that calculates the sum of longs.
 *
 * @author Bastian Gloeckle
 */
@Function(name = SumDoubleFunction.NAME)
public class SumDoubleFunction
    implements AggregationFunction<Double, IntermediaryResult<Double, Object, Object>, Double> {

  public static final String NAME = "sum";

  private double sum = 0;

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void provideConstantParameter(int idx, Double value) {
    // noop.
  }

  @Override
  public void addIntermediary(IntermediaryResult<Double, Object, Object> intermediary) {
    sum += intermediary.getLeft();
  }

  @Override
  public void removeIntermediary(IntermediaryResult<Double, Object, Object> intermediary) {
    sum -= intermediary.getLeft();
  }

  @Override
  public void addValues(org.diqube.function.AggregationFunction.ValueProvider<Double> valueProvider) {
    for (Double d : valueProvider.getValues())
      sum += d;
  }

  @Override
  public IntermediaryResult<Double, Object, Object> calculateIntermediary() throws FunctionException {
    return new IntermediaryResult<Double, Object, Object>(sum, null, null, ColumnType.DOUBLE);
  }

  @Override
  public Double calculate() throws FunctionException {
    return sum;
  }

  @Override
  public ColumnType getOutputType() {
    return ColumnType.DOUBLE;
  }

  @Override
  public ColumnType getInputType() {
    return ColumnType.DOUBLE;
  }

}
