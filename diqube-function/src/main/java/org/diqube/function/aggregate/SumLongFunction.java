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
@Function(name = SumLongFunction.NAME)
public class SumLongFunction implements AggregationFunction<Long, IntermediaryResult<Long, Object, Object>, Long> {

  public static final String NAME = "sum";

  private long sum = 0;

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void provideConstantParameter(int idx, Long value) {
    // noop.
  }

  @Override
  public void addIntermediary(IntermediaryResult<Long, Object, Object> intermediary) {
    sum += intermediary.getLeft();
  }

  @Override
  public void removeIntermediary(IntermediaryResult<Long, Object, Object> intermediary) {
    sum -= intermediary.getLeft();
  }

  @Override
  public void addValues(org.diqube.function.AggregationFunction.ValueProvider<Long> valueProvider) {
    for (Long l : valueProvider.getValues())
      sum += l;
  }

  @Override
  public IntermediaryResult<Long, Object, Object> calculateIntermediary() throws FunctionException {
    return new IntermediaryResult<Long, Object, Object>(sum, null, null, ColumnType.LONG);
  }

  @Override
  public Long calculate() throws FunctionException {
    return sum;
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
  public boolean needsActualValues() {
    return true;
  }

}
