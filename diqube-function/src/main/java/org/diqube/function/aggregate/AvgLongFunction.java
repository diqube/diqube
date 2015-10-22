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

import org.diqube.data.column.ColumnType;
import org.diqube.function.AggregationFunction;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;
import org.diqube.function.IntermediaryResult;

/**
 * Average function that takes Longs as input.
 *
 * @author Bastian Gloeckle
 */
@Function(name = AvgLongFunction.NAME)
public class AvgLongFunction implements AggregationFunction<Long, IntermediaryResult<Double, Long, Object>, Double> {

  public static final String NAME = "avg";

  private double avg = 0.;
  private long count = 0L;

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void addIntermediary(IntermediaryResult<Double, Long, Object> intermediary) {
    double otherAvg = intermediary.getLeft();
    long otherCount = intermediary.getMiddle();

    if (otherCount == 0)
      return;

    avg =
        (avg * (((double) count) / (count + otherCount))) + (otherAvg * (((double) otherCount) / (count + otherCount)));
    count += otherCount;
  }

  @Override
  public void removeIntermediary(IntermediaryResult<Double, Long, Object> intermediary) {
    double otherAvg = intermediary.getLeft();
    long otherCount = intermediary.getMiddle();

    if (otherCount == 0)
      return;

    if (otherCount == count) {
      avg = 0.;
      count = 0;
      return;
    }

    avg =
        (avg * (count / (((double) count) - otherCount))) - (otherAvg * (((double) otherCount) / (count - otherCount)));
    count -= otherCount;
  }

  @Override
  public void addValues(ValueProvider<Long> valueProvider) {
    Long[] values = valueProvider.getValues();

    for (Long value : values) {
      avg = (avg * (((double) count) / (count + 1))) + (((double) value) / (count + 1));
      count++;
    }
  }

  @Override
  public IntermediaryResult<Double, Long, Object> calculateIntermediary() throws FunctionException {
    return new IntermediaryResult<Double, Long, Object>(avg, count, null, ColumnType.LONG);
  }

  @Override
  public Double calculate() throws FunctionException {
    return avg;
  }

  @Override
  public ColumnType getOutputType() {
    return ColumnType.DOUBLE;
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
