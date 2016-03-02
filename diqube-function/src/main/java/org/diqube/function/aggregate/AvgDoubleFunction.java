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
import org.diqube.function.aggregate.result.IntermediaryResultValueIterator;
import org.diqube.function.aggregate.result.IntermediaryResultValueSink;

/**
 * Average function that takes Doubles as input.
 *
 * @author Bastian Gloeckle
 */
@Function(name = AvgDoubleFunction.NAME)
public class AvgDoubleFunction implements AggregationFunction<Double, Double> {

  public static final String NAME = "avg";

  private double avg = .0;
  private long count = 0L;

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void addIntermediary(IntermediaryResultValueIterator intermediary) {
    double otherAvg = (Double) intermediary.next();
    long otherCount = (Long) intermediary.next();

    if (otherCount == 0)
      return;

    avg =
        (avg * (((double) count) / (count + otherCount))) + (otherAvg * (((double) otherCount) / (count + otherCount)));
    count += otherCount;
  }

  @Override
  public void removeIntermediary(IntermediaryResultValueIterator intermediary) {
    double otherAvg = (Double) intermediary.next();
    long otherCount = (Long) intermediary.next();

    if (otherCount == 0)
      return;

    if (otherCount == count) {
      avg = 0.;
      count = 0;
      return;
    }

    avg =
        (avg * (((double) count) / (count - otherCount))) - (otherAvg * (((double) otherCount) / (count - otherCount)));
    count -= otherCount;
  }

  @Override
  public void addValues(ValueProvider<Double> valueProvider) {
    Double[] values = valueProvider.getValues();

    for (Double value : values) {
      avg = (avg * (((double) count) / (count + 1))) + (value / (count + 1));
      count++;
    }
  }

  @Override
  public void populateIntermediary(IntermediaryResultValueSink res) throws FunctionException {
    res.pushValue(avg);
    res.pushValue(count);
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
