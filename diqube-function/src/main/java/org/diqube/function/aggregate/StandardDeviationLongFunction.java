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
 * Calculates the standard deviation of the population variance.
 * 
 * <p>
 * sqrt(var(x))
 *
 * @author Bastian Gloeckle
 */
@Function(name = StandardDeviationLongFunction.NAME)
public class StandardDeviationLongFunction implements AggregationFunction<Long, Double> {
  public static final String NAME = "sd";

  private VarLongFunction varianceFunction;

  public StandardDeviationLongFunction() {
    varianceFunction = new VarLongFunction();
  }

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void provideConstantParameter(int idx, Long value) {
    // noop
  }

  @Override
  public void addValues(ValueProvider<Long> valueProvider) {
    varianceFunction.addValues(valueProvider);
  }

  @Override
  public void addIntermediary(IntermediaryResultValueIterator intermediary) {
    varianceFunction.addIntermediary(intermediary);
  }

  @Override
  public void removeIntermediary(IntermediaryResultValueIterator intermediary) {
    varianceFunction.removeIntermediary(intermediary);
  }

  @Override
  public void populateIntermediary(IntermediaryResultValueSink res) throws FunctionException {
    varianceFunction.populateIntermediary(res);
  }

  @Override
  public Double calculate() throws FunctionException {
    double variance = varianceFunction.calculate();

    return Math.sqrt(variance);
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
  public boolean needsActualValues() {
    return true;
  }
}
