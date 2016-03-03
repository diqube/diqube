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

import java.util.ArrayList;
import java.util.List;

import org.diqube.data.column.ColumnType;
import org.diqube.function.AggregationFunction;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;
import org.diqube.function.aggregate.result.IntermediaryResultValueIterator;
import org.diqube.function.aggregate.result.IntermediaryResultValueSink;
import org.diqube.util.DoubleUtil;

/**
 * Function that creates a result of 0 or 1, depending on if a specific value was received.
 * 
 * The value everything is compared to is expected to be a constant parameter to the function.
 *
 * @author Bastian Gloeckle
 */
@Function(name = AnyDoubleFunction.NAME)
public class AnyDoubleFunction implements AggregationFunction<Double, Long> {

  public static final String NAME = "any";

  private List<Double> constantParameters = new ArrayList<>();

  private int matched = 0;

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void addIntermediary(IntermediaryResultValueIterator intermediary) {
    matched += (Long) intermediary.next();
  }

  @Override
  public void removeIntermediary(IntermediaryResultValueIterator intermediary) {
    matched -= (Long) intermediary.next();
  }

  @Override
  public void addValues(ValueProvider<Double> valueProvider) {
    for (Double val : valueProvider.getValues())
      matched += DoubleUtil.equals(val, constantParameters.get(0)) ? 1 : 0;
  }

  @Override
  public void populateIntermediary(IntermediaryResultValueSink res) throws FunctionException {
    res.pushValue(calculate());
  }

  @Override
  public Long calculate() throws FunctionException {
    return (matched > 0) ? 1L : 0L;
  }

  @Override
  public ColumnType getOutputType() {
    return ColumnType.LONG;
  }

  @Override
  public ColumnType getInputType() {
    return ColumnType.DOUBLE;
  }

  @Override
  public void provideConstantParameter(int idx, Object value) throws FunctionException {
    if (!(value instanceof Double))
      throw new FunctionException(
          "Parameter to " + NAME + " function can be a DOUBLE only, because the column is a DOUBLE column.");

    while (constantParameters.size() <= idx)
      constantParameters.add(null);
    constantParameters.set(idx, (Double) value);
  }

  @Override
  public boolean needsActualValues() {
    return true;
  }

}
