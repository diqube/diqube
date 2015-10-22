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
import org.diqube.function.IntermediaryResult;

/**
 * Function that creates a result of 0 or 1, depending on if a specific value was received.
 * 
 * The value everything is compared to is expected to be a constant parameter to the function.
 *
 * @author Bastian Gloeckle
 */
@Function(name = AnyLongFunction.NAME)
public class AnyLongFunction implements AggregationFunction<Long, IntermediaryResult<Long, Object, Object>, Long> {

  public static final String NAME = "any";

  private List<Long> constantParameters = new ArrayList<>();

  private int matched = 0;

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void addIntermediary(IntermediaryResult<Long, Object, Object> intermediary) {
    matched += intermediary.getLeft();
  }

  @Override
  public void removeIntermediary(IntermediaryResult<Long, Object, Object> intermediary) {
    matched -= intermediary.getLeft();
  }

  @Override
  public void addValues(org.diqube.function.AggregationFunction.ValueProvider<Long> valueProvider) {
    for (Long val : valueProvider.getValues())
      matched += val.equals(constantParameters.get(0)) ? 1 : 0;
  }

  @Override
  public IntermediaryResult<Long, Object, Object> calculateIntermediary() throws FunctionException {
    return new IntermediaryResult<Long, Object, Object>(calculate(), null, null, ColumnType.LONG);
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
    return ColumnType.LONG;
  }

  @Override
  public void provideConstantParameter(int idx, Long value) {
    while (constantParameters.size() <= idx)
      constantParameters.add(null);
    constantParameters.set(idx, value);
  }

  @Override
  public boolean needsActualValues() {
    return true;
  }

}
