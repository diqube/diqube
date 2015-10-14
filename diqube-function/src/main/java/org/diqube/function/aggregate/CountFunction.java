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
 * Count function.
 *
 * @author Bastian Gloeckle
 */
@Function(name = CountFunction.NAME)
public class CountFunction implements AggregationFunction<Object, IntermediaryResult<Long, Object, Object>, Long> {

  public static final String NAME = "count";

  private long curCount = 0;

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void addIntermediary(IntermediaryResult<Long, Object, Object> intermediary) {
    curCount += intermediary.getLeft();
  }

  @Override
  public void removeIntermediary(IntermediaryResult<Long, Object, Object> intermediary) {
    curCount -= intermediary.getLeft();
  }

  @Override
  public void addValues(ValueProvider<Object> valueProvider) {
    curCount += valueProvider.size();
  }

  @Override
  public IntermediaryResult<Long, Object, Object> calculateIntermediary() throws FunctionException {
    return new IntermediaryResult<Long, Object, Object>(curCount, null, null, null);
  }

  @Override
  public Long calculate() throws FunctionException {
    return curCount;
  }

  @Override
  public ColumnType getOutputType() {
    return ColumnType.LONG;
  }

  @Override
  public ColumnType getInputType() {
    // we do not expect a parameter, so input type is null.
    return null;
  }

  @Override
  public void provideConstantParameter(int idx, Object value) {
    // noop.
  }

  @Override
  public boolean needsActualValues() {
    return false;
  }

}
