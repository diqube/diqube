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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.diqube.data.ColumnType;
import org.diqube.function.AggregationFunction;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;
import org.diqube.function.IntermediaryResult;

/**
 * Aggregation function that concatenates all string values. A custom delimiter can be specified as constant param.
 *
 * @author Bastian Gloeckle
 */
@Function(name = ConcatGroupFunction.NAME)
public class ConcatGroupFunction
    implements AggregationFunction<String, IntermediaryResult<String, Object, Object>, String> {

  public static final String NAME = "concatgroup";

  public static final String DEFAULT_DELIMITER = ",";

  private List<String> values = new LinkedList<>();
  private String delim = DEFAULT_DELIMITER;

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void addIntermediary(IntermediaryResult<String, Object, Object> intermediary) {
    values.add(intermediary.getLeft());
  }

  @Override
  public void removeIntermediary(IntermediaryResult<String, Object, Object> intermediary) {
    values.remove(intermediary.getLeft()); // remove any value - if the same value is in the list twice, it does not
                                           // matter which one we remove.
  }

  @Override
  public void addValues(org.diqube.function.AggregationFunction.ValueProvider<String> valueProvider) {
    values.addAll(Arrays.asList(valueProvider.getValues()));
  }

  @Override
  public IntermediaryResult<String, Object, Object> calculateIntermediary() throws FunctionException {
    return new IntermediaryResult<String, Object, Object>(calculate(), null, null, ColumnType.STRING);
  }

  @Override
  public String calculate() throws FunctionException {
    return String.join(delim, values);
  }

  @Override
  public ColumnType getOutputType() {
    return ColumnType.STRING;
  }

  @Override
  public ColumnType getInputType() {
    return ColumnType.STRING;
  }

  @Override
  public void provideConstantParameter(int idx, String value) {
    if (idx == 0)
      delim = value;
  }

  @Override
  public boolean needsActualValues() {
    return true;
  }

}
