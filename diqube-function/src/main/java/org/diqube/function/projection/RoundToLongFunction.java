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
package org.diqube.function.projection;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.diqube.data.ColumnType;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;
import org.diqube.function.ProjectionFunction;

/**
 * Round a double value to Long.
 *
 * @author Bastian Gloeckle
 */
@Function(name = RoundToLongFunction.NAME)
public class RoundToLongFunction implements ProjectionFunction<Double, Long> {

  public static final String NAME = "round";

  private Double[] values;
  private Double constantValue;

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public Double[] createEmptyInputArray(int length) {
    return new Double[length];
  }

  @Override
  public void provideParameter(int parameterIdx, Double[] value) {
    constantValue = null;
    values = value;
  }

  @Override
  public void provideConstantParameter(int parameterIdx, Double value) {
    values = null;
    constantValue = value;
  }

  @Override
  public Long[] execute() throws FunctionException {
    if (constantValue != null)
      return new Long[] { Math.round(constantValue) };
    Long[] res = new Long[values.length];
    for (int i = 0; i < values.length; i++)
      res[i] = Math.round(values[i]);
    return res;
  }

  @Override
  public int numberOfParameters() {
    return 1;
  }

  @Override
  public List<Set<Integer>> exchangeableParameterIndices() {
    return new ArrayList<>();
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
