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

import java.util.List;
import java.util.Set;

import org.diqube.data.ColumnType;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;
import org.diqube.function.ProjectionFunction;

/**
 * Simple identity function
 *
 * @author Bastian Gloeckle
 */
@Function(name = IdDoubleFunction.NAME)
public class IdDoubleFunction implements ProjectionFunction<Double, Double> {

  public static final String NAME = "id";

  private Double[] param = null;

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
    if (parameterIdx != 0)
      return;
    param = value;
  }

  @Override
  public void provideConstantParameter(int parameterIdx, Double value) {
    if (parameterIdx != 0)
      return;
    param = new Double[] { value };
  }

  @Override
  public Double[] execute() throws FunctionException {
    return param;
  }

  @Override
  public int numberOfParameters() {
    return 1;
  }

  @Override
  public List<Set<Integer>> exchangeableParameterIndices() {
    return null;
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
