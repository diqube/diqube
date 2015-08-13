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
 * Divide a value by another one.
 *
 * @author Bastian Gloeckle
 */
@Function(name = DivDoubleFunction.NAME)
public class DivDoubleFunction implements ProjectionFunction<Double, Double> {

  public static final String NAME = "div";

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  private boolean[] isArray = new boolean[2];
  private Double[][] arrayValues = new Double[2][];
  private Double[] constantValues = new Double[2];

  @Override
  public void provideParameter(int parameterIdx, Double[] value) {
    arrayValues[parameterIdx] = value;
    isArray[parameterIdx] = true;
  }

  @Override
  public void provideConstantParameter(int parameterIdx, Double value) {
    constantValues[parameterIdx] = value;
    isArray[parameterIdx] = false;
  }

  @Override
  public Double[] execute() throws FunctionException {
    if (!isArray[0] && !isArray[1])
      return new Double[] { constantValues[0] / constantValues[1] };

    if (isArray[0] ^ isArray[1]) {
      Double[] array = (isArray[0]) ? arrayValues[0] : arrayValues[1];
      Double constant = (!isArray[0]) ? constantValues[0] : constantValues[1];
      Double[] res = new Double[array.length];
      for (int i = 0; i < res.length; i++)
        res[i] = array[i] / constant;
      return res;
    }

    if (arrayValues[0].length != arrayValues[1].length)
      throw new FunctionException("Arrays have to be of same length for " + NAME + "!");

    Double[] res = new Double[arrayValues[0].length];

    for (int i = 0; i < res.length; i++)
      res[i] = arrayValues[0][i] / arrayValues[1][i];

    return res;
  }

  @Override
  public int numberOfParameters() {
    return 2;
  }

  @Override
  public List<Set<Integer>> exchangeableParameterIndices() {
    return new ArrayList<>();
  }

  @Override
  public Double[] createEmptyInputArray(int length) {
    return new Double[length];
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
