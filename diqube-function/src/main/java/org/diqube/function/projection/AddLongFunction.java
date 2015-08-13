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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.diqube.data.ColumnType;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;
import org.diqube.function.ProjectionFunction;

/**
 * Calculate the sum of two values.
 *
 * @author Bastian Gloeckle
 */
@Function(name = AddLongFunction.NAME)
public class AddLongFunction implements ProjectionFunction<Long, Long> {

  public static final String NAME = "add";

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  private boolean[] isArray = new boolean[2];
  private Long[][] arrayValues = new Long[2][];
  private Long[] constantValues = new Long[2];

  @Override
  public void provideParameter(int parameterIdx, Long[] value) {
    arrayValues[parameterIdx] = value;
    isArray[parameterIdx] = true;
  }

  @Override
  public void provideConstantParameter(int parameterIdx, Long value) {
    constantValues[parameterIdx] = value;
    isArray[parameterIdx] = false;
  }

  @Override
  public Long[] execute() throws FunctionException {
    if (!isArray[0] && !isArray[1])
      return new Long[] { constantValues[0] + constantValues[1] };

    if (isArray[0] ^ isArray[1]) {
      Long[] array = (isArray[0]) ? arrayValues[0] : arrayValues[1];
      Long constant = (!isArray[0]) ? constantValues[0] : constantValues[1];
      Long[] res = new Long[array.length];
      for (int i = 0; i < res.length; i++)
        res[i] = array[i] + constant;
      return res;
    }

    if (arrayValues[0].length != arrayValues[1].length)
      throw new FunctionException("Arrays have to be of same length for " + NAME + "!");

    Long[] res = new Long[arrayValues[0].length];

    for (int i = 0; i < res.length; i++)
      res[i] = arrayValues[0][i] + arrayValues[1][i];

    return res;
  }

  @Override
  public int numberOfParameters() {
    return 2;
  }

  @Override
  public List<Set<Integer>> exchangeableParameterIndices() {
    Set<Integer> exchangeable = new HashSet<Integer>();
    exchangeable.add(0);
    exchangeable.add(1);
    List<Set<Integer>> res = new ArrayList<>();
    res.add(exchangeable);
    return res;
  }

  @Override
  public Long[] createEmptyInputArray(int length) {
    return new Long[length];
  }

  @Override
  public ColumnType getOutputType() {
    return ColumnType.LONG;
  }

  @Override
  public ColumnType getInputType() {
    return ColumnType.LONG;
  }

}
