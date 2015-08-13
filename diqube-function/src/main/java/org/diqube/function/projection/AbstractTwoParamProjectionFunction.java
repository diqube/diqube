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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import org.diqube.data.ColumnType;
import org.diqube.function.FunctionException;
import org.diqube.function.ProjectionFunction;

/**
 * Abstract implementation for projection functions with a two params.
 * 
 * @author Bastian Gloeckle
 */
public abstract class AbstractTwoParamProjectionFunction<I, O> implements ProjectionFunction<I, O> {

  private String nameLowercase;
  private BiFunction<I, I, O> fn;
  private ColumnType inputType;
  private ColumnType outputType;

  private boolean[] isArray = new boolean[2];
  private I[][] arrayValues;
  private I[] values;
  private boolean paramsAreExchangeable;

  @SuppressWarnings("unchecked")
  protected AbstractTwoParamProjectionFunction(String nameLowercase, ColumnType inputType, ColumnType outputType,
      boolean paramsAreExchangeable, BiFunction<I, I, O> fn) {
    this.nameLowercase = nameLowercase;
    this.inputType = inputType;
    this.outputType = outputType;
    this.fn = fn;
    this.paramsAreExchangeable = paramsAreExchangeable;
    values = this.createEmptyInputArray(2);
    switch (inputType) {
    case LONG:
      arrayValues = (I[][]) Array.newInstance(Long.class, 2, 0);
      break;
    case DOUBLE:
      arrayValues = (I[][]) Array.newInstance(Double.class, 2, 0);
      break;
    default:
      arrayValues = (I[][]) Array.newInstance(String.class, 2, 0);
    }
  }

  @Override
  public String getNameLowerCase() {
    return nameLowercase;
  }

  @Override
  public I[] createEmptyInputArray(int length) {
    return createArray(inputType, length);
  }

  @SuppressWarnings("unchecked")
  private <T> T[] createArray(ColumnType type, int length) {
    if (type.equals(ColumnType.LONG))
      return (T[]) new Long[length];
    if (type.equals(ColumnType.DOUBLE))
      return (T[]) new Double[length];
    return (T[]) new String[length];
  }

  @Override
  public void provideParameter(int parameterIdx, I[] value) {
    arrayValues[parameterIdx] = value;
    isArray[parameterIdx] = true;
  }

  @Override
  public void provideConstantParameter(int parameterIdx, I value) {
    values[parameterIdx] = value;
    isArray[parameterIdx] = false;
  }

  @Override
  public O[] execute() throws FunctionException {
    if (!isArray[0] && !isArray[1]) {
      O[] res = createArray(outputType, 1);
      res[0] = fn.apply(values[0], values[1]);
      return res;
    }

    if (isArray[0] ^ isArray[1]) {
      I[] array = (isArray[0]) ? arrayValues[0] : arrayValues[1];
      O[] res = createArray(outputType, array.length);
      if (isArray[0]) {
        for (int i = 0; i < res.length; i++)
          res[i] = fn.apply(array[i], values[1]);
      } else {
        for (int i = 0; i < res.length; i++)
          res[i] = fn.apply(values[0], array[i]);
      }
      return res;
    }

    if (arrayValues[0].length != arrayValues[1].length)
      throw new FunctionException("Arrays have to be of same length for " + getNameLowerCase() + "!");

    O[] res = createArray(outputType, arrayValues[0].length);

    for (int i = 0; i < res.length; i++)
      res[i] = fn.apply(arrayValues[0][i], arrayValues[1][i]);

    return res;
  }

  @Override
  public int numberOfParameters() {
    return 2;
  }

  @Override
  public List<Set<Integer>> exchangeableParameterIndices() {
    if (paramsAreExchangeable) {
      Set<Integer> exchangeable = new HashSet<Integer>();
      exchangeable.add(0);
      exchangeable.add(1);
      List<Set<Integer>> res = new ArrayList<>();
      res.add(exchangeable);
      return res;
    }
    return new ArrayList<>();
  }

  @Override
  public ColumnType getOutputType() {
    return inputType;
  }

  @Override
  public ColumnType getInputType() {
    return outputType;
  }

}
