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
import java.util.function.Function;

import org.diqube.data.column.ColumnType;
import org.diqube.function.FunctionException;
import org.diqube.function.ProjectionFunction;

/**
 * Abstract implementation for projection functions with a single param.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractSingleParamProjectionFunction<I, O> implements ProjectionFunction<I, O> {

  private String nameLowercase;
  private Function<I, O> fn;

  private I value;
  private I[] valueArray;
  private ColumnType inputType;
  private ColumnType outputType;

  protected AbstractSingleParamProjectionFunction(String nameLowercase, ColumnType inputType, ColumnType outputType,
      Function<I, O> fn) {
    this.nameLowercase = nameLowercase;
    this.inputType = inputType;
    this.outputType = outputType;
    this.fn = fn;
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
    valueArray = value;
    this.value = null;
  }

  @Override
  public void provideConstantParameter(int parameterIdx, I value) {
    this.value = value;
    valueArray = null;
  }

  @Override
  public O[] execute() throws FunctionException {
    if (value != null) {
      O[] res = createArray(outputType, 1);
      res[0] = fn.apply(value);
      return res;
    }

    O[] res = createArray(outputType, valueArray.length);
    for (int i = 0; i < res.length; i++)
      res[i] = fn.apply(valueArray[i]);

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
    return outputType;
  }

  @Override
  public ColumnType getInputType() {
    return inputType;
  }

}
