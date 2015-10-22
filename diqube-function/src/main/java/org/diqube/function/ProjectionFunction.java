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
package org.diqube.function;

import java.util.List;
import java.util.Set;

import org.diqube.data.column.ColumnType;

/**
 * A function that projects some input to an output. The inputs can either be literal values or other columns.
 *
 * <p>
 * Each function is stateful and will be provided with needed parameters after each other. Classes do not need to be
 * thread-safe.
 * 
 * <p>
 * A projection function always projects something. Therefore it always needs at least one input parameter and it
 * calculates the result out of the _values_ of that input. The input can either be the values of a column (which are
 * resolved before passing them on to the function object) or may be constants.
 *
 * <p>
 * All input parameters need to be of the same data type. There may be several function objects with the same function
 * name, but then each function needs to have a different input data type.
 * 
 * @param <I>
 *          Input type
 * @param <O>
 *          Output type
 * 
 * @author Bastian Gloeckle
 */
public interface ProjectionFunction<I, O> {
  /**
   * @return Name of the function, lowercase.
   */
  public String getNameLowerCase();

  /**
   * Create and return an array of the given length of the input data type.
   */
  public I[] createEmptyInputArray(int length);

  /**
   * Provide columnar values for a specific parameter.
   * 
   * @param parameterIdx
   *          The index of the parameter to which values are provided.
   * @param value
   *          An array of values for that parameter - these are values for multiple rows of the column that is provided
   *          as parameter. If there are multiple parameters to this function, for each columnar parameter, the provided
   *          value array will have the same length. For non-columnar parameters (=constants),
   *          {@link #provideConstantParameter(int, Object)} will be called.
   */
  public void provideParameter(int parameterIdx, I[] value);

  /**
   * Provide the value of a constant parameter (for example the "1" in a call of "add(colA, 1)").
   */
  public void provideConstantParameter(int parameterIdx, I value);

  /**
   * Executes this function, produces and returns the output.
   * 
   * <p>
   * If only constant parameters were provided ({@link #provideConstantParameter(int, Object)}), the result is expected
   * to be "constant", too. This means that only the value at the first index of the result will be inspected and used.
   * 
   * @throws FunctionException
   *           If the result cannot be calculated.
   */
  public O[] execute() throws FunctionException;

  /**
   * @return Number of parameters this method needs.
   */
  public int numberOfParameters();

  /**
   * TODO #43 use this.
   * 
   * @return Sets of parameter indices whose value could be exchanged but the result of the function would still be the
   *         same.
   */
  public List<Set<Integer>> exchangeableParameterIndices();

  /**
   * @return Data Type of output of this function.
   */
  public ColumnType getOutputType();

  /**
   * @return Data Type of input of this function.
   */
  public ColumnType getInputType();
}
