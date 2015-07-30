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
package org.diqube.plan.request;

import java.util.ArrayList;
import java.util.List;

import org.diqube.plan.util.FunctionBasedColumnNameBuilder;
import org.diqube.util.ColumnOrValue;

/**
 * A FunctionRequest represents either the execution of a projection or an aggregation function during the execution of
 * a select stmt.
 * 
 * <p>
 * A projection function is a function that takes some inputs (usually at least including one column name) and
 * transforms these inputs to build a new column with the derived values.
 * 
 * <p>
 * An aggregation function is a function that combines multiple values into one. There are two types of aggregation
 * functions, first <b>row aggregation</b> is the aggregation that combines the values of the same column in multiple
 * rows (a typical GROUP BY aggregation), and second the <b>column aggregation</b> aggregates the values of various
 * columns in a single row (aggregating over a repeated field).
 * 
 * <p>
 * Please note that the output column name has to be <b>unique for the logic being executed by the function</b>. That
 * means if two {@link FunctionRequest}s are created which execute the same logic (= same function name, same list of
 * parameters), their output column name needs to be the same. The planning phase later relies on this to remove
 * unneeded function executions (because we do not need to calculate the same function on the same arguments twice). One
 * should use {@link FunctionBasedColumnNameBuilder} to create the output column names.
 *
 * @author Bastian Gloeckle
 */
public class FunctionRequest {
  public static enum Type {
    PROJECTION, REPEATED_PROJECTION, AGGREGATION_ROW, AGGREGATION_COL
  }

  private List<ColumnOrValue> inputParameters = new ArrayList<>();

  private String outputColumn;

  private String functionName;

  private Type type;

  /**
   * @return Input params to the function.
   */
  public List<ColumnOrValue> getInputParameters() {
    return inputParameters;
  }

  public void setInputParameters(List<ColumnOrValue> inputParameters) {
    this.inputParameters = inputParameters;
  }

  /**
   * @return Each function produces an output column, this is the name of the column that is created by this function.
   */
  public String getOutputColumn() {
    return outputColumn;
  }

  public void setOutputColumn(String outputColumn) {
    this.outputColumn = outputColumn;
  }

  /**
   * @return Function name to be executed, lowercase.
   */
  public String getFunctionName() {
    return functionName;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  /**
   * @return Identifying either a projection function or an aggregation function.
   */
  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

}
