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
package org.diqube.plan.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Calculates the name of the result column when executing a function on some input data.
 * 
 * <p>
 * Each function (aggregation or projection) that is executed on some set of data (constants or columns) creates a
 * column which will hold the result values of applying that function to the input data. This class can calculate the
 * name of this output column in a way that it is unique for the executed operation within the execution of one query.
 *
 * @author Bastian Gloeckle
 */
public class FunctionBasedColumnNameBuilder {
  private List<String> parameterNames = new ArrayList<>();

  private String functionName;

  public FunctionBasedColumnNameBuilder addParameterColumnName(String parameterColumnName) {
    parameterNames.add("col%" + parameterColumnName);
    return this;
  }

  public FunctionBasedColumnNameBuilder addParameterLiteralString(String parameterLiteralString) {
    parameterNames.add("lits%" + parameterLiteralString);
    return this;
  }

  public FunctionBasedColumnNameBuilder addParameterLiteralLong(long parameterLiteralLong) {
    parameterNames.add("litl%" + parameterLiteralLong);
    return this;
  }

  public FunctionBasedColumnNameBuilder addParameterLiteralDouble(double parameterLiteralDouble) {
    parameterNames.add("litd%" + parameterLiteralDouble);
    return this;
  }

  public FunctionBasedColumnNameBuilder withFunctionName(String functionName) {
    this.functionName = functionName;
    return this;
  }

  public String build() {
    StringBuilder sb = new StringBuilder();
    sb.append(functionName);
    sb.append("{");
    // TODO #26 if function does not care about order of parameters, we should create a standardized name (enables
    // better caching)
    // TODO #26 for caching of aggregate functions, we need to have the group-by cols in the name, too.
    for (String paramName : parameterNames) {
      sb.append(paramName);
      sb.append(",");
    }
    sb.append("}");
    return sb.toString();
  }
}
