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

import org.diqube.data.ColumnType;
import org.diqube.util.Triple;

/**
 * An intermediary result of an aggregation function.
 * 
 * <p>
 * As each aggregation function may have different type of intermediary results, this is simply a {@link Triple}.
 * 
 * @author Bastian Gloeckle
 */
public class IntermediaryResult<X, Y, Z> extends Triple<X, Y, Z> {

  private ColumnType inputColumnType;

  private String outputColName;

  public IntermediaryResult(X left, Y middle, Z right, ColumnType inputColumnType) {
    super(left, middle, right);
    this.inputColumnType = inputColumnType;
  }

  /**
   * @return The column type of the parameter to the aggregation function - this defines exactly which implementation of
   *         {@link AggregationFunction} should be used. This is <code>null</code> if the aggregation function does not
   *         have a parameter.
   */
  public ColumnType getInputColumnType() {
    return inputColumnType;
  }

  /**
   * This is an intermediary result that is needed to calculate the given output column.
   */
  public String getOutputColName() {
    return outputColName;
  }

  public void setOutputColName(String outputColName) {
    this.outputColName = outputColName;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(super.toString());
    sb.append(",outputCol=");
    sb.append(outputColName);
    sb.append(",inputColType=");
    sb.append(inputColumnType);
    sb.append("]");
    return sb.toString();
  }

}
