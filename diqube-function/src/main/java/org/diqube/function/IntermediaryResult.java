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

import java.util.ArrayList;
import java.util.List;

import org.diqube.data.column.ColumnType;
import org.diqube.function.aggregate.result.IntermediaryResultValueIterator;
import org.diqube.function.aggregate.result.IntermediaryResultValueSink;

/**
 * An intermediary result of an aggregation function.
 * 
 * @author Bastian Gloeckle
 */
public class IntermediaryResult implements IntermediaryResultValueSink {

  private ColumnType inputColumnType;

  private String outputColName;

  private List<Object> values = new ArrayList<>();

  public IntermediaryResult(String outputColName, ColumnType inputColumnType) {
    this.outputColName = outputColName;
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

  @Override
  public void pushValue(Object o) {
    values.add(o);
  }

  /**
   * @return A new iterator on the values of this intermediary result.
   */
  public IntermediaryResultValueIterator createValueIterator() {
    return new IntermediaryResultValueIterator(values.iterator());
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
