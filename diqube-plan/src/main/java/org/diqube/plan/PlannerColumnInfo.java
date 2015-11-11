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
package org.diqube.plan;

import java.util.HashSet;
import java.util.Set;

import org.diqube.diql.request.ExecutionRequest;
import org.diqube.diql.request.FunctionRequest;
import org.diqube.diql.request.FunctionRequest.Type;

/**
 * Information about a column that is needed while planning the execution of a {@link ExecutionRequest}.
 *
 * @author Bastian Gloeckle
 */
public class PlannerColumnInfo {
  private final String name;

  private Set<String> dependsOnColumns = new HashSet<>();

  private Set<String> columnsDependingOnThis = new HashSet<>();

  private FunctionRequest.Type type;

  private boolean transitivelyDependsOnRowAggregation;

  private boolean transitivelyDependsOnColAggregation;

  private boolean transitivelyDependsOnLiteralsOnly;

  private boolean isArrayResult;

  private boolean usedInHaving;

  private FunctionRequest providedByFunctionRequest;

  public PlannerColumnInfo(String name) {
    this.name = name;
  }

  /**
   * @return Names of columns this column depends on - that means that these columns need to be available before the
   *         calculation of this column can fully succeed.
   */
  public Set<String> getDependsOnColumns() {
    return dependsOnColumns;
  }

  public void setDependsOnColumns(Set<String> dependsOnColumns) {
    this.dependsOnColumns = dependsOnColumns;
  }

  /**
   * @return Inverse view of {@link #getDependsOnColumns()}.
   */
  public Set<String> getColumnsDependingOnThis() {
    return columnsDependingOnThis;
  }

  public void setColumnsDependingOnThis(Set<String> columnsDependingOnThis) {
    this.columnsDependingOnThis = columnsDependingOnThis;
  }

  /**
   * @return Type of function that creates this column.
   */
  public FunctionRequest.Type getType() {
    return type;
  }

  public void setType(FunctionRequest.Type type) {
    this.type = type;
  }

  /**
   * @return true if any of the parents (transitively) is an aggregation function that aggregates multiple rows (GROUP
   *         BY aggregation).
   */
  public boolean isTransitivelyDependsOnRowAggregation() {
    return transitivelyDependsOnRowAggregation;
  }

  public void setTransitivelyDependsOnRowAggregation(boolean transitivelyDependsOnRowAggregation) {
    this.transitivelyDependsOnRowAggregation = transitivelyDependsOnRowAggregation;
  }

  /**
   * @return true if any of the parents (transitively) is an aggregation function that aggregates multiple columns
   *         (repeated field aggregation).
   */
  public boolean isTransitivelyDependsOnColAggregation() {
    return transitivelyDependsOnColAggregation;
  }

  public void setTransitivelyDependsOnColAggregation(boolean transitivelyDependsOnColAggregation) {
    this.transitivelyDependsOnColAggregation = transitivelyDependsOnColAggregation;
  }

  /**
   * @return Name of the column.
   */
  public String getName() {
    return name;
  }

  /**
   * @return true if the columns this one depends on (transitively) only depend on literals and not on actual data
   *         column values.
   */
  public boolean isTransitivelyDependsOnLiteralsOnly() {
    return transitivelyDependsOnLiteralsOnly;
  }

  public void setTransitivelyDependsOnLiteralsOnly(boolean transitivelyDependsOnLiteralsOnly) {
    this.transitivelyDependsOnLiteralsOnly = transitivelyDependsOnLiteralsOnly;
  }

  /**
   * @return The {@link FunctionRequest} that is used to describe the creation of this column.
   */
  public FunctionRequest getProvidedByFunctionRequest() {
    return providedByFunctionRequest;
  }

  public void setProvidedByFunctionRequest(FunctionRequest providedByFunctionRequest) {
    this.providedByFunctionRequest = providedByFunctionRequest;
  }

  /**
   * @return true if this column is (directly) used in a HAVING statement.
   */
  public boolean isUsedInHaving() {
    return usedInHaving;
  }

  public void setUsedInHaving(boolean usedInHaving) {
    this.usedInHaving = usedInHaving;
  }

  /**
   * @return true if the result of calling a function is an array. This is typically the case for
   *         {@link Type#REPEATED_PROJECTION}, a projection function executed on a repeated field ('[*]' syntax).
   */
  public boolean isArrayResult() {
    return isArrayResult;
  }

  public void setArrayResult(boolean isArrayResult) {
    this.isArrayResult = isArrayResult;
  }

}
