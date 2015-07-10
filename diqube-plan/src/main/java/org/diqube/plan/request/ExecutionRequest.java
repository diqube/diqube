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

import org.diqube.plan.visitors.SelectStmtVisitor;

/**
 * Represents all the data that was contained in a select statement and has been evaluated by {@link SelectStmtVisitor}.
 *
 * @author Bastian Gloeckle
 */
public class ExecutionRequest {

  private String tableName;

  private ComparisonRequest having;

  private ComparisonRequest where;

  private List<ResolveValueRequest> resolveValues = new ArrayList<>();

  private GroupRequest group;

  private List<FunctionRequest> projectAndAggregate = new ArrayList<>();

  private OrderRequest order;

  public ExecutionRequest() {

  }

  /**
   * @return Name of the table to select from
   */
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return <code>null</code> or a {@link ComparisonRequest} representing the HAVING clause of the select stmt.
   */
  public ComparisonRequest getHaving() {
    return having;
  }

  public void setHaving(ComparisonRequest having) {
    this.having = having;
  }

  /**
   * @return <code>null</code> or a {@link ComparisonRequest} representing the WHERE clause of the select stmt.
   */
  public ComparisonRequest getWhere() {
    return where;
  }

  public void setWhere(ComparisonRequest where) {
    this.where = where;
  }

  /**
   * @return For each selected value in the select stmt there is a {@link ResolveValueRequest}.
   */
  public List<ResolveValueRequest> getResolveValues() {
    return resolveValues;
  }

  public void setResolveValues(List<ResolveValueRequest> resolveValues) {
    this.resolveValues = resolveValues;
  }

  /**
   * @return <code>null</code> or details of the GROUP BY of the select stmt.
   */
  public GroupRequest getGroup() {
    return group;
  }

  public void setGroup(GroupRequest group) {
    this.group = group;
  }

  /**
   * @return List of projection and aggregate functions to be executed.
   */
  public List<FunctionRequest> getProjectAndAggregate() {
    return projectAndAggregate;
  }

  public void setProjectAndAggregate(List<FunctionRequest> project) {
    this.projectAndAggregate = project;
  }

  /**
   * @return <code>null</code> or an {@link OrderRequest} representing the ORDER BY of the select stmt.
   */
  public OrderRequest getOrder() {
    return order;
  }

  public void setOrder(OrderRequest order) {
    this.order = order;
  }

}
