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
package org.diqube.diql.request;

import java.util.ArrayList;
import java.util.List;

import org.diqube.diql.visitors.SelectStmtVisitor;

/**
 * Represents all the data that was contained in a select statement and has been evaluated by {@link SelectStmtVisitor}.
 *
 * <p>
 * Correctly implements {@link Object#equals(Object)} and {@link Object#hashCode()}.
 *
 * @author Bastian Gloeckle
 */
public class ExecutionRequest {

  private FromRequest fromRequest;

  private ComparisonRequest having;

  private ComparisonRequest where;

  private List<ResolveValueRequest> resolveValues = new ArrayList<>();

  private GroupRequest group;

  private List<FunctionRequest> projectAndAggregate = new ArrayList<>();

  private OrderRequest order;

  public ExecutionRequest() {

  }

  /**
   * @return From where to select
   */
  public FromRequest getFromRequest() {
    return fromRequest;
  }

  public void setFromRequest(FromRequest fromRequest) {
    this.fromRequest = fromRequest;
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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fromRequest == null) ? 0 : fromRequest.hashCode());
    result = prime * result + ((group == null) ? 0 : group.hashCode());
    result = prime * result + ((having == null) ? 0 : having.hashCode());
    result = prime * result + ((order == null) ? 0 : order.hashCode());
    result = prime * result + ((projectAndAggregate == null) ? 0 : projectAndAggregate.hashCode());
    result = prime * result + ((resolveValues == null) ? 0 : resolveValues.hashCode());
    result = prime * result + ((where == null) ? 0 : where.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof ExecutionRequest))
      return false;
    ExecutionRequest other = (ExecutionRequest) obj;
    if (fromRequest == null) {
      if (other.fromRequest != null)
        return false;
    } else if (!fromRequest.equals(other.fromRequest))
      return false;
    if (group == null) {
      if (other.group != null)
        return false;
    } else if (!group.equals(other.group))
      return false;
    if (having == null) {
      if (other.having != null)
        return false;
    } else if (!having.equals(other.having))
      return false;
    if (order == null) {
      if (other.order != null)
        return false;
    } else if (!order.equals(other.order))
      return false;
    if (projectAndAggregate == null) {
      if (other.projectAndAggregate != null)
        return false;
    } else if (!projectAndAggregate.equals(other.projectAndAggregate))
      return false;
    if (resolveValues == null) {
      if (other.resolveValues != null)
        return false;
    } else if (!resolveValues.equals(other.resolveValues))
      return false;
    if (where == null) {
      if (other.where != null)
        return false;
    } else if (!where.equals(other.where))
      return false;
    return true;
  }
}
