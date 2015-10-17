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
import java.util.function.Predicate;

import org.diqube.util.Pair;

/**
 * Represents the ordering requested in a select stmt and an optional LIMIT clause.
 *
 * <p>
 * Correctly implements {@link Object#equals(Object)} and {@link Object#hashCode()}.
 * 
 * @author Bastian Gloeckle
 */
public class OrderRequest {
  private List<Pair<String, Boolean>> columns = new ArrayList<>();

  private Long limit;

  private Long limitStart;

  private Long softLimit;

  /**
   * @return Ordering requested. Left side of {@link Pair} is column name, right side is <code>true</code> if it
   *         requested to sort <b>ascending</b>, <code>false</code> if <b>descending</b>.
   */
  public List<Pair<String, Boolean>> getColumns() {
    return columns;
  }

  public void setColumns(List<Pair<String, Boolean>> columns) {
    this.columns = columns;
  }

  /**
   * @return <code>null</code> or the number of lines to limit to.
   */
  public Long getLimit() {
    return limit;
  }

  public void setLimit(Long limit) {
    this.limit = limit;
  }

  /**
   * @return <code>null</code> or the id of the first row to return when limiting.
   */
  public Long getLimitStart() {
    return limitStart;
  }

  public void setLimitStart(Long limitStart) {
    this.limitStart = limitStart;
  }

  /**
   * @return A Soft limit is used in the case that the query master orders the cluster nodes to return 'at least' that
   *         number of rows. Any row with ordering index > softLimit but which has equal values for the ordering columns
   *         has to be included in the result, too. If this is set, {@link #getLimit()} and {@link #getLimitStart()}
   *         have to return <code>null</code>.
   */
  public Long getSoftLimit() {
    return softLimit;
  }

  public void setSoftLimit(Long softLimit) {
    this.softLimit = softLimit;
  }

  /**
   * Creates a OrderRequest that contains all the columns that this object contains, up to the index when the given
   * predicate first returns true.
   */
  public OrderRequest createSubOrderRequestUpTo(Predicate<Pair<String, Boolean>> firstNotIncludedColpredicate) {
    List<Pair<String, Boolean>> newColumns = new ArrayList<>();
    for (Pair<String, Boolean> curCol : columns) {
      if (firstNotIncludedColpredicate.test(curCol))
        break;
      newColumns.add(new Pair<>(curCol));
    }

    OrderRequest res = new OrderRequest();
    res.setLimit(this.limit);
    res.setLimitStart(this.limitStart);
    res.setColumns(newColumns);
    return res;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((columns == null) ? 0 : columns.hashCode());
    result = prime * result + ((limit == null) ? 0 : limit.hashCode());
    result = prime * result + ((limitStart == null) ? 0 : limitStart.hashCode());
    result = prime * result + ((softLimit == null) ? 0 : softLimit.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof OrderRequest))
      return false;
    OrderRequest other = (OrderRequest) obj;
    if (columns == null) {
      if (other.columns != null)
        return false;
    } else if (!columns.equals(other.columns))
      return false;
    if (limit == null) {
      if (other.limit != null)
        return false;
    } else if (!limit.equals(other.limit))
      return false;
    if (limitStart == null) {
      if (other.limitStart != null)
        return false;
    } else if (!limitStart.equals(other.limitStart))
      return false;
    if (softLimit == null) {
      if (other.softLimit != null)
        return false;
    } else if (!softLimit.equals(other.softLimit))
      return false;
    return true;
  }

}
