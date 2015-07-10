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

}
