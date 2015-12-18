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
package org.diqube.execution;

import java.util.List;

/**
 * Information about what an ExecutablePlan actually does when being executed.
 *
 * @author Bastian Gloeckle
 */
public class ExecutablePlanInfo {
  private List<String> selectedColumnNames;

  private List<String> selectionRequests;

  private boolean isOrdered;

  private boolean isGrouped;

  private boolean having;

  /* package */ ExecutablePlanInfo(List<String> selectedColumnNames, List<String> selectionRequests, boolean isOrdered,
      boolean isGrouped, boolean having) {
    this.selectedColumnNames = selectedColumnNames;
    this.selectionRequests = selectionRequests;
    this.isOrdered = isOrdered;
    this.isGrouped = isGrouped;
    this.having = having;
  }

  /**
   * @return Names of the columns that were requested as result in the query. These are the output column names of the
   *         plan.
   */
  public List<String> getSelectedColumnNames() {
    return selectedColumnNames;
  }

  /**
   * @return The selection requests that were provided in the query. The index corresponds to the elements returned in
   *         {@link #getSelectedColumnNames()}. These are not the columnNames that are the output of the plan, but
   *         rather the string that was used in the select statement that was used to request the corresponding output
   *         column. This information will <b>NOT</b> be available on the query remotes, <code>null</code> will be
   *         returned there!
   */
  public List<String> getSelectionRequests() {
    return selectionRequests;
  }

  public boolean isOrdered() {
    return isOrdered;
  }

  public boolean isGrouped() {
    return isGrouped;
  }

  /**
   * @return <code>true</code> if executable plan executes a HAVING statement (can only be true on query master)
   */
  public boolean isHaving() {
    return having;
  }
}
