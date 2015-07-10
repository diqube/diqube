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

  private boolean isOrdered;

  private boolean isGrouped;

  /* package */ExecutablePlanInfo(List<String> selectedColumnNames, boolean isOrdered, boolean isGrouped) {
    this.selectedColumnNames = selectedColumnNames;
    this.isOrdered = isOrdered;
    this.isGrouped = isGrouped;
  }

  /**
   * @return Names of the columns that were requested as result in the query.
   */
  public List<String> getSelectedColumnNames() {
    return selectedColumnNames;
  }

  public boolean isOrdered() {
    return isOrdered;
  }

  public boolean isGrouped() {
    return isGrouped;
  }
}
