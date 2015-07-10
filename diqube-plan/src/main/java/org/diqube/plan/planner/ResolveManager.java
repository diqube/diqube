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
package org.diqube.plan.planner;

import java.util.List;

/**
 * Creates steps that are needed for resolving values of specific columns.
 *
 * @author Bastian Gloeckle
 */
public interface ResolveManager<T> {
  /**
   * Make sure that the given columns values will be resolved for all active row IDs.
   */
  public void resolveValuesOfColumn(String colName);

  /**
   * Build all steps needed to resolve the values of all columns for which {@link #resolveValuesOfColumn(String)} was
   * called.
   * 
   * <p>
   * Please note that implementing classes might need to be provided additional steps, see their JavaDoc.
   * 
   * @param rowIdSourceStep
   *          The step that provides all active row IDs. The steps created by this method will be wired to that steps
   *          output in order to consume the active rowIds and resolve the right values.
   * @return all steps built.
   */
  public List<T> build(T rowIdSourceStep);
}
