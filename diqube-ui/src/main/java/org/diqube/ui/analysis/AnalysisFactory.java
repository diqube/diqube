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
package org.diqube.ui.analysis;

import java.util.List;

import org.diqube.context.AutoInstatiate;

/**
 * Factory for classes that represent an "analysis" in the UI.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class AnalysisFactory {
  public UiAnalysis createAnalysis(String id, String name, String table) {
    return new UiAnalysis(id, name, table);
  }

  public UiSlice createSlice(String name, List<UiSliceDisjunction> disjunctions) {
    return new UiSlice(name, disjunctions);
  }

  public UiQube createQube(String name, String sliceName) {
    return new UiQube(name, sliceName);
  }
}
