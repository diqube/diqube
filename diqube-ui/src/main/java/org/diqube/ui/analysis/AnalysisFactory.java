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
