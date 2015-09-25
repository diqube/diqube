package org.diqube.ui;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.diqube.context.AutoInstatiate;
import org.diqube.ui.analysis.UiAnalysis;

/**
 * Registry for all available {@link UiAnalysis}s.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class AnalysisRegistry {
  private ConcurrentMap<String, UiAnalysis> analysis = new ConcurrentHashMap<>();

  public void registerUiAnalysis(UiAnalysis analysis) {
    this.analysis.put(analysis.getId(), analysis);
  }

  public UiAnalysis getAnalysis(String id) {
    return analysis.get(id);
  }

  public Collection<UiAnalysis> getAllAnalysis() {
    return new ArrayList<>(analysis.values());
  }
}
