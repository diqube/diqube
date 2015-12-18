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
package org.diqube.ui.db;

import java.util.Map;

import org.diqube.ui.analysis.UiAnalysis;

/**
 * Interface for interacting with the "UI database" which stores {@link UiAnalysis}.
 *
 * @author Bastian Gloeckle
 */
public interface UiDatabase {
  /**
   * Store a specific analysis version.
   * 
   * Note that no validation at all is performed: Neither if the user noted in the analysis actually "owns" the analysis
   * with that Id, nor if the version already exists.
   * 
   * @param analysis
   *          The analysis.
   * @throws StoreException
   *           If analysis cannot be stored.
   */
  public void storeAnalysisVersion(UiAnalysis analysis) throws StoreException;

  /**
   * Load a specific version of an analysis.
   * 
   * @param analysisId
   *          The ID of the analysis
   * @param version
   *          The version to be loaded.
   * @return The loaded version or <code>null</code> if analysis cannot be loaded.
   */
  public UiAnalysis loadAnalysisVersion(String analysisId, long version);

  /**
   * Identify the newest versions of all analysis' of a specific user
   * 
   * @param user
   *          The user to find analysis of.
   * @return The analysis ID to the newest version of it.
   */
  public Map<String, Long> findNewestAnalysisVersionsOfUser(String user);

  /**
   * Return a map with all analysis of a user and the newest name of each.
   * 
   * @param user
   *          The user to find analysis of.
   * @return Map from analysisId to name.
   */
  public Map<String, String> findNewestAnalysisNamesOfUser(String user);

  /**
   * Find the newest version of a specific analysis.
   * 
   * @param analysisId
   * @return The newest version or <code>null</code> if not exists.
   */
  public Long findNewestAnalysisVersion(String analysisId);

  /**
   * Find the owner of the given analysis.
   * 
   * @return Owner or <code>null</code> if it cannot be found.
   */
  public String findOwnerOfAnalysis(String analysisId);

  /**
   * Call when shuttding down the database. No more queries can be sent to the DB after this.
   */
  public void shutdown();

  public static class StoreException extends Exception {
    private static final long serialVersionUID = 1L;

    public StoreException(String msg) {
      super(msg);
    }

    public StoreException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
