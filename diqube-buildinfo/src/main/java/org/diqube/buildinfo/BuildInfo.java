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
package org.diqube.buildinfo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Provides information about when the buildinfo project was built.
 * 
 * If the buildinfo project is built together with other diqube executables, this information is also valid for those
 * executables etc.
 * 
 * @author Bastian Gloeckle
 */
public class BuildInfo {
  /** properties fiel containign the information. This is prepared by maven. */
  private static final String PROPERTIES_FILE = "buildinfo.properties";

  private static final String PROP_TIMESTAMP = "buildinfo.timestamp";
  private static final String PROP_GIT_COMMIT_SHORT = "buildinfo.git-commit-short";
  private static final String PROP_GIT_COMMIT_LONG = "buildinfo.git-commit-long";

  private static final String timestamp;
  private static final String gitCommitShort;
  private static final String gitCommitLong;

  /**
   * @return The timestamp when this was built.
   */
  public static String getTimestamp() {
    return timestamp;
  }

  /**
   * @return A short string denoting the git commit hash from which this was built.
   */
  public static String getGitCommitShort() {
    return gitCommitShort;
  }

  /**
   * @return A long string denoting the full git commit hash from which this was built.
   */
  public static String getGitCommitLong() {
    return gitCommitLong;
  }

  static {
    try (InputStream propStream = BuildInfo.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
      if (propStream == null)
        throw new RuntimeException("Could not load build properties");
      Properties p = new Properties();
      p.load(propStream);
      timestamp = p.getProperty(PROP_TIMESTAMP);
      gitCommitShort = p.getProperty(PROP_GIT_COMMIT_SHORT);
      gitCommitLong = p.getProperty(PROP_GIT_COMMIT_LONG);
    } catch (IOException e) {
      throw new RuntimeException("Could not load build properties", e);
    }
  }
}
