package org.diqube.file.internaldb;

import java.io.File;

/**
 * Utility class for internaldb files.
 *
 * @author Bastian Gloeckle
 */
public class InternalDbFileUtil {
  /**
   * Parses and returns the "consensus commit index" from the filename of an internalDb file.
   * 
   * @param file
   *          The file.
   * @param filePrefix
   *          Prefix of the file
   * @param fileSuffix
   *          Suffix of the file
   * @return commit index parsed from filename.
   */
  public static long parseCommitIndex(File file, String filePrefix, String fileSuffix) {
    String longStr = file.getName().substring(filePrefix.length(), file.getName().length() - fileSuffix.length());
    return Long.parseLong(longStr);
  }
}
