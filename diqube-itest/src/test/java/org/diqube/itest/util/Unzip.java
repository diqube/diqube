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
package org.diqube.itest.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

/**
 * Utility class that can unzip a file into a directory.
 *
 * @author Bastian Gloeckle
 */
public class Unzip {
  private static final Logger logger = LoggerFactory.getLogger(Unzip.class);

  private File zipFile;

  public Unzip(File zipFile) {
    this.zipFile = zipFile;
  }

  public void unzip(File targetDir) {
    if (!targetDir.exists())
      if (!targetDir.mkdirs())
        throw new RuntimeException("Could not create target dir " + targetDir);

    logger.info("Extracting '{}' to '{}'.", zipFile.getAbsolutePath(), targetDir.getAbsolutePath());

    int numberOfFiles = 0;

    try (FileInputStream fis = new FileInputStream(zipFile)) {
      ZipInputStream is = new ZipInputStream(fis);

      ZipEntry entry;
      while ((entry = is.getNextEntry()) != null) {
        File targetFile = new File(targetDir, entry.getName());

        if (entry.isDirectory()) {
          if (!targetFile.mkdirs())
            throw new RuntimeException("Could not create " + targetFile);
          continue;
        }

        numberOfFiles++;
        if (!targetFile.getParentFile().exists())
          if (!targetFile.getParentFile().mkdirs())
            throw new RuntimeException("Could not create " + targetFile.getParent());

        try (FileOutputStream fos = new FileOutputStream(targetFile)) {
          ByteStreams.copy(is, fos);
        }

        is.closeEntry();
      }

      is.close();
    } catch (IOException e) {
      throw new RuntimeException("Could not unzip " + zipFile, e);
    }

    logger.info("Extracted {} files.", numberOfFiles);
  }

}
