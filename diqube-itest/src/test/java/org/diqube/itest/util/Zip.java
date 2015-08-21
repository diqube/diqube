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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

/**
 * Create a ZIP out of an input directory.
 *
 * @author Bastian Gloeckle
 */
public class Zip {
  private static final Logger logger = LoggerFactory.getLogger(Zip.class);

  /**
   * Create a .zip file of all the contents of the source directory.
   */
  public void zip(File sourceDirectory, File targetFile) {
    if (!sourceDirectory.exists() || !sourceDirectory.isDirectory())
      throw new RuntimeException("Source directory is no valid directory: " + sourceDirectory.getAbsolutePath());

    if (targetFile.exists() && targetFile.isDirectory())
      throw new RuntimeException("Target file exists and is a directory: " + targetFile.getAbsolutePath());

    if (targetFile.exists())
      if (!targetFile.delete())
        throw new RuntimeException("Target file exists but could not be deleted: " + targetFile.getAbsolutePath());

    Path sourcePath = sourceDirectory.toPath();
    try (OutputStream os = new BufferedOutputStream(new FileOutputStream(targetFile))) {
      try (ZipOutputStream zos = new ZipOutputStream(os)) {
        Files.walkFileTree(sourcePath, new FileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            String name = sourcePath.relativize(file).toString();

            zos.putNextEntry(new ZipEntry(name));
            try (FileInputStream fis = new FileInputStream(file.toFile())) {
              ByteStreams.copy(fis, zos);
            }
            zos.closeEntry();

            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            logger.warn("Visit file failed: {}", file, exc);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
          }
        });
      }
    } catch (IOException e) {
      throw new RuntimeException("Could not zip result", e);
    }
  }
}
