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
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

/**
 * Controls the execution of diqube-transpose in a separate process.
 *
 * @author Bastian Gloeckle
 */
public class TransposeControl {
  private static final Logger logger = LoggerFactory.getLogger(TransposeControl.class);

  public static final String TYPE_JSON = "json";
  public static final String TYPE_CSV = "csv";

  private File transposeJarFile;

  public TransposeControl(File transposeJarFile) {
    this.transposeJarFile = transposeJarFile;
  }

  /**
   * Transpose the given file.
   * 
   * <p>
   * This is to be called in tests, as it asserts some expected circumstances after the transposing is done.
   * 
   * <p>
   * This method will not return until either there is an error case or the transposing is done.
   * 
   * @param inputFileType
   *          {@link #TYPE_JSON} or {@link #TYPE_CSV}.
   */
  public void transpose(File inputFile, String inputFileType, File outputFile) {
    logger.info("Starting to transpose '{}' of type {} to '{}'...", inputFile, inputFileType, outputFile);
    logger.info("====================================================");
    try {
      Files.deleteIfExists(outputFile.toPath());
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not delete output file before starting the transpose process: " + outputFile.getAbsolutePath(), e);
    }

    ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", transposeJarFile.getAbsolutePath(), "-i",
        inputFile.getAbsolutePath(), "-o", outputFile.getAbsolutePath(), "-t", inputFileType).inheritIO();

    Process p;
    try {
      p = processBuilder.start();
    } catch (IOException e) {
      throw new RuntimeException("Could not start transposing of " + inputFile.getAbsolutePath(), e);
    }

    try {
      boolean stopped = p.waitFor(1, TimeUnit.MINUTES);

      if (!stopped) {
        logger.error("Transposing did not stop within timeout period, killing it forcibly.");
        p.destroyForcibly();
        throw new RuntimeException("Transposing did not stop within timeout period, killed it forcibly.");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for transposing to complete.", e);
    }

    logger.info("====================================================");
    Assert.assertEquals(p.exitValue(), 0, "Expected the tranposing sub-process to exit with a non-error exit value.");
    Assert.assertTrue(outputFile.exists(),
        "Expected that the transposing actually created the output file, but it did not.");
  }
}
