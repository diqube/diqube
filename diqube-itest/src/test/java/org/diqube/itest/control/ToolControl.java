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
package org.diqube.itest.control;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.diqube.tool.merge.Merge;
import org.diqube.tool.transpose.Transpose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

/**
 * Controls the execution of diqube-tool in a separate process.
 *
 * @author Bastian Gloeckle
 */
public class ToolControl {
  private static final Logger logger = LoggerFactory.getLogger(ToolControl.class);

  public static final String TYPE_JSON = "json";
  public static final String TYPE_CSV = "csv";

  private File toolJarFile;

  public ToolControl(File toolJarFile) {
    this.toolJarFile = toolJarFile;
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

    executeTool("transpose", Arrays.asList("java", "-jar", toolJarFile.getAbsolutePath(), Transpose.FUNCTION_NAME, "-i",
        inputFile.getAbsolutePath(), "-o", outputFile.getAbsolutePath(), "-t", inputFileType), outputFile);
  }

  /**
   * Merge the input .diqube files into a single output .diqube file.
   * 
   * <p>
   * This is to be called in tests, as it asserts some expected circumstances after the transposing is done.
   * 
   * <p>
   * This method will not return until either there is an error case or the transposing is done.
   * 
   */
  public void merge(List<File> inputFiles, File outputFile) {
    logger.info("Starting to merge {} to '{}'...", inputFiles, outputFile);

    List<String> cmd = new ArrayList<>();
    cmd.addAll(Arrays.asList("java", "-jar", toolJarFile.getAbsolutePath(), Merge.FUNCTION_NAME, "-o",
        outputFile.getAbsolutePath(), "-i"));
    cmd.addAll(inputFiles.stream().map(f -> f.getAbsolutePath()).collect(Collectors.toList()));

    executeTool("merge", cmd, outputFile);
  }

  private void executeTool(String processDescription, List<String> cmd, File outputFile) {
    try {
      Files.deleteIfExists(outputFile.toPath());
    } catch (IOException e) {
      throw new RuntimeException("Could not delete output file before starting the " + processDescription + " process: "
          + outputFile.getAbsolutePath(), e);
    }

    ProcessBuilder processBuilder = new ProcessBuilder(cmd).inheritIO();

    logger.info("====================================================");
    Process p;
    try {
      p = processBuilder.start();
    } catch (IOException e) {
      throw new RuntimeException("Could not start '" + processDescription + "'", e);
    }

    try {
      boolean stopped = p.waitFor(1, TimeUnit.MINUTES);

      if (!stopped) {
        logger.error("'" + processDescription + "' did not stop within timeout period, killing it forcibly.");
        p.destroyForcibly();
        throw new RuntimeException(
            "'" + processDescription + "' did not stop within timeout period, killed it forcibly.");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for '" + processDescription + "' to complete.", e);
    }

    logger.info("====================================================");
    Assert.assertEquals(p.exitValue(), 0,
        "Expected the '" + processDescription + "' sub-process to exit with a non-error exit value.");
    Assert.assertTrue(outputFile.exists(),
        "Expected that the '" + processDescription + "' actually created the output file, but it did not.");
  }
}
