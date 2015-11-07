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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Bastian Gloeckle
 */
public class TestDataGenerator {
  private static final Logger logger = LoggerFactory.getLogger(TestDataGenerator.class);

  /**
   * Generate a json test data file.
   * 
   * <p>
   * Each row of the output file will be made up of several "levels". Each level contains the same field names. The
   * values of the fields are typically arrays which in turn contain objects of the next level. On the loweset level,
   * the arrays will contain longs.
   * 
   * @param outputFile
   *          Target file.
   * @param rows
   *          Number of rows to generate.
   * @param eachRowDepth
   *          Number of levels to create for each row.
   * @param fieldsOnEachLevel
   *          Name of the fields each level of a row should contain.
   * @param repetitionEachLevel
   *          The size of the arrays that each level in each row will have.
   */
  public static void generateJsonTestData(File outputFile, int rows, int eachRowDepth, String[] fieldsOnEachLevel,
      int repetitionEachLevel) throws FileNotFoundException, IOException {
    logger.info("Generating test data...");
    try (FileOutputStream dataFOS = new FileOutputStream(outputFile)) {
      dataFOS.write("[".getBytes("UTF-8"));
      for (int i = 0; i < rows; i++) {
        if (i > 0)
          dataFOS.write(",".getBytes("UTF-8"));
        generateJsonRow(eachRowDepth, fieldsOnEachLevel, repetitionEachLevel, dataFOS);
      }
      dataFOS.write("]".getBytes("UTF-8"));
    }
    logger.info("Test data generated.");
  }

  private static void generateJsonRow(int depth, String[] fieldsOnEachLevel, int repetitionCountEachLevel,
      OutputStream outStream) throws UnsupportedEncodingException, IOException {
    outStream.write("{".getBytes("UTF-8"));
    boolean first = true;
    for (String fieldName : fieldsOnEachLevel) {
      if (!first)
        outStream.write(",".getBytes("UTF-8"));
      first = false;

      outStream.write(("\"" + fieldName + "\" : [").getBytes("UTF-8"));
      for (int i = 0; i < repetitionCountEachLevel; i++) {
        if (i > 0)
          outStream.write(",".getBytes("UTF-8"));
        if (depth == 1) {
          outStream.write(Integer.toString(i).getBytes("UTF-8"));
        } else
          generateJsonRow(depth - 1, fieldsOnEachLevel, repetitionCountEachLevel, outStream);
      }
      outStream.write("]".getBytes("UTF-8"));
    }
    outStream.write("}\n".getBytes("UTF-8"));
  }
}
