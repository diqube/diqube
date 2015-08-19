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
package org.diqube.tranpose;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.diqube.loader.CsvLoader;
import org.diqube.loader.JsonLoader;
import org.diqube.loader.Loader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main method for diqube tranpose.
 * 
 * <p>
 * diqube transpose reads input data of all supported formats and outputs .diqube files (= serialized data from
 * diqube-data classes, which is already tranposed and compressed and simply needs to be de-serialized by the
 * diqube-server process).
 *
 * @author Bastian Gloeckle
 */
public class Transpose {
  private static final Logger logger = LoggerFactory.getLogger(Transpose.class);

  private static final String OPT_HELP = "h";
  private static final String OPT_INPUT = "i";
  private static final String OPT_OUTPUT = "o";
  private static final String OPT_TYPE = "t";
  private static final String OPT_COLINFO = "c";
  private static final String TYPE_JSON = "json";
  private static final String TYPE_CSV = "csv";

  public static void main(String[] args) {
    Options cliOpt = createCliOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    boolean showHelp = false;
    try {
      cmd = parser.parse(cliOpt, args);
      showHelp |= cmd.hasOption(OPT_HELP);
    } catch (ParseException e) {
      logger.error(e.getMessage());
      showHelp = true;
    }

    if (showHelp) {
      HelpFormatter formatter = new HelpFormatter();
      formatter
          .printHelp("transpose [options]",
              "\nTransposes and compressed input data for diqube. This enables the diqube server "
                  + "process to more easily load the data and not take up a lot of memory when doing it.\n\n",
              cliOpt, "");
      return;
    }

    File inputFile = new File(cmd.getOptionValue(OPT_INPUT));
    File outputFile = new File(cmd.getOptionValue(OPT_OUTPUT));
    String inputType = cmd.getOptionValue(OPT_TYPE).toLowerCase();
    String colInfoFileString = cmd.getOptionValue(OPT_COLINFO);
    File colInfoFile = null;

    if (!inputFile.isFile() || !inputFile.exists()) {
      logger.error("{} idoes not exist or is a directory.", inputFile.getAbsolutePath());
      return;
    }

    if (outputFile.exists() && outputFile.isDirectory()) {
      logger.error("{} exists and is a directory.", outputFile.getAbsolutePath());
      return;
    }
    if (colInfoFileString != null) {
      colInfoFile = new File(colInfoFileString);
      if (!colInfoFile.isFile() || !colInfoFile.exists()) {
        logger.error("{} idoes not exist or is a directory.", colInfoFile.getAbsolutePath());
        return;
      }
    }

    Class<? extends Loader> loaderClass = null;
    switch (inputType) {
    case TYPE_JSON:
      loaderClass = JsonLoader.class;
      break;
    case TYPE_CSV:
      loaderClass = CsvLoader.class;
      break;
    }

    if (loaderClass == null) {
      logger.error("Unkown input type: {}", inputType);
      return;
    }

    new TransposeImplementation(inputFile, outputFile, colInfoFile, loaderClass).transpose();
  }

  private static Options createCliOptions() {
    Options res = new Options();
    res.addOption(Option.builder(OPT_INPUT).longOpt("input").numberOfArgs(1).argName("file")
        .desc("The input file to read from (required).").required().build());
    res.addOption(Option.builder(OPT_TYPE).longOpt("type").numberOfArgs(1).argName("type")
        .desc("Type of the input data. One of \"json\", \"csv\" (required).").required().build());
    res.addOption(Option.builder(OPT_OUTPUT).longOpt("output").numberOfArgs(1).argName("file")
        .desc("Output file name (required).").required().build());
    res.addOption(Option.builder(OPT_COLINFO).longOpt("colinfo").numberOfArgs(1).argName("file")
        .desc("File containing information about the columns to be created.").build());
    res.addOption(Option.builder(OPT_HELP).longOpt("help").desc("Show this help.").build());
    return res;
  }
}
