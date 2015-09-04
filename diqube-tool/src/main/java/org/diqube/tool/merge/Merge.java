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
package org.diqube.tool.merge;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.diqube.tool.ToolFunction;
import org.diqube.tool.ToolFunctionName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Merges two .diqube files into a single one, containing all the TableShards of both source files.
 *
 * @author Bastian Gloeckle
 */
@ToolFunctionName(Merge.FUNCTION_NAME)
public class Merge implements ToolFunction {
  private static final Logger logger = LoggerFactory.getLogger(Merge.class);

  public static final String FUNCTION_NAME = "merge";

  private static final String OPT_HELP = "h";
  private static final String OPT_INPUT = "i";
  private static final String OPT_OUTPUT = "o";
  private static final String OPT_COMMENT = "c";

  @Override
  public void execute(String[] args) {
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
      formatter.printHelp(Merge.FUNCTION_NAME + " [options]",
          "\nReads two .diqube files and merges the contents into an output .diqube file. All TableShards of the "
              + "input files are simply copied into the output file.\n\n",
          cliOpt, "");
      return;
    }

    List<File> inputFiles =
        Stream.of(cmd.getOptionValues(OPT_INPUT)).map(f -> new File(f)).collect(Collectors.toList());
    File outputFile = new File(cmd.getOptionValue(OPT_OUTPUT));

    for (File inputFile : inputFiles) {
      if (!inputFile.isFile() || !inputFile.exists()) {
        logger.error("{} does not exist or is a directory.", inputFile.getAbsolutePath());
        return;
      }
    }

    if (outputFile.exists() && outputFile.isDirectory()) {
      logger.error("{} exists and is a directory.", outputFile.getAbsolutePath());
      return;
    }

    String comment = cmd.getOptionValue(OPT_COMMENT);

    new MergeImplementation(inputFiles, outputFile, comment).merge();
  }

  private Options createCliOptions() {
    Options res = new Options();
    res.addOption(
        Option.builder(OPT_INPUT).longOpt("input").numberOfArgs(Option.UNLIMITED_VALUES).argName("file> <file> <...")
            .desc("The input file(s) to read from (.diqube format, at least one required).").required().build());
    res.addOption(Option.builder(OPT_OUTPUT).longOpt("output").numberOfArgs(1).argName("file")
        .desc("Output file name (required).").required().build());
    res.addOption(Option.builder(OPT_COMMENT).longOpt("comment").numberOfArgs(1).argName("string")
        .desc("Comment to write into the output file.").build());
    res.addOption(Option.builder(OPT_HELP).longOpt("help").desc("Show this help.").build());
    return res;
  }
}
