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
package org.diqube.tool.info;

import java.io.File;

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
 * Display information about a .diqube file.
 *
 * @author Bastian Gloeckle
 */
@ToolFunctionName(Info.FUNCTION_NAME)
public class Info implements ToolFunction {
  public static final Logger logger = LoggerFactory.getLogger(Info.class);

  public static final String FUNCTION_NAME = "info";

  private static final String OPT_HELP = "h";
  private static final String OPT_INPUT = "i";

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
      formatter.printHelp(Info.FUNCTION_NAME + " [options]", "\nDisplays information about a .diqube file.\n\n", cliOpt,
          "");
      return;
    }

    File inputFile = new File(cmd.getOptionValue(OPT_INPUT));

    if (!inputFile.isFile() || !inputFile.exists()) {
      logger.error("{} does not exist or is a directory.", inputFile.getAbsolutePath());
      return;
    }

    new InfoImplementation(inputFile).printInfo();
  }

  private Options createCliOptions() {
    Options res = new Options();
    res.addOption(Option.builder(OPT_INPUT).longOpt("input").numberOfArgs(1).argName("file")
        .desc("The .diqube file to print details of (required).").required().build());
    res.addOption(Option.builder(OPT_HELP).longOpt("help").desc("Show this help.").build());
    return res;
  }
}
