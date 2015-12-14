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
package org.diqube.tool.im;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.diqube.remote.query.thrift.IdentityService;
import org.diqube.tool.ToolFunction;
import org.diqube.tool.ToolFunctionName;
import org.diqube.tool.info.Info;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

/**
 * Tool function that interacts with {@link IdentityService}.
 *
 * @author Bastian Gloeckle
 */
@ToolFunctionName(IdentityToolFunction.FUNCTION_NAME)
public class IdentityToolFunction implements ToolFunction {
  public static final Logger logger = LoggerFactory.getLogger(IdentityToolFunction.class);

  private static final String BASE_PKG = "org.diqube.tool.im";

  public static final String FUNCTION_NAME = "im";

  public static final String OPT_HELP = "h";
  public static final String OPT_ACTUAL_FUNCTION = "f";
  public static final String OPT_LOGIN_USER = "lu";
  public static final String OPT_LOGIN_PASSWORD = "lp";
  public static final String OPT_PARAM_USER = "u";
  public static final String OPT_PARAM_PASSWORD = "p";
  public static final String OPT_PARAM_PERMISSION = "a";
  public static final String OPT_PARAM_PERMISSION_OBJECT = "o";
  public static final String OPT_PARAM_EMAIL = "e";
  public static final String OPT_SERVER = "s";

  private Map<String, Pair<AbstractActualIdentityToolFunction, IsActualIdentityToolFunction>> actualFunctions;

  @Override
  public void execute(String[] args) {
    actualFunctions = loadActualFunctions();

    Options cliOpt = createCliOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    boolean showHelp = false;
    String actualFunctionNameForHelp = null;
    try {
      cmd = parser.parse(cliOpt, args);

      if (cmd.hasOption(OPT_HELP)) {
        showHelp = true;
        actualFunctionNameForHelp = cmd.getOptionValue(OPT_HELP);
      }
    } catch (ParseException e) {
      logger.error(e.getMessage());
      showHelp = true;

      // check if there is a detailed -h option
      try {
        Options helpCliOptions = createCliOptionsOnlyHelp();
        cmd = new DefaultParser().parse(helpCliOptions, args);

        if (cmd.hasOption(OPT_HELP)) {
          showHelp = true;
          actualFunctionNameForHelp = cmd.getOptionValue(OPT_HELP);
        }
      } catch (ParseException e2) {
        // swallow, we show help anyway.
      }
    }

    if (showHelp) {
      if (actualFunctionNameForHelp != null) {
        if (actualFunctions.containsKey(actualFunctionNameForHelp)) {
          System.out.println("Help for function '" + actualFunctionNameForHelp + "':");
          System.out.println();
          System.out.println(actualFunctions.get(actualFunctionNameForHelp).getRight().shortDescription());
          return;
        }
        System.out.println("Unknown function: " + actualFunctionNameForHelp);
      }
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(Info.FUNCTION_NAME + " [options]",
          "\nInteracts with the identity service of a diqube server.\n\n", cliOpt, "");
      return;
    }

    String function = cmd.getOptionValue(OPT_ACTUAL_FUNCTION);
    String loginUser = cmd.getOptionValue(OPT_LOGIN_USER);
    String loginPassword = cmd.getOptionValue(OPT_LOGIN_PASSWORD);
    String paramUser = cmd.getOptionValue(OPT_PARAM_USER);
    String paramPassword = cmd.getOptionValue(OPT_PARAM_PASSWORD);
    String paramPermission = cmd.getOptionValue(OPT_PARAM_PERMISSION);
    String paramPermissionObject = cmd.getOptionValue(OPT_PARAM_PERMISSION_OBJECT);
    String paramEmail = cmd.getOptionValue(OPT_PARAM_EMAIL);
    String server = cmd.getOptionValue(OPT_SERVER);

    if (!actualFunctions.containsKey(function)) {
      logger.error("Unknown function '{}'", function);
      return;
    }

    AbstractActualIdentityToolFunction fn = actualFunctions.get(function).getLeft();

    fn.initializeGeneral(server, loginUser, loginPassword);
    fn.initializeOptionalParams(paramUser, paramPassword, paramPermission, paramPermissionObject, paramEmail);

    logger.info("Executing function '{}'...", function);

    fn.execute();

    logger.info("Function executed successfully.", function);
  }

  private Map<String, Pair<AbstractActualIdentityToolFunction, IsActualIdentityToolFunction>> loadActualFunctions() {
    try {
      Collection<ClassInfo> classInfos =
          ClassPath.from(getClass().getClassLoader()).getTopLevelClassesRecursive(BASE_PKG);

      Map<String, Pair<AbstractActualIdentityToolFunction, IsActualIdentityToolFunction>> toolFunctions =
          new HashMap<>();

      for (ClassInfo classInfo : classInfos) {
        Class<?> clazz = classInfo.load();
        IsActualIdentityToolFunction isActualIdentityToolFunctionAnnotation =
            clazz.getAnnotation(IsActualIdentityToolFunction.class);
        if (isActualIdentityToolFunctionAnnotation != null) {
          AbstractActualIdentityToolFunction functionInstance =
              (AbstractActualIdentityToolFunction) clazz.newInstance();

          toolFunctions.put(isActualIdentityToolFunctionAnnotation.identityFunctionName(),
              new Pair<>(functionInstance, isActualIdentityToolFunctionAnnotation));
        }
      }

      return toolFunctions;
    } catch (IOException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private Options createCliOptions() {
    Options res = new Options();
    res.addOption(Option.builder(OPT_ACTUAL_FUNCTION).longOpt("function").numberOfArgs(1).argName("name")
        .desc("Name of the function to execute on the identity service (required). One of the following: "
            + actualFunctions.keySet())
        .required().build());
    res.addOption(Option.builder(OPT_SERVER).longOpt("server").numberOfArgs(1).argName("ip:port")
        .desc("Server address of a running diqube server.").required().build());
    res.addOption(Option.builder(OPT_LOGIN_USER).longOpt("login-user").numberOfArgs(1).argName("name")
        .desc("Username to use for authentication at the identity service (required).").required().build());
    res.addOption(Option.builder(OPT_LOGIN_PASSWORD).longOpt("login-password").numberOfArgs(1).argName("password")
        .desc("Password to use for authentication at the identity service (required).").required().build());

    res.addOption(Option.builder(OPT_PARAM_USER).longOpt("user").numberOfArgs(1).argName("name")
        .desc("Username to use as parameter to the function.").build());
    res.addOption(Option.builder(OPT_PARAM_PASSWORD).longOpt("password").numberOfArgs(1).argName("password")
        .desc("Password to use as parameter to the function.").build());
    res.addOption(Option.builder(OPT_PARAM_PERMISSION).longOpt("permission").numberOfArgs(1).argName("name")
        .desc("Permission to use as parameter to the function.").build());
    res.addOption(Option.builder(OPT_PARAM_PERMISSION_OBJECT).longOpt("object").numberOfArgs(1).argName("name")
        .desc("Permission object to use as parameter to the function.").build());
    res.addOption(Option.builder(OPT_PARAM_EMAIL).longOpt("email").numberOfArgs(1).argName("address")
        .desc("E-mail address to use as parameter to the function.").build());

    addHelpOption(res);

    return res;
  }

  private void addHelpOption(Options res) {
    res.addOption(Option.builder(OPT_HELP).longOpt("help").numberOfArgs(1).argName("function name")
        .desc("Show this help or the help of a specific function (see flag '" + OPT_ACTUAL_FUNCTION + "').").build());
  }

  private Options createCliOptionsOnlyHelp() {
    Options res = new Options();
    addHelpOption(res);
    return res;
  }
}
