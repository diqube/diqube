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
package org.diqube.tool;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

/**
 * Main class of diqube tool, which provides command line functions for diqube.
 *
 * @author Bastian Gloeckle
 */
public class Tool implements ToolFunction {
  private static final String BASE_PKG = "org.diqube.tool";

  private static final Logger logger = LoggerFactory.getLogger(Tool.class);

  public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
    Collection<ClassInfo> classInfos =
        ClassPath.from(Tool.class.getClassLoader()).getTopLevelClassesRecursive(BASE_PKG);

    Map<String, ToolFunction> toolFunctions = new HashMap<>();

    for (ClassInfo classInfo : classInfos) {
      Class<?> clazz = classInfo.load();
      ToolFunctionName toolFunctionName = clazz.getAnnotation(ToolFunctionName.class);
      if (toolFunctionName != null) {
        ToolFunction functionInstance = (ToolFunction) clazz.newInstance();
        toolFunctions.put(toolFunctionName.value(), functionInstance);
      }
    }

    new Tool(toolFunctions).execute(args);
  }

  private Map<String, ToolFunction> toolFunctions;

  private Tool(Map<String, ToolFunction> toolFunctions) {
    this.toolFunctions = toolFunctions;
  }

  @Override
  public void execute(String[] args) {
    if (args.length == 0 || !toolFunctions.containsKey(args[0])) {
      logger.error("No function name given or function unknown. Available functions: {}", toolFunctions.keySet());
      System.out.println();
      System.out.println("Copyright (C) 2015 Bastian Gloeckle");
      System.out.println();
      System.out.println(
          "diqube is free software: you can redistribute it and/or modify it under the terms of the GNU Affero "
              + "General Public License as published by the Free Software Foundation, either version 3 of the License, or "
              + "(at your option) any later version. This program is distributed in the hope that it will be useful, but "
              + "WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR "
              + "PURPOSE. See the GNU Affero General Public License for more details.");
      System.out
          .println("For source code and more information see http://diqube.org and http://github.com/diqube/diqube.");
      System.exit(1);
      return;
    }

    String[] remainingArgs = new String[args.length - 1];
    for (int i = 0; i < args.length - 1; i++)
      remainingArgs[i] = args[i + 1];

    toolFunctions.get(args[0]).execute(remainingArgs);
  }
}
