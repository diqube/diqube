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
package org.diqube.tool.version;

import org.diqube.buildinfo.BuildInfo;
import org.diqube.tool.ToolFunction;
import org.diqube.tool.ToolFunctionName;

/**
 * {@link ToolFunction} which simply displays some version information.
 *
 * @author Bastian Gloeckle
 */
@ToolFunctionName(Version.FUNCTION_NAME)
public class Version implements ToolFunction {
  public static final String FUNCTION_NAME = "version";

  @Override
  public void execute(String[] args) {
    System.out.println("diqube-tool");
    System.out.println();
    System.out.println("Copyright (C) 2015 Bastian Gloeckle");
    System.out.println();
    System.out
        .println("diqube is free software: you can redistribute it and/or modify it under the terms of the GNU Affero "
            + "General Public License as published by the Free Software Foundation, either version 3 of the License, or "
            + "(at your option) any later version. This program is distributed in the hope that it will be useful, but "
            + "WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR "
            + "PURPOSE. See the GNU Affero General Public License for more details.");
    System.out
        .println("For source code and more information see http://diqube.org and http://github.com/diqube/diqube.");
    System.out.println();
    System.out.println("Build commit:\t\t" + BuildInfo.getGitCommitLong());
    System.out.println("Build timestamp:\t" + BuildInfo.getTimestamp());
  }
}
