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

import java.lang.reflect.Field;

import org.diqube.itest.annotations.NeedsProcessPid;
import org.springframework.util.ReflectionUtils;

/**
 * Util to find out the PID of a {@link Process}.
 * 
 * This works only on unix systems currently.
 *
 * @author Bastian Gloeckle
 */
public class ProcessPidUtil {

  /**
   * Try to find the process ID (PID) of the given process.
   * 
   * <p>
   * Use {@link NeedsProcessPid} annotation on test method.
   * 
   * @throws IllegalStateException
   *           If the PID cannot be identified.
   */
  public static int getPid(Process process) throws IllegalStateException {
    try {
      Field pidField = process.getClass().getDeclaredField("pid");
      ReflectionUtils.makeAccessible(pidField);
      return pidField.getInt(process);
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
      throw new IllegalStateException(
          "Could not find the PID of the server process, cannot get thread dump therefore.");
    }
  }
}
