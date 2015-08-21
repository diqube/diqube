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

import java.util.function.Supplier;

/**
 * Helper class capable of waiting until a specific circumstance is true.
 *
 * @author Bastian Gloeckle
 */
public class Waiter {
  /**
   * Block this thread until the checkFn returns true.
   * 
   * @param waitFor
   *          Strign describing what we wait for.
   * @param timeoutSeconds
   *          Number of seconds after which we timeout.
   * @param retryMs
   *          Number of milliseconds to wait between calling the checkFn.
   * @param checkFn
   *          The function which returns true if the expected circumstance is true.
   * @throws InterruptedException
   *           If interrupted.
   */
  public void waitUntil(String waitFor, int timeoutSeconds, int retryMs, Supplier<Boolean> checkFn) {
    if (checkFn.get())
      return;

    Object sync = new Object();
    int maxRetries = (int) Math.ceil(timeoutSeconds * 1000. / retryMs);
    for (int i = 0; i < maxRetries; i++) {
      synchronized (sync) {
        try {
          sync.wait(retryMs);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while waiting for: " + waitFor);
        }
      }

      if (checkFn.get())
        return;
    }

    throw new RuntimeException("Timed out (" + timeoutSeconds + " sec) waiting for: " + waitFor);
  }
}
