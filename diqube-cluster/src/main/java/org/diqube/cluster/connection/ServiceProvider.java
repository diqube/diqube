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
package org.diqube.cluster.connection;

import java.io.Closeable;

/**
 * Provides a specific service and supports "closing" that service in a meaningful way as soon as it is not needed any
 * more.
 * 
 * <p>
 * {@link Closeable#close()} <b>must</b> be called by users of instances of this.
 * 
 * <p>
 * Note that typically calls to the service at {@link #getService()} need to be synchronized (at least when the service
 * is remote, as not multiple threads can write to the same connection simultaneously). Synchronize on this
 * {@link ServiceProvider} object.
 *
 * @author Bastian Gloeckle
 */
public interface ServiceProvider<T> extends Closeable {
  /**
   * @return The instance of the service this instance provides.
   * @throws IllegalStateException
   *           if connection not possible.
   */
  public T getService() throws IllegalStateException;

  /**
   * @return true if the service returned by {@link #getService()} is local (= no network connect), <code>false</code>
   *         otherwise.
   */
  public boolean isLocal();
}
