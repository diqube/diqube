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
package org.diqube.connection;

import org.apache.thrift.transport.TTransport;
import org.diqube.connection.Connection;
import org.diqube.connection.ConnectionFactory;
import org.diqube.connection.ConnectionPool;
import org.mockito.Mockito;

/**
 * Util class for unit tests which need to adjust {@link ConnectionPool}.
 * 
 * This is in the sam epackage as {@link ConnectionPool} to be able to use the package-level methods.
 *
 * @author Bastian Gloeckle
 */
public class ConnectionPoolTestUtil {
  public static void setConnectionFactory(ConnectionPool pool, ConnectionFactory factory) {
    pool.setConnectionFactory(factory);
  }

  public static <T> Connection<T> createConnection(ConnectionPool parentPool, Class<T> service) {
    return new Connection<T>(parentPool, service, Mockito.mock(service, Mockito.RETURNS_MOCKS),
        Mockito.mock(TTransport.class, Mockito.RETURNS_MOCKS), null);
  }
}
