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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.diqube.cluster.connection.DiqubeClientSocketTestFactory;
import org.diqube.itest.util.ServerControl.ServerAddr;

/**
 * Factory for thrift connections that should be used in tests.
 *
 * @author Bastian Gloeckle
 */
public class TestThriftConnectionFactory {

  /**
   * Open a connection to the given remote.
   * 
   * @throws TestConnectionException
   *           If anything goes wrong.
   */
  public static <O> TestConnection<O> open(ServerAddr addr, Class<? extends O> thriftClientClass, String serviceName)
      throws TestConnectionException {
    try {
      TTransport transport = DiqubeClientSocketTestFactory.createSocket(addr.getHost(), addr.getPort(), 1000, () -> {
      });
      transport = new TFramedTransport(transport);
      TProtocol protocol = new TMultiplexedProtocol(new TCompactProtocol(transport), serviceName);
      O client = thriftClientClass.getConstructor(TProtocol.class).newInstance(protocol);
      transport.open();

      return new TestConnection<O>(transport, client);
    } catch (RuntimeException | InstantiationException | IllegalAccessException | InvocationTargetException
        | NoSuchMethodException | TTransportException e) {
      throw new TestConnectionException("Could not open connection", e);
    }
  }

  public static class TestConnectionException extends Exception {
    private static final long serialVersionUID = 1L;

    TestConnectionException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  /**
   * Connection to a remote that is closable.
   */
  public static class TestConnection<O> implements Closeable {
    private O client;
    private TTransport transport;

    private TestConnection(TTransport transport, O client) {
      this.transport = transport;
      this.client = client;
    }

    public O getService() {
      return client;
    }

    @Override
    public void close() throws IOException {
      transport.close();
    }
  }
}
