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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * A thrift-usable socket customized for the use in diqube.
 *
 * @author Bastian Gloeckle
 */
public class DiqubeClientSocket extends TSocket {

  private SocketListener listener;
  private boolean listenerInformed = false;

  /**
   * @param timeout
   *          milliseconds
   */
  /* package */ DiqubeClientSocket(String host, int port, int timeout, SocketListener listener) {
    super(host, port, timeout);
    this.listener = listener;
  }

  @Override
  public void open() throws TTransportException {
    super.open();

    inputStream_ = new InputStreamFacade(inputStream_);
    outputStream_ = new OutputStreamFacade(outputStream_);
  }

  private synchronized void connectionDied() {
    if (!listenerInformed) {
      close();
      listener.connectionDied();
      listenerInformed = true;
    }
  }

  private class InputStreamFacade extends InputStream {
    private InputStream delegate;

    public InputStreamFacade(InputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public int read() throws IOException {
      try {
        return delegate.read();
      } catch (IOException e) {
        connectionDied();
        throw new IOException(e);
      }
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public int read(byte[] b) throws IOException {
      try {
        return delegate.read(b);
      } catch (IOException e) {
        connectionDied();
        throw new IOException(e);
      }
    }

    @Override
    public boolean equals(Object obj) {
      return delegate.equals(obj);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      try {
        return delegate.read(b, off, len);
      } catch (IOException e) {
        connectionDied();
        throw new IOException(e);
      }
    }

    @Override
    public long skip(long n) throws IOException {
      try {
        return delegate.skip(n);
      } catch (IOException e) {
        connectionDied();
        throw new IOException(e);
      }
    }

    @Override
    public String toString() {
      return delegate.toString();
    }

    @Override
    public int available() throws IOException {
      try {
        return delegate.available();
      } catch (IOException e) {
        connectionDied();
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        delegate.close();
      } catch (IOException e) {
        connectionDied();
        throw new IOException(e);
      }
    }

    @Override
    public void mark(int readlimit) {
      delegate.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
      delegate.reset();
    }

    @Override
    public boolean markSupported() {
      return delegate.markSupported();
    }

  }

  private class OutputStreamFacade extends OutputStream {
    private OutputStream delegate;

    public OutputStreamFacade(OutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(int b) throws IOException {
      try {
        delegate.write(b);
      } catch (IOException e) {
        connectionDied();
        throw new IOException(e);
      }
    }

    @Override
    public int hashCode() {
      return delegate.hashCode();
    }

    @Override
    public void write(byte[] b) throws IOException {
      try {
        delegate.write(b);
      } catch (IOException e) {
        connectionDied();
        throw new IOException(e);
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      try {
        delegate.write(b, off, len);
      } catch (IOException e) {
        connectionDied();
        throw new IOException(e);
      }
    }

    @Override
    public boolean equals(Object obj) {
      return delegate.equals(obj);
    }

    @Override
    public void flush() throws IOException {
      try {
        delegate.flush();
      } catch (IOException e) {
        connectionDied();
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        delegate.close();
      } catch (IOException e) {
        connectionDied();
        throw new IOException(e);
      }
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }
}
