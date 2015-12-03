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
package org.diqube.thriftutil;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

/**
 * {@link TTransport} facade that can remember bytes read/written.
 *
 * @author Bastian Gloeckle
 */
public class RememberingTransport extends TTransport {

  private TTransport delegate;
  private List<byte[]> rememberedReadBytes;
  private List<byte[]> rememberedWriteBytes;

  public RememberingTransport(TTransport delegate) {
    this.delegate = delegate;
  }

  public void startRemeberingReadBytes() {
    rememberedReadBytes = new ArrayList<>();
  }

  public byte[] stopRememberingReadBytes() {
    int resLen = 0;
    for (byte[] b : rememberedReadBytes)
      resLen += b.length;

    byte[] res = new byte[resLen];

    int outPos = 0;
    for (byte[] b : rememberedReadBytes) {
      System.arraycopy(b, 0, res, outPos, b.length);
      outPos += b.length;
    }

    rememberedReadBytes = null;
    return res;
  }

  public void startRemeberingWriteBytes() {
    rememberedWriteBytes = new ArrayList<>();
  }

  public byte[] stopRememberingWriteBytes() {
    int resLen = 0;
    for (byte[] b : rememberedWriteBytes)
      resLen += b.length;

    byte[] res = new byte[resLen];

    int outPos = 0;
    for (byte[] b : rememberedWriteBytes) {
      System.arraycopy(b, 0, res, outPos, b.length);
      outPos += b.length;
    }

    rememberedReadBytes = null;
    return res;
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public boolean peek() {
    return delegate.peek();
  }

  @Override
  public void open() throws TTransportException {
    delegate.open();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    int res = delegate.read(buf, off, len);
    if (rememberedReadBytes != null) {
      byte[] remember = new byte[len];
      System.arraycopy(buf, off, remember, 0, res);
      rememberedReadBytes.add(remember);
    }
    return res;
  }

  @Override
  public int readAll(byte[] buf, int off, int len) throws TTransportException {
    int res = delegate.readAll(buf, off, len);
    if (rememberedReadBytes != null) {
      byte[] remember = new byte[len];
      System.arraycopy(buf, off, remember, 0, res);
      rememberedReadBytes.add(remember);
    }
    return res;
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public void write(byte[] buf) throws TTransportException {
    delegate.write(buf);
    if (rememberedWriteBytes != null) {
      byte[] remember = new byte[buf.length];
      System.arraycopy(buf, 0, remember, 0, buf.length);
      rememberedWriteBytes.add(remember);
    }
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    delegate.write(buf, off, len);
    if (rememberedWriteBytes != null) {
      byte[] remember = new byte[len];
      System.arraycopy(buf, off, remember, 0, len);
      rememberedWriteBytes.add(remember);
    }
  }

  @Override
  public void flush() throws TTransportException {
    delegate.flush();
  }

  @Override
  public byte[] getBuffer() {
    // do not allow the underlying bufer to be read if there is one, as we cannot remember anything then.
    return null;
  }

  @Override
  public int getBufferPosition() {
    // do not allow the underlying bufer to be read if there is one, as we cannot remember anything then.
    return -1;
  }

  @Override
  public int getBytesRemainingInBuffer() {
    // do not allow the underlying bufer to be read if there is one, as we cannot remember anything then.
    return -1;
  }

  @Override
  public boolean equals(Object obj) {
    return delegate.equals(obj);
  }

  @Override
  public void consumeBuffer(int len) {
    // do not allow the underlying bufer to be read if there is one, as we cannot remember anything then.
    // noop.
  }

  @Override
  public String toString() {
    return "[RememberingTransport:" + delegate.toString() + "]";
  }

  public static class Factory extends TTransportFactory {
    private TTransportFactory delegateFactory;

    public Factory(TTransportFactory delegateFactory) {
      this.delegateFactory = delegateFactory;
    }

    @Override
    public TTransport getTransport(TTransport trans) {
      return new RememberingTransport(delegateFactory.getTransport(trans));
    }
  }
}
