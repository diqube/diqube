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
package org.diqube.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * A InputStream facade that counts the number of bytes that have been read from the InputStream.
 *
 * @author Bastian Gloeckle
 */
public class ReadCountInputStream extends InputStream {

  private InputStream delegate;
  private long numberOfBytesRead = 0L;

  public ReadCountInputStream(InputStream delegate) {
    this.delegate = delegate;
  }

  /**
   * @return Number of bytes that have been read from this stream until now
   */
  public long getNumberOfBytesRead() {
    return numberOfBytesRead;
  }

  @Override
  public int read() throws IOException {
    int res = delegate.read();
    if (res != -1)
      numberOfBytesRead++;
    return res;
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public int read(byte[] b) throws IOException {
    int res = delegate.read(b);
    if (res != -1)
      numberOfBytesRead += res;
    return res;
  }

  @Override
  public boolean equals(Object obj) {
    return delegate.equals(obj);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int res = delegate.read(b, off, len);
    if (res != -1)
      numberOfBytesRead += res;
    return res;
  }

  @Override
  public long skip(long n) throws IOException {
    return delegate.skip(n);
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  @Override
  public int available() throws IOException {
    return delegate.available();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
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
