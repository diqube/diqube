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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * This is a buffer of bytes that is accessible only using absolute indices. It is somewhat similar to
 * {@link ByteBuffer} but is not restricted to the 2GB limit.
 *
 * @author Bastian Gloeckle
 */
public class BigByteBuffer implements Closeable {
  private static final int DEFAULT_MAX_SINGLE_SIZE = Integer.MAX_VALUE;

  private ByteBuffer[] byteBuffers;
  private long totalSize;

  private long shardSize;

  public BigByteBuffer(FileChannel channel, MapMode mode, Consumer<MappedByteBuffer> initializer) throws IOException {
    this(channel, mode, initializer, DEFAULT_MAX_SINGLE_SIZE);
  }

  public BigByteBuffer(FileChannel channel, MapMode mode, Consumer<MappedByteBuffer> initializer, int maxSingleShardSize)
      throws IOException {
    int numberOfByteBuffers = (int) (channel.size() / maxSingleShardSize);

    if (channel.size() % maxSingleShardSize > 0)
      numberOfByteBuffers++;

    ByteBuffer[] bufs = new ByteBuffer[numberOfByteBuffers];
    for (int i = 0; i < numberOfByteBuffers; i++) {
      long sizeOfBuf = maxSingleShardSize;
      if (i == numberOfByteBuffers - 1)
        sizeOfBuf =
            (channel.size() % maxSingleShardSize != 0) ? channel.size() % maxSingleShardSize : maxSingleShardSize;

      MappedByteBuffer newBuf = channel.map(mode, i * maxSingleShardSize, sizeOfBuf);
      if (initializer != null)
        initializer.accept(newBuf);
      bufs[i] = newBuf;
    }
    this.byteBuffers = bufs;
    this.totalSize = channel.size();
    this.shardSize = maxSingleShardSize;
  }

  public BigByteBuffer(byte[] bytes) {
    this(new ByteBuffer[] { ByteBuffer.wrap(bytes) });
  }

  public BigByteBuffer(ByteBuffer[] byteBuffers) throws IllegalArgumentException {
    this(byteBuffers, findShardSize(byteBuffers));
  }

  protected BigByteBuffer(ByteBuffer[] byteBuffers, int bufferSize) {
    this.byteBuffers = byteBuffers;
    this.shardSize = bufferSize;
    this.totalSize = Stream.<ByteBuffer> of(byteBuffers).mapToLong(buf -> buf.limit()).sum();
  }

  private static int findShardSize(ByteBuffer[] bufs) throws IllegalArgumentException {
    if (bufs.length > 1) {
      // ensure all buffers unless the last one have the same length
      // the last buffer must have a length <= the other ones.
      long numberOfDistinctLengths = Stream.of(bufs).limit(bufs.length - 1).mapToInt(b -> b.limit()).distinct().count();
      if (numberOfDistinctLengths != 1 || bufs[bufs.length - 1].limit() > bufs[0].limit())
        throw new IllegalArgumentException("The provided ByteBuffers have invalid lengths.");
    }

    return bufs[0].limit();
  }

  /**
   * Get a single byte from a specific index.
   */
  public byte get(long byteIdx) throws ArrayIndexOutOfBoundsException {
    if (byteIdx < 0 || byteIdx >= totalSize)
      throw new ArrayIndexOutOfBoundsException("Tried to access index " + byteIdx + " on buffer of size " + totalSize);

    int bufIdx = (int) (byteIdx / shardSize);
    int idx = (int) (byteIdx % shardSize);
    return byteBuffers[bufIdx].get(idx);
  }

  /**
   * Get an array of bytes from the buffer, similar to {@link InputStream#read(byte[], int, int)}.
   * 
   * @param byteIdx
   *          The index of the first byte in the buffer to read.
   * @return number of bytes actually read.
   */
  public int get(long byteIdx, byte[] target, int targetOffset, int length) {
    if (target == null)
      throw new NullPointerException();

    if (byteIdx == totalSize)
      return -1;

    if (byteIdx < 0 || byteIdx > totalSize || length < 0 || target.length < targetOffset + length)
      throw new ArrayIndexOutOfBoundsException("Tried to access index " + byteIdx + " length " + length
          + " but size available is " + totalSize + ". Target arrays length is " + target.length + ", target offset "
          + targetOffset);

    if (length == 0)
      return 0;

    length = (int) Math.min(length, totalSize - byteIdx);

    int bufIdx = (int) (byteIdx / shardSize);
    int idx = (int) (byteIdx % shardSize);

    if (idx <= shardSize - length) {
      // single ByteBuffer contains result.
      synchronized (this) {
        byteBuffers[bufIdx].position(idx);
        byteBuffers[bufIdx].get(target, targetOffset, length);
        byteBuffers[bufIdx].rewind();
      }
    } else {
      // multiple ByteBuffers contain result.
      synchronized (this) {
        int firstLength = (int) (shardSize - idx);
        byteBuffers[bufIdx].position(idx);
        byteBuffers[bufIdx].get(target, targetOffset, firstLength);
        byteBuffers[bufIdx].rewind();
        int lengthLeft = length - firstLength;
        int i = 1;
        targetOffset += firstLength;
        while (lengthLeft > 0) {
          int lengthThisBuf;
          if (lengthLeft <= byteBuffers[bufIdx + i].limit())
            lengthThisBuf = lengthLeft;
          else
            lengthThisBuf = byteBuffers[bufIdx + i].limit();

          byteBuffers[bufIdx + i].rewind();
          byteBuffers[bufIdx + i].get(target, targetOffset, lengthThisBuf);
          byteBuffers[bufIdx + i].rewind();
          targetOffset += lengthThisBuf;
          lengthLeft -= lengthThisBuf;
          i++;
        }
      }
    }
    return length;
  }

  public long size() {
    return totalSize;
  }

  @Override
  public void close() throws IOException {
    byteBuffers = null;
  }

  /**
   * @return A new InputStream that will return all bytes contained in this {@link BigByteBuffer}.
   */
  public InputStream createInputStream() {
    return createPartialInputStream(0L, totalSize);
  }

  /**
   * @return A new InputStream that will return the bytes of this {@link BigByteBuffer} in the given index range.
   */
  public InputStream createPartialInputStream(long firstIdx, long lastIdxExclusive) {
    if (lastIdxExclusive > totalSize)
      return null;

    return new InputStream() {
      private long pos = firstIdx;

      @Override
      public int read() throws IOException {
        if (pos >= lastIdxExclusive)
          return -1;

        return BigByteBuffer.this.get(pos++);
      }

      @Override
      public long skip(long n) throws IOException {
        long skipTarget = Math.min(pos + n, lastIdxExclusive);
        long skippedBytes = skipTarget - pos;
        pos = skipTarget;
        return skippedBytes;
      }

      @Override
      public int available() throws IOException {
        return (int) (lastIdxExclusive - pos);
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        if (pos + len > lastIdxExclusive) {
          len = (int) (lastIdxExclusive - pos);
          if (len == 0)
            return -1;
        }

        int read = BigByteBuffer.this.get(pos, b, off, len);
        pos += read;
        return read;
      }

    };
  }
}
