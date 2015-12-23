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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 *
 * @author Bastian Gloeckle
 */
public class BigByteBufferTest {

  private List<Runnable> cleanupMethods = new ArrayList<>();

  @AfterMethod
  public void cleanup() {
    for (Runnable cleanupRun : cleanupMethods)
      cleanupRun.run();
    cleanupMethods.clear();
  }

  @Test
  public void singleShardTest() throws IOException {
    // GIVEN
    try (BigByteBuffer buf = new BigByteBuffer(new byte[] { 1, 2, 3, 4, 5 })) {

      // WHEN THEN
      Assert.assertEquals(1, buf.get(0));
      Assert.assertEquals(5, buf.get(4));

      byte[] tmp = new byte[2];
      buf.get(0, tmp, 0, 2);
      Assert.assertEquals(new byte[] { 1, 2 }, tmp);

      tmp = new byte[3];
      tmp[0] = 0;
      buf.get(0, tmp, 1, 2);
      Assert.assertEquals(new byte[] { 0, 1, 2 }, tmp);

      tmp = new byte[3];
      buf.get(2, tmp, 0, 3);
      Assert.assertEquals(new byte[] { 3, 4, 5 }, tmp);

      tmp = new byte[5];
      buf.get(0, tmp, 0, 5);
      Assert.assertEquals(new byte[] { 1, 2, 3, 4, 5 }, tmp);

      tmp = new byte[1];
      buf.get(4, tmp, 0, 1);
      Assert.assertEquals(new byte[] { 5 }, tmp);
    }
  }

  @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
  public void outOfBoundsTest() throws IOException {
    // GIVEN
    try (BigByteBuffer buf = new BigByteBuffer(new byte[] { 1 })) {

      // WHEN THEN
      buf.get(1);
    }
  }

  @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
  public void negativeTest() throws IOException {
    // GIVEN
    try (BigByteBuffer buf = new BigByteBuffer(new byte[] { 1, 2 })) {

      // WHEN THEN
      buf.get(-1);
    }
  }

  @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
  public void wrongTargetIndexTest() throws IOException {
    // GIVEN
    try (BigByteBuffer buf = new BigByteBuffer(new byte[] { 1, 2 })) {

      // WHEN THEN
      byte[] tmp = new byte[2];
      buf.get(0, tmp, 1, 2);
    }
  }

  @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
  public void negativeLengthTest() throws IOException {
    // GIVEN
    try (BigByteBuffer buf = new BigByteBuffer(new byte[] { 1, 2 })) {

      // WHEN THEN
      byte[] tmp = new byte[1];
      buf.get(0, tmp, 1, -1);
    }
  }

  @Test
  public void twoShardTest() throws IOException {
    // GIVEN
    ByteBuffer buf1 = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4, 5 });
    ByteBuffer buf2 = ByteBuffer.wrap(new byte[] { 11, 12, 13, 14, 15 });
    try (BigByteBuffer buf = new BigByteBuffer(new ByteBuffer[] { buf1, buf2 })) {

      // WHEN THEN
      Assert.assertEquals(1, buf.get(0));
      Assert.assertEquals(5, buf.get(4));
      Assert.assertEquals(11, buf.get(5));
      Assert.assertEquals(15, buf.get(9));

      byte[] tmp = new byte[5];
      buf.get(0, tmp, 0, 5);
      Assert.assertEquals(new byte[] { 1, 2, 3, 4, 5 }, tmp);

      tmp = new byte[5];
      buf.get(5, tmp, 0, 5);
      Assert.assertEquals(new byte[] { 11, 12, 13, 14, 15 }, tmp);

      tmp = new byte[2];
      buf.get(4, tmp, 0, 2);
      Assert.assertEquals(new byte[] { 5, 11 }, tmp);

      tmp = new byte[10];
      buf.get(0, tmp, 0, 10);
      Assert.assertEquals(new byte[] { 1, 2, 3, 4, 5, 11, 12, 13, 14, 15 }, tmp);

      tmp = new byte[9];
      buf.get(1, tmp, 0, 9);
      Assert.assertEquals(new byte[] { 2, 3, 4, 5, 11, 12, 13, 14, 15 }, tmp);

      tmp = new byte[11];
      tmp[0] = 0;
      buf.get(0, tmp, 1, 10);
      Assert.assertEquals(new byte[] { 0, 1, 2, 3, 4, 5, 11, 12, 13, 14, 15 }, tmp);
    }
  }

  @Test
  public void filePerfectMappingTest() throws IOException {
    // GIVEN
    File tmpFile = createTempFile();
    FileOutputStream fos = new FileOutputStream(tmpFile);
    fos.write(new byte[] { 1, 2, 3, 4, 5, 11, 12, 13, 14, 15 });
    fos.flush();
    fos.close();
    try (RandomAccessFile f = new RandomAccessFile(tmpFile, "r")) {
      try (BigByteBuffer buf = new BigByteBuffer(f.getChannel(), MapMode.READ_ONLY, b -> b.load(), 5 // 5 per shard,
                                                                                                     // maps perfect on
                                                                                                     // 10 length
      )) {

        // WHEN THEN
        Assert.assertEquals(1, buf.get(0));
        Assert.assertEquals(5, buf.get(4));
        Assert.assertEquals(11, buf.get(5));
        Assert.assertEquals(15, buf.get(9));

        byte[] tmp = new byte[5];
        buf.get(0, tmp, 0, 5);
        Assert.assertEquals(new byte[] { 1, 2, 3, 4, 5 }, tmp);

        tmp = new byte[5];
        buf.get(5, tmp, 0, 5);
        Assert.assertEquals(new byte[] { 11, 12, 13, 14, 15 }, tmp);

        tmp = new byte[2];
        buf.get(4, tmp, 0, 2);
        Assert.assertEquals(new byte[] { 5, 11 }, tmp);

        tmp = new byte[10];
        buf.get(0, tmp, 0, 10);
        Assert.assertEquals(new byte[] { 1, 2, 3, 4, 5, 11, 12, 13, 14, 15 }, tmp);

        tmp = new byte[9];
        buf.get(1, tmp, 0, 9);
        Assert.assertEquals(new byte[] { 2, 3, 4, 5, 11, 12, 13, 14, 15 }, tmp);

        tmp = new byte[11];
        tmp[0] = 0;
        buf.get(0, tmp, 1, 10);
        Assert.assertEquals(new byte[] { 0, 1, 2, 3, 4, 5, 11, 12, 13, 14, 15 }, tmp);
      }
    }
  }

  @Test
  public void fileUnperfectMappingTest() throws IOException {
    // GIVEN
    File tmpFile = createTempFile();
    FileOutputStream fos = new FileOutputStream(tmpFile);
    fos.write(new byte[] { 1, 2, 3, 4, 5, 11, 12, 13, 14, 15 });
    fos.flush();
    fos.close();
    try (RandomAccessFile f = new RandomAccessFile(tmpFile, "r")) {
      try (BigByteBuffer buf = new BigByteBuffer(f.getChannel(), MapMode.READ_ONLY, b -> b.load(), 4 // 4 per shard
      )) {

        // WHEN THEN
        Assert.assertEquals(1, buf.get(0));
        Assert.assertEquals(4, buf.get(3));
        Assert.assertEquals(5, buf.get(4));
        Assert.assertEquals(13, buf.get(7));
        Assert.assertEquals(14, buf.get(8));
        Assert.assertEquals(15, buf.get(9));

        byte[] tmp = new byte[5];
        buf.get(0, tmp, 0, 5);
        Assert.assertEquals(new byte[] { 1, 2, 3, 4, 5 }, tmp);

        tmp = new byte[5];
        buf.get(5, tmp, 0, 5);
        Assert.assertEquals(new byte[] { 11, 12, 13, 14, 15 }, tmp);

        tmp = new byte[2];
        buf.get(4, tmp, 0, 2);
        Assert.assertEquals(new byte[] { 5, 11 }, tmp);

        tmp = new byte[10];
        buf.get(0, tmp, 0, 10);
        Assert.assertEquals(new byte[] { 1, 2, 3, 4, 5, 11, 12, 13, 14, 15 }, tmp);

        tmp = new byte[9];
        buf.get(1, tmp, 0, 9);
        Assert.assertEquals(new byte[] { 2, 3, 4, 5, 11, 12, 13, 14, 15 }, tmp);

        tmp = new byte[11];
        tmp[0] = 0;
        buf.get(0, tmp, 1, 10);
        Assert.assertEquals(new byte[] { 0, 1, 2, 3, 4, 5, 11, 12, 13, 14, 15 }, tmp);

        tmp = new byte[4];
        int readCount = buf.get(7, tmp, 0, 4);
        Assert.assertEquals(new byte[] { 13, 14, 15, 0 }, tmp);
        Assert.assertEquals(3, readCount);

        tmp = new byte[4];
        readCount = buf.get(10, tmp, 0, 4);
        Assert.assertEquals(-1, readCount);
      }
    }
  }

  @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
  public void fileUnperfectMappingExceptionTest() throws IOException {
    // GIVEN
    File tmpFile = createTempFile();
    FileOutputStream fos = new FileOutputStream(tmpFile);
    fos.write(new byte[] { 1, 2, 3, 4, 5, 11, 12, 13, 14, 15 });
    fos.flush();
    fos.close();
    try (RandomAccessFile f = new RandomAccessFile(tmpFile, "r")) {
      try (BigByteBuffer buf = new BigByteBuffer(f.getChannel(), MapMode.READ_ONLY, b -> b.load(), 4 // 4 per shard
      )) {

        // WHEN THEN
        byte[] tmp = new byte[4];
        buf.get(11, tmp, 0, 4);
      }
    }
  }

  @Test
  public void bigFileSimulationTest() throws IOException {
    FileChannel mockedChannel = Mockito.mock(FileChannel.class);

    // use a file size that is larger than an int.
    Mockito.when(mockedChannel.size()).thenReturn(Integer.MAX_VALUE * 100L);
    try (BigByteBuffer buf = new BigByteBuffer(mockedChannel, MapMode.READ_ONLY, null)) {
      // expected: no exception.
      // Note that we do not test to read from that BigByteBuffer here, since we cannot mock MappedByteBuffer nicely,
      // since it has final methods. But the other methods test reading from BigByteBuffers with multiple internal
      // MappedByteBuffers, so we should be fine.
    }
  }

  private File createTempFile() throws IOException {
    File res = File.createTempFile(this.getClass().getSimpleName(), "tmp");
    cleanupMethods.add(new Runnable() {
      @Override
      public void run() {
        res.delete();
      }
    });
    return res;
  }
}
