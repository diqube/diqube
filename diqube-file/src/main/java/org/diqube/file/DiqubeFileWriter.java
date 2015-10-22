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
package org.diqube.file;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.diqube.buildinfo.BuildInfo;
import org.diqube.data.serialize.DataSerializer;
import org.diqube.data.serialize.DataSerializer.ObjectDoneConsumer;
import org.diqube.data.table.TableShard;
import org.diqube.data.serialize.SerializationException;
import org.diqube.file.v1.SDiqubeFileFooter;
import org.diqube.file.v1.SDiqubeFileFooterInfo;
import org.diqube.file.v1.SDiqubeFileHeader;

import com.google.common.io.ByteStreams;

/**
 * Writes a single .diqube file which can contain multiple serialized {@link TableShard}s.
 * 
 * This class is {@link Closeable} and {@link #close()} needs to be called therefore. It will though not close the
 * {@link OutputStream} automatically, which it writes to.
 *
 * @author Bastian Gloeckle
 */
public class DiqubeFileWriter implements Closeable {
  public static final String MAGIC_STRING = "diqube";
  public static final int FILE_VERSION = 1;

  private DataSerializer serializer;
  private OutputStream outputStream;

  private TSerializer compactSerializer = new TSerializer(new TCompactProtocol.Factory());

  private long numberOfRows = 0;
  private int numberOfTableShards = 0;
  private String comment = null;

  /* package */ DiqubeFileWriter(DataSerializer serializer, OutputStream outputStream) throws IOException {
    this.serializer = serializer;
    this.outputStream = outputStream;

    SDiqubeFileHeader fileHeader = new SDiqubeFileHeader();
    fileHeader.setMagic(MAGIC_STRING);
    fileHeader.setFileVersion(FILE_VERSION);
    fileHeader.setContentVersion(DataSerializer.DATA_VERSION);
    fileHeader.setWriterBuildGitCommit(BuildInfo.getGitCommitLong());
    fileHeader.setWriterBuildTimestamp(BuildInfo.getTimestamp());

    try {
      byte[] headerBytes = compactSerializer.serialize(fileHeader);
      outputStream.write(headerBytes);
      outputStream.flush();
    } catch (TException | IOException e) {
      throw new IOException("Could not serialize/write file header", e);
    }
  }

  /**
   * Serialize & write a TableShard into the output stream and flush that stream.
   * 
   * @param tableShard
   *          The shard to serialize & write
   * @param objectDoneConsumer
   *          Called after serialization is done on a specific object, see
   *          {@link DataSerializer#serialize(org.diqube.data.serialize.DataSerialization, OutputStream, ObjectDoneConsumer)}
   * @throws SerializationException
   *           Thrown if anything happens.
   */
  public void writeTableShard(TableShard tableShard, ObjectDoneConsumer objectDoneConsumer)
      throws SerializationException {
    // remember number of rows before the objectDoneConsumer is called, but add the number of rows only after
    // serializing, if an exception is thrown.
    long numberOfRowsDelta = tableShard.getNumberOfRowsInShard();
    serializer.serialize(tableShard, outputStream, objectDoneConsumer);
    numberOfTableShards++;
    numberOfRows += numberOfRowsDelta;
  }

  /**
   * Write data of already serialized table shards to the file.
   * 
   * @param serializedTableShards
   *          The serialized data of one or multiple table shards
   * @param totalNumberOfRows
   *          The total number of rows all the TableShards contain
   * @param numberOfTableShards
   *          The number of table shards that are provided
   * @throws IOException
   *           If anything cannot be written.
   */
  public void writeSerializedTableShards(InputStream serializedTableShards, long totalNumberOfRows,
      int numberOfTableShards) throws IOException {
    ByteStreams.copy(serializedTableShards, outputStream);
    outputStream.flush();
    this.numberOfTableShards += numberOfTableShards;
    this.numberOfRows += totalNumberOfRows;
  }

  /**
   * Writes the files footer.
   */
  @Override
  public void close() throws IOException {
    SDiqubeFileFooter footer = new SDiqubeFileFooter();
    footer.setComment((comment != null) ? comment : "");
    footer.setNumberOfRows(numberOfRows);
    footer.setNumberOfTableShards(numberOfTableShards);

    try {
      byte[] footerBytes = compactSerializer.serialize(footer);
      outputStream.write(footerBytes);

      SDiqubeFileFooterInfo fileFooterInfo = new SDiqubeFileFooterInfo();
      fileFooterInfo.setFooterLengthBytes(footerBytes.length);
      byte[] fileFooterInfoBytes = new TSerializer(new TBinaryProtocol.Factory()).serialize(fileFooterInfo);
      outputStream.write(fileFooterInfoBytes);

      outputStream.flush();
    } catch (TException | IOException e) {
      throw new IOException("Could not serialize/write footer", e);
    }
  }

  /**
   * @param comment
   *          Put this comment string in the generated file. Call this before {@link #close()}.
   */
  public void setComment(String comment) {
    this.comment = comment;
  }

}
