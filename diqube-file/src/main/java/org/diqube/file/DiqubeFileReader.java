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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.diqube.data.TableShard;
import org.diqube.data.serialize.DataDeserializer;
import org.diqube.data.serialize.DataSerializer;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.file.v1.SDiqubeFileFooter;
import org.diqube.file.v1.SDiqubeFileFooterInfo;
import org.diqube.file.v1.SDiqubeFileHeader;
import org.diqube.util.BigByteBuffer;
import org.diqube.util.ReadCountInputStream;

/**
 * Reads a single .diqube file and is capable of deserializing the {@link TableShard}s stored in it.
 *
 * @author Bastian Gloeckle
 */
public class DiqubeFileReader {
  /** Number of bytes {@link SDiqubeFileFooterInfo} takes up at the end of the file. */
  private static long FILE_FOOTER_LENGTH_BYTES = -1L;

  private DataDeserializer deserializer;

  private SDiqubeFileFooter footer;
  private BigByteBuffer data;
  private long firstTableShardByteIndex;
  private long lastTableShardByteIndex;

  /* package */ DiqubeFileReader(DataDeserializer deserializer, BigByteBuffer data) throws IOException {
    this.deserializer = deserializer;
    this.data = data;

    // validate file header.
    try (ReadCountInputStream is = new ReadCountInputStream(data.createInputStream())) {
      TIOStreamTransport transport = new TIOStreamTransport(is);
      TProtocol compactProt = new TCompactProtocol(transport);

      SDiqubeFileHeader header = new SDiqubeFileHeader();
      header.read(compactProt);

      // first TableShard byte is followed the SDiqubeFileHeader directly.
      firstTableShardByteIndex = is.getNumberOfBytesRead();

      if (!DiqubeFileWriter.MAGIC_STRING.equals(header.getMagic()))
        throw new IOException("File is invalid.");

      if (header.getFileVersion() != DiqubeFileWriter.FILE_VERSION)
        throw new IOException("Only file version " + DiqubeFileWriter.FILE_VERSION + " supported, but found version "
            + header.getFileVersion());

      if (header.getContentVersion() != DataSerializer.DATA_VERSION)
        throw new IOException("Only content version " + DataSerializer.DATA_VERSION + " supported, but found version "
            + header.getContentVersion());
    } catch (TException e) {
      throw new IOException("Could not load file header", e);
    }

    if (FILE_FOOTER_LENGTH_BYTES == -1) {
      // calculate the length of SDiqubeFileFooterInfo. This is constant and equal for all files, as it is
      // de-/serialized using TBinaryProtocol.
      SDiqubeFileFooterInfo fileFooterInfo = new SDiqubeFileFooterInfo();
      fileFooterInfo.setFooterLengthBytes(1);
      try {
        byte[] fileFooterInfoBytes = new TSerializer(new TBinaryProtocol.Factory()).serialize(fileFooterInfo);
        FILE_FOOTER_LENGTH_BYTES = fileFooterInfoBytes.length;
      } catch (TException e) {
        throw new IOException("Could not calculate length of SDiqubeFileFooterInfo", e);
      }
    }

    // read footer info
    int footerLengthBytes;

    try (InputStream is = data.createPartialInputStream(data.size() - FILE_FOOTER_LENGTH_BYTES, data.size())) {
      TIOStreamTransport transport = new TIOStreamTransport(is);
      TProtocol binaryProt = new TBinaryProtocol(transport);

      SDiqubeFileFooterInfo footerInfo = new SDiqubeFileFooterInfo();
      footerInfo.read(binaryProt);
      footerLengthBytes = footerInfo.getFooterLengthBytes();
    } catch (TException e) {
      throw new IOException("Could not read length of file footer", e);
    }

    lastTableShardByteIndex = data.size() - FILE_FOOTER_LENGTH_BYTES - footerLengthBytes - 1;

    // read footer.
    try (InputStream is = data.createPartialInputStream(lastTableShardByteIndex + 1, data.size())) {
      TIOStreamTransport transport = new TIOStreamTransport(is);
      TProtocol compactProt = new TCompactProtocol(transport);

      footer = new SDiqubeFileFooter();
      footer.read(compactProt);
    } catch (TException e) {
      throw new IOException("Could not read footer", e);
    }
  }

  /**
   * @return The number of rows contained in the file (sum of the number of rows of all table shards in the file).
   */
  public long getNumberOfRows() {
    return footer.getNumberOfRows();
  }

  /**
   * @return Number of {@link TableShard}s stored in the file.
   */
  public int getNumberOfTableShards() {
    return footer.getNumberOfTableShards();
  }

  /**
   * @return The comment that is stored in the file.
   */
  public String getComment() {
    return footer.getComment();
  }

  /**
   * Expert: Get the index of the first byte in the file that contains data for a TableShard.
   */
  public long getTableShardDataFirstByteIndex() {
    return firstTableShardByteIndex;
  }

  /**
   * Expert: Get the index of the last byte in the file that contains data for a TableShard.
   */
  public long getTableShardDataLastByteIndex() {
    return lastTableShardByteIndex;
  }

  /**
   * Deserializes all {@link TableShard}s stored in the file.
   */
  public Collection<TableShard> loadAllTableShards() throws IOException, DeserializationException {
    List<TableShard> res = new ArrayList<>();

    try (InputStream is = data.createInputStream()) {
      TIOStreamTransport transport = new TIOStreamTransport(is);
      TProtocol compactProt = new TCompactProtocol(transport);

      SDiqubeFileHeader header = new SDiqubeFileHeader();
      header.read(compactProt);

      for (int i = 0; i < getNumberOfTableShards(); i++) {
        TableShard tableShard = deserializer.deserialize(TableShard.class, is);
        res.add(tableShard);
      }
    } catch (TException e) {
      throw new IOException("Could not load table shards", e);
    }

    return res;
  }
}
