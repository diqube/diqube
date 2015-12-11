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
package org.diqube.file.internaldb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.diqube.file.internaldb.v1.SInternalDbFileHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads "internal db" files which actually contain a collection of arbitrary thrift {@link TBase}s.
 *
 * <p>
 * InternalDb files do not only maintain a common file content, but also a common filename layout:
 * prefix-commitIndex-suffix. The commitIndex is the index of the consensus cluster commit that initiated this write.
 * This commitIndex is therefore increasing always, means higher index is newer file. The {@link InternalDbFileReader}
 * reads the newest available file.
 * 
 * @author Bastian Gloeckle
 */
public class InternalDbFileReader<T extends TBase<?, ?>> {
  private static final Logger logger = LoggerFactory.getLogger(InternalDbFileReader.class);
  private String dataType;
  private String filenamePrefix;
  private File inputDir;
  private Supplier<T> factory;

  public InternalDbFileReader(String dataType, String filenamePrefix, File inputDir, Supplier<T> factory) {
    this.dataType = dataType;
    this.filenamePrefix = filenamePrefix;
    this.inputDir = inputDir;
    this.factory = factory;
  }

  /**
   * Read the newest file.
   * 
   * @return <code>null</code> if no file is available.
   * @throws ReadException
   *           If file cannot be read.
   */
  public List<T> readNewest() throws ReadException {
    File inputFile = findFileToReadFrom();
    if (inputFile == null)
      return null;

    logger.info("Loading {} entities from '{}'...", dataType, inputFile.getAbsolutePath());
    try (FileInputStream fis = new FileInputStream(inputFile)) {
      try (TIOStreamTransport transport = new TIOStreamTransport(fis)) {
        TCompactProtocol protocol = new TCompactProtocol(transport);

        SInternalDbFileHeader header = new SInternalDbFileHeader();
        header.read(protocol);
        if (header.getVersion() != InternalDbFileWriter.VERSION)
          throw new ReadException("Bad version number: " + header.getVersion());
        if (!header.getDataType().equals(dataType))
          throw new ReadException("Bad data type. Expected: " + dataType + " but got: " + header.getDataType());

        List<T> res = new ArrayList<>();

        long size = header.getSize();
        while (size-- > 0) {
          T newObj = factory.get();
          newObj.read(protocol);
          res.add(newObj);
        }

        logger.info("Loaded {} entities from '{}'.", dataType, inputFile.getAbsolutePath());
        return res;
      } catch (TException e) {
        throw new ReadException("Could not read identities from " + inputFile.getAbsolutePath(), e);
      }
    } catch (IOException e1) {
      throw new ReadException("Could not read identities from " + inputFile.getAbsolutePath(), e1);
    }
  }

  private File findFileToReadFrom() throws ReadException {
    File[] allDbFiles = inputDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return dir.equals(inputDir) && name.startsWith(filenamePrefix)
            && name.endsWith(InternalDbFileWriter.FILENAME_SUFFIX);
      }
    });

    if (allDbFiles.length == 0)
      return null;
    if (allDbFiles.length == 1) {
      return allDbFiles[0];
    } else {
      OptionalLong maxId =
          Stream.of(allDbFiles)
              .map(f -> f.getName().substring(filenamePrefix.length(),
                  f.getName().length() - InternalDbFileWriter.FILENAME_SUFFIX.length()))
              .mapToLong(Long::parseLong).max();
      if (!maxId.isPresent())
        throw new ReadException("Could not identify maximum internaldb file of type " + dataType);
      File dbFile = Stream.of(allDbFiles)
          .filter(f -> f.getName().endsWith(maxId + InternalDbFileWriter.FILENAME_SUFFIX)).findAny().get();

      return dbFile;
    }
  }

  public static class ReadException extends Exception {
    private static final long serialVersionUID = 1L;

    /* package */ ReadException(String msg) {
      super(msg);
    }

    /* package */ ReadException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
