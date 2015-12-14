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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.diqube.file.internaldb.v1.SInternalDbFileHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer for files of "internal db" that writes and reads arbitrary collections of thrift {@link TBase}s.
 * 
 * <p>
 * InternalDb files do not only maintain a common file content, but also a common filename layout:
 * prefix-commitIndex-suffix. The commitIndex is the index of the consensus cluster commit that initiated this write.
 * This commitIndex is therefore increasing always, means higher index is newer file. The {@link InternalDbFileWriter}
 * deletes old files.
 *
 * @author Bastian Gloeckle
 */
public class InternalDbFileWriter<T extends TBase<?, ?>> {
  private static final Logger logger = LoggerFactory.getLogger(InternalDbFileWriter.class);
  /** File version currently supported by {@link InternalDbFileWriter} and {@link InternalDbFileReader} */
  /* package */ static final int VERSION = 1;
  /* package */static final String FILENAME_SUFFIX = ".db";

  private File outputDir;
  private String dataType;
  private String filenamePrefix;

  public InternalDbFileWriter(String dataType, String filenamePrefix, File outputDir) {
    this.dataType = dataType;
    this.filenamePrefix = filenamePrefix;
    this.outputDir = outputDir;
  }

  public void write(long consensusCommitIndex, List<T> entities) throws WriteException {
    File[] filesToDelete =
        outputDir.listFiles((dir, name) -> name.startsWith(filenamePrefix) && name.endsWith(FILENAME_SUFFIX));

    File newFile = new File(outputDir, filenamePrefix + String.format("%020d", consensusCommitIndex) + FILENAME_SUFFIX);

    logger.info("Writing updated {} entities to '{}'...", dataType, newFile.getAbsolutePath());
    try (FileOutputStream fos = new FileOutputStream(newFile)) {
      try (TIOStreamTransport transport = new TIOStreamTransport(fos)) {
        TCompactProtocol protocol = new TCompactProtocol(transport);

        SInternalDbFileHeader header = new SInternalDbFileHeader();
        header.setVersion(VERSION);
        header.setDataType(dataType);
        header.setSize(entities.size());
        header.write(protocol);

        for (T e : entities) {
          e.write(protocol);
        }
        logger.info("Updated internaldb file '{}'.", newFile.getAbsolutePath());

        for (File f : filesToDelete) {
          logger.info("Deleting old internaldb file '{}'", f.getAbsolutePath());

          if (InternalDbFileUtil.parseCommitIndex(f, filenamePrefix, FILENAME_SUFFIX) > consensusCommitIndex)
            // This could mean that consensus replays some commits during startup. That replay might be broken, if e.g.
            // SerializationExceptions happen during the process and the classes of the serialized objects changed (=
            // new version installed?).
            logger.warn("Overwriting a presumably newer version of an {} internalDb file with an older version.",
                dataType);

          f.delete();
        }
      } catch (TException e) {
        throw new WriteException("Could not write internaldb file '" + newFile.getAbsolutePath() + "'", e);
      }
    } catch (IOException e1) {
      throw new WriteException("Could not write internaldb file '" + newFile.getAbsolutePath() + "'", e1);
    }
  }

  public static class WriteException extends Exception {
    private static final long serialVersionUID = 1L;

    /* package */ WriteException(String msg) {
      super(msg);
    }

    /* package */ WriteException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
