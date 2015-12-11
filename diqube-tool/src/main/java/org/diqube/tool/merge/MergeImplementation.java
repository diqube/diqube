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
package org.diqube.tool.merge;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.diqube.context.Profiles;
import org.diqube.file.DiqubeFileFactory;
import org.diqube.file.DiqubeFileReader;
import org.diqube.file.DiqubeFileWriter;
import org.diqube.util.BigByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Implementation of merging two .diqube files into a single one.
 *
 * @author Bastian Gloeckle
 */
public class MergeImplementation {
  private static final Logger logger = LoggerFactory.getLogger(MergeImplementation.class);
  private File outputFile;
  private String comment;
  private List<File> inputFiles;

  public MergeImplementation(List<File> inputFiles, File outputFile, String comment) {
    this.inputFiles = inputFiles;
    this.outputFile = outputFile;
    this.comment = (comment != null) ? comment : "";
  }

  public void merge() {
    try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
      ctx.getEnvironment().setActiveProfiles(Profiles.CONFIG, Profiles.TOOL);
      ctx.scan("org.diqube");
      ctx.refresh();

      DiqubeFileFactory fileFactory = ctx.getBean(DiqubeFileFactory.class);

      Map<File, FileInfo> fileInfos = new HashMap<>();

      logger.info("Reading metadata of input files...");
      // instantiate DiqubeFileReaders, catch IOExceptions thrown as these indicate an invalid file.
      for (File inputFile : inputFiles) {
        try (FileChannel inputFileChannel = new RandomAccessFile(inputFile, "r").getChannel()) {
          DiqubeFileReader reader =
              fileFactory.createDiqubeFileReader(new BigByteBuffer(inputFileChannel, MapMode.READ_ONLY, null));

          FileInfo info = new FileInfo();
          info.totalNumberOfRows = reader.getNumberOfRows();
          info.numberOfTableShards = reader.getNumberOfTableShards();
          info.firstTableShardByte = reader.getTableShardDataFirstByteIndex();
          info.lastTableShardByte = reader.getTableShardDataLastByteIndex();

          fileInfos.put(inputFile, info);
        } catch (IOException e) {
          logger.error("Cannot read {}.", inputFile.getAbsolutePath(), e);
          return;
        }
      }

      try (FileOutputStream fos = new FileOutputStream(outputFile)) {
        try (DiqubeFileWriter fileWriter = fileFactory.createDiqubeFileWriter(fos)) {
          fileWriter.setComment(comment);

          for (File inputFile : inputFiles) {
            FileInfo fileInfo = fileInfos.get(inputFile);
            logger.info("Copying data of {}", inputFile.getAbsolutePath());

            try (RandomAccessFile inputRandomFile = new RandomAccessFile(inputFile, "r")) {
              BigByteBuffer buf = new BigByteBuffer(inputRandomFile.getChannel(), MapMode.READ_ONLY, null);
              fileWriter.writeSerializedTableShards(
                  buf.createPartialInputStream(fileInfo.firstTableShardByte, fileInfo.lastTableShardByte + 1),
                  fileInfo.totalNumberOfRows, fileInfo.numberOfTableShards);
              buf.close();
            }
          }
        }
      } catch (IOException e) {
        logger.error("Could not write output file", e);
        return;
      }
      logger.info("Done.");
    }
  }

  private static class FileInfo {
    private long totalNumberOfRows;
    private int numberOfTableShards;
    private long firstTableShardByte;
    private long lastTableShardByte;
  }
}
