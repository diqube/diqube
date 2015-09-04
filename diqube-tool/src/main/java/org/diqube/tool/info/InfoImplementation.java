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
package org.diqube.tool.info;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.diqube.file.DiqubeFileFactory;
import org.diqube.file.DiqubeFileReader;
import org.diqube.util.BigByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 *
 * @author Bastian Gloeckle
 */
public class InfoImplementation {
  public static final Logger logger = LoggerFactory.getLogger(InfoImplementation.class);
  private File diqubeFile;

  public InfoImplementation(File diqubeFile) {
    this.diqubeFile = diqubeFile;
  }

  public void printInfo() {
    try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
      // do not enable newDataWatcher and/or Config.
      ctx.scan("org.diqube");
      ctx.refresh();

      DiqubeFileFactory fileFactory = ctx.getBean(DiqubeFileFactory.class);

      try (FileChannel channel = new RandomAccessFile(diqubeFile, "r").getChannel()) {
        BigByteBuffer fileBigByteBuffer = new BigByteBuffer(channel, MapMode.READ_ONLY, null);
        DiqubeFileReader reader = fileFactory.createDiqubeFileReader(fileBigByteBuffer);

        System.out.println("Number of table shards:\t" + reader.getNumberOfTableShards());
        System.out.println("Total number of rows:\t" + reader.getNumberOfRows());
        System.out.println("Comment:\t\t" + reader.getComment());
      } catch (IOException e) {
        logger.error("Could not find information", e);
      }
    }
  }
}
