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
package org.diqube.tool.transpose;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.diqube.context.Profiles;
import org.diqube.data.column.ColumnType;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.DataSerializer.ObjectDoneConsumer;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.table.TableShard;
import org.diqube.file.DiqubeFileFactory;
import org.diqube.file.DiqubeFileWriter;
import org.diqube.loader.LoadException;
import org.diqube.loader.Loader;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.util.NullUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

/**
 * Implements transposing an input file into .diqube representation.
 *
 * @author Bastian Gloeckle
 */
public class TransposeImplementation {
  private static final Logger logger = LoggerFactory.getLogger(TransposeImplementation.class);

  private static final String TABLE_NAME = "TransposeImportTable";

  private File inputFile;
  private File outputFile;
  private File colInfoFile;

  private Class<? extends Loader> loaderClass;

  /**
   * @param colInfoFile
   *          can be <code>null</code>.
   */
  public TransposeImplementation(File inputFile, File outputFile, File colInfoFile,
      Class<? extends Loader> loaderClass) {
    this.inputFile = inputFile;
    this.outputFile = outputFile;
    this.colInfoFile = colInfoFile;
    this.loaderClass = loaderClass;
  }

  public void transpose() {
    logger.info("Starting diqube context...");

    try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
      ctx.getEnvironment().setActiveProfiles(Profiles.CONFIG, Profiles.TOOL);
      ctx.scan("org.diqube");
      ctx.refresh();

      DiqubeFileFactory fileFactory = ctx.getBean(DiqubeFileFactory.class);
      Loader loader = ctx.getBean(loaderClass);

      // for JSON it does not mater what we use here, as the JsonLoader will detect the col type automatically.
      LoaderColumnInfo colInfo = new LoaderColumnInfo(ColumnType.LONG);
      if (colInfoFile != null)
        colInfo = loadColInfo(colInfoFile);
      else
        logger.info("Using column info with default column type Long.");

      try (FileOutputStream outStream = new FileOutputStream(outputFile)) {
        logger.info("Starting to load data into temporary in-memory table '{}'", TABLE_NAME);

        // loader is either CSV or JSON, both return a single TableShard element!
        TableShard tableShard =
            Iterables.getOnlyElement(loader.load(0L, inputFile.getAbsolutePath(), TABLE_NAME, colInfo));

        logger.info("Data loaded into in-memory table '{}', starting to serialize that data into output file '{}'",
            TABLE_NAME, outputFile.getAbsolutePath());
        try (DiqubeFileWriter writer = fileFactory.createDiqubeFileWriter(outStream)) {
          writer.writeTableShard(tableShard, new ObjectDoneConsumer() {
            @Override
            public void accept(DataSerialization<?> t) {
              // right after we're done with serializing an object, we "null" all its properties to try to free up some
              // memory.
              NullUtil.setAllPropertiesToNull(t,
                  // just log on exception, it is not as bad if a field cannot be nulled.
                  (fieldToNull, e) -> logger.trace("Could not null {} on {}", fieldToNull, e));
            }
          });
        }
        logger.info("Successfully serialized data to '{}'", outputFile.getAbsolutePath());
      } catch (IOException | LoadException | SerializationException e) {
        logger.error("Could not proceed.", e);
        return;
      }
    }

    logger.info("Done.");
  }

  private LoaderColumnInfo loadColInfo(File colInfoFile) throws RuntimeException {
    Properties prop = new Properties();
    try (InputStream is = new FileInputStream(colInfoFile)) {
      prop.load(new InputStreamReader(is, Charset.forName("UTF-8")));

      logger.info("Loading column type info from '{}'", colInfoFile.getAbsolutePath());

      Map<String, ColumnType> colTypes = new HashMap<>();
      ColumnType defaultColType = ColumnType.LONG;

      Iterator<Object> it = Iterators.forEnumeration(prop.keys());
      while (it.hasNext()) {
        String colName = (String) it.next();
        if ("*".equals(colName))
          defaultColType = resolveColumnType(prop.getProperty(colName));
        else
          colTypes.put(colName, resolveColumnType(prop.getProperty(colName)));
      }

      LoaderColumnInfo res = new LoaderColumnInfo(defaultColType);
      for (Entry<String, ColumnType> e : colTypes.entrySet())
        res.registerColumnType(e.getKey(), e.getValue());

      logger.info("Using column information with default column type '{}' and specific column types: {}",
          defaultColType, colTypes);

      return res;
    } catch (IOException e) {
      throw new RuntimeException("Could not read Column Info file", e);
    }
  }

  private ColumnType resolveColumnType(String controlFileString) throws RuntimeException {
    try {
      return ColumnType.valueOf(controlFileString.toUpperCase());
    } catch (RuntimeException e) {
      throw new RuntimeException(controlFileString + " is no valid ColumnType.");
    }
  }

}
