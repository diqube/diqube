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
package org.diqube.tranpose;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.diqube.data.ColumnType;
import org.diqube.data.TableShard;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.DataSerializationManager;
import org.diqube.data.serialize.DataSerializer;
import org.diqube.data.serialize.DataSerializer.ObjectDoneConsumer;
import org.diqube.data.serialize.SerializationException;
import org.diqube.loader.LoadException;
import org.diqube.loader.Loader;
import org.diqube.loader.LoaderColumnInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.util.ReflectionUtils;

import com.google.common.collect.Iterators;

/**
 * Implements transposing an input file into .diqube representation.
 *
 * @author Bastian Gloeckle
 */
public class TransposeImplementation {
  private static final Logger logger = LoggerFactory.getLogger(TransposeImplementation.class);

  private static final String TABLE_NAME = "TransposeImportTable";

  private static final Set<Class<?>> PRIMITIVE_TYPES = new HashSet<>(Arrays.asList(Long.TYPE, Integer.TYPE, Byte.TYPE,
      Short.TYPE, Float.TYPE, Double.TYPE, Character.TYPE, Boolean.TYPE));

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
      // do not enable newDataWatcher and/or Config.
      ctx.scan("org.diqube");
      ctx.refresh();

      DataSerializationManager serializationManager = ctx.getBean(DataSerializationManager.class);
      DataSerializer serializer = serializationManager.createSerializer(TableShard.class);
      Loader loader = ctx.getBean(loaderClass);

      // for JSON it does not mater what we use here, as the JsonLoader will detect the col type automatically.
      LoaderColumnInfo colInfo = new LoaderColumnInfo(ColumnType.LONG);
      if (colInfoFile != null)
        colInfo = loadColInfo(colInfoFile);
      else
        logger.info("Using column info with default column type Long.");

      try (FileOutputStream outStream = new FileOutputStream(outputFile)) {
        logger.info("Starting to load data into temporary in-memory table '{}'", TABLE_NAME);
        TableShard tableShard = loader.load(0L, inputFile.getAbsolutePath(), TABLE_NAME, colInfo);

        logger.info("Data loaded into in-memory table '{}', starting to serialize that data into output file '{}'",
            TABLE_NAME, outputFile.getAbsolutePath());
        serializer.serialize(tableShard, outStream, new ObjectDoneConsumer() {
          @Override
          public void accept(DataSerialization<?> t) {
            // right after we're done with serializing an object, we "null" all its properties to try to free up some
            // memory.
            setAllPropertiesToNull(t);
          }
        });
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

  private void setAllPropertiesToNull(Object o) {
    List<Field> allFields = new ArrayList<>();
    Deque<Class<?>> allClasses = new LinkedList<>();
    allClasses.add(o.getClass());
    while (!allClasses.isEmpty()) {
      Class<?> curClass = allClasses.pop();
      if (curClass.equals(Object.class))
        continue;

      for (Field declaredField : curClass.getDeclaredFields()) {
        if (!PRIMITIVE_TYPES.contains(declaredField.getType()))
          allFields.add(declaredField);
      }
      allClasses.add(curClass.getSuperclass());
    }

    for (Field fieldToNull : allFields) {
      ReflectionUtils.makeAccessible(fieldToNull);
      try {
        fieldToNull.set(o, null);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        // swallow - we did not succeed in nulling the field, the just ignore.
        logger.trace("Could not null {} on {}", fieldToNull, o);
      }
    }
  }
}
