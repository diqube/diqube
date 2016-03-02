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
package org.diqube.remote.cluster;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.diqube.data.column.ColumnType;
import org.diqube.function.IntermediaryResult;
import org.diqube.function.aggregate.result.IntermediaryResultValueIterator;
import org.diqube.function.aggregate.result.serialization.IntermediateResultSerialization;
import org.diqube.function.aggregate.result.serialization.IntermediateResultSerializationResolver;
import org.diqube.remote.cluster.thrift.RColumnType;
import org.diqube.remote.cluster.thrift.RIntermediateAggregationResult;
import org.diqube.remote.cluster.thrift.RIntermediateAggregationResultValue;
import org.diqube.thrift.base.thrift.RValue;
import org.diqube.thrift.base.util.RValueUtil;
import org.diqube.util.SafeObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

/**
 * Util for {@link RIntermediateAggregationResult}.
 * 
 * Serialization/deserialization adheres to {@link IntermediateResultSerialization}.
 *
 * @author Bastian Gloeckle
 */
public class RIntermediateAggregationResultUtil {
  private static final String ROOT_PKG = "org.diqube";

  private static final Logger logger = LoggerFactory.getLogger(RIntermediateAggregationResultUtil.class);

  private volatile static Set<String> whitelistedSerializableClassNames = null;

  /**
   * Deserialize a {@link RIntermediateAggregationResult} to a {@link IntermediaryResult}.
   * 
   * @throws IllegalArgumentException
   *           if data cannot be deserialized.
   */
  public static IntermediaryResult buildIntermediateAggregationResult(RIntermediateAggregationResult input)
      throws IllegalArgumentException {
    if (whitelistedSerializableClassNames == null)
      initialize();

    ColumnType type = null;
    if (input.isSetInputColumnType()) {
      switch (input.getInputColumnType()) {
      case LONG:
        type = ColumnType.LONG;
        break;
      case DOUBLE:
        type = ColumnType.DOUBLE;
        break;
      default:
        type = ColumnType.STRING;
        break;
      }
    }

    IntermediaryResult res = new IntermediaryResult(input.getOutputColName(), type);

    for (RIntermediateAggregationResultValue val : input.getValues()) {
      if (val.isSetValue()) {
        res.pushValue(RValueUtil.createValue(val.getValue()));
      } else {
        byte[] serialized = val.getSerialized();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
          try (ObjectInputStream ois = new SafeObjectInputStream(bais, whitelistedSerializableClassNames)) {
            res.pushValue(ois.readObject());
          }
        } catch (IOException | ClassNotFoundException e) {
          logger.error("Could not deserialize intermediate result", e);
          throw new IllegalArgumentException("Could not deserialize intermediate result", e);
        }
      }
    }

    return res;
  }

  /**
   * Serialize a {@link IntermediaryResult}.
   * 
   * @throws IllegalArgumentException
   *           If cannot be serialized
   */
  public static RIntermediateAggregationResult buildRIntermediateAggregationResult(IntermediaryResult input)
      throws IllegalArgumentException {
    if (whitelistedSerializableClassNames == null)
      initialize();

    RIntermediateAggregationResult res = new RIntermediateAggregationResult();
    res.setOutputColName(input.getOutputColName());
    if (input.getInputColumnType() != null) {
      switch (input.getInputColumnType()) {
      case STRING:
        res.setInputColumnType(RColumnType.STRING);
        break;
      case LONG:
        res.setInputColumnType(RColumnType.LONG);
        break;
      case DOUBLE:
        res.setInputColumnType(RColumnType.DOUBLE);
        break;
      }
    }

    List<RIntermediateAggregationResultValue> values = new ArrayList<>();
    IntermediaryResultValueIterator it = input.createValueIterator();
    while (it.hasNext()) {
      Object valueObject = it.next();

      RIntermediateAggregationResultValue resValue = new RIntermediateAggregationResultValue();

      RValue rvalue = RValueUtil.createRValue(valueObject);
      if (rvalue != null) {
        resValue.setValue(rvalue);
      } else {
        if (!whitelistedSerializableClassNames.contains(valueObject.getClass().getName()))
          // only a shallow check, but better than no check at all.
          throw new IllegalArgumentException("Class " + valueObject.getClass().getName() + " is not whitelisted.");

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
          try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(valueObject);
          }

          resValue.setSerialized(baos.toByteArray());
        } catch (IOException e) {
          logger.error("Could not serialize intermediary result", e);
          throw new IllegalArgumentException("Could not serialize intermediary result", e);
        }
      }

      values.add(resValue);
    }
    res.setValues(values);

    return res;
  }

  private synchronized static void initialize() {
    if (whitelistedSerializableClassNames != null)
      return;

    ClassPath cp;
    try {
      cp = ClassPath.from(RIntermediateAggregationResultUtil.class.getClassLoader());
    } catch (IOException e) {
      throw new RuntimeException("Could not initialize classpath scanning!", e);
    }
    ImmutableSet<ClassInfo> classInfos = cp.getTopLevelClassesRecursive(ROOT_PKG);

    whitelistedSerializableClassNames = new HashSet<>();

    for (ClassInfo classInfo : classInfos) {
      Class<?> clazz = classInfo.load();
      if (clazz.getAnnotation(IntermediateResultSerialization.class) != null) {
        if (!IntermediateResultSerializationResolver.class.isAssignableFrom(clazz)) {
          logger.warn("Class {} has {} annotation, but does not implement {}. Ignoring.", clazz.getName(),
              IntermediateResultSerialization.class.getSimpleName(),
              IntermediateResultSerializationResolver.class.getName());
          continue;
        }

        try {
          IntermediateResultSerializationResolver resolver =
              (IntermediateResultSerializationResolver) clazz.newInstance();

          resolver.resolve(cls -> {
            whitelistedSerializableClassNames.add(cls.getName());
            logger.debug("Whitelisted class {} for being de-/serialized for intermediate aggregation results", cls);
          });
        } catch (InstantiationException | IllegalAccessException e) {
          logger.warn("Could not instantiate {}. Ignoring.", clazz.getName(), e);
        }
      }
    }
  }
}
