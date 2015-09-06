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
package org.diqube.function;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import javax.annotation.PostConstruct;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.ColumnType;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

/**
 * A factory for all {@link AggregationFunction}s and {@link ProjectionFunction}s.
 * 
 * <p>
 * This factory initializes itself by scanning the classpath and evaluating {@link Function} annotations.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class FunctionFactory {
  private static final String BASE_PKG = "org.diqube.function";

  /**
   * FuncName -> Input data type -> factory supplier.
   */
  private Map<String, Map<ColumnType, Supplier<ProjectionFunction<?, ?>>>> projectionFunctionFactories;
  private Map<String, Map<ColumnType, Supplier<AggregationFunction<?, ?, ?>>>> aggregationFunctionFactories;

  /**
   * @return The projection function or <code>null</code> if not found.
   */
  @SuppressWarnings("unchecked")
  public <I, O> ProjectionFunction<I, O> createProjectionFunction(String functionNameLowerCase,
      ColumnType inputColumnType) {
    if (!projectionFunctionFactories.containsKey(functionNameLowerCase))
      return null;
    if (!projectionFunctionFactories.get(functionNameLowerCase).containsKey(inputColumnType))
      return null;
    return (ProjectionFunction<I, O>) projectionFunctionFactories.get(functionNameLowerCase).get(inputColumnType).get();
  }

  /**
   * @return The projection function or <code>null</code> if not found.
   */
  @SuppressWarnings("unchecked")
  public <I, M extends IntermediaryResult<?, ?, ?>, O> AggregationFunction<I, M, O> createAggregationFunction(
      String functionNameLowerCase, ColumnType inputColumnType) {
    if (!aggregationFunctionFactories.containsKey(functionNameLowerCase)
        || !aggregationFunctionFactories.get(functionNameLowerCase).containsKey(inputColumnType))
      return null;
    return (AggregationFunction<I, M, O>) aggregationFunctionFactories.get(functionNameLowerCase).get(inputColumnType)
        .get();
  }

  @PostConstruct
  private void initialize() {
    ImmutableSet<ClassInfo> classInfos;
    try {
      classInfos = ClassPath.from(this.getClass().getClassLoader()).getTopLevelClassesRecursive(BASE_PKG);
    } catch (IOException e) {
      throw new RuntimeException("Could not parse ClassPath.");
    }

    projectionFunctionFactories = new HashMap<>();
    aggregationFunctionFactories = new HashMap<>();

    for (ClassInfo classInfo : classInfos) {
      Class<?> clazz = classInfo.load();
      Function funcAnnotation = clazz.getAnnotation(Function.class);
      if (funcAnnotation != null) {
        String funcName = funcAnnotation.name();
        if (ProjectionFunction.class.isAssignableFrom(clazz)) {
          if (!projectionFunctionFactories.containsKey(funcName))
            projectionFunctionFactories.put(funcName, new HashMap<>());

          Supplier<ProjectionFunction<?, ?>> supplier = () -> {
            try {
              return (ProjectionFunction<?, ?>) clazz.newInstance();
            } catch (Exception e) {
              throw new RuntimeException("Could not instantiate " + clazz.getName(), e);
            }
          };

          ProjectionFunction<?, ?> tempInstance = supplier.get();

          if (projectionFunctionFactories.get(funcName).put(tempInstance.getInputType(), supplier) != null)
            throw new RuntimeException("There are multiple ProjectionFunctions with name '" + funcName
                + "' and input data type " + tempInstance.getInputType());

        } else if (AggregationFunction.class.isAssignableFrom(clazz)) {
          if (!aggregationFunctionFactories.containsKey(funcName))
            aggregationFunctionFactories.put(funcName, new HashMap<>());

          Supplier<AggregationFunction<?, ?, ?>> supplier = () -> {
            try {
              return (AggregationFunction<?, ?, ?>) clazz.newInstance();
            } catch (Exception e) {
              throw new RuntimeException("Could not instantiate " + clazz.getName());
            }
          };

          AggregationFunction<?, ?, ?> tempInstance = supplier.get();

          aggregationFunctionFactories.get(funcName).put(tempInstance.getInputType(), supplier);
        }
      }
    }
  }
}
