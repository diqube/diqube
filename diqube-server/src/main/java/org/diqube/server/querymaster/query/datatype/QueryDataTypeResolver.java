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
package org.diqube.server.querymaster.query.datatype;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.diqube.data.column.ColumnType;
import org.diqube.diql.request.FunctionRequest;
import org.diqube.function.AggregationFunction;
import org.diqube.function.FunctionFactory;
import org.diqube.function.ProjectionFunction;
import org.diqube.metadata.create.FieldUtil;
import org.diqube.name.RepeatedColumnNameGenerator;
import org.diqube.thrift.base.thrift.FieldMetadata;
import org.diqube.util.ColumnOrValue;
import org.diqube.util.TopologicalSort;

/**
 * Resolves data types of all temporary columns that are used e.g. in a query.
 *
 * @author Bastian Gloeckle
 */
public class QueryDataTypeResolver {
  private Function<String, FieldMetadata> columnMetadataResolve;

  private FunctionFactory functionFactory;

  private RepeatedColumnNameGenerator repeatedColumnNameGenerator;

  /**
   * @param columnMetadataResolve
   *          Function that resolves the {@link FieldMetadata} of a column name. That column name should be part of the
   *          underlying table, if it is not or no {@link FieldMetadata} is available, return <code>null</code>.
   */
  public QueryDataTypeResolver(Function<String, FieldMetadata> columnMetadataResolve, FunctionFactory functionFactory,
      RepeatedColumnNameGenerator repeatedColumnNameGenerator) {
    this.columnMetadataResolve = columnMetadataResolve;
    this.functionFactory = functionFactory;
    this.repeatedColumnNameGenerator = repeatedColumnNameGenerator;
  }

  /**
   * Given a set of {@link FunctionRequest}s, calculate the types of the resulting columns.
   * 
   * @param functionRequests
   *          The {@link FunctionRequest}s that should be executed. Each might reference other {@link FunctionRequest}s,
   *          which though also have to be part of the collection. On the other hand, the {@link FunctionRequest}s can
   *          reference literals or columns, whose data type should be resolvable using the function provided in the
   *          constructor.
   * @return Map from field name to corresponding {@link FieldMetadata}. This will contain an entry for each field that
   *         is produced by a provided {@link FunctionRequest}.
   * @throws DataTypeInvalidException
   *           If the stacking of functions is not valid (e.g. a function depends on a STRING column in the
   *           {@link FunctionRequest}, but actually there is no function implementation that can handle String inputs).
   */
  public Map<String, FieldMetadata> calculateDataTypes(List<FunctionRequest> functionRequests)
      throws DataTypeInvalidException {
    List<FunctionRequest> sortedFunctionRequests = topologicalSort(functionRequests);

    Map<String, FieldMetadata> res = new HashMap<>();

    for (FunctionRequest req : sortedFunctionRequests) {
      ColumnType inputType = null;
      ColumnType firstLiteralType = null;
      for (ColumnOrValue cov : req.getInputParameters()) {
        if (cov.getType().equals(ColumnOrValue.Type.COLUMN)) {
          ColumnType paramColType;
          String inputField = FieldUtil.toFieldName(cov.getColumnName());
          if (res.containsKey(inputField))
            paramColType = toColumnType(res.get(inputField));
          else {
            if (cov.getColumnName().endsWith(repeatedColumnNameGenerator.lengthIdentifyingSuffix()))
              // [length] cols are always LONG.
              paramColType = ColumnType.LONG;
            else {
              FieldMetadata m = columnMetadataResolve.apply(cov.getColumnName());
              if (m != null)
                paramColType = toColumnType(m);
              else
                paramColType = null;
            }
          }

          if (inputType != null && !inputType.equals(paramColType))
            throw new DataTypeInvalidException("Function '" + req.getFunctionName()
                + "' has columns of various types as input: " + req.getInputParameters());

          if (inputType == null)
            inputType = paramColType;
        } else {
          if (firstLiteralType == null)
            firstLiteralType = toColumnType(cov.getValue());
        }
      }

      boolean isRepeated = false;
      ColumnType outputType;
      if (req.getType().equals(FunctionRequest.Type.AGGREGATION_ROW)
          || req.getType().equals(FunctionRequest.Type.AGGREGATION_COL)) {
        AggregationFunction<?, ?> tmpFn = functionFactory.createAggregationFunction(req.getFunctionName(), inputType);
        if (tmpFn == null)
          throw new DataTypeInvalidException("There is no function '" + req.getFunctionName() + "' with input type "
              + inputType + ". Parameters: " + req.getInputParameters());

        outputType = tmpFn.getOutputType();
      } else {
        ColumnType projectionInputType = inputType;
        if (projectionInputType == null)
          // Special case for Projection function with no input column. Cannot be RepeatedProject. TODO #111
          projectionInputType = firstLiteralType;

        ProjectionFunction<?, ?> tmpFn =
            functionFactory.createProjectionFunction(req.getFunctionName(), projectionInputType);
        if (tmpFn == null)
          throw new DataTypeInvalidException("There is no function '" + req.getFunctionName() + "' with input type "
              + projectionInputType + ". Parameters: " + req.getInputParameters());

        outputType = tmpFn.getOutputType();

        isRepeated = req.getType().equals(FunctionRequest.Type.REPEATED_PROJECTION);
      }

      String outputField = FieldUtil.toFieldName(req.getOutputColumn());
      FieldMetadata newFieldInfo = new FieldMetadata(outputField, FieldUtil.toFieldType(outputType), isRepeated);
      res.put(outputField, newFieldInfo);
    }

    return res;
  }

  /**
   * Sort the {@link FunctionRequest}s topologically.
   * 
   * @return List containing {@link FunctionRequest}s first that need to be executed first.
   */
  private List<FunctionRequest> topologicalSort(List<FunctionRequest> functionRequests) {
    Map<FunctionRequest, Long> id = new HashMap<>();
    long i = 0;
    Map<String, List<FunctionRequest>> successors = new HashMap<>();
    for (FunctionRequest r : functionRequests) {
      id.put(r, i++);
      if (!successors.containsKey(r.getOutputColumn()))
        successors.put(r.getOutputColumn(), new ArrayList<>());

      for (ColumnOrValue cov : r.getInputParameters()) {
        if (cov.getType().equals(ColumnOrValue.Type.COLUMN)) {
          if (!successors.containsKey(cov.getColumnName()))
            successors.put(cov.getColumnName(), new ArrayList<>());
          successors.get(cov.getColumnName()).add(r);
        }
      }
    }

    TopologicalSort<FunctionRequest> topSort =
        new TopologicalSort<>(r -> successors.get(r.getOutputColumn()), r -> id.get(r), null);
    List<FunctionRequest> sortedFunctionRequests = topSort.sort(functionRequests);
    return sortedFunctionRequests;
  }

  private ColumnType toColumnType(FieldMetadata m) {
    switch (m.getFieldType()) {
    case STRING:
      return ColumnType.STRING;
    case LONG:
      return ColumnType.LONG;
    case DOUBLE:
      return ColumnType.DOUBLE;
    default:
      return null;
    }
  }

  private ColumnType toColumnType(Object val) {
    if (val instanceof Double)
      return ColumnType.DOUBLE;
    if (val instanceof Long)
      return ColumnType.LONG;
    return ColumnType.STRING;
  }
}
