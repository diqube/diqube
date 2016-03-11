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
package org.diqube.server.querymaster.query.validate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.column.ColumnType;
import org.diqube.diql.request.ComparisonRequest;
import org.diqube.diql.request.ExecutionRequest;
import org.diqube.diql.request.FunctionRequest;
import org.diqube.metadata.TableMetadataManager;
import org.diqube.metadata.create.FieldUtil;
import org.diqube.metadata.inspect.TableMetadataInspector;
import org.diqube.metadata.inspect.TableMetadataInspectorFactory;
import org.diqube.metadata.inspect.exception.ColumnNameInvalidException;
import org.diqube.name.FlattenedTableNameUtil;
import org.diqube.plan.PlannerColumnInfo;
import org.diqube.plan.exception.ValidationException;
import org.diqube.plan.validate.ExecutionRequestValidator;
import org.diqube.server.querymaster.query.datatype.DataTypeInvalidException;
import org.diqube.server.querymaster.query.datatype.QueryDataTypeResolver;
import org.diqube.server.querymaster.query.datatype.QueryDataTypeResolverFactory;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.FieldMetadata;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.diqube.util.ColumnOrValue;

/**
 * Additional validation in diqube-server of queries being built, which takes into account potentially available
 * {@link TableMetadata}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class MasterExecutionRequestValidator implements ExecutionRequestValidator {

  @Inject
  private FlattenedTableNameUtil flattenTableNameUtil;

  @Inject
  private TableMetadataManager metadataManager;

  @Inject
  private TableMetadataInspectorFactory tableMetadataInspectorFactory;

  @Inject
  private QueryDataTypeResolverFactory queryDataTypeResolverFactory;

  @Override
  public void validate(ExecutionRequest executionRequest, Map<String, PlannerColumnInfo> colInfos)
      throws ValidationException {
    if (executionRequest.getFromRequest().isFlattened()) {
      // validate things on ORIGINAL table in case we select from a flattened one.

      try {
        TableMetadata originalMetadata =
            metadataManager.getCurrentTableMetadata(executionRequest.getFromRequest().getTable());
        if (originalMetadata != null) {
          TableMetadataInspector originalInspector = tableMetadataInspectorFactory.createInspector(originalMetadata);
          try {
            originalInspector.findFieldMetadata(executionRequest.getFromRequest().getFlattenByField());
          } catch (ColumnNameInvalidException e) {
            // flattenBy field invalid on original table!
            throw new ValidationException(e.getMessage(), e);
          }
        }
      } catch (AuthorizationException e) {
        // swallow. At max, there is no metadata available, in which case we simply do not validate.
      }
    }

    // prepare validating things on the table that is actually selected from.
    String finalTableName;
    if (executionRequest.getFromRequest().isFlattened())
      finalTableName = flattenTableNameUtil.createIncompleteFlattenedTableName(
          executionRequest.getFromRequest().getTable(), executionRequest.getFromRequest().getFlattenByField());
    else
      finalTableName = executionRequest.getFromRequest().getTable();

    TableMetadata tableMetadata;

    // try to load metadata and validate columnName if metadata is available.
    try {
      tableMetadata = metadataManager.getCurrentTableMetadata(finalTableName);
    } catch (AuthorizationException e) {
      tableMetadata = null;
    }

    if (tableMetadata != null)
      // Note that the tableMetadata we loaded might actually differ from the one that the ExecutablePlan (=
      // FlattenStep) actually picks up later! That could be in the case the underlying table is changed in between (new
      // shards loaded, shards unloaded). But anyway, if we find a metadata here, we validte using that one. If the
      // table is really currently changed and the new TabelMetadata would differ from the current /and/ our query will
      // be invalid on the new one - then the user can simply re-run the query.
      validate(executionRequest, colInfos, tableMetadata);
  }

  /**
   * Validate the {@link ExecutionRequest} based on the laoded {@link TableMetadata} of the table that the request
   * selects from.
   * 
   * @throws ValidationException
   *           If invalid.
   */
  private void validate(ExecutionRequest executionRequest, Map<String, PlannerColumnInfo> colInfos,
      TableMetadata metadata) throws ValidationException {
    TableMetadataInspector inspector = tableMetadataInspectorFactory.createInspector(metadata);

    Map<String, FieldMetadata> rootColMetadata = validateRootColumnsAvailable(executionRequest, inspector);

    Map<String, FieldMetadata> tempMetadata = validateFunctionRequestDataTypes(executionRequest, inspector);

    // all interesting metadata by field name.
    Map<String, FieldMetadata> allMetadata = new HashMap<>(rootColMetadata);
    allMetadata.putAll(tempMetadata);

    validateComparisonDataTypes(executionRequest, allMetadata);
  }

  /**
   * validate information on all the "root columns", i.e. those columns of the table that are actually accessed by the
   * query.
   * 
   * @return Map from field name to {@link FieldMetadata} of the root columns.
   */
  private Map<String, FieldMetadata> validateRootColumnsAvailable(ExecutionRequest executionRequest,
      TableMetadataInspector inspector) {
    Map<String, FieldMetadata> rootColMetadata = new HashMap<>();
    for (String rootColName : executionRequest.getAdditionalInfo().getColumnNamesRequired()) {
      try {
        FieldMetadata m = inspector.findFieldMetadata(rootColName);
        if (m != null)
          rootColMetadata.put(rootColName, m);
      } catch (ColumnNameInvalidException e) {
        // column name is invalid/does not exist.
        throw new ValidationException(e.getMessage(), e);
      }
    }
    return rootColMetadata;
  }

  /**
   * validate the data types of {@link FunctionRequest}s: Check if there are actually valid projection/aggregation
   * functions available for these data types.
   * 
   * @return Map from Field Name to {@link FieldMetadata} of those temporary columns/fields produced by the
   *         {@link FunctionRequest}s.
   */
  private Map<String, FieldMetadata> validateFunctionRequestDataTypes(ExecutionRequest executionRequest,
      TableMetadataInspector inspector) {
    QueryDataTypeResolver dataTypeResolver = queryDataTypeResolverFactory.create(colName -> {
      try {
        return inspector.findFieldMetadata(colName);
      } catch (ColumnNameInvalidException e) {
        // swallow as this should actually never happen - we checked the cols before!
        return null;
      }
    });

    try {
      return dataTypeResolver.calculateDataTypes(executionRequest.getProjectAndAggregate());
    } catch (DataTypeInvalidException e) {
      // invalid data types/functions not available.
      throw new ValidationException(e.getMessage(), e);
    }
  }

  /**
   * Validate all comparisons in request (WHERE and HAVING) that left and right side of comparison is of the same data
   * type.
   */
  private void validateComparisonDataTypes(ExecutionRequest executionRequest, Map<String, FieldMetadata> allMetadata) {
    Collection<ComparisonRequest.Leaf> whereLeafs;
    if (executionRequest.getWhere() != null)
      whereLeafs = executionRequest.getWhere().findRecursivelyAllOfType(ComparisonRequest.Leaf.class);
    else
      whereLeafs = new ArrayList<>();
    Collection<ComparisonRequest.Leaf> havingLeafs;
    if (executionRequest.getHaving() != null)
      havingLeafs = executionRequest.getHaving().findRecursivelyAllOfType(ComparisonRequest.Leaf.class);
    else
      havingLeafs = new ArrayList<>();

    Set<ComparisonRequest.Leaf> comparisonLeafs = new HashSet<>(whereLeafs);
    comparisonLeafs.addAll(havingLeafs);

    for (ComparisonRequest.Leaf l : comparisonLeafs) {
      ColumnType leftType = findColumnType(allMetadata, l.getLeftColumnName());

      ColumnType rightType;
      if (l.getRight().getType().equals(ColumnOrValue.Type.COLUMN))
        rightType = findColumnType(allMetadata, l.getRight().getColumnName());
      else
        rightType = toColumnType(l.getRight().getValue());

      if (leftType != null && rightType != null) {
        // only check if we're sure about both data types

        if (!leftType.equals(rightType))
          throw new ValidationException(
              "Datatypes incompatible (" + leftType + "<->" + rightType + ") on comparison: " + l.toString());
      }
    }
  }

  private ColumnType findColumnType(Map<String, FieldMetadata> metadata, String columnName) {
    FieldMetadata m = metadata.get(FieldUtil.toFieldName(columnName));
    if (m == null && FieldUtil.isLengthColumn(columnName))
      return ColumnType.LONG;

    if (m != null)
      return FieldUtil.toColumnType(m);
    return null;
  }

  private ColumnType toColumnType(Object val) {
    if (val instanceof Double)
      return ColumnType.DOUBLE;
    if (val instanceof Long)
      return ColumnType.LONG;
    if (val instanceof String)
      return ColumnType.STRING;
    return null;
  }
}
