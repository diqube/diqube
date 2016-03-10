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
package org.diqube.server.querymaster.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.diql.request.ExecutionRequest;
import org.diqube.metadata.TableMetadataManager;
import org.diqube.metadata.inspect.TableMetadataInspector;
import org.diqube.metadata.inspect.TableMetadataInspectorFactory;
import org.diqube.metadata.inspect.exception.ColumnNameInvalidException;
import org.diqube.name.FlattenedTableNameUtil;
import org.diqube.plan.PlannerColumnInfo;
import org.diqube.plan.exception.ValidationException;
import org.diqube.plan.validate.ExecutionRequestValidator;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.FieldMetadata;
import org.diqube.thrift.base.thrift.TableMetadata;

import com.google.common.collect.Iterables;

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
            originalInspector.findAllFieldMetadata(executionRequest.getFromRequest().getFlattenByField());
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

    // validate information on all the "root columns", i.e. those columns of the table that are actually accessed by the
    // query.
    Map<String, FieldMetadata> rootColMetadata = new HashMap<>();
    for (String rootColName : executionRequest.getAdditionalInfo().getColumnNamesRequired()) {
      try {
        List<FieldMetadata> m = inspector.findAllFieldMetadata(rootColName);
        rootColMetadata.put(rootColName, Iterables.getLast(m));
      } catch (ColumnNameInvalidException e) {
        // column name is invalid/does not exist.
        throw new ValidationException(e.getMessage(), e);
      }
    }
  }

}
