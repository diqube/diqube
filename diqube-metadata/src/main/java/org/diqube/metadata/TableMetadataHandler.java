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
package org.diqube.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.cluster.ClusterLayout;
import org.diqube.connection.ConnectionException;
import org.diqube.connection.ConnectionOrLocalHelper;
import org.diqube.connection.ServiceProvider;
import org.diqube.consensus.ConsensusClient;
import org.diqube.consensus.ConsensusClient.ClosableProvider;
import org.diqube.consensus.ConsensusClient.ConsensusClusterUnavailableException;
import org.diqube.context.AutoInstatiate;
import org.diqube.diql.DiqlParseUtil;
import org.diqube.diql.antlr.DiqlParser.DiqlStmtContext;
import org.diqube.diql.request.ExecutionRequest;
import org.diqube.diql.visitors.SelectStmtVisitor;
import org.diqube.metadata.consensus.TableMetadataStateMachine;
import org.diqube.metadata.consensus.TableMetadataStateMachine.GetTableMetadata;
import org.diqube.name.FlattenedTableNameUtil;
import org.diqube.name.FunctionBasedColumnNameBuilderFactory;
import org.diqube.name.RepeatedColumnNameGenerator;
import org.diqube.permission.TableAccessPermissionUtil;
import org.diqube.remote.cluster.thrift.ClusterFlattenService;
import org.diqube.remote.cluster.thrift.RFlattenException;
import org.diqube.remote.cluster.thrift.ROptionalUuid;
import org.diqube.remote.query.thrift.ROptionalTableMetadata;
import org.diqube.remote.query.thrift.TableMetadataService;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.RNodeAddress;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.thrift.base.util.RUuidUtil;
import org.diqube.ticket.TicketValidityService;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for retrieving {@link TableMetadata} of a specific table, if metadata is available.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class TableMetadataHandler implements TableMetadataService.Iface {
  private static final Logger logger = LoggerFactory.getLogger(TableMetadataHandler.class);

  @Inject
  private TableAccessPermissionUtil tableAccessPermissionUtil;

  @Inject
  private TicketValidityService ticketValidityService;

  @Inject
  private ClusterLayout clusterLayout;

  @Inject
  private ConsensusClient consensusClient;

  @Inject
  private FlattenedTableNameUtil flattenedTableNameUtil;

  @Inject
  private RepeatedColumnNameGenerator repeatedColumnNameGenerator;

  @Inject
  private FunctionBasedColumnNameBuilderFactory functionBasedColumnNameBuilderFactory;

  @Inject
  private ConnectionOrLocalHelper connectionOrLocalHelper;

  /**
   * @param tableName
   *          Name of the table to get the metadata of. Can either be a normal tablename, or the name of a flattened
   *          table. The latter can either be a full one (with the flatten ID = one created by
   *          {@link FlattenedTableNameUtil}) or one without the ID (such as the ones used in diql queries; in this
   *          case, the newest locally available flattening will be used).
   */
  @Override
  public ROptionalTableMetadata getTableMetadata(Ticket ticket, String tableName)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!tableAccessPermissionUtil.hasAccessToTable(ticket, tableName))
      throw new AuthorizationException("No access to table or table does not exist.");

    Set<String> allTables;
    try {
      allTables = clusterLayout.getAllTablesServed();
    } catch (InterruptedException | ConsensusClusterUnavailableException e) {
      logger.warn("Interrupted while loading list of all tables", e);
      return new ROptionalTableMetadata();
    }
    if (flattenedTableNameUtil.isFlattenedTableName(tableName)) {
      if (!allTables.contains(flattenedTableNameUtil.getOriginalTableNameFromFlatten(tableName)))
        throw new AuthorizationException("No access to table or table does not exist.");
    } else if (!allTables.contains(tableName))
      throw new AuthorizationException("No access to table or table does not exist.");

    try (ClosableProvider<TableMetadataStateMachine> p =
        consensusClient.getStateMachineClient(TableMetadataStateMachine.class)) {

      String fixTableName = null;

      if (flattenedTableNameUtil.isFlattenedTableName(tableName)
          && !flattenedTableNameUtil.isFullFlattenedTableName(tableName)) {
        fixTableName = tableName;
        tableName = enhanceIncompleteFlattenedTableNameWithNewestFlattenId(tableName);
      }

      Pair<TableMetadata, Long> resPair = p.getClient().getTableMetadata(GetTableMetadata.local(tableName));
      if (resPair == null || resPair.getLeft() == null)
        return new ROptionalTableMetadata();

      ROptionalTableMetadata res = new ROptionalTableMetadata();
      if (fixTableName != null) {
        TableMetadata fixedMetadata = new TableMetadata(resPair.getLeft());
        // do not tell the client about the flattenId if he did not know it himself - do not leak internal information!
        fixedMetadata.setTableName(fixTableName);
        res.setTableMetadata(fixedMetadata);
      } else
        res.setTableMetadata(resPair.getLeft());
      return res;
    } catch (ConsensusClusterUnavailableException e) {
      logger.warn("Could not find table metadata of table '{}' because consensus cluster is unavailable", e);
      return new ROptionalTableMetadata();
    }
  }

  /**
   * Takes a table "name" of the form "flatten(TABLE, FIELD)", i.e. without the flattenId, and finds out the potentially
   * newest flattenId for it and returns a valid table name including that ID. The returned table name can be used to
   * resolve table metadata.
   */
  private String enhanceIncompleteFlattenedTableNameWithNewestFlattenId(String tableName)
      throws AuthorizationException, RFlattenException, TException {
    // provided table name is of the form flatten(TABLE, FIELD), i.e. without the flattenId. Append newest.
    RuntimeException error = null;
    ExecutionRequest req = null;
    try {
      DiqlStmtContext stmt = DiqlParseUtil.parseWithAntlr("select a from " + tableName);
      req = stmt.accept(new SelectStmtVisitor(repeatedColumnNameGenerator, functionBasedColumnNameBuilderFactory));
      if (!req.getFromRequest().isFlattened())
        error = new RuntimeException(
            "Unkown table name, do not know how to handle it in order to load metadata: " + tableName);
    } catch (RuntimeException e) {
      error = e;
    }
    if (error != null) {
      logger.warn("Could not find metadata of table {}.", tableName, error);
      throw new AuthorizationException("No access to table or table does not exist.");
    }

    String origTable = req.getFromRequest().getTable();
    String flattenBy = req.getFromRequest().getFlattenByField();

    Collection<RNodeAddress> nodes;
    try {
      nodes = clusterLayout.findNodesServingTable(origTable);
    } catch (InterruptedException | ConsensusClusterUnavailableException e) {
      throw new RuntimeException(
          "Could not find nodes serving table " + origTable + " for finding metadata of '" + tableName + "'", e);
    }

    if (nodes.isEmpty()) {
      logger.warn("No nodes serving '{}', cannot identify metadata for '{}' therefore", origTable, tableName);
      throw new AuthorizationException("No access to table or table does not exist.");
    }

    RNodeAddress node = new ArrayList<>(nodes).get(ThreadLocalRandom.current().nextInt(nodes.size()));

    UUID flattenId = null;
    try (ServiceProvider<ClusterFlattenService.Iface> sp =
        connectionOrLocalHelper.getService(ClusterFlattenService.Iface.class, node, null)) {

      ROptionalUuid optId = sp.getService().getLatestValidFlattening(origTable, flattenBy);

      if (optId.isSetUuid())
        flattenId = RUuidUtil.toUuid(optId.getUuid());

    } catch (IOException | ConnectionException | InterruptedException e) {
      logger.warn("Could not find newest flattenId for flattening of table {} by {} from {}", origTable, flattenBy,
          node, e);
      throw new AuthorizationException("No access to table or table does not exist.");
    }

    logger.debug("Chosing flattenId {} for request to load metadata for table '{}'", flattenId, tableName);
    return flattenedTableNameUtil.createFlattenedTableName(origTable, flattenBy, flattenId);
  }

}
