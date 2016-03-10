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
package org.diqube.metadata.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.cluster.ClusterLayout;
import org.diqube.connection.ConnectionException;
import org.diqube.connection.ConnectionOrLocalHelper;
import org.diqube.connection.ServiceProvider;
import org.diqube.consensus.ConsensusClient.ConsensusClusterUnavailableException;
import org.diqube.context.AutoInstatiate;
import org.diqube.diql.DiqlParseUtil;
import org.diqube.diql.antlr.DiqlParser.DiqlStmtContext;
import org.diqube.diql.request.ExecutionRequest;
import org.diqube.diql.visitors.SelectStmtVisitor;
import org.diqube.name.FlattenedTableNameUtil;
import org.diqube.name.FunctionBasedColumnNameBuilderFactory;
import org.diqube.name.RepeatedColumnNameGenerator;
import org.diqube.remote.cluster.thrift.ClusterFlattenService;
import org.diqube.remote.cluster.thrift.ROptionalUuid;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.RNodeAddress;
import org.diqube.thrift.base.util.RUuidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to help find out the currently valid table name of a flattened table where no flattenId is available
 * yet.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class CurrentFlattenedTableNameUtil {
  private static final Logger logger = LoggerFactory.getLogger(CurrentFlattenedTableNameUtil.class);

  @Inject
  private RepeatedColumnNameGenerator repeatedColumnNameGenerator;

  @Inject
  private FunctionBasedColumnNameBuilderFactory functionBasedColumnNameBuilderFactory;

  @Inject
  private ConnectionOrLocalHelper connectionOrLocalHelper;

  @Inject
  private ClusterLayout clusterLayout;

  @Inject
  private FlattenedTableNameUtil flattenedTableNameUtil;

  /**
   * Takes a table "name" of the form "flatten(TABLE, FIELD)", i.e. without the flattenId, and finds out the potentially
   * newest flattenId for it and returns a valid table name including that ID. The returned table name can be used to
   * resolve table metadata.
   * 
   * @throws FlattenIdentificationImpossibleException
   *           If no currently valid table name can be identified.
   * @throws AuthorizationException
   *           in case either the original table does not exist - in that case an {@link AuthorizationException} should
   *           be reported in order to not let the user know if the table actually does not exist or if he simply does
   *           not have access to it.
   */
  public String enhanceIncompleteFlattenedTableNameWithNewestFlattenId(String tableName)
      throws FlattenIdentificationImpossibleException, AuthorizationException {
    // provided table name is of the form flatten(TABLE, FIELD), i.e. without the flattenId. Append newest.
    RuntimeException error = null;
    ExecutionRequest req = null;
    try {
      DiqlStmtContext stmt = DiqlParseUtil.parseWithAntlr("select a from " + tableName);
      req = stmt.accept(new SelectStmtVisitor(repeatedColumnNameGenerator, functionBasedColumnNameBuilderFactory));
      if (!req.getFromRequest().isFlattened())
        error = new RuntimeException("Unkown table name, do not know how to handle it: " + tableName);
    } catch (RuntimeException e) {
      error = e;
    }
    if (error != null) {
      logger.warn("Could not parse table information '{}'.", tableName, error);
      throw new FlattenIdentificationImpossibleException("Could not parse table information '" + tableName + "'.");
    }

    String origTable = req.getFromRequest().getTable();
    String flattenBy = req.getFromRequest().getFlattenByField();

    Collection<RNodeAddress> nodes;
    try {
      nodes = clusterLayout.findNodesServingTable(origTable);
    } catch (InterruptedException | ConsensusClusterUnavailableException e) {
      throw new FlattenIdentificationImpossibleException(
          "Could not find nodes serving table " + origTable + " for finding metadata of '" + tableName + "'", e);
    }

    if (nodes.isEmpty()) {
      logger.warn("No nodes serving '{}'.", origTable);
      throw new AuthorizationException("No access to table or table does not exist.");
    }

    RNodeAddress node = new ArrayList<>(nodes).get(ThreadLocalRandom.current().nextInt(nodes.size()));

    UUID flattenId = null;
    try (ServiceProvider<ClusterFlattenService.Iface> sp =
        connectionOrLocalHelper.getService(ClusterFlattenService.Iface.class, node, null)) {

      ROptionalUuid optId = sp.getService().getLatestValidFlattening(origTable, flattenBy);

      if (optId.isSetUuid())
        flattenId = RUuidUtil.toUuid(optId.getUuid());

    } catch (IOException | ConnectionException | InterruptedException | TException | RuntimeException e) {
      logger.warn("Could not find newest flattenId for flattening of table {} by {} from {}", origTable, flattenBy,
          node, e);
      throw new FlattenIdentificationImpossibleException("Could not find out current flattenId", e);
    }

    logger.debug("Chosing flattenId {} for table '{}'", flattenId, tableName);
    return flattenedTableNameUtil.createFlattenedTableName(origTable, flattenBy, flattenId);
  }

  public static class FlattenIdentificationImpossibleException extends Exception {
    private static final long serialVersionUID = 1L;

    public FlattenIdentificationImpossibleException(String msg) {
      super(msg);
    }

    public FlattenIdentificationImpossibleException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
