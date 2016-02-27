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
package org.diqube.server.metadata;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.metadata.create.TableMetadataRecomputeRequestListener;
import org.diqube.server.ControlFileManager;
import org.diqube.server.metadata.ServerTableMetadataPublisher.MergeImpossibleException;
import org.diqube.server.metadata.ServerTableMetadataPublisher.TableNotExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main {@link TableMetadataRecomputeRequestListener} of diqube-server which will trigger local recomputation of
 * table metadata.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ServerTableMetadataRecomputeRequestListener implements TableMetadataRecomputeRequestListener {
  private static final Logger logger = LoggerFactory.getLogger(ServerTableMetadataRecomputeRequestListener.class);

  @Inject
  private ServerTableMetadataPublisher serverTableMetadataPublisher;
  @Inject
  private ControlFileManager controlFileManager;

  @Override
  public void tableMetadataRecomputeRequestReceived(String tableName) {
    try {
      serverTableMetadataPublisher.publishMetadataOfTable(tableName);
    } catch (TableNotExistsException e) {
      // swallow. We do not know anything of the table, so we have no metadata to publish....
    } catch (MergeImpossibleException e) {
      logger.error("Undeploying this table from this node.", e);
      undeployTable(tableName, e);
    }
  }

  private void undeployTable(String tableName, Throwable t) {
    controlFileManager.undeployTableBecauseOfError(tableName, t,
        false /*
               * do not publish metadata change of removal of table, since the cluster does not contain the details of
               * this servers metadata currently, anyway.
               */);
  }

}
