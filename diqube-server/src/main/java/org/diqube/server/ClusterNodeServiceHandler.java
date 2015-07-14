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
package org.diqube.server;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.cluster.thrift.ClusterNodeService.Iface;
import org.diqube.remote.cluster.thrift.RClusterResultTable;
import org.diqube.remote.cluster.thrift.RExecutionException;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.cluster.thrift.RPartialResultTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * TODO #11 implement.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterNodeServiceHandler implements Iface {
  private static final Logger logger = LoggerFactory.getLogger(ClusterNodeServiceHandler.class);

  @Inject
  private ExecutionPlanExecutor executor;

  @Override
  public RUUID executeOnAllShards(RExecutionPlan executionPlan) throws TException {
    // try {
    return executor.execute(executionPlan);
    // } catch (RuntimeException e) {
    // TODO #11 handle error case

    // ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // PrintWriter writer = new PrintWriter(baos);
    // e.printStackTrace(writer);
    // writer.flush();
    // logger.error(new String(baos.toByteArray()));
    //
    // throw new RExecutionException(e.getMessage());
    // }
  }

  @Override
  public void partialResultAvailable(RUUID nodeExecutionId, RPartialResultTable partialValueTable) throws TException {
  }

  @Override
  public void executionException(RUUID nodeExecutionId, RExecutionException executionException) throws TException {
  }

  @Override
  public void resultTableAvailable(RUUID nodeExecutionId, RClusterResultTable valueTable) throws TException {
  }

}
