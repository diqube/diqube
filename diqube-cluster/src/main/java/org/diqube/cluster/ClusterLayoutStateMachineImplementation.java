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
package org.diqube.cluster;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.diqube.connection.NodeAddress;
import org.diqube.consensus.ConsensusStateMachineImplementation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.copycat.server.Commit;

/**
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachineImplementation
public class ClusterLayoutStateMachineImplementation implements ClusterLayoutStateMachine {
  private static final Logger logger = LoggerFactory.getLogger(ClusterLayoutStateMachineImplementation.class);

  private Map<NodeAddress, Commit<SetLayoutOfNode>> previousLayout = new ConcurrentHashMap<>();

  @Override
  public void setLayoutOfNode(Commit<SetLayoutOfNode> commit) {
    Commit<SetLayoutOfNode> prev = previousLayout.put(commit.operation().getNode(), commit);

    logger.info("New tables for node {}: {}", commit.operation().getNode(), commit.operation().getTables());

    if (prev != null)
      prev.clean();
  }

}
