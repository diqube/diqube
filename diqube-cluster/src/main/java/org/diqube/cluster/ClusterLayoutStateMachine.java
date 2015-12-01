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

import java.util.Collection;

import org.diqube.connection.NodeAddress;
import org.diqube.consensus.ConsensusMethod;
import org.diqube.consensus.ConsensusStateMachine;
import org.diqube.consensus.DiqubeConsensusUtil;

import io.atomix.copycat.client.Command;
import io.atomix.copycat.server.Commit;

/**
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachine
public interface ClusterLayoutStateMachine {

  @ConsensusMethod(dataClass = SetLayoutOfNode.class)
  public void setLayoutOfNode(Commit<SetLayoutOfNode> commit);

  public static class SetLayoutOfNode implements Command<Void> {
    private static final long serialVersionUID = 1L;
    private NodeAddress node;
    private Collection<String> tables;

    public NodeAddress getNode() {
      return node;
    }

    public Collection<String> getTables() {
      return tables;
    }

    public static Commit<SetLayoutOfNode> local(NodeAddress node, Collection<String> tables) {
      SetLayoutOfNode res = new SetLayoutOfNode();
      res.node = node;
      res.tables = tables;
      return DiqubeConsensusUtil.localCommit(res);
    }
  }
}
