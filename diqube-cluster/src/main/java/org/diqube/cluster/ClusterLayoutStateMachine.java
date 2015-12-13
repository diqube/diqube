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
import java.util.Set;

import org.diqube.connection.NodeAddress;
import org.diqube.consensus.ConsensusMethod;
import org.diqube.consensus.ConsensusStateMachine;
import org.diqube.consensus.ConsensusUtil;

import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.server.Commit;

/**
 * A cluster-safe state machine to distribute data of what nodes are in the cluster and what nodes know what tables.
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachine
public interface ClusterLayoutStateMachine {

  /**
   * Sets the tables available on a node. A node without any tables denotes an available empty node.
   */
  @ConsensusMethod(dataClass = SetTablesOfNode.class)
  public void setTablesOfNode(Commit<SetTablesOfNode> commit);

  /**
   * Remove a node from the cluster layout - when a node left the cluster.
   */
  @ConsensusMethod(dataClass = RemoveNode.class)
  public void removeNode(Commit<RemoveNode> commit);

  /**
   * Query that returns those node addresses which serve a specific table.
   */
  @ConsensusMethod(dataClass = FindNodesServingTable.class)
  public Set<NodeAddress> findNodesServingTable(Commit<FindNodesServingTable> commit);

  /**
   * Query that returns a list of all known reachable nodes.
   */
  @ConsensusMethod(dataClass = GetAllNodes.class)
  public Set<NodeAddress> getAllNodes(Commit<GetAllNodes> commit);

  /**
   * Query that returns whether a specific node is known to be reachable in the Cluster layout.
   */
  @ConsensusMethod(dataClass = IsNodeKnown.class)
  public Boolean isNodeKnown(Commit<IsNodeKnown> commit);

  /**
   * Query that returns the names of all tables available in the cluster.
   */
  @ConsensusMethod(dataClass = GetAllTablesServed.class)
  public Set<String> getAllTablesServed(Commit<GetAllTablesServed> commit);

  public static class GetAllTablesServed implements Query<Set<String>> {
    private static final long serialVersionUID = 1L;

    public static Commit<GetAllTablesServed> local() {
      GetAllTablesServed res = new GetAllTablesServed();
      return ConsensusUtil.localCommit(res);
    }
  }

  public static class IsNodeKnown implements Query<Boolean> {
    private static final long serialVersionUID = 1L;

    private NodeAddress node;

    public NodeAddress getNode() {
      return node;
    }

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.BOUNDED_LINEARIZABLE;
    }

    public static Commit<IsNodeKnown> local(NodeAddress node) {
      IsNodeKnown res = new IsNodeKnown();
      res.node = node;
      return ConsensusUtil.localCommit(res);
    }
  }

  public static class GetAllNodes implements Query<Set<NodeAddress>> {
    private static final long serialVersionUID = 1L;

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.BOUNDED_LINEARIZABLE;
    }

    public static Commit<GetAllNodes> local() {
      GetAllNodes res = new GetAllNodes();
      return ConsensusUtil.localCommit(res);
    }
  }

  public static class FindNodesServingTable implements Query<Set<NodeAddress>> {
    private static final long serialVersionUID = 1L;

    private String tableName;

    public String getTableName() {
      return tableName;
    }

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.BOUNDED_LINEARIZABLE;
    }

    public static Commit<FindNodesServingTable> local(String tableName) {
      FindNodesServingTable res = new FindNodesServingTable();
      res.tableName = tableName;
      return ConsensusUtil.localCommit(res);
    }
  }

  public static class RemoveNode implements Command<Void> {
    private static final long serialVersionUID = 1L;
    private NodeAddress node;

    @Override
    public PersistenceLevel persistence() {
      return PersistenceLevel.PERSISTENT;
    }

    public NodeAddress getNode() {
      return node;
    }

    public static Commit<RemoveNode> local(NodeAddress node) {
      RemoveNode res = new RemoveNode();
      res.node = node;
      return ConsensusUtil.localCommit(res);
    }
  }

  public static class SetTablesOfNode implements Command<Void> {
    private static final long serialVersionUID = 1L;
    private NodeAddress node;
    private Collection<String> tables;

    public NodeAddress getNode() {
      return node;
    }

    public Collection<String> getTables() {
      return tables;
    }

    public static Commit<SetTablesOfNode> local(NodeAddress node, Collection<String> tables) {
      SetTablesOfNode res = new SetTablesOfNode();
      res.node = node;
      res.tables = tables;
      return ConsensusUtil.localCommit(res);
    }
  }
}
