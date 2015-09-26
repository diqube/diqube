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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains addresses of all cluster nodes known and the tables the respective node is serving data of.
 * 
 * This includes information about our node.
 *
 * @author Bastian Gloeckle
 */
public class ClusterLayout {
  private static final Logger logger = LoggerFactory.getLogger(ClusterLayout.class);

  private Map<NodeAddress, Pair<Long, NavigableSet<String>>> tables = new ConcurrentHashMap<>();

  /* package */ synchronized boolean setTables(NodeAddress addr, long version, Collection<String> newTables) {
    if (tables.containsKey(addr) && tables.get(addr).getLeft() >= version)
      return false;

    Pair<Long, NavigableSet<String>> newPair = new Pair<>(version, new ConcurrentSkipListSet<>(newTables));
    tables.put(addr, newPair);

    return true;
  }

  /* package */synchronized void addNode(NodeAddress addr) {
    tables.put(addr, new Pair<>(0L, new ConcurrentSkipListSet<>()));
  }

  /* package */ synchronized boolean removeNode(NodeAddress addr) {
    logger.info("Dead cluster node {}", addr);
    return tables.remove(addr) != null;
  }

  /**
   * @return Number of cluster nodes known (including our node).
   */
  public int getNumberOfNodes() {
    return tables.size();
  }

  /**
   * @return Addresses of all cluster nodes that are known (including our node).
   */
  public Set<NodeAddress> getNodes() {
    return new HashSet<>(tables.keySet());
  }

  /**
   * @return A cluster layout that can be used for sending to remote nodes.
   */
  public Map<RNodeAddress, Map<Long, List<String>>> createRemoteLayout() {
    Map<RNodeAddress, Map<Long, List<String>>> res = new HashMap<>();

    for (Entry<NodeAddress, Pair<Long, NavigableSet<String>>> e : tables.entrySet()) {
      Map<Long, List<String>> detailRes = new HashMap<>();
      detailRes.put(e.getValue().getLeft(), new ArrayList<>(e.getValue().getRight()));
      res.put(e.getKey().createRemote(), detailRes);
    }

    return res;
  }

  /**
   * Finds the addresses of nodes of which is known that they serve parts of a specific table.
   */
  public Collection<RNodeAddress> findNodesServingTable(String table) {
    List<RNodeAddress> res = new ArrayList<>();

    for (Entry<NodeAddress, Pair<Long, NavigableSet<String>>> e : tables.entrySet()) {
      if (e.getValue().getRight().contains(table))
        res.add(e.getKey().createRemote());
    }

    return res;
  }

  /**
   * @return A set with all tablenames that are served from at least one cluster node.
   */
  public Set<String> getAllTablesServed() {
    return tables.values().stream().flatMap(p -> p.getRight().stream()).collect(Collectors.toSet());
  }

  /* package */ synchronized Pair<Long, List<String>> getVersionedTableList(NodeAddress addr) {
    List<String> resList = new ArrayList<>(tables.get(addr).getRight());
    return new Pair<>(tables.get(addr).getLeft(), resList);
  }

  /* package */ synchronized Long addTable(NodeAddress addr, String tableName) {
    if (!tables.containsKey(addr))
      return null;

    if (tables.get(addr).getRight().contains(tableName))
      return null;

    Long newVersion = tables.get(addr).getLeft() + 1;
    Pair<Long, NavigableSet<String>> newPair =
        new Pair<>(newVersion, new ConcurrentSkipListSet<>(tables.get(addr).getRight()));
    newPair.getRight().add(tableName);

    tables.put(addr, newPair);

    return newVersion;
  }

  /* package */ synchronized Long removeTable(NodeAddress addr, String tableName) {
    if (!tables.containsKey(addr))
      return null;

    if (!tables.get(addr).getRight().contains(tableName))
      return null;

    Long newVersion = tables.get(addr).getLeft() + 1;
    Pair<Long, NavigableSet<String>> newPair =
        new Pair<>(newVersion, new ConcurrentSkipListSet<>(tables.get(addr).getRight()));
    newPair.getRight().remove(tableName);

    tables.put(addr, newPair);

    return newVersion;
  }
}
