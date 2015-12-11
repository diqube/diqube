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

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.diqube.cluster.thrift.v1.SClusterNodeTables;
import org.diqube.config.Config;
import org.diqube.config.DerivedConfigKey;
import org.diqube.connection.NodeAddress;
import org.diqube.consensus.ConsensusStateMachineImplementation;
import org.diqube.file.internaldb.InternalDbFileReader;
import org.diqube.file.internaldb.InternalDbFileReader.ReadException;
import org.diqube.file.internaldb.InternalDbFileWriter;
import org.diqube.file.internaldb.InternalDbFileWriter.WriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.copycat.server.Commit;

/**
 * Implementation of {@link ClusterLayoutStateMachine} which is executed on the "server side" of each cluster node.
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachineImplementation
public class ClusterLayoutStateMachineImplementation implements ClusterLayoutStateMachine {
  private static final Logger logger = LoggerFactory.getLogger(ClusterLayoutStateMachineImplementation.class);

  private static final String INTERNALDB_FILE_PREFIX = "clusterlayout-";
  private static final String INTERNALDB_DATA_TYPE = "clusterlayout_v1";

  private Map<NodeAddress, Commit<?>> previousCommand = new ConcurrentHashMap<>();
  private Map<NodeAddress, Set<String>> tables = new ConcurrentHashMap<>();

  @Config(DerivedConfigKey.FINAL_INTERNAL_DB_DIR)
  private String internalDbDir;

  private InternalDbFileWriter<SClusterNodeTables> internalDbFileWriter;

  @PostConstruct
  public void initialize() {
    File internalDbDirFile = new File(internalDbDir);
    if (!internalDbDirFile.exists())
      if (!internalDbDirFile.mkdirs())
        throw new RuntimeException("Could not create directory " + internalDbDir);

    try {
      InternalDbFileReader<SClusterNodeTables> internalDbFileReader = new InternalDbFileReader<>(INTERNALDB_DATA_TYPE,
          INTERNALDB_FILE_PREFIX, internalDbDirFile, () -> new SClusterNodeTables());
      List<SClusterNodeTables> tableInfos = internalDbFileReader.readNewest();
      if (tableInfos != null)
        for (SClusterNodeTables tableInfo : tableInfos) {
          this.tables.put(new NodeAddress(tableInfo.getNodeAddr()), new HashSet<>(tableInfo.getTables()));
        }
      else
        logger.info("No internaldb for cluster layout available");
    } catch (ReadException e) {
      throw new RuntimeException("Could not load cluster layout file", e);
    }

    internalDbFileWriter = new InternalDbFileWriter<>(INTERNALDB_DATA_TYPE, INTERNALDB_FILE_PREFIX, internalDbDirFile);
  }

  @Override
  public void setTablesOfNode(Commit<SetTablesOfNode> commit) {
    Commit<?> prev = previousCommand.put(commit.operation().getNode(), commit);

    logger.info("New tables for node {}: {}", commit.operation().getNode(), commit.operation().getTables());
    tables.put(commit.operation().getNode(), new HashSet<>(commit.operation().getTables()));

    writeCurrentLayoutToInternalDb(commit.index());

    if (prev != null)
      prev.clean();
  }

  @Override
  public void removeNode(Commit<RemoveNode> commit) {
    Commit<?> prev = previousCommand.put(commit.operation().getNode(), commit);

    logger.info("Node removed from cluster layout: {}", commit.operation().getNode());
    tables.remove(commit.operation().getNode());

    writeCurrentLayoutToInternalDb(commit.index());

    if (prev != null)
      prev.clean();
    commit.clean();
  }

  @Override
  public Set<NodeAddress> findNodesServingTable(Commit<FindNodesServingTable> commit) {
    String tableName = commit.operation().getTableName();
    commit.close();

    Set<NodeAddress> res = new HashSet<>();
    for (Entry<NodeAddress, Set<String>> e : tables.entrySet()) {
      if (e.getValue().contains(tableName))
        res.add(e.getKey());
    }

    return res;
  }

  @Override
  public Set<NodeAddress> getAllNodes(Commit<GetAllNodes> commit) {
    commit.close();

    return new HashSet<>(tables.keySet());
  }

  @Override
  public Boolean isNodeKnown(Commit<IsNodeKnown> commit) {
    NodeAddress addr = commit.operation().getNode();
    commit.close();
    return tables.containsKey(addr);
  }

  @Override
  public Set<String> getAllTablesServed(Commit<GetAllTablesServed> commit) {
    commit.close();

    Set<String> res = new HashSet<>();
    for (Entry<NodeAddress, Set<String>> e : tables.entrySet())
      res.addAll(e.getValue());

    return res;
  }

  public Set<NodeAddress> getLocalKnownNodesInsecure() {
    return new HashSet<>(tables.keySet());
  }

  private void writeCurrentLayoutToInternalDb(long consensusIndex) {
    List<SClusterNodeTables> res = new ArrayList<>();
    for (Entry<NodeAddress, Set<String>> e : tables.entrySet()) {
      SClusterNodeTables newObj = new SClusterNodeTables();
      newObj.setNodeAddr(e.getKey().createRemote());
      newObj.setTables(new ArrayList<>(e.getValue()));
      res.add(newObj);
    }
    try {
      internalDbFileWriter.write(consensusIndex, new ArrayList<>(res));
    } catch (WriteException e1) {
      logger.error("Could not write cluster layout internaldb file!", e1);
      // this is an error, but we try to continue anyway. When the file is missing, the node might not be able to
      // recover correctly, but for now we can keep working. The admin might want to copy a internaldb file from a
      // different node.
    }
  }
}
