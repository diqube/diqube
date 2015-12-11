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
package org.diqube.im.callback;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.diqube.config.Config;
import org.diqube.config.DerivedConfigKey;
import org.diqube.connection.NodeAddress;
import org.diqube.consensus.ConsensusStateMachineImplementation;
import org.diqube.context.InjectOptional;
import org.diqube.file.internaldb.InternalDbFileReader;
import org.diqube.file.internaldb.InternalDbFileReader.ReadException;
import org.diqube.file.internaldb.InternalDbFileWriter;
import org.diqube.file.internaldb.InternalDbFileWriter.WriteException;
import org.diqube.im.thrift.v1.SCallback;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.copycat.server.Commit;

/**
 * Implementation of {@link IdentityCallbackRegistryStateMachine}.
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachineImplementation
public class IdentityCallbackRegistryStateMachineImplementation implements IdentityCallbackRegistryStateMachine {
  private static final Logger logger =
      LoggerFactory.getLogger(IdentityCallbackRegistryStateMachineImplementation.class);

  private static final String INTERNALDB_FILE_PREFIX = "identitycallback-";
  private static final String INTERNALDB_DATA_TYPE = "identitycallbacks_v1";

  private Map<RNodeAddress, Long> registered = new ConcurrentHashMap<>();
  private Map<RNodeAddress, Commit<?>> lastCommit = new ConcurrentHashMap<>();

  @InjectOptional
  private List<IdentityCallbackRegistryListener> listeners;

  @Config(DerivedConfigKey.FINAL_INTERNAL_DB_DIR)
  private String internalDbDir;
  private InternalDbFileWriter<SCallback> internalDbFileWriter;

  @PostConstruct
  public void initialize() {
    File internalDbDirFile = new File(internalDbDir);
    if (!internalDbDirFile.exists())
      if (!internalDbDirFile.mkdirs())
        throw new RuntimeException("Could not create directory " + internalDbDir);

    try {
      InternalDbFileReader<SCallback> internalDbFileReader = new InternalDbFileReader<>(INTERNALDB_DATA_TYPE,
          INTERNALDB_FILE_PREFIX, internalDbDirFile, () -> new SCallback());
      List<SCallback> callbacks = internalDbFileReader.readNewest();
      if (callbacks != null)
        for (SCallback callback : callbacks) {
          this.registered.put(callback.getCallbackAddr(), callback.getRegisteredAt());
        }
      else
        logger.info("No internaldb for callbacks available");
    } catch (ReadException e) {
      throw new RuntimeException("Could not load callbacks file", e);
    }

    internalDbFileWriter = new InternalDbFileWriter<>(INTERNALDB_DATA_TYPE, INTERNALDB_FILE_PREFIX, internalDbDirFile);
  }

  @Override
  public void register(Commit<Register> commit) {
    Commit<?> prev = lastCommit.put(commit.operation().getCallbackNode(), commit);

    RNodeAddress callbackNode = commit.operation().getCallbackNode();
    long registerTime = commit.operation().getRegisterTimeMs();
    registered.put(callbackNode, registerTime);

    writeCurrentCallbacksToInternalDb(commit.index());

    if (prev != null)
      prev.clean();

    if (listeners != null)
      listeners.forEach(l -> l.registered(callbackNode, registerTime));
  }

  @Override
  public void unregister(Commit<Unregister> commit) {
    Commit<?> lastWriteCommit = lastCommit.remove(commit.operation().getCallbackNode());

    RNodeAddress callbackNode = commit.operation().getCallbackNode();
    Long lastRegisterTime = registered.remove(callbackNode); // null in case unregister was called for already
                                                             // unregistered node!

    writeCurrentCallbacksToInternalDb(commit.index());

    if (lastWriteCommit != null)
      lastWriteCommit.clean();
    commit.clean();

    if (lastRegisterTime != null && listeners != null)
      listeners.forEach(l -> l.unregistered(callbackNode, lastRegisterTime));
  }

  @Override
  public List<RNodeAddress> getAllRegistered(Commit<GetAllRegistered> commit) {
    commit.close();

    List<RNodeAddress> res = new ArrayList<>();
    res.addAll(registered.keySet());
    return res;
  }

  /**
   * @return The list of callbacks this node currently knows of. This list might be incomplete or contain elements that
   *         were already unregistered!
   */
  public Set<NodeAddress> getRegisteredNodesInsecure() {
    Set<NodeAddress> res = new HashSet<>();
    registered.keySet().forEach(node -> res.add(new NodeAddress(node)));
    return res;
  }

  private void writeCurrentCallbacksToInternalDb(long consensusIndex) {
    List<SCallback> callbacks = new ArrayList<>();
    for (Entry<RNodeAddress, Long> e : registered.entrySet())
      callbacks.add(new SCallback(e.getKey(), e.getValue()));
    try {
      internalDbFileWriter.write(consensusIndex, callbacks);
    } catch (WriteException e1) {
      logger.error("Could not write callbacks internaldb file!", e1);
      // this is an error, but we try to continue anyway. When the file is missing, the node might not be able to
      // recover correctly, but for now we can keep working. The admin might want to copy a internaldb file from a
      // different node.
    }
  }

  /* package */ Long getCurrentRegisterTime(RNodeAddress callbackNode) {
    return registered.get(callbackNode);
  }

  /* package */ static interface IdentityCallbackRegistryListener {
    public void registered(RNodeAddress callbackNode, long registerTime);

    public void unregistered(RNodeAddress callbackNode, long lastRegisterTime);
  }
}
