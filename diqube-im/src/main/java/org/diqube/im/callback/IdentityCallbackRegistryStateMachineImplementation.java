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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.diqube.connection.NodeAddress;
import org.diqube.consensus.ConsensusStateMachineImplementation;
import org.diqube.context.InjectOptional;
import org.diqube.remote.base.thrift.RNodeAddress;

import io.atomix.copycat.server.Commit;

/**
 * Implementation of {@link IdentityCallbackRegistryStateMachine}.
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachineImplementation
public class IdentityCallbackRegistryStateMachineImplementation implements IdentityCallbackRegistryStateMachine {

  private Map<RNodeAddress, Long> registered = new ConcurrentHashMap<>();
  private Map<RNodeAddress, Commit<?>> lastCommit = new ConcurrentHashMap<>();

  @InjectOptional
  private List<IdentityCallbackRegistryListener> listeners;

  @Override
  public void register(Commit<Register> commit) {
    Commit<?> prev = lastCommit.put(commit.operation().getCallbackNode(), commit);

    RNodeAddress callbackNode = commit.operation().getCallbackNode();
    long registerTime = commit.operation().getRegisterTimeMs();
    registered.put(callbackNode, registerTime);

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

  /* package */ Long getCurrentRegisterTime(RNodeAddress callbackNode) {
    return registered.get(callbackNode);
  }

  /* package */ static interface IdentityCallbackRegistryListener {
    public void registered(RNodeAddress callbackNode, long registerTime);

    public void unregistered(RNodeAddress callbackNode, long lastRegisterTime);
  }
}
