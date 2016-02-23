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

import java.util.List;

import org.diqube.consensus.ConsensusMethod;
import org.diqube.consensus.ConsensusStateMachine;
import org.diqube.consensus.ConsensusUtil;
import org.diqube.remote.query.thrift.IdentityCallbackService;
import org.diqube.thrift.base.thrift.RNodeAddress;

import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.server.Commit;

/**
 * State machine that manages the registered endpoints that implements {@link IdentityCallbackService} and want to be
 * informed about any events.
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachine
public interface IdentityCallbackRegistryStateMachine {

  /**
   * Register a node that implements {@link IdentityCallbackService} and that wants to be informed about everything in
   * the future.
   */
  @ConsensusMethod(dataClass = Register.class)
  public void register(Commit<Register> commit);

  /**
   * Unregister a node.
   */
  @ConsensusMethod(dataClass = Unregister.class)
  public void unregister(Commit<Unregister> commit);

  /**
   * Get all currently registered callbacks.
   */
  @ConsensusMethod(dataClass = GetAllRegistered.class)
  public List<RNodeAddress> getAllRegistered(Commit<GetAllRegistered> commit);

  public static class Register implements Command<Void> {
    private static final long serialVersionUID = 1L;

    private RNodeAddress callbackNode;

    private long registerTimeMs;

    public RNodeAddress getCallbackNode() {
      return callbackNode;
    }

    public long getRegisterTimeMs() {
      return registerTimeMs;
    }

    public static Commit<Register> local(RNodeAddress callbackNode, long registerTimeMs) {
      Register res = new Register();
      res.callbackNode = callbackNode;
      res.registerTimeMs = registerTimeMs;
      return ConsensusUtil.localCommit(res);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.FULL;
    }
  }

  public static class Unregister implements Command<Void> {
    private static final long serialVersionUID = 1L;

    private RNodeAddress callbackNode;

    public RNodeAddress getCallbackNode() {
      return callbackNode;
    }

    public static Commit<Unregister> local(RNodeAddress callbackNode) {
      Unregister res = new Unregister();
      res.callbackNode = callbackNode;
      return ConsensusUtil.localCommit(res);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  public static class GetAllRegistered implements Query<List<RNodeAddress>> {
    private static final long serialVersionUID = 1L;

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.BOUNDED_LINEARIZABLE;
    }

    public static Commit<GetAllRegistered> local() {
      GetAllRegistered res = new GetAllRegistered();
      return ConsensusUtil.localCommit(res);
    }
  }
}
