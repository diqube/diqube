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
package org.diqube.im;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.diqube.consensus.AbstractConsensusStateMachine;
import org.diqube.consensus.ConsensusStateMachineImplementation;
import org.diqube.context.InjectOptional;
import org.diqube.im.thrift.v1.SUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.copycat.server.Commit;

/**
 * Implementation of {@link IdentityStateMachine} that is executed locally on all cluster nodes.
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachineImplementation
public class IdentityStateMachineImplementation extends AbstractConsensusStateMachine<SUser>
    implements IdentityStateMachine {
  private static final Logger logger = LoggerFactory.getLogger(IdentityStateMachineImplementation.class);

  private static final String INTERNALDB_FILE_PREFIX = "identity-";
  private static final String INTERNALDB_DATA_TYPE = "identities_v1";

  private ConcurrentMap<String, Commit<?>> previous = new ConcurrentHashMap<>();
  private ConcurrentMap<String, SUser> users = new ConcurrentHashMap<>();

  @InjectOptional
  private List<UserChangedListener> userChangedListeners;

  public IdentityStateMachineImplementation() {
    super(INTERNALDB_FILE_PREFIX, INTERNALDB_DATA_TYPE, () -> new SUser());
  }

  @Override
  protected void doInitialize(List<SUser> entriesLoadedFromInternalDb) {
    if (entriesLoadedFromInternalDb != null)
      for (SUser u : entriesLoadedFromInternalDb) {
        users.put(u.getUsername(), u);
      }
  }

  @Override
  public void setUser(Commit<SetUser> commit) {
    Commit<?> prev = previous.put(commit.operation().getUser().getUsername(), commit);

    String username = commit.operation().getUser().getUsername();
    logger.info("Adjusting user '{}'...", username);
    users.put(username, commit.operation().getUser());
    writeCurrentStateToInternalDb(commit.index(), users.values());

    if (prev != null)
      prev.close();

    if (userChangedListeners != null)
      userChangedListeners.forEach(l -> l.userChanged(username));
  }

  @Override
  public SUser getUser(Commit<GetUser> commit) {
    String userName = commit.operation().getUserName();
    commit.close();
    return users.get(userName);
  }

  @Override
  public void deleteUser(Commit<DeleteUser> commit) {
    Commit<?> prev = previous.remove(commit.operation().getUserName());
    String username = commit.operation().getUserName();
    logger.info("Deleting user '{}'...", username);
    users.remove(username);
    writeCurrentStateToInternalDb(commit.index(), users.values());

    if (prev != null)
      prev.close();
    commit.close();

    if (userChangedListeners != null)
      userChangedListeners.forEach(l -> l.userChanged(username));
  }

}
