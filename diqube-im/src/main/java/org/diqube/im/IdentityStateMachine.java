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

import org.diqube.consensus.ConsensusMethod;
import org.diqube.consensus.ConsensusStateMachine;
import org.diqube.consensus.DiqubeConsensusUtil;
import org.diqube.im.thrift.v1.SUser;

import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.server.Commit;

/**
 * State machine for distributing information on users/passwords/ACLs across the cluster.
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachine
public interface IdentityStateMachine {

  /**
   * Sets properties of a user.
   */
  @ConsensusMethod(dataClass = SetUser.class)
  public void setUser(Commit<SetUser> commit);

  /**
   * Retrieves information of a specific user.
   * 
   * @return Either the {@link SUser} or <code>null</code> in case user does not exist.
   */
  @ConsensusMethod(dataClass = GetUser.class)
  public SUser getUser(Commit<GetUser> commit);

  /**
   * Delete a specific user.
   */
  @ConsensusMethod(dataClass = DeleteUser.class)
  public void deleteUser(Commit<DeleteUser> commit);

  public static class DeleteUser implements Command<Void> {
    private static final long serialVersionUID = 1L;

    private String userName;

    public String getUserName() {
      return userName;
    }

    public static Commit<DeleteUser> local(String userName) {
      DeleteUser res = new DeleteUser();
      res.userName = userName;
      return DiqubeConsensusUtil.localCommit(res);
    }

    @Override
    public PersistenceLevel persistence() {
      return PersistenceLevel.PERSISTENT;
    }
  }

  public static class SetUser implements Command<Void> {
    private static final long serialVersionUID = 1L;

    private SUser user;

    public SUser getUser() {
      return user;
    }

    public static Commit<SetUser> local(SUser user) {
      SetUser res = new SetUser();
      res.user = user;
      return DiqubeConsensusUtil.localCommit(res);
    }
  }

  public static class GetUser implements Query<SUser> {
    private static final long serialVersionUID = 1L;

    private String userName;

    public String getUserName() {
      return userName;
    }

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.BOUNDED_LINEARIZABLE;
    }

    public static Commit<GetUser> local(String userName) {
      GetUser res = new GetUser();
      res.userName = userName;
      return DiqubeConsensusUtil.localCommit(res);
    }
  }
}
