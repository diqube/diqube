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
import org.diqube.remote.query.thrift.Ticket;

import io.atomix.copycat.client.Command;
import io.atomix.copycat.server.Commit;

/**
 * A state machine which safely distributes logouts of specific {@link Ticket}s.
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachine
public interface LogoutStateMachine {

  /**
   * Execute a logout of a specific Ticket.
   */
  @ConsensusMethod(dataClass = Logout.class)
  public void logout(Commit<Logout> commit);

  public static class Logout implements Command<Void> {
    private static final long serialVersionUID = 1L;

    private Ticket ticket;

    public Ticket getTicket() {
      return ticket;
    }

    public static Commit<Logout> local(Ticket ticket) {
      Logout res = new Logout();
      res.ticket = ticket;
      return DiqubeConsensusUtil.localCommit(res);
    }

    @Override
    public PersistenceLevel persistence() {
      return PersistenceLevel.PERSISTENT;
    }
  }
}
