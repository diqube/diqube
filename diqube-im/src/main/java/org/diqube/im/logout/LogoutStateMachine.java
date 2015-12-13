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
package org.diqube.im.logout;

import java.util.List;

import org.diqube.consensus.ConsensusMethod;
import org.diqube.consensus.ConsensusStateMachine;
import org.diqube.consensus.ConsensusUtil;
import org.diqube.remote.query.thrift.IdentityCallbackService;
import org.diqube.remote.query.thrift.Ticket;

import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.server.Commit;

/**
 * A state machine which safely distributes logouts of specific {@link Ticket}s.
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachine
public interface LogoutStateMachine {

  /**
   * Execute a logout of a specific Ticket, including informing all registered {@link IdentityCallbackService}s.
   */
  @ConsensusMethod(dataClass = Logout.class)
  public void logout(Commit<Logout> commit);

  /**
   * Get all {@link Ticket}s that are currently marked as invalid.
   */
  @ConsensusMethod(dataClass = GetInvalidTickets.class)
  public List<Ticket> getInvalidTickets(Commit<GetInvalidTickets> commit);

  /**
   * Remove a ticket from the logged out tickets. This is called automatically by {@link LogoutCleaner}.
   */
  @ConsensusMethod(dataClass = CleanLogoutTicket.class)
  public void cleanLogoutTicket(Commit<CleanLogoutTicket> commit);

  public static class Logout implements Command<Void> {
    private static final long serialVersionUID = 1L;

    private Ticket ticket;

    public Ticket getTicket() {
      return ticket;
    }

    public static Commit<Logout> local(Ticket ticket) {
      Logout res = new Logout();
      res.ticket = ticket;
      return ConsensusUtil.localCommit(res);
    }
  }

  public static class CleanLogoutTicket implements Command<Void> {
    private static final long serialVersionUID = 1L;

    private Ticket ticket;

    public Ticket getTicket() {
      return ticket;
    }

    public static Commit<CleanLogoutTicket> local(Ticket ticket) {
      CleanLogoutTicket res = new CleanLogoutTicket();
      res.ticket = ticket;
      return ConsensusUtil.localCommit(res);
    }

    @Override
    public PersistenceLevel persistence() {
      return PersistenceLevel.PERSISTENT;
    }
  }

  public static class GetInvalidTickets implements Query<List<Ticket>> {
    private static final long serialVersionUID = 1L;

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.BOUNDED_LINEARIZABLE;
    }

    public static Commit<GetInvalidTickets> local() {
      return ConsensusUtil.localCommit(new GetInvalidTickets());
    }
  }
}
