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
package org.diqube.ticket;

import javax.inject.Inject;

import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.query.thrift.Ticket;
import org.diqube.remote.query.thrift.TicketClaim;

/**
 * Creates new {@link Ticket}s for authenticated users.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class TicketVendor {

  @Inject
  private TicketSignatureService ticketSignatureService;

  @Config(ConfigKey.TICKET_TIMEOUT_MIN)
  private long ticketTimeoutMin;

  /**
   * Create a fresh, signed {@link Ticket} for the given user with default timeout (valid until).
   * 
   * <p>
   * This method must only be called after successfully authenticating the user.
   * 
   * @param username
   *          Name of the user to create a ticket for.
   * @param isSuperUser
   *          true if this is a superuser. A superuser has permission to do everything.
   * @return The new {@link Ticket}.
   * @throws IllegalStateException
   *           If the ticket cannot be created, e.g. because there are no private keys available on this node.
   */
  public Ticket createDefaultTicketForUser(String username, boolean isSuperUser) throws IllegalStateException {
    long newTicketTimeout = System.currentTimeMillis() + ticketTimeoutMin * 60 * 1_000L;
    Ticket t = new Ticket();
    t.setClaim(new TicketClaim());
    t.getClaim().setUsername(username);
    t.getClaim().setValidUntil(newTicketTimeout);
    t.getClaim().setIsSuperUser(isSuperUser);

    ticketSignatureService.signTicket(t);

    return t;
  }
}
