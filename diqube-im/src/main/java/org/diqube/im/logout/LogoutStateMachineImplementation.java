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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.connection.ConnectionException;
import org.diqube.connection.ConnectionOrLocalHelper;
import org.diqube.connection.NodeAddress;
import org.diqube.connection.ServiceProvider;
import org.diqube.consensus.ConsensusStateMachineImplementation;
import org.diqube.consensus.DiqubeCopycatClient;
import org.diqube.consensus.DiqubeCopycatClient.ClosableProvider;
import org.diqube.context.InjectOptional;
import org.diqube.im.callback.IdentityCallbackRegistryStateMachine;
import org.diqube.im.callback.IdentityCallbackRegistryStateMachine.GetAllRegistered;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.query.TicketInfoUtil;
import org.diqube.remote.query.thrift.IdentityCallbackService;
import org.diqube.remote.query.thrift.Ticket;
import org.diqube.ticket.TicketValidityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.copycat.server.Commit;

/**
 * Implementation of {@link LogoutStateMachine}
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachineImplementation
public class LogoutStateMachineImplementation implements LogoutStateMachine {
  private static final Logger logger = LoggerFactory.getLogger(LogoutStateMachineImplementation.class);

  private Set<Ticket> invalidTickets = new ConcurrentSkipListSet<>();

  private Map<Ticket, Commit<?>> previousCommit = new ConcurrentHashMap<>();

  @Inject
  private TicketValidityService ticketValidityService;

  @Inject
  private ConnectionOrLocalHelper connectionOrLocalHelper;

  @Inject
  private DiqubeCopycatClient consensusClient;

  @InjectOptional
  private List<LogoutStateMachineListener> listeners;

  @Override
  public void logout(Commit<Logout> commit) {
    Ticket t = commit.operation().getTicket();

    Commit<?> prev = previousCommit.put(t, commit);

    ticketValidityService.markTicketAsInvalid(TicketInfoUtil.fromTicket(t));
    invalidTickets.add(t);

    // inform all registered callbacks.
    Set<NodeAddress> callbackAddresses = new HashSet<>();
    try (ClosableProvider<IdentityCallbackRegistryStateMachine> p =
        consensusClient.getStateMachineClient(IdentityCallbackRegistryStateMachine.class)) {
      List<RNodeAddress> addrs = p.getClient().getAllRegistered(GetAllRegistered.local());

      addrs.forEach(a -> callbackAddresses.add(new NodeAddress(a)));
    }

    // note that here again we might not reach all registered callbacks (e.g. because of network partitions). The
    // callbacks must poll a fresh list of invalidated tickets periodically and should not accept any tickets if the
    // can't reach the cluster.
    for (NodeAddress callbackAddr : callbackAddresses) {
      try (ServiceProvider<IdentityCallbackService.Iface> p =
          connectionOrLocalHelper.getService(IdentityCallbackService.Iface.class, callbackAddr.createRemote(), null)) {

        p.getService().ticketBecameInvalid(TicketInfoUtil.fromTicket(t));

      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while communicating to " + callbackAddr, e);
      } catch (TException | IOException | ConnectionException e) {
        logger.warn("Could not send invalidation of ticket (logout) to registered callback node {}.", callbackAddr);
      }
    }

    if (prev != null)
      prev.clean();

    if (listeners != null)
      listeners.forEach(l -> l.ticketBecameInvalid(t));
  }

  @Override
  public List<Ticket> getInvalidTickets(Commit<GetInvalidTickets> commit) {
    return new ArrayList<>(invalidTickets);
  }

  @Override
  public void cleanLogoutTicket(Commit<CleanLogoutTicket> commit) {
    Ticket t = commit.operation().getTicket();
    Commit<?> prev = previousCommit.remove(t);

    invalidTickets.remove(t);

    if (prev != null)
      prev.clean();
    commit.clean();
  }

  /**
   * Simple listener that is informed about tickets that became invalid (commited ones)
   */
  /* package */static interface LogoutStateMachineListener {
    public void ticketBecameInvalid(Ticket t);
  }
}
