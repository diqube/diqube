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

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.diqube.consensus.ConsensusStateMachineClientInterruptedException;
import org.diqube.consensus.ConsensusClient;
import org.diqube.consensus.ConsensusClient.ClosableProvider;
import org.diqube.context.AutoInstatiate;
import org.diqube.im.logout.LogoutStateMachine.CleanLogoutTicket;
import org.diqube.im.logout.LogoutStateMachineImplementation.LogoutStateMachineListener;
import org.diqube.remote.query.thrift.Ticket;
import org.diqube.remote.query.thrift.TicketClaim;
import org.diqube.ticket.TicketValidityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleans Tickets from {@link LogoutStateMachine} that are invalid now anyway (i.e. because the
 * {@link TicketClaim#getValidUntil()} has passed, the tickets do nat have to be tracked manually anymore).
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class LogoutCleaner implements LogoutStateMachineListener {
  private static final Logger logger = LoggerFactory.getLogger(LogoutCleaner.class);

  /** max time between polls for old logged out tickets that can be cleaned. */
  private static final long MAX_POLL_SEC = 120; // 4 min

  @Inject
  private ConsensusClient consensusClient;

  private LogoutCleanupThread thread;

  /** Map from {@link TicketClaim#getValidUntil()} to the tickets that have been invalidated. */
  private NavigableMap<Long, Set<Ticket>> timeouts = new ConcurrentSkipListMap<>();

  @PostConstruct
  public void intialize() {
    thread = new LogoutCleanupThread();
    thread.start();
  }

  @PreDestroy
  public void cleanup() {
    thread.interrupt();
  }

  @Override
  public void ticketBecameInvalid(Ticket t) {
    timeouts.compute(t.getClaim().getValidUntil(), (key, oldValue) -> {
      Set<Ticket> res = new ConcurrentSkipListSet<>();
      res.add(t);
      if (oldValue != null)
        res.addAll(oldValue);
      return res;
    });
  }

  private class LogoutCleanupThread extends Thread {
    LogoutCleanupThread() {
      super("logout-cleanup-thread");
    }

    @Override
    public void run() {
      long randomWaitDiff = ThreadLocalRandom.current().nextLong((MAX_POLL_SEC / 2) * 1_000L);
      while (true) {
        try {
          Thread.sleep((MAX_POLL_SEC / 2) * 1_000L + randomWaitDiff); // random so not everybody executes simultaneously
        } catch (InterruptedException e) {
          // exit quietly
          return;
        }

        Map<Long, Set<Ticket>> timeoutedTickets =
            timeouts.headMap(System.currentTimeMillis() - TicketValidityService.CLEANUP_NO_REMOVE_MOST_RECENT_MS);

        if (!timeoutedTickets.isEmpty()) {
          logger.info("Found {} tickets that have been logged out but are invalid now anyway. "
              + "Removing them from the consensus cluster.", timeoutedTickets.size());

          try (ClosableProvider<LogoutStateMachine> p =
              consensusClient.getStateMachineClient(LogoutStateMachine.class)) {
            Set<Ticket> allTickets =
                timeoutedTickets.values().stream().flatMap(s -> s.stream()).collect(Collectors.toSet());
            for (Ticket t : allTickets)
              p.getClient().cleanLogoutTicket(CleanLogoutTicket.local(t));

            timeoutedTickets.clear();
          } catch (ConsensusStateMachineClientInterruptedException e) {
            // exit quietly
            return;
          } catch (RuntimeException e) {
            logger.warn("Could not contact consensus cluster to remove old logged out tickets", e);
          }
        }
      }
    }

  }
}
