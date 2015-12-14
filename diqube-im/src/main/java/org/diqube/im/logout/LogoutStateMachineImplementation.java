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
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.connection.ConnectionException;
import org.diqube.connection.ConnectionOrLocalHelper;
import org.diqube.connection.ServiceProvider;
import org.diqube.consensus.AbstractConsensusStateMachine;
import org.diqube.consensus.ConsensusClient;
import org.diqube.consensus.ConsensusClient.ClosableProvider;
import org.diqube.consensus.ConsensusStateMachineClientInterruptedException;
import org.diqube.consensus.ConsensusStateMachineImplementation;
import org.diqube.context.InjectOptional;
import org.diqube.im.callback.IdentityCallbackRegistryStateMachine;
import org.diqube.im.callback.IdentityCallbackRegistryStateMachine.GetAllRegistered;
import org.diqube.remote.query.TicketInfoUtil;
import org.diqube.remote.query.thrift.IdentityCallbackService;
import org.diqube.threads.ExecutorManager;
import org.diqube.thrift.base.thrift.RNodeAddress;
import org.diqube.thrift.base.thrift.Ticket;
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
public class LogoutStateMachineImplementation extends AbstractConsensusStateMachine<Ticket>
    implements LogoutStateMachine {
  private static final Logger logger = LoggerFactory.getLogger(LogoutStateMachineImplementation.class);

  private static final String INTERNALDB_FILE_PREFIX = "logout-";
  private static final String INTERNALDB_DATA_TYPE = "logout_v1";

  private Set<Ticket> invalidTickets = new ConcurrentSkipListSet<>();

  private Map<Ticket, Commit<?>> previousCommit = new ConcurrentHashMap<>();

  @Inject
  private TicketValidityService ticketValidityService;

  @Inject
  private ConnectionOrLocalHelper connectionOrLocalHelper;

  @Inject
  private ConsensusClient consensusClient;

  @InjectOptional
  private List<LogoutStateMachineListener> listeners;

  @Inject
  private ExecutorManager executorManager;

  private ExecutorService executorService;

  public LogoutStateMachineImplementation() {
    super(INTERNALDB_FILE_PREFIX, INTERNALDB_DATA_TYPE, () -> new Ticket());
  }

  @Override
  protected void doInitialize(List<Ticket> entriesLoadedFromInternalDb) {
    executorService = executorManager.newCachedThreadPoolWithMax("logout-state-machine-async-worker-%d",
        new UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread t, Throwable e) {
            // Log, but ignore otherwise - at least one cluster node should succeed. And if not: We did not guarantee
            // that
            // we'd reach all registered callbacks! Usually we should succeed though.
            logger.warn("Failed to execute async work in {}", LogoutStateMachineImplementation.class.getSimpleName(),
                e);
          }
        }, 5);

    if (entriesLoadedFromInternalDb != null)
      invalidTickets.addAll(entriesLoadedFromInternalDb);
  }

  @PreDestroy
  public void cleanup() {
    if (executorService != null)
      executorService.shutdownNow();
  }

  @Override
  public void logout(Commit<Logout> commit) {
    Ticket t = commit.operation().getTicket();

    Commit<?> prev = previousCommit.put(t, commit);

    ticketValidityService.markTicketAsInvalid(TicketInfoUtil.fromTicket(t));
    invalidTickets.add(t);

    writeCurrentStateToInternalDb(commit.index(), invalidTickets);

    // inform all registered callbacks, but do this asynchronously. This is needed since we use a consensus client here
    // again which might conenct to the local node: We would end up in a deadlock, since the local consensus server is
    // executign something already. It is not vital that the callbacks are called synchrounously anyway.
    executorService.execute(() -> {
      Set<RNodeAddress> callbackAddresses = new HashSet<>();
      try (ClosableProvider<IdentityCallbackRegistryStateMachine> p =
          consensusClient.getStateMachineClient(IdentityCallbackRegistryStateMachine.class)) {
        List<RNodeAddress> addrs = p.getClient().getAllRegistered(GetAllRegistered.local());

        addrs.forEach(a -> callbackAddresses.add(a));
      } catch (ConsensusStateMachineClientInterruptedException e) {
        // quietly exit
        return;
      } catch (RuntimeException e) {
        logger.warn("Could not get addresses of logout callbacks from consensus cluster. Will not inform any.", e);
        return;
      }

      // note that here again we might not reach all registered callbacks (e.g. because of network partitions). The
      // callbacks must poll a fresh list of invalidated tickets periodically and should not accept any tickets if the
      // can't reach the cluster.
      for (RNodeAddress callbackAddr : callbackAddresses) {
        try (ServiceProvider<IdentityCallbackService.Iface> p =
            connectionOrLocalHelper.getService(IdentityCallbackService.Iface.class, callbackAddr, null)) {

          p.getService().ticketBecameInvalid(TicketInfoUtil.fromTicket(t));

        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while communicating to " + callbackAddr, e);
        } catch (TException | IOException | ConnectionException | RuntimeException e) {
          logger.warn("Could not send invalidation of ticket (logout) to registered callback node {}.", callbackAddr);
        }
      }
    });

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

    writeCurrentStateToInternalDb(commit.index(), invalidTickets);

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
