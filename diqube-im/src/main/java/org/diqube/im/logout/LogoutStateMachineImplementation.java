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

import java.io.File;
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

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.config.Config;
import org.diqube.config.DerivedConfigKey;
import org.diqube.connection.ConnectionException;
import org.diqube.connection.ConnectionOrLocalHelper;
import org.diqube.connection.NodeAddress;
import org.diqube.connection.ServiceProvider;
import org.diqube.consensus.ConsensusStateMachineImplementation;
import org.diqube.consensus.DiqubeConsensusStateMachineClientInterruptedException;
import org.diqube.consensus.DiqubeCopycatClient;
import org.diqube.consensus.DiqubeCopycatClient.ClosableProvider;
import org.diqube.context.InjectOptional;
import org.diqube.file.internaldb.InternalDbFileReader;
import org.diqube.file.internaldb.InternalDbFileReader.ReadException;
import org.diqube.file.internaldb.InternalDbFileWriter;
import org.diqube.file.internaldb.InternalDbFileWriter.WriteException;
import org.diqube.im.callback.IdentityCallbackRegistryStateMachine;
import org.diqube.im.callback.IdentityCallbackRegistryStateMachine.GetAllRegistered;
import org.diqube.remote.query.TicketInfoUtil;
import org.diqube.remote.query.thrift.IdentityCallbackService;
import org.diqube.remote.query.thrift.Ticket;
import org.diqube.threads.ExecutorManager;
import org.diqube.thrift.base.thrift.RNodeAddress;
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

  private static final String INTERNALDB_FILE_PREFIX = "logout-";
  private static final String INTERNALDB_DATA_TYPE = "logout_v1";

  private Set<Ticket> invalidTickets = new ConcurrentSkipListSet<>();

  private Map<Ticket, Commit<?>> previousCommit = new ConcurrentHashMap<>();

  @Config(DerivedConfigKey.FINAL_INTERNAL_DB_DIR)
  private String internalDbDir;

  @Inject
  private TicketValidityService ticketValidityService;

  @Inject
  private ConnectionOrLocalHelper connectionOrLocalHelper;

  @Inject
  private DiqubeCopycatClient consensusClient;

  @InjectOptional
  private List<LogoutStateMachineListener> listeners;

  @Inject
  private ExecutorManager executorManager;

  private ExecutorService executorService;

  private InternalDbFileWriter<Ticket> internalDbFileWriter;

  @PostConstruct
  public void initialize() {
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

    File internalDbDirFile = new File(internalDbDir);
    if (!internalDbDirFile.exists())
      if (!internalDbDirFile.mkdirs())
        throw new RuntimeException("Could not create directory " + internalDbDir);

    try {
      InternalDbFileReader<Ticket> internalDbFileReader = new InternalDbFileReader<>(INTERNALDB_DATA_TYPE,
          INTERNALDB_FILE_PREFIX, internalDbDirFile, () -> new Ticket());
      List<Ticket> tickets = internalDbFileReader.readNewest();
      if (tickets != null)
        for (Ticket ticket : tickets) {
          this.invalidTickets.add(ticket);
        }
      else
        logger.info("No internaldb for logouts available");
    } catch (ReadException e) {
      throw new RuntimeException("Could not load logouts file", e);
    }

    internalDbFileWriter = new InternalDbFileWriter<>(INTERNALDB_DATA_TYPE, INTERNALDB_FILE_PREFIX, internalDbDirFile);
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

    writeCurrentLogoutsToInternalDb(commit.index());

    // inform all registered callbacks, but do this asynchronously. This is needed since we use a consensus client here
    // again which might conenct to the local node: We would end up in a deadlock, since the local consensus server is
    // executign something already. It is not vital that the callbacks are called synchrounously anyway.
    executorService.execute(() -> {
      Set<NodeAddress> callbackAddresses = new HashSet<>();
      try (ClosableProvider<IdentityCallbackRegistryStateMachine> p =
          consensusClient.getStateMachineClient(IdentityCallbackRegistryStateMachine.class)) {
        List<RNodeAddress> addrs = p.getClient().getAllRegistered(GetAllRegistered.local());

        addrs.forEach(a -> callbackAddresses.add(new NodeAddress(a)));
      } catch (DiqubeConsensusStateMachineClientInterruptedException e) {
        // quietly exit
        return;
      } catch (RuntimeException e) {
        logger.warn("Could not get addresses of logout callbacks from consensus cluster. Will not inform any.", e);
        return;
      }

      // note that here again we might not reach all registered callbacks (e.g. because of network partitions). The
      // callbacks must poll a fresh list of invalidated tickets periodically and should not accept any tickets if the
      // can't reach the cluster.
      for (NodeAddress callbackAddr : callbackAddresses) {
        try (ServiceProvider<IdentityCallbackService.Iface> p = connectionOrLocalHelper
            .getService(IdentityCallbackService.Iface.class, callbackAddr.createRemote(), null)) {

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

    writeCurrentLogoutsToInternalDb(commit.index());

    if (prev != null)
      prev.clean();
    commit.clean();
  }

  private void writeCurrentLogoutsToInternalDb(long consensusIndex) {
    try {
      internalDbFileWriter.write(consensusIndex, new ArrayList<>(invalidTickets));
    } catch (WriteException e1) {
      logger.error("Could not write logouts internaldb file!", e1);
      // this is an error, but we try to continue anyway. When the file is missing, the node might not be able to
      // recover correctly, but for now we can keep working. The admin might want to copy a internaldb file from a
      // different node.
    }
  }

  /**
   * Simple listener that is informed about tickets that became invalid (commited ones)
   */
  /* package */static interface LogoutStateMachineListener {
    public void ticketBecameInvalid(Ticket t);
  }
}
