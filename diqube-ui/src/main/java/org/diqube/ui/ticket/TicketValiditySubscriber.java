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
package org.diqube.ui.ticket;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.query.IdentityServiceConstants;
import org.diqube.remote.query.thrift.IdentityService;
import org.diqube.remote.query.thrift.TicketInfo;
import org.diqube.ticket.TicketValidityService;
import org.diqube.ui.DiqubeServletConfig;
import org.diqube.ui.DiqubeServletConfig.ServletConfigListener;
import org.diqube.ui.websocket.WebSocketEndpoint.WebSocketEndpointListener;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subscribes for logout-events sent by diqube-servers (see
 * {@link IdentityService.Iface#registerCallback(org.diqube.thrift.base.thrift.RNodeAddress)} and takes care of
 * regularily fetching updates on invalidated tickets.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class TicketValiditySubscriber
    implements WebSocketEndpointListener, TicketsAcceptableProvider, ServletConfigListener {
  private static final Logger logger = LoggerFactory.getLogger(TicketValiditySubscriber.class);

  /**
   * Re-subscribe to updates received from diqube-servers after this amount of minutes. This is needed, since
   * diqube-server will automatically remove our registration after about 1h.
   */
  private static final long RESUBSCRIBE_MIN = 19;

  @Inject
  private DiqubeServletConfig config;

  @Inject
  private TicketValidityService ticketValidityService;

  private volatile long nextTimestampToFetchListFromServer = 0L;
  private volatile long nextTimestampToResubscribeToServer = 0L;
  private AtomicBoolean lastFetchSuccessful = new AtomicBoolean(false);

  @Override
  public void servletConfigurationAvailable() {
    // as soon as configuration is available, we try to connect and subscribe.
    work();
  }

  @PreDestroy
  public void cleanup() {
    if (nextTimestampToResubscribeToServer != 0L) // we probably are subscribed. If not any more, it is not bad to call.
      try {
        openIdentityService().unregisterCallback(config.createClusterResponseAddr());
      } catch (TException e) {
        throw new RuntimeException("Could not register to receive logout updates.");
      }
  }

  /**
   * Work method. Gets called pretty often and registers at the server/fetches updated logged out lists.
   */
  private void work() {
    if (System.currentTimeMillis() > nextTimestampToResubscribeToServer) {
      synchronized (this) {
        if (System.currentTimeMillis() > nextTimestampToResubscribeToServer) {
          try {
            openIdentityService().registerCallback(config.createClusterResponseAddr());
            logger.info("Registered at a diqube server for updates on logged out tickets.");
            nextTimestampToResubscribeToServer = System.currentTimeMillis() + RESUBSCRIBE_MIN * 60 * 1_000L;
          } catch (RuntimeException | TException e) {
            logger.warn("Could not subscribe to logout updates at diqube-servers!", e);
          }
        }
      }
    }

    if (System.currentTimeMillis() > nextTimestampToFetchListFromServer) {
      // This is meant to block all threads until a new list has been fetched! We must not accept anything before we did
      // not fetch a new list!
      synchronized (this) {
        if (System.currentTimeMillis() > nextTimestampToFetchListFromServer) {
          try {
            List<TicketInfo> invalidTickets = openIdentityService().getInvalidTicketInfos();

            logger.info("Fetched up-to-date list of logged out tickets.");

            // actually mark tickets as invalid.
            for (TicketInfo t : invalidTickets)
              ticketValidityService.markTicketAsInvalid(t);

            lastFetchSuccessful.set(true);
            nextTimestampToFetchListFromServer = System.currentTimeMillis() + config.getLogoutTicketFetchSec() * 1_000L;
          } catch (RuntimeException | TException e) {
            lastFetchSuccessful.set(false);
            logger.warn("Could not fetch up-to-date logout list from diqube-server!", e);
            // retry in 2r seconds
            nextTimestampToFetchListFromServer = System.currentTimeMillis() + 2 * 1_000L;
          }
        }
      }
    }
  }

  @Override
  public void socketOpened() {
    work();
  }

  @Override
  public void socketMessage() {
    work();
  }

  @Override
  public boolean areTicketsAcceptable() {
    // Only accept any Ticket in the UI if we were able to reach a diqube-server recently and were able to load a
    // current list of Tickets which seem to be valid, but have been logged out (and therefore are not valid anymore).
    // If we did not reach any diqube-server, then we will accept NO tickets at all, since they could have been logged
    // out in the meantime and we simply do not know about it.
    return lastFetchSuccessful.get();
  }

  private IdentityService.Iface openIdentityService() {
    IdentityService.Iface identityService =
        openConnection(IdentityService.Client.class, IdentityServiceConstants.SERVICE_NAME);
    if (identityService == null)
      throw new RuntimeException("Could not connect to any cluster node!");

    return identityService;
  }

  private <T> T openConnection(Class<T> thriftClientClass, String serviceName) {
    for (Pair<String, Short> node : config.getClusterServers()) {
      TTransport transport = new TFramedTransport(new TSocket(node.getLeft(), node.getRight()));
      TProtocol protocol = new TMultiplexedProtocol(new TCompactProtocol(transport), serviceName);

      T res;
      try {
        res = thriftClientClass.getConstructor(TProtocol.class).newInstance(protocol);
      } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
          | NoSuchMethodException | SecurityException e) {
        throw new RuntimeException("Could not instantiate thrift client", e);
      }

      try {
        transport.open();
      } catch (TTransportException e) {
        continue;
      }
      return res;
    }
    return null;
  }

}
