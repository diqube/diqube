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
package org.diqube.ui;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.websocket.Session;

import org.diqube.context.AutoInstatiate;
import org.diqube.remote.query.thrift.QueryResultService;
import org.diqube.util.Pair;
import org.diqube.util.Triple;

/**
 * A registry for queries that have been sent to the cluster nodes for processing - and the result handlers that should
 * be called as soon as some result data is available.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class UiQueryRegistry {
  /**
   * queryUuid to triple of
   * <ul>
   * <li>websocket session that initialized the query
   * <li>Pair of hostname and port of the diqube-server the query was sent to (the query master)
   * <li>The result handler.
   * </ul>
   */
  private Map<UUID, Triple<Session, Pair<String, Short>, QueryResultService.Iface>> infoByQueryUuid =
      new ConcurrentHashMap<>();
  private Map<Session, Map<String, UUID>> sessionsToRequestIdToQueryUuid = new ConcurrentHashMap<>();

  /**
   * Register a {@link QueryResultService} callback that will be informed as soon as results for the given query have
   * been received.
   * 
   * @param session
   *          The Websocket {@link Session} that initialized the query. This is needed as the following parameter,
   *          requestId, is unique only within one {@link Session}.
   * @param requestId
   *          The ID provided by the client that uniquely identifies the request it sent in the context of the websocket
   *          session. The installed result handler will provide result data for that request.
   * @param diqubeServerAddr
   *          The server address to which the query is sent to for execution.
   * @param queryUuid
   *          The query that has been sent to the diqube-server to identify the query.
   * @param resultHandler
   *          The handler which will get informed.
   */
  public void registerThriftResultCallback(Session session, String requestId, Pair<String, Short> diqubeServerAddr,
      UUID queryUuid, QueryResultService.Iface resultHandler) {
    infoByQueryUuid.put(queryUuid, new Triple<>(session, diqubeServerAddr, resultHandler));
    Map<String, UUID> sessionMap = sessionsToRequestIdToQueryUuid.get(session);
    if (sessionMap == null) {
      synchronized (sessionsToRequestIdToQueryUuid) {
        if (!sessionsToRequestIdToQueryUuid.containsKey(session))
          sessionsToRequestIdToQueryUuid.put(session, new ConcurrentHashMap<>());
        sessionMap = sessionsToRequestIdToQueryUuid.get(session);
      }
    }
    sessionMap.put(requestId, queryUuid);
  }

  /**
   * Unregister the callback of a specific query.
   */
  public void unregisterQuery(String requestId, UUID queryUuid) {
    Triple<Session, Pair<String, Short>, QueryResultService.Iface> oldTriple = infoByQueryUuid.remove(queryUuid);
    if (oldTriple != null) {
      Session session = oldTriple.getLeft();
      sessionsToRequestIdToQueryUuid.get(session).remove(requestId);
      if (sessionsToRequestIdToQueryUuid.get(session).isEmpty()) {
        synchronized (sessionsToRequestIdToQueryUuid) {
          if (sessionsToRequestIdToQueryUuid.get(session).isEmpty())
            sessionsToRequestIdToQueryUuid.remove(session);
        }
      }
    }
  }

  /**
   * Find and return the queryUuid that was used to start a query on the diqube-servers based on the given request by a
   * client.
   * 
   * @param session
   *          The websocket session in which we initiated the request.
   * @param requestId
   *          The request ID that was sent by th client to uniquely identify the request within the session.
   * @return The Query UUID of the query that was triggered, or <code>null</code> if not available.
   */
  public UUID getQueryUuid(Session session, String requestId) {
    Map<String, UUID> sessionMap = sessionsToRequestIdToQueryUuid.get(session);
    if (sessionMap == null)
      return null;
    return sessionMap.get(requestId);
  }

  /**
   * Finds the address of the diqube-server that a specific query was sent to for execution.
   * 
   * @param queryUuid
   *          The UUID of the query that we're searching.
   * @return Pair of hostname and port or <code>null</code> if not available.
   */
  public Pair<String, Short> getDiqubeServerAddr(UUID queryUuid) {
    Triple<Session, Pair<String, Short>, QueryResultService.Iface> p = infoByQueryUuid.get(queryUuid);
    if (p == null)
      return null;
    return p.getMiddle();
  }

  /**
   * Get the handler that is registered for a specific query or <code>null</code> if there is none.
   */
  public QueryResultService.Iface getHandler(UUID queryUuid) {
    Triple<Session, Pair<String, Short>, QueryResultService.Iface> p = infoByQueryUuid.get(queryUuid);
    if (p == null)
      return null;
    return p.getRight();
  }
}
