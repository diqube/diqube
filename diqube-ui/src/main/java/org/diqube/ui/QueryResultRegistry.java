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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.websocket.Session;

import org.diqube.context.AutoInstatiate;
import org.diqube.remote.query.thrift.QueryResultService;
import org.diqube.util.Pair;

/**
 * A registry for queries that have been sent to the cluster nodes for processing - and the result handlers that should
 * be called as soon as some result data is available.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class QueryResultRegistry {
  private Map<UUID, Pair<Session, QueryResultService.Iface>> handlers = new ConcurrentHashMap<>();
  private Map<Session, Set<UUID>> sessions = new ConcurrentHashMap<>();

  /**
   * Register a {@link QueryResultService} callback that will be informed as soon as results for the given query have
   * been received.
   * 
   * @param session
   *          The Websocket {@link Session} that initialized the query - this is needed to keep track of what queries a
   *          session created and what should be cleaned up when the {@link Session} dies e.g.
   * @param queryUuid
   *          The query that has been sent
   * @param resultHandler
   *          The handler which will get informed.
   */
  public void registerThriftResultCallback(Session session, UUID queryUuid, QueryResultService.Iface resultHandler) {
    handlers.put(queryUuid, new Pair<>(session, resultHandler));
    Set<UUID> sessionSet = sessions.get(session);
    if (sessionSet == null) {
      synchronized (sessions) {
        if (!sessions.containsKey(session))
          sessions.put(session, new ConcurrentSkipListSet<>());
        sessionSet = sessions.get(session);
      }
    }
    sessionSet.add(queryUuid);
  }

  /**
   * Unregister the callback of a specific query.
   */
  public void unregisterQuery(UUID queryUuid) {
    Pair<Session, QueryResultService.Iface> oldPair = handlers.remove(queryUuid);
    if (oldPair != null) {
      Session session = oldPair.getLeft();
      sessions.get(session).remove(queryUuid);
      if (sessions.get(session).isEmpty()) {
        synchronized (sessions) {
          if (sessions.get(session).isEmpty())
            sessions.remove(session);
        }
      }
    }
  }

  /**
   * Unregister the callbacks of all queries the given session created.
   */
  public void unregisterSession(Session session) {
    Set<UUID> oldUuids = sessions.remove(session);
    if (oldUuids != null)
      for (UUID oldUuid : oldUuids)
        handlers.remove(oldUuid);
  }

  /**
   * Get the handler that is registered for a specific query or <code>null</code> if there is none.
   */
  public QueryResultService.Iface getHandler(UUID queryUuid) {
    Pair<Session, QueryResultService.Iface> p = handlers.get(queryUuid);
    if (p == null)
      return null;
    return p.getRight();
  }
}
