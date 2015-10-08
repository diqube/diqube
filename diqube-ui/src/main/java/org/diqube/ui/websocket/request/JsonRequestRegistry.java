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
package org.diqube.ui.websocket.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import javax.websocket.Session;

import org.diqube.context.AutoInstatiate;

/**
 * Registry for {@link JsonRequest}s that are being executed.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class JsonRequestRegistry {
  private Map<Session, Deque<JsonRequest>> liveRequests = new ConcurrentHashMap<>();

  public void registerRequest(Session websocketSession, JsonRequest request) {
    synchronized (websocketSession) {
      if (!liveRequests.containsKey(websocketSession))
        liveRequests.put(websocketSession, new ConcurrentLinkedDeque<>());
      liveRequests.get(websocketSession).add(request);
    }
  }

  public void unregisterRequest(Session websocketSession, JsonRequest request) {
    synchronized (websocketSession) {
      if (liveRequests.containsKey(websocketSession)) {
        liveRequests.get(websocketSession).remove(request);
        if (liveRequests.get(websocketSession).isEmpty())
          liveRequests.remove(websocketSession);
      }
    }
  }

  public Collection<JsonRequest> getRequestsOfSession(Session websocketSession) {
    Deque<JsonRequest> requests = liveRequests.get(websocketSession);
    if (requests == null)
      return new ArrayList<>();
    return new ArrayList<>(requests);
  }
}
