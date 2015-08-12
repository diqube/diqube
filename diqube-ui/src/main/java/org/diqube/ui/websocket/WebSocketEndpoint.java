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
package org.diqube.ui.websocket;

import javax.websocket.CloseReason;
import javax.websocket.EndpointConfig;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;

import org.diqube.ui.DiqubeServletContextListener;
import org.diqube.ui.QueryResultRegistry;
import org.diqube.ui.websocket.json.JsonCommand;
import org.diqube.ui.websocket.json.JsonExceptionPayload;
import org.diqube.ui.websocket.json.JsonPayload;
import org.diqube.ui.websocket.json.JsonPayloadDeserializer;
import org.diqube.ui.websocket.json.JsonPayloadDeserializer.JsonPayloadDeserializerException;
import org.diqube.ui.websocket.json.JsonPayloadSerializer;
import org.diqube.ui.websocket.json.JsonPayloadSerializer.JsonPayloadSerializerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

/**
 * Websocket endpoint that will be used by the JavaScript UI.
 * 
 * This endpoint is instantiated programatically, see {@link DiqubeServletContextListener}. For more information, see
 * JSR 356, v1.1, 6.4 "Programmatic Server Deployment".
 *
 * @author Bastian Gloeckle
 */
public class WebSocketEndpoint {
  private static final Logger logger = LoggerFactory.getLogger(WebSocketEndpoint.class);

  /** The URL mapping under which this endpoint will be available */
  public static final String ENDPOINT_URL_MAPPING = "/socket";

  /**
   * Property name in this endpoints {@link EndpointConfig#getUserProperties()} and in all
   * {@link Session#getUserProperties()} whose value is an {@link ApplicationContext}.
   */
  public static final String PROP_BEAN_CONTEXT = "diqube.springContext";

  @OnOpen
  public void onOpen(Session session, EndpointConfig config) {
    ApplicationContext ctx = (ApplicationContext) config.getUserProperties().get(PROP_BEAN_CONTEXT);
    session.getUserProperties().put(PROP_BEAN_CONTEXT, ctx);
  }

  @OnMessage
  public void onMessage(String msg, Session session) {
    try {
      JsonPayload payload = getBeanCtx(session).getBean(JsonPayloadDeserializer.class).deserialize(msg, session);

      if (!(payload instanceof JsonCommand))
        throw new RuntimeException("Could not correctly deserialize command!");

      ((JsonCommand) payload).execute();
    } catch (JsonPayloadDeserializerException e) {
      throw new RuntimeException("Could not correctly deserialize command!");
    } catch (RuntimeException e) {
      JsonExceptionPayload exPayload = new JsonExceptionPayload();
      exPayload.setText(e.getMessage());
      try {
        String serializedException = getBeanCtx(session).getBean(JsonPayloadSerializer.class).serialize(exPayload);
        session.getAsyncRemote().sendText(serializedException);
        logger.error("Uncaught RuntimeException", e);
      } catch (JsonPayloadSerializerException e2) {
        logger.error("Uncaught RuntimeException in onMessage and was not able to serialize the exception message", e,
            e2);
      }
    }
  }

  @OnClose
  public void onClose(Session session, CloseReason reason) {
    System.out.println("Received CLOSE (" + session + "): " + reason);
    getBeanCtx(session).getBean(QueryResultRegistry.class).unregisterSession(session);
  }

  @OnError
  public void onError(Session session, Throwable throwable) {
    System.out.println("Received ERROR (" + session + "): " + throwable.toString());
  }

  private ApplicationContext getBeanCtx(Session session) {
    return (ApplicationContext) session.getUserProperties().get(PROP_BEAN_CONTEXT);
  }
}
