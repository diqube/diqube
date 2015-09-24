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
package org.diqube.ui.websocket.json.request;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.websocket.Session;

import org.diqube.remote.query.thrift.QueryResultService.Iface;
import org.diqube.ui.DiqubeServletConfig;
import org.diqube.ui.QueryRegistry;
import org.diqube.ui.websocket.json.request.commands.AsyncJsonCommand;
import org.diqube.ui.websocket.json.request.commands.JsonCommand;
import org.diqube.ui.websocket.json.result.ExceptionJsonResult;
import org.diqube.ui.websocket.json.result.JsonResult;
import org.diqube.ui.websocket.json.result.JsonResultEnvelope;
import org.diqube.ui.websocket.json.result.JsonResultSerializer;
import org.diqube.ui.websocket.json.result.JsonResultSerializer.JsonPayloadSerializerException;
import org.diqube.util.Holder;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A request that was sent by the client and we should execute - it contains a {@link JsonCommand}, and the context of
 * this request. The context is defined by the session/requestId pair which is unique per request from the client - it
 * though can happen that two commands are sent using the same session/requestId pair, in which case this class ensures
 * that the corresponding commands get the same "environment".
 * 
 * <p>
 * An example could be that first a "query" command is executed with a session/requestId pair and after that a "cancel"
 * command - the latter will then cancel the former, because they have the same environment.
 * 
 * <p>
 * The environment is basically specified by instances of {@link CommandClusterInteraction} and
 * {@link CommandResultHandler} whcih are provided by this class to the {@link JsonCommand}.
 * 
 *
 * @author Bastian Gloeckle
 */
public class JsonRequest {
  private static final Logger logger = LoggerFactory.getLogger(JsonRequest.class);

  /**
   * The requestID that was created by the client to uniquely identify this request. Note that the uniqueness is <b>not
   * global</b>, but only local to the {@link #session}. This means that different sessions might (and actually will)
   * use the same request IDs to reference different requests!
   * 
   * <p>
   * Therefore one always needs to inspect both values, {@link #session} and {@link #requestId} to globally uniquely
   * identify a request.
   */
  private String requestId;
  /** The session that can be used to send data back to the client. Sync on this object when sending! */
  private Session session;
  private JsonCommand jsonCommand;

  @Inject
  @JsonIgnore
  private DiqubeServletConfig config;

  @Inject
  @JsonIgnore
  private QueryRegistry queryResultRegistry;

  @Inject
  @JsonIgnore
  private JsonResultSerializer serializer;

  private Holder<UUID> queryUuidHolder;

  /**
   * {@link CommandClusterInteraction} that is passed to the command which can safely interact with the diqube cluster
   * through this instance.
   */
  private CommandClusterInteraction commandClusterInteraction;
  private JsonRequestRegistry requestRegistry;

  /* package */ JsonRequest(Session session, String requestId, JsonCommand jsonCommand,
      JsonRequestRegistry requestRegistry) {
    this.session = session;
    this.requestId = requestId;
    this.jsonCommand = jsonCommand;
    this.requestRegistry = requestRegistry;
  }

  @PostConstruct
  public void initialize() {
    commandClusterInteraction = new AbstractCommandClusterInteraction(config) {
      @Override
      protected void unregisterLastQueryThriftResultCallback() {
        cleanup();
      }

      @Override
      protected void registerQueryThriftResultCallback(Pair<String, Short> node, UUID queryUuid, Iface resultHandler) {
        queryResultRegistry.registerThriftResultCallback(session, requestId, node, queryUuid, resultHandler);
        queryUuidHolder.setValue(queryUuid);
      }

      @Override
      protected Pair<UUID, Pair<String, Short>> findQueryUuidAndServerAddr() {
        UUID queryUuid = queryResultRegistry.getQueryUuid(session, requestId);
        Pair<String, Short> node = queryResultRegistry.getDiqubeServerAddr(queryUuid);

        if (queryUuid == null || node == null)
          return null;
        return new Pair<>(queryUuid, node);
      }
    };
  }

  /**
   * Execute the command of this request. Fully handles all interaction needed with the client.
   * 
   * It is expected that the request was registered at {@link JsonRequestRegistry}. It will unregister itself as soon as
   * it has completed.
   */
  public void executeCommand() {
    AtomicBoolean doneSent = new AtomicBoolean(false);
    queryUuidHolder = new Holder<>();

    CommandResultHandler commandResultHandler = new CommandResultHandler() {
      @Override
      public void sendException(Throwable t) {
        JsonRequest.this.sendException(queryUuidHolder, t);
      }

      @Override
      public void sendDone() {
        doneSent.set(true);
        JsonRequest.this.sendDone(queryUuidHolder);
      }

      @Override
      public void sendData(JsonResult data) {
        try {
          String serialized = serializer.serializeWithEnvelope(requestId, JsonResultEnvelope.STATUS_DATA, data);
          synchronized (session) {
            try {
              session.getBasicRemote().sendText(serialized);
            } catch (IOException e) {
              logger.warn("Could not send data to client", e);
            }
          }
        } catch (JsonPayloadSerializerException e) {
          throw new RuntimeException("Could not serialize data", e);
        }
      }
    };

    try {
      jsonCommand.execute(commandResultHandler, commandClusterInteraction);
    } catch (RuntimeException e) {
      sendException(queryUuidHolder, e);
      throw e;
    }

    if (!AsyncJsonCommand.class.isAssignableFrom(jsonCommand.getClass()) && !doneSent.get()) {
      sendDone(queryUuidHolder);
    }
  }

  /**
   * Cancel the request. This can only take effect on asynchronous commands! Otherwise this method will simply return.
   * 
   * The request will definitely be cleaned up in {@link JsonRequestRegistry}.
   */
  public void cancel() {
    cleanup();

    if (!AsyncJsonCommand.class.isAssignableFrom(jsonCommand.getClass()))
      // is no async command
      return;

    AsyncJsonCommand asyncCommand = (AsyncJsonCommand) jsonCommand;
    asyncCommand.cancel(commandClusterInteraction);
  }

  /**
   * Send a "done" to the client.
   * 
   * @param queryUuidHolder
   *          Holder for the queryUuid, if the command created a remote query.
   */
  private void sendDone(Holder<UUID> queryUuidHolder) {
    cleanup();

    try {
      String serialized = serializer.serializeWithEnvelope(requestId, JsonResultEnvelope.STATUS_DONE, null);
      synchronized (session) {
        try {
          session.getBasicRemote().sendText(serialized);
        } catch (IOException e) {
          logger.warn("Could not send done to client", e);
        }
      }
    } catch (JsonPayloadSerializerException e) {
      throw new RuntimeException("Could not serialize 'done'", e);
    }
  }

  /**
   * Send an exception to the client.
   * 
   * @param queryUuidHolder
   *          Holder for the queryUuid if the command created a remote query.
   * @param t
   *          The exception.
   */
  private void sendException(Holder<UUID> queryUuidHolder, Throwable t) {
    cleanup();

    ExceptionJsonResult ex = new ExceptionJsonResult();
    ex.setText(t.getMessage());
    String serialized;
    try {
      serialized = serializer.serializeWithEnvelope(requestId, JsonResultEnvelope.STATUS_EXCEPTION, ex);
    } catch (JsonPayloadSerializerException e) {
      throw new RuntimeException("Could not serialize result", e);
    }
    synchronized (session) {
      try {
        session.getBasicRemote().sendText(serialized);
      } catch (IOException e) {
        logger.warn("Could not send exception to client", e);
      }
    }
  }

  private void cleanup() {
    if (queryUuidHolder.getValue() != null)
      queryResultRegistry.unregisterQuery(requestId, queryUuidHolder.getValue());

    requestRegistry.unregisterRequest(session, this);
  }
}
