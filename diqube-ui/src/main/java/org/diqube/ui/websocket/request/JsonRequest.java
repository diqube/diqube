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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.websocket.Session;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.diqube.remote.query.thrift.QueryResultService.Iface;
import org.diqube.ui.DiqubeServletConfig;
import org.diqube.ui.UiQueryRegistry;
import org.diqube.ui.websocket.request.commands.AsyncJsonCommand;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.ExceptionJsonResult;
import org.diqube.ui.websocket.result.JsonResult;
import org.diqube.ui.websocket.result.JsonResultEnvelope;
import org.diqube.ui.websocket.result.JsonResultSerializer;
import org.diqube.ui.websocket.result.JsonResultSerializer.JsonPayloadSerializerException;
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
  private UiQueryRegistry queryResultRegistry;

  @Inject
  @JsonIgnore
  private JsonResultSerializer serializer;

  /** {@link Runnable}s that need to be executed to clean up. */
  @JsonIgnore
  private List<Runnable> cleanupActions = new ArrayList<>();

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
      protected void registerQueryThriftResultCallback(Pair<String, Short> node, UUID queryUuid, Iface resultHandler) {
        queryResultRegistry.registerThriftResultCallback(session, requestId, node, queryUuid, resultHandler);

        cleanupActions.add(() -> queryResultRegistry.unregisterQuery(requestId, queryUuid));
      }

      @Override
      protected Pair<UUID, Pair<String, Short>> findQueryUuidAndServerAddr() {
        UUID queryUuid = queryResultRegistry.getQueryUuid(session, requestId);
        Pair<String, Short> node = queryResultRegistry.getDiqubeServerAddr(queryUuid);

        if (queryUuid == null || node == null)
          return null;
        return new Pair<>(queryUuid, node);
      }

      @Override
      protected <T extends TServiceClient> T openConnection(Class<? extends T> thriftClientClass, String serviceName,
          Pair<String, Short> node) {
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
          return null;
        }

        cleanupActions.add(() -> transport.close());

        return res;
      }
    };
  }

  /**
   * Execute the command of this request. Fully handles all interaction needed with the client.
   * 
   * It is expected that the request was registered at {@link JsonRequestRegistry}. It will unregister itself as soon as
   * it has completed.
   * 
   * If the command throws an exception, {@link #sendException(Throwable)} will be run and the exception will not be
   * re-thrown!
   */
  public void executeCommand() {
    AtomicBoolean doneSent = new AtomicBoolean(false);

    CommandResultHandler commandResultHandler = new CommandResultHandler() {
      @Override
      public void sendException(Throwable t) {
        JsonRequest.this.sendException(t);
      }

      @Override
      public void sendDone() {
        doneSent.set(true);
        JsonRequest.this.sendDone();
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
      sendException(e);
      logger.warn("Exception while executing command", e);
      return;
    }

    if (!AsyncJsonCommand.class.isAssignableFrom(jsonCommand.getClass()) && !doneSent.get()) {
      sendDone();
    }
  }

  /**
   * Cancel the request. This can only take effect on asynchronous commands! Otherwise this method will simply return.
   * 
   * The request will definitely be cleaned up in {@link JsonRequestRegistry}.
   */
  public void cancel() {
    if (!AsyncJsonCommand.class.isAssignableFrom(jsonCommand.getClass())) {
      cleanup();
      return;
    }

    AsyncJsonCommand asyncCommand = (AsyncJsonCommand) jsonCommand;
    asyncCommand.cancel(commandClusterInteraction);

    cleanup();
  }

  /**
   * Send a "done" to the client.
   */
  private void sendDone() {
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
   * @param t
   *          The exception.
   */
  private void sendException(Throwable t) {
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
    for (Runnable r : cleanupActions)
      try {
        r.run();
      } catch (RuntimeException e) {
        logger.warn("Could not clean up request correctly", e);
        // continue with next cleanup action.
      }

    requestRegistry.unregisterRequest(session, this);
  }
}
