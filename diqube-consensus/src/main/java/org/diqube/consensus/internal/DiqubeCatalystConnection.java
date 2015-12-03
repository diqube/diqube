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
package org.diqube.consensus.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.thrift.TException;
import org.diqube.connection.ConnectionException;
import org.diqube.connection.ConnectionOrLocalHelper;
import org.diqube.connection.NodeAddress;
import org.diqube.connection.OurNodeAddressProvider;
import org.diqube.connection.ServiceProvider;
import org.diqube.connection.SocketListener;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.cluster.thrift.ClusterConsensusService;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.MessageHandler;
import io.atomix.catalyst.transport.TransportException;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.catalyst.util.ReferenceCounted;
import io.atomix.catalyst.util.concurrent.ThreadContext;

/**
 * A diqube Catalyst connection that is used internally by copycat and which is implemented to encapsulate any catalyst
 * message to be sent/received with thrift and send/receive it through {@link ClusterConsensusService}.
 * 
 * <p>
 * Each Catalyst connection is identified by two {@link #getConnectionEndpointUuid()}s - one on the one end of the
 * connection the other on the other end. These are transported via {@link ClusterConsensusService}.
 * 
 * <p>
 * When the connection is closed, it is automatically unregistered in {@link ClusterConsensusConnectionRegistry}.
 * 
 * <p>
 * After instantiating, call either {@link #acceptAndRegister(UUID, RNodeAddress)} or {@link #openAndRegister(Address)}.
 *
 * @author Bastian Gloeckle
 */
public class DiqubeCatalystConnection implements Connection {
  private static final Logger logger = LoggerFactory.getLogger(DiqubeCatalystConnection.class);

  /**
   * Registered handlers for incoming messages
   */
  private Map<Class<?>, Pair<MessageHandler<Object, Object>, ThreadContext>> handlers = new ConcurrentHashMap<>();

  /**
   * Currently active requests for which we wait on a response.
   */
  private Map<UUID, Pair<CompletableFuture<Object>, ThreadContext>> requests = new ConcurrentHashMap<>();

  private final Listeners<Throwable> exceptionListeners = new Listeners<>();
  private final Listeners<Connection> closeListeners = new Listeners<>();

  private ConnectionOrLocalHelper connectionOrLocalHelper;
  /** true if there was an exception on the socket, means the connection is dead. */
  private boolean connectionDiedAlready = false;
  /** The ID identifying this endpoint of the catalyst connection. <code>null</code> if connection is closed */
  private UUID connectionEndpointUuid = null;

  /** Address we maintain a catalyst connection to */
  private RNodeAddress remoteAddr;
  /** The endpoint ID of the other side of this connection. */
  private UUID remoteEndpointUuid;
  /** Provider of the node addresses of our cluster */
  private OurNodeAddressProvider ourNodeAddressProvider;
  /** The registry we register to and deregister from when the connection is closed */
  private ClusterConsensusConnectionRegistry registry;
  /** The context to be used to de-/serialize data if we have no other context available */
  private ThreadContext generalContext;

  private SocketListener socketListener = new SocketListener() {
    @Override
    public void connectionDied(String cause) {
      connectionDiedAlready = true;
      connectionEndpointUuid = null;
      exceptionListeners.accept(new TransportException("Connection died"));
    }
  };

  public DiqubeCatalystConnection(ClusterConsensusConnectionRegistry registry,
      ConnectionOrLocalHelper connectionOrLocalHelper, OurNodeAddressProvider ourNodeAddressProvider,
      ThreadContext generalContext) {
    this.registry = registry;
    this.connectionOrLocalHelper = connectionOrLocalHelper;
    this.ourNodeAddressProvider = ourNodeAddressProvider;
    this.generalContext = generalContext;
  }

  /**
   * Accept a connection that was initialized by another peer already. Will register the new connection with
   * {@link ClusterConsensusConnectionRegistry}.
   * 
   * @param remoteConnectionUuid
   *          The ID of the connection endpoint fo the other side.
   * @param remoteAddr
   *          The address of the remote.
   */
  public UUID acceptAndRegister(UUID remoteConnectionUuid, RNodeAddress remoteAddr) {
    this.connectionEndpointUuid = UUID.randomUUID();
    this.remoteEndpointUuid = remoteConnectionUuid;
    this.remoteAddr = remoteAddr;
    registry.registerConnectionEndpoint(connectionEndpointUuid, this);
    return connectionEndpointUuid;
  }

  /**
   * Opens a new connection to a peer.
   * 
   * @param catylstRemoteAddr
   *          The catalyst-style address of the remote.
   * @throws TransportException
   *           If connection cannot be opened.
   */
  public void openAndRegister(Address catylstRemoteAddr) throws TransportException {
    NodeAddress nodeAddress = new NodeAddress(catylstRemoteAddr.host(), (short) catylstRemoteAddr.port());
    this.remoteAddr = nodeAddress.createRemote();
    connectionEndpointUuid = UUID.randomUUID();

    try (ServiceProvider<ClusterConsensusService.Iface> sp =
        connectionOrLocalHelper.getService(ClusterConsensusService.Iface.class, remoteAddr, socketListener)) {

      RUUID remoteEndpointRUuid = sp.getService().open(RUuidUtil.toRUuid(connectionEndpointUuid),
          ourNodeAddressProvider.getOurNodeAddress().createRemote());

      this.remoteEndpointUuid = RUuidUtil.toUuid(remoteEndpointRUuid);

    } catch (ConnectionException | IOException | InterruptedException | IllegalStateException | TException e) {
      throw new TransportException("Could not establish connection to " + remoteAddr);
    }

    registry.registerConnectionEndpoint(connectionEndpointUuid, this);
  }

  /**
   * Handle a response that was received for this catalyst connection.
   * 
   * @param requestUuid
   * @param data
   */
  public void handleResponse(UUID requestUuid, ByteBuffer data) {
    Pair<CompletableFuture<Object>, ThreadContext> p = requests.remove(requestUuid);

    if (p != null) {
      byte[] bytes = new byte[data.remaining()];
      data.get(bytes);
      Object message = p.getRight().serializer().readObject(new ByteArrayInputStream(bytes));

      p.getRight().executor().execute(() -> p.getLeft().complete(message));
    }
  }

  /**
   * Handle the response of a request this connection sent, if the response is exceptional.
   * 
   * @param requestUuid
   * @param data
   */
  public void handleResponseException(UUID requestUuid, ByteBuffer data) {
    Pair<CompletableFuture<Object>, ThreadContext> p = requests.remove(requestUuid);

    if (p != null) {
      byte[] bytes = new byte[data.remaining()];
      data.get(bytes);
      Object message = p.getRight().serializer().readObject(new ByteArrayInputStream(bytes));

      p.getRight().executor().execute(() -> p.getLeft().completeExceptionally((Throwable) message));
    }
  }

  /**
   * Handle the response of a request this connection sent, if the response is not exceptional.
   * 
   * @param requestUuid
   * @param data
   */
  public void handleRequest(UUID requestUuid, ByteBuffer data) {
    byte[] bytes = new byte[data.remaining()];
    data.get(bytes);
    Object message = generalContext.serializer().readObject(new ByteArrayInputStream(bytes));

    Pair<MessageHandler<Object, Object>, ThreadContext> p = handlers.get(message.getClass());

    if (p != null) {
      p.getRight().executor().execute(() -> {
        CompletableFuture<Object> result = p.getLeft().handle(message);
        handleRequestResult(requestUuid, p.getRight(), result);
      });
    } else {
      CompletableFuture<Object> res = new CompletableFuture<>();
      res.completeExceptionally(new RuntimeException("Handler unknown: " + message.getClass().getName()));
      handleRequestResult(requestUuid, generalContext, res);
    }
  }

  private void handleRequestResult(UUID requestUuid, ThreadContext context, CompletableFuture<Object> result) {
    result.whenComplete((response, error) -> {
      try (ServiceProvider<ClusterConsensusService.Iface> sp =
          connectionOrLocalHelper.getService(ClusterConsensusService.Iface.class, remoteAddr, socketListener)) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if (error != null) {
          context.serializer().writeObject(error, baos);

          sp.getService().replyException(RUuidUtil.toRUuid(remoteEndpointUuid), RUuidUtil.toRUuid(requestUuid),
              ByteBuffer.wrap(baos.toByteArray()));
        } else {
          context.serializer().writeObject(response, baos);

          sp.getService().reply(RUuidUtil.toRUuid(remoteEndpointUuid), RUuidUtil.toRUuid(requestUuid),
              ByteBuffer.wrap(baos.toByteArray()));
        }
      } catch (ConnectionException | IOException | InterruptedException | IllegalStateException | TException e) {
        logger.error("Could not send result/exception to {}", remoteAddr, e);
        throw new RuntimeException("Could not send result/exception to " + remoteAddr, e);
      }
    });
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T, U> CompletableFuture<U> send(T message) {
    ThreadContext context = ThreadContext.currentContextOrThrow();

    CompletableFuture<U> res = new CompletableFuture<>();

    UUID requestUuid = UUID.randomUUID();

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      context.serializer().writeObject(message, baos);
      if (message instanceof ReferenceCounted) {
        ((ReferenceCounted<?>) message).release();
      }

      requests.put(requestUuid, new Pair<>((CompletableFuture<Object>) res, context));

      try (ServiceProvider<ClusterConsensusService.Iface> sp =
          connectionOrLocalHelper.getService(ClusterConsensusService.Iface.class, remoteAddr, socketListener)) {
        sp.getService().request(RUuidUtil.toRUuid(remoteEndpointUuid), RUuidUtil.toRUuid(requestUuid),
            ByteBuffer.wrap(baos.toByteArray()));
      }

    } catch (IOException | IllegalStateException | TException | TransportException | InterruptedException e) {
      requests.remove(requestUuid);
      res.completeExceptionally(new TransportException("Failed to send request", e));
    }

    return res;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T, U> Connection handler(Class<T> type, MessageHandler<T, U> handler) {
    handlers.put(type, new Pair<>((MessageHandler<Object, Object>) handler, ThreadContext.currentContextOrThrow()));
    return null;
  }

  @Override
  public Listener<Throwable> exceptionListener(Consumer<Throwable> listener) {
    if (connectionDiedAlready)
      listener.accept(new TransportException("Connection died."));

    return exceptionListeners.add(listener);
  }

  @Override
  public Listener<Connection> closeListener(Consumer<Connection> listener) {
    if (connectionEndpointUuid == null)
      listener.accept(this);

    return closeListeners.add(listener);
  }

  @Override
  public CompletableFuture<Void> close() {
    if (connectionEndpointUuid != null) {
      UUID oldConnectionEndpointUuid = connectionEndpointUuid;
      connectionEndpointUuid = null;

      Iterator<Pair<CompletableFuture<Object>, ThreadContext>> it = requests.values().iterator();
      while (it.hasNext()) {
        Pair<CompletableFuture<Object>, ThreadContext> p = it.next();
        it.remove();

        p.getRight().executor()
            .execute(() -> p.getLeft().completeExceptionally(new TransportException("Connection closed.")));
      }

      registry.removeConnectionEndpoint(oldConnectionEndpointUuid);
      closeListeners.accept(this);

      if (!remoteAddr.equals(ourNodeAddressProvider.getOurNodeAddress().createRemote())) {
        // send "close" on catalyst connection - but not if the connection is to "local", as we cannot fetch the
        // corresponding bean anymore if we're shutting down currently.
        try (ServiceProvider<ClusterConsensusService.Iface> sp =
            connectionOrLocalHelper.getService(ClusterConsensusService.Iface.class, remoteAddr, socketListener)) {

          sp.getService().close(RUuidUtil.toRUuid(remoteEndpointUuid));

        } catch (ConnectionException | IOException | InterruptedException | IllegalStateException | TException e) {
          logger.info("Could not send 'close' to remote at {}", remoteAddr);
          // swallow otherwise, since we're trying to close the conn anyway.
        }
      }
    }

    CompletableFuture<Void> res = new CompletableFuture<Void>();
    res.complete(null);

    return res;
  }

  /**
   * @return ID of the catalyst connection this object represents.
   */
  public UUID getConnectionEndpointUuid() {
    return connectionEndpointUuid;
  }

}
