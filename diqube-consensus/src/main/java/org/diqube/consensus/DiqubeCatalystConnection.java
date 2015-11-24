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
package org.diqube.consensus;

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
 *
 * @author Bastian Gloeckle
 */
public class DiqubeCatalystConnection implements Connection {
  private static final Logger logger = LoggerFactory.getLogger(DiqubeCatalystConnection.class);

  private ServiceProvider<ClusterConsensusService.Iface> diqubeServiceProvider = null;

  private Map<Class<?>, Pair<MessageHandler<Object, Object>, ThreadContext>> handlers = new ConcurrentHashMap<>();

  private Map<UUID, Pair<CompletableFuture<Object>, ThreadContext>> requests = new ConcurrentHashMap<>();

  private final Listeners<Throwable> exceptionListeners = new Listeners<>();
  private final Listeners<Connection> closeListeners = new Listeners<>();

  private ConnectionOrLocalHelper connectionOrLocalHelper;
  private boolean connectionDiedAlready = false;
  /** null if closed */
  private UUID connectionUuid = null;

  private RNodeAddress remoteAddr;

  private SocketListener socketListener = new SocketListener() {
    @Override
    public void connectionDied() {
      connectionDiedAlready = true;
      connectionUuid = null;
      exceptionListeners.accept(new TransportException("Connection died"));
    }
  };

  private OurNodeAddressProvider ourNodeAddressProvider;

  private ClusterConsensusConnectionRegistry registry;

  public DiqubeCatalystConnection(ClusterConsensusConnectionRegistry registry,
      ConnectionOrLocalHelper connectionOrLocalHelper, OurNodeAddressProvider ourNodeAddressProvider) {
    this.registry = registry;
    this.connectionOrLocalHelper = connectionOrLocalHelper;
    this.ourNodeAddressProvider = ourNodeAddressProvider;
  }

  public void acceptAndRegister(UUID connectionUuid, RNodeAddress remoteAddr) {
    this.connectionUuid = connectionUuid;
    this.remoteAddr = remoteAddr;
    registry.registerConnection(connectionUuid, this);
  }

  public void openAndRegister(Address catylstRemoteAddr) throws TransportException {
    NodeAddress nodeAddress = new NodeAddress(catylstRemoteAddr.host(), (short) catylstRemoteAddr.port());
    this.remoteAddr = nodeAddress.createRemote();
    connectionUuid = UUID.randomUUID();

    try (ServiceProvider<ClusterConsensusService.Iface> sp =
        connectionOrLocalHelper.getService(ClusterConsensusService.Iface.class, remoteAddr, socketListener)) {

      sp.getService().open(RUuidUtil.toRUuid(connectionUuid),
          ourNodeAddressProvider.getOurNodeAddress().createRemote());

    } catch (ConnectionException | IOException | InterruptedException | IllegalStateException | TException e) {
      throw new TransportException("Could not establish connection to " + remoteAddr);
    }

    registry.registerConnection(connectionUuid, this);
  }

  public void handleResponse(UUID requestUuid, ByteBuffer data) {
    Pair<CompletableFuture<Object>, ThreadContext> p = requests.remove(requestUuid);

    if (p != null) {
      byte[] bytes = new byte[data.remaining()];
      data.get(bytes);
      Object message = p.getRight().serializer().readObject(new ByteArrayInputStream(bytes));

      p.getRight().executor().execute(() -> p.getLeft().complete(message));
    }
  }

  public void handleResponseException(UUID requestUuid, ByteBuffer data) {
    Pair<CompletableFuture<Object>, ThreadContext> p = requests.remove(requestUuid);

    if (p != null) {
      byte[] bytes = new byte[data.remaining()];
      data.get(bytes);
      Object message = p.getRight().serializer().readObject(new ByteArrayInputStream(bytes));

      p.getRight().executor().execute(() -> p.getLeft().completeExceptionally((Throwable) message));
    }
  }

  public void handleRequest(UUID requestUuid, ByteBuffer data) {
    CompletableFuture<Object> future = new CompletableFuture<>();
    ThreadContext context = ThreadContext.currentContextOrThrow();

    requests.put(requestUuid, new Pair<>(future, context));

    byte[] bytes = new byte[data.remaining()];
    data.get(bytes);
    Object message = context.serializer().readObject(new ByteArrayInputStream(bytes));

    Pair<MessageHandler<Object, Object>, ThreadContext> p = handlers.get(message.getClass());

    if (p != null) {
      p.getRight().executor().execute(() -> {
        CompletableFuture<Object> result = p.getLeft().handle(message);
        handleRequestResult(requestUuid, context, result);
      });
    } else {
      CompletableFuture<Object> res = new CompletableFuture<>();
      res.completeExceptionally(new RuntimeException("Handler unkown"));
      handleRequestResult(requestUuid, context, res);
    }
  }

  private void handleRequestResult(UUID requestUuid, ThreadContext context, CompletableFuture<Object> result) {
    result.whenComplete((response, error) -> {
      try (ServiceProvider<ClusterConsensusService.Iface> sp =
          connectionOrLocalHelper.getService(ClusterConsensusService.Iface.class, remoteAddr, socketListener)) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if (error != null) {
          context.serializer().writeObject(error, baos);

          sp.getService().replyException(RUuidUtil.toRUuid(connectionUuid), RUuidUtil.toRUuid(requestUuid),
              ByteBuffer.wrap(baos.toByteArray()));
        } else {
          context.serializer().writeObject(response, baos);

          sp.getService().reply(RUuidUtil.toRUuid(connectionUuid), RUuidUtil.toRUuid(requestUuid),
              ByteBuffer.wrap(baos.toByteArray()));
        }
      } catch (ConnectionException | IOException | InterruptedException | IllegalStateException | TException e) {
        logger.error("Could not send result/exception to {}", remoteAddr);
        // TODO
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
        sp.getService().request(RUuidUtil.toRUuid(connectionUuid), RUuidUtil.toRUuid(requestUuid),
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
    if (diqubeServiceProvider == null)
      listener.accept(this);

    return closeListeners.add(listener);
  }

  @Override
  public CompletableFuture<Void> close() {
    if (connectionUuid != null) {
      UUID oldConnectionUuid = connectionUuid;
      connectionUuid = null;

      Iterator<Pair<CompletableFuture<Object>, ThreadContext>> it = requests.values().iterator();
      while (it.hasNext()) {
        Pair<CompletableFuture<Object>, ThreadContext> p = it.next();
        it.remove();

        p.getRight().executor()
            .execute(() -> p.getLeft().completeExceptionally(new TransportException("Connection closed.")));
      }

      registry.removeConnection(oldConnectionUuid);
      closeListeners.accept(this);

      try (ServiceProvider<ClusterConsensusService.Iface> sp =
          connectionOrLocalHelper.getService(ClusterConsensusService.Iface.class, remoteAddr, socketListener)) {

        sp.getService().close(RUuidUtil.toRUuid(oldConnectionUuid));

      } catch (ConnectionException | IOException | InterruptedException | IllegalStateException | TException e) {
        logger.info("Could not send 'close' to remote at {}", remoteAddr);
        // swallow otherwise, since we're trying to close the conn anyway.
      }
    }

    CompletableFuture<Void> res = new CompletableFuture<Void>();
    res.complete(null);

    return res;
  }

  public UUID getConnectionUuid() {
    return connectionUuid;
  }

}
