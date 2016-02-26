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
package org.diqube.im;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.thrift.TException;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.generators.PKCS5S2ParametersGenerator;
import org.bouncycastle.crypto.params.KeyParameter;
import org.diqube.cluster.ClusterLayout;
import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.connection.Connection;
import org.diqube.connection.ConnectionException;
import org.diqube.connection.ConnectionPool;
import org.diqube.connection.OurNodeAddressProvider;
import org.diqube.consensus.ConsensusClient;
import org.diqube.consensus.ConsensusClient.ClosableProvider;
import org.diqube.consensus.ConsensusClient.ConsensusClusterUnavailableException;
import org.diqube.context.AutoInstatiate;
import org.diqube.im.IdentityStateMachine.DeleteUser;
import org.diqube.im.IdentityStateMachine.GetUser;
import org.diqube.im.IdentityStateMachine.SetUser;
import org.diqube.im.callback.IdentityCallbackRegistryStateMachine;
import org.diqube.im.callback.IdentityCallbackRegistryStateMachine.Register;
import org.diqube.im.callback.IdentityCallbackRegistryStateMachine.Unregister;
import org.diqube.im.callback.IdentityCallbackRegistryStateMachineImplementation;
import org.diqube.im.logout.LogoutStateMachine;
import org.diqube.im.logout.LogoutStateMachine.GetInvalidTickets;
import org.diqube.im.logout.LogoutStateMachine.Logout;
import org.diqube.im.thrift.v1.SPassword;
import org.diqube.im.thrift.v1.SPermission;
import org.diqube.im.thrift.v1.SUser;
import org.diqube.remote.query.TicketInfoUtil;
import org.diqube.remote.query.thrift.IdentityCallbackService;
import org.diqube.remote.query.thrift.IdentityService;
import org.diqube.remote.query.thrift.OptionalString;
import org.diqube.remote.query.thrift.TicketInfo;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.RNodeAddress;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.thrift.base.util.RUuidUtil;
import org.diqube.ticket.TicketSignatureService;
import org.diqube.ticket.TicketUtil;
import org.diqube.ticket.TicketValidityService;
import org.diqube.ticket.TicketVendor;
import org.diqube.util.BouncyCastleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * Implementation of an identity service.
 * 
 * <p>
 * This implementation does not use {@link SUserProvider}, as it wants to work on the newest objects always.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class IdentityHandler implements IdentityService.Iface {
  private static final Logger logger = LoggerFactory.getLogger(IdentityHandler.class);

  private static final int PBKDF2_ITERATIONS = 200_000;
  private static final int SALT_LENGTH_BYTES = 64;
  private static final int HASH_LENGTH_BYTES = 64;

  private static final String TRUE = "true";

  @Config(ConfigKey.SUPERUSER)
  private String superuser;

  @Config(ConfigKey.SUPERUSER_PASSWORD)
  private String superuserPassword;

  @Config(ConfigKey.TICKET_USE_STRONG_RANDOM)
  private String useStrongRandomConfigValue;
  private boolean useStrongRandom;

  @Inject
  private ConsensusClient consensusClient;

  @Inject
  private TicketVendor ticketVendor;

  @Inject
  private ClusterLayout clusterLayout;

  @Inject
  private OurNodeAddressProvider ourNodeAddressProvider;

  @Inject
  private ConnectionPool connectionPool;

  @Inject
  private TicketValidityService ticketValidityService;

  @Inject
  private PermissionCheckUtil permissionCheck;

  @Inject
  private TicketSignatureService ticketSignatureService;

  @Inject
  private IdentityCallbackRegistryStateMachineImplementation callbackRegistry;

  @PostConstruct
  public void initialize() {
    useStrongRandom =
        useStrongRandomConfigValue != null && useStrongRandomConfigValue.toLowerCase().trim().equals(TRUE);

    if (useStrongRandom)
      logger.info("Using STRONG random to calculate new salts for hashing passwords. Ensure that the system "
          + "provides enough strong randomness!");
    else
      logger.info("Using normal random to calculate new salts for hashing passwords.");
  }

  @Override
  public Ticket login(String userName, String password) throws AuthenticationException, TException {
    if (userName == null || "".equals(userName.trim()))
      throw new AuthenticationException("Empty username.");

    if (password == null || "".equals(password.trim()))
      throw new AuthenticationException("Empty password.");

    if (permissionCheck.isSuperuser(userName)) {
      if (!password.equals(superuserPassword))
        throw new AuthenticationException("Invalid credentials.");

      logger.info("Successful login by superuser '{}'", userName);

      // we have a successfully authenticated superuser!
      return ticketVendor.createDefaultTicketForUser(superuser, true);
    }

    SUser user;
    try (ClosableProvider<IdentityStateMachine> p = consensusClient.getStateMachineClient(IdentityStateMachine.class)) {
      user = p.getClient().getUser(GetUser.local(userName));
    } catch (ConsensusClusterUnavailableException e) {
      logger.warn("Consensus cluster offline, cannot load user!", e);
      user = null;
    }
    if (user == null) {
      logger.info("User '{}' tried to login, but does not exist", userName);
      throw new AuthenticationException("Invalid credentials.");
    }

    byte[] userProvidedPassword = password.getBytes(Charset.forName("UTF-8"));
    byte[] salt = user.getPassword().getSalt();

    BouncyCastleUtil.ensureInitialized();

    PKCS5S2ParametersGenerator pbkdf2sha256 = new PKCS5S2ParametersGenerator(new SHA256Digest());
    pbkdf2sha256.init(userProvidedPassword, salt, PBKDF2_ITERATIONS);
    byte[] userProvidedHash = ((KeyParameter) pbkdf2sha256.generateDerivedParameters(HASH_LENGTH_BYTES * 8)).getKey();

    if (!Arrays.equals(userProvidedHash, user.getPassword().getHash())) {
      logger.info("User '{}' provided bad password for login", userName);
      throw new AuthenticationException("Invalid credentials.");
    }

    // authenticated successfully!
    Ticket res = ticketVendor.createDefaultTicketForUser(userName, false);

    logger.info("User '{}' logged in successfully! Returning new ticket {} valid until {}.", userName,
        RUuidUtil.toUuid(res.getClaim().getTicketId()), res.getClaim().getValidUntil());

    return res;
  }

  @Override
  public void logout(Ticket ticket) throws TException, AuthorizationException {
    if (!ticketSignatureService
        .isValidTicketSignature(TicketUtil.deserialize(ByteBuffer.wrap(TicketUtil.serialize(ticket))))) {
      // filter out tickets with invalid signature, since we do not want to let users flood the consensus cluster with
      // requests.
      logger.info("Someone tried to logout with an invalid ticket. Username provided in ticket is '{}'",
          ticket.getClaim().getUsername());
      throw new AuthorizationException("Ticket signaure invalid.");
    }

    logger.info("Logging out user '{}', ticket valid until {}", ticket.getClaim().getUsername(),
        ticket.getClaim().getValidUntil());

    ticketValidityService.markTicketAsInvalid(TicketInfoUtil.fromTicket(ticket));

    // quickly (but unreliably) distribute the logout to all known cluster nodes and all interested callbacks.
    for (RNodeAddress addr : Sets.union(
        clusterLayout.getNodesInsecure().stream().map(addr -> addr.createRemote()).collect(Collectors.toSet()),
        callbackRegistry.getRegisteredNodesInsecure())) {
      if (addr.equals(ourNodeAddressProvider.getOurNodeAddress()))
        continue;

      try (Connection<IdentityCallbackService.Iface> con =
          connectionPool.reserveConnection(IdentityCallbackService.Iface.class, addr, null)) {
        con.getService().ticketBecameInvalid(TicketInfoUtil.fromTicket(ticket));
      } catch (ConnectionException | IOException e) {
        // swallow, as we distribute the information reliably using the state machine below.
      } catch (InterruptedException e) {
        logger.warn("Interrupted while distributing logout information.", e);
        return;
      }
    }

    // then: distribute logout reliably (but probably slower) across the consensus cluster. This will again ensure that
    // all registered callbacks are called accordingly.
    try (ClosableProvider<LogoutStateMachine> p = consensusClient.getStateMachineClient(LogoutStateMachine.class)) {
      p.getClient().logout(Logout.local(ticket));
    } catch (ConsensusClusterUnavailableException e) {
      throw new RuntimeException("Consensus cluster unavailable", e);
    }

    logger.info("Logout of user '{}', ticket {}, valid until {} successful.", ticket.getClaim().getUsername(),
        RUuidUtil.toUuid(ticket.getClaim().getTicketId()), ticket.getClaim().getValidUntil());
  }

  @Override
  public void changePassword(Ticket ticket, String username, String newPassword)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!ticket.getClaim().getUsername().equals(username) && !permissionCheck.isSuperuser(ticket))
      throw new AuthorizationException();

    if (permissionCheck.isSuperuser(username))
      throw new AuthorizationException("Superuser password cannot be changed. Change in configuration of server.");

    logger.info("Password of user '{}' is being changed, authorized by ticket of '{}'", username,
        ticket.getClaim().getUsername());

    try (ClosableProvider<IdentityStateMachine> p = consensusClient.getStateMachineClient(IdentityStateMachine.class)) {
      SUser user = p.getClient().getUser(GetUser.local(username));
      if (user == null)
        throw new AuthorizationException("User does not exist");

      internalSetUserPassword(user, newPassword);

      p.getClient().setUser(SetUser.local(user));
    } catch (ConsensusClusterUnavailableException e) {
      throw new RuntimeException("Consensus cluster unavailable", e);
    }
  }

  @Override
  public void changeEmail(Ticket ticket, String username, String newEmail)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!ticket.getClaim().getUsername().equals(username) && !permissionCheck.isSuperuser(ticket))
      throw new AuthorizationException();

    if (!permissionCheck.isSuperuser(username))
      throw new AuthorizationException("Superuser password cannot be changed. Change in configuration of server.");

    logger.info("E-Mail of user '{}' is being changed, authorized by ticket of '{}'", username,
        ticket.getClaim().getUsername());

    try (ClosableProvider<IdentityStateMachine> p = consensusClient.getStateMachineClient(IdentityStateMachine.class)) {
      SUser user = p.getClient().getUser(GetUser.local(username));
      if (user == null)
        throw new AuthorizationException("User does not exist");

      user.setEmail(newEmail);

      p.getClient().setUser(SetUser.local(user));
    } catch (ConsensusClusterUnavailableException e) {
      throw new RuntimeException("Consensus cluster unavailable", e);
    }
  }

  @Override
  public void addPermission(Ticket ticket, String username, String permission, OptionalString object)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!permissionCheck.isSuperuser(ticket))
      throw new AuthorizationException();

    if (permissionCheck.isSuperuser(username))
      throw new AuthorizationException("Superuser permissions cannot be changed.");

    logger.info("Permission ({}/{}) is added to user '{}', authorized by ticket of '{}'", permission, object, username,
        ticket.getClaim().getUsername());

    try (ClosableProvider<IdentityStateMachine> p = consensusClient.getStateMachineClient(IdentityStateMachine.class)) {
      SUser user = p.getClient().getUser(GetUser.local(username));
      if (user == null)
        throw new AuthorizationException("User does not exist");

      if (!user.isSetPermissions())
        user.setPermissions(new ArrayList<>());

      boolean found = false;
      for (SPermission perm : user.getPermissions())
        if (perm.getPermissionName().equals(permission)) {
          found = true;
          if (object.isSetValue() && !perm.isSetObjects())
            perm.setObjects(new ArrayList<>());
          if (object.isSetValue())
            perm.getObjects().add(object.getValue());
          break;
        }

      if (!found) {
        SPermission newPerm = new SPermission();
        newPerm.setPermissionName(permission);
        if (object.isSetValue()) {
          newPerm.setObjects(new ArrayList<>());
          newPerm.getObjects().add(object.getValue());
        }
        user.getPermissions().add(newPerm);
      }

      p.getClient().setUser(SetUser.local(user));
    } catch (ConsensusClusterUnavailableException e) {
      throw new RuntimeException("Consensus cluster unavailable", e);
    }
  }

  @Override
  public void removePermission(Ticket ticket, String username, String permission, OptionalString object)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!permissionCheck.isSuperuser(ticket))
      throw new AuthorizationException();

    if (permissionCheck.isSuperuser(username))
      throw new AuthorizationException("Superuser permissions cannot be changed.");

    logger.info("Permission ({}/{}) is removed from user '{}', authorized by ticket of '{}'", permission, object,
        username, ticket.getClaim().getUsername());

    try (ClosableProvider<IdentityStateMachine> p = consensusClient.getStateMachineClient(IdentityStateMachine.class)) {
      SUser user = p.getClient().getUser(GetUser.local(username));
      if (user == null)
        throw new AuthorizationException("User does not exist");

      if (!user.isSetPermissions())
        return;

      for (Iterator<SPermission> it = user.getPermissions().iterator(); it.hasNext();) {
        SPermission perm = it.next();
        if (perm.getPermissionName().equals(permission)) {
          if (object.isSetValue() && !perm.isSetObjects()) {
            // nothing to remove
            return;
          } else if (object.isSetValue()) {
            if (!perm.getObjects().remove(object.getValue())) {
              // object was not in perm.
              return;
            }
          } else
            // we want to remove the whole permission, not just an object.
            it.remove();
          break;
        }
      }

      p.getClient().setUser(SetUser.local(user));
    } catch (ConsensusClusterUnavailableException e) {
      throw new RuntimeException("Consensus cluster unavailable", e);
    }
  }

  @Override
  public Map<String, List<String>> getPermissions(Ticket ticket, String username)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!ticket.getClaim().getUsername().equals(username) && !permissionCheck.isSuperuser(ticket))
      throw new AuthorizationException();

    if (permissionCheck.isSuperuser(username))
      throw new AuthorizationException("Cannot query permissions of superuser.");

    SUser user;
    try (ClosableProvider<IdentityStateMachine> p = consensusClient.getStateMachineClient(IdentityStateMachine.class)) {
      user = p.getClient().getUser(GetUser.local(username));
    } catch (ConsensusClusterUnavailableException e) {
      throw new RuntimeException("Consensus cluster unavailable", e);
    }
    if (user == null)
      throw new AuthorizationException("User does not exist");

    if (!user.isSetPermissions())
      return new HashMap<>();

    Map<String, List<String>> res = new HashMap<>();
    for (SPermission perm : user.getPermissions()) {
      List<String> objects = new ArrayList<>();
      if (perm.isSetObjects())
        objects.addAll(perm.getObjects());
      res.put(perm.getPermissionName(), objects);
    }

    return res;
  }

  @Override
  public void createUser(Ticket ticket, String username, String email, String password)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!permissionCheck.isSuperuser(ticket))
      throw new AuthorizationException();

    if (permissionCheck.isSuperuser(username))
      throw new AuthorizationException("Superuser permissions cannot be changed.");

    logger.info("User '{}' is being created, authorized by ticket of '{}'", username, ticket.getClaim().getUsername());

    SUser user = new SUser();
    user.setUsername(username);
    user.setEmail(email);
    internalSetUserPassword(user, password);

    try (ClosableProvider<IdentityStateMachine> p = consensusClient.getStateMachineClient(IdentityStateMachine.class)) {
      p.getClient().setUser(SetUser.local(user));
    } catch (ConsensusClusterUnavailableException e) {
      throw new RuntimeException("Consensus cluster unavailable", e);
    }
  }

  @Override
  public void deleteUser(Ticket ticket, String username)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!permissionCheck.isSuperuser(ticket))
      throw new AuthorizationException();

    if (permissionCheck.isSuperuser(username))
      throw new AuthorizationException("Superuser cannot be deleted.");

    logger.info("User '{}' is being deleted, authorized by ticket of '{}'", username, ticket.getClaim().getUsername());

    try (ClosableProvider<IdentityStateMachine> p = consensusClient.getStateMachineClient(IdentityStateMachine.class)) {
      p.getClient().deleteUser(DeleteUser.local(username));
    } catch (ConsensusClusterUnavailableException e) {
      throw new RuntimeException("Consensus cluster unavailable", e);
    }
  }

  @Override
  public String getEmail(Ticket ticket, String username) throws TException {
    ticketValidityService.validateTicket(ticket);

    if (!ticket.getClaim().getUsername().equals(username) && !permissionCheck.isSuperuser(ticket))
      throw new AuthorizationException();

    if (permissionCheck.isSuperuser(username))
      throw new AuthorizationException("Cannot query permissions of superuser.");

    SUser user;
    try (ClosableProvider<IdentityStateMachine> p = consensusClient.getStateMachineClient(IdentityStateMachine.class)) {
      user = p.getClient().getUser(GetUser.local(username));
    } catch (ConsensusClusterUnavailableException e) {
      throw new RuntimeException("Consensus cluster unavailable", e);
    }
    if (user == null)
      throw new AuthorizationException("User does not exist");

    return user.getEmail();
  }

  @Override
  public void registerCallback(RNodeAddress nodeAddress) throws TException {
    try (ClosableProvider<IdentityCallbackRegistryStateMachine> p =
        consensusClient.getStateMachineClient(IdentityCallbackRegistryStateMachine.class)) {
      p.getClient().register(Register.local(nodeAddress, System.currentTimeMillis()));
    } catch (ConsensusClusterUnavailableException e) {
      throw new RuntimeException("Consensus cluster unavailable", e);
    }
    logger.info("Registered identity callback at '{}' (service level).", nodeAddress);
  }

  @Override
  public void unregisterCallback(RNodeAddress nodeAddress) throws TException {
    try (ClosableProvider<IdentityCallbackRegistryStateMachine> p =
        consensusClient.getStateMachineClient(IdentityCallbackRegistryStateMachine.class)) {
      p.getClient().unregister(Unregister.local(nodeAddress));
    } catch (ConsensusClusterUnavailableException e) {
      throw new RuntimeException("Consensus cluster unavailable", e);
    }
    logger.info("Removed identity callback at '{}' (service level).", nodeAddress);
  }

  @Override
  public List<TicketInfo> getInvalidTicketInfos() throws TException {
    List<Ticket> invalidTickets;
    try (ClosableProvider<LogoutStateMachine> p = consensusClient.getStateMachineClient(LogoutStateMachine.class)) {
      invalidTickets = p.getClient().getInvalidTickets(GetInvalidTickets.local());
    } catch (ConsensusClusterUnavailableException e) {
      throw new RuntimeException("Consensus cluster unavailable", e);
    }

    return invalidTickets.stream().map(t -> TicketInfoUtil.fromTicket(t)).collect(Collectors.toList());
  }

  private void internalSetUserPassword(SUser user, String newPassword) throws TException {
    BouncyCastleUtil.ensureInitialized();

    byte[] newSalt = new byte[SALT_LENGTH_BYTES];

    if (useStrongRandom) {
      try {
        SecureRandom.getInstanceStrong().nextBytes(newSalt);
      } catch (NoSuchAlgorithmException e) {
        logger.error("Internal error when calculating new salt for new password", e);
        throw new TException("Internal error.", e);
      }
    } else {
      // use non-string random.
      ThreadLocalRandom.current().nextBytes(newSalt);
    }

    PKCS5S2ParametersGenerator pbkdf2sha256 = new PKCS5S2ParametersGenerator(new SHA256Digest());
    pbkdf2sha256.init(newPassword.getBytes(Charset.forName("UTF-8")), newSalt, PBKDF2_ITERATIONS);
    byte[] newHash = ((KeyParameter) pbkdf2sha256.generateDerivedParameters(HASH_LENGTH_BYTES * 8)).getKey();

    user.setPassword(new SPassword());
    user.getPassword().setHash(newHash);
    user.getPassword().setSalt(newSalt);
  }

}
