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
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.diqube.connection.NodeAddress;
import org.diqube.connection.OurNodeAddressProvider;
import org.diqube.consensus.DiqubeCopycatClient;
import org.diqube.context.AutoInstatiate;
import org.diqube.im.IdentityStateMachine.DeleteUser;
import org.diqube.im.IdentityStateMachine.GetUser;
import org.diqube.im.IdentityStateMachine.SetUser;
import org.diqube.im.LogoutStateMachine.Logout;
import org.diqube.im.thrift.v1.SPermission;
import org.diqube.im.thrift.v1.SUser;
import org.diqube.remote.base.thrift.AuthenticationException;
import org.diqube.remote.base.thrift.AuthorizationException;
import org.diqube.remote.query.thrift.IdentityCallbackService;
import org.diqube.remote.query.thrift.IdentityService;
import org.diqube.remote.query.thrift.OptionalString;
import org.diqube.remote.query.thrift.Ticket;
import org.diqube.ticket.TicketValidityService;
import org.diqube.ticket.TicketVendor;
import org.diqube.util.BouncyCastleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of an identity service.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class IdentityHandler implements IdentityService.Iface {
  private static final Logger logger = LoggerFactory.getLogger(IdentityHandler.class);

  private static final String NONE = "none";

  private static final int PBKDF2_ITERATIONS = 200_000;
  private static final int SALT_LENGTH_BYTES = 64;
  private static final int HASH_LENGTH_BYTES = 64;

  @Config(ConfigKey.SUPERUSER)
  private String superuser;

  @Config(ConfigKey.SUPERUSER_PASSWORD)
  private String superuserPassword;

  @Inject
  private DiqubeCopycatClient consensusClient;

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

  @Override
  public Ticket login(String userName, String password) throws AuthenticationException, TException {
    if (!superuser.equals(NONE) && userName.equals(superuser)) {
      if (!password.equals(superuserPassword))
        throw new AuthenticationException("Invalid credentials.");

      // we have a successfully authenticated superuser!
      return ticketVendor.createDefaultTicketForUser(superuser, true);
    }

    SUser user = consensusClient.getStateMachineClient(IdentityStateMachine.class).getUser(GetUser.local(userName));
    if (user == null)
      throw new AuthenticationException("Invalid credentials.");

    byte[] userProvidedPassword = password.getBytes(Charset.forName("UTF-8"));
    byte[] salt = user.getPassword().getSalt();

    BouncyCastleUtil.ensureInitialized();

    PKCS5S2ParametersGenerator pbkdf2sha256 = new PKCS5S2ParametersGenerator(new SHA256Digest());
    pbkdf2sha256.init(userProvidedPassword, salt, PBKDF2_ITERATIONS);
    byte[] userProvidedHash = ((KeyParameter) pbkdf2sha256.generateDerivedParameters(HASH_LENGTH_BYTES * 8)).getKey();

    if (!Arrays.equals(userProvidedHash, user.getPassword().getHash()))
      throw new AuthenticationException("Invalid credentials.");

    // authenticated successfully!
    return ticketVendor.createDefaultTicketForUser(userName, false);
  }

  @Override
  public void logout(Ticket ticket) throws TException {
    // quickly (but unreliably) distribute the logout to all known cluster nodes first
    for (NodeAddress addr : clusterLayout.getNodesInsecure()) {
      if (addr.equals(ourNodeAddressProvider.getOurNodeAddress()))
        continue;

      try (Connection<IdentityCallbackService.Iface> con =
          connectionPool.reserveConnection(IdentityCallbackService.Iface.class, addr.createRemote(), null)) {
        con.getService().ticketBecameInvalid(ticket);
      } catch (ConnectionException | IOException e) {
        // swallow, as we distribute the information reliably using the state machine below.
      } catch (InterruptedException e) {
        logger.warn("Interrupted while distributing logout information.", e);
        return;
      }
    }

    // then: distribute logout reliably (but probably slower) across the consensus cluster
    consensusClient.getStateMachineClient(LogoutStateMachine.class).logout(Logout.local(ticket));

    // TODO #71 inform nodes like UI servers of the logout.
  }

  @Override
  public void changePassword(Ticket ticket, String username, String newPassword)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!ticket.getClaim().getUsername().equals(username) && !ticket.getClaim().isIsSuperUser())
      throw new AuthorizationException();

    if (username.equals(superuser))
      throw new TException("Superuser password cannot be changed. Change in configuration of server.");

    SUser user = consensusClient.getStateMachineClient(IdentityStateMachine.class).getUser(GetUser.local(username));

    internalSetUserPassword(user, newPassword);

    consensusClient.getStateMachineClient(IdentityStateMachine.class).setUser(SetUser.local(user));
  }

  @Override
  public void changeEmail(Ticket ticket, String username, String newEmail)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!ticket.getClaim().getUsername().equals(username) && !ticket.getClaim().isIsSuperUser())
      throw new AuthorizationException();

    if (username.equals(superuser))
      throw new TException("Superuser password cannot be changed. Change in configuration of server.");

    SUser user = consensusClient.getStateMachineClient(IdentityStateMachine.class).getUser(GetUser.local(username));

    user.setEmail(newEmail);

    consensusClient.getStateMachineClient(IdentityStateMachine.class).setUser(SetUser.local(user));
  }

  @Override
  public void addPermission(Ticket ticket, String username, String permission, OptionalString object)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!ticket.getClaim().isIsSuperUser())
      throw new AuthorizationException();

    if (username.equals(superuser))
      throw new TException("Superuser permissions cannot be changed.");

    SUser user = consensusClient.getStateMachineClient(IdentityStateMachine.class).getUser(GetUser.local(username));

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

    consensusClient.getStateMachineClient(IdentityStateMachine.class).setUser(SetUser.local(user));
  }

  @Override
  public void removePermission(Ticket ticket, String username, String permission, OptionalString object)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!ticket.getClaim().isIsSuperUser())
      throw new AuthorizationException();

    if (username.equals(superuser))
      throw new TException("Superuser permissions cannot be changed.");

    SUser user = consensusClient.getStateMachineClient(IdentityStateMachine.class).getUser(GetUser.local(username));

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

    consensusClient.getStateMachineClient(IdentityStateMachine.class).setUser(SetUser.local(user));
  }

  @Override
  public Map<String, List<String>> getPermissions(Ticket ticket, String username)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!ticket.getClaim().getUsername().equals(username) && !ticket.getClaim().isIsSuperUser())
      throw new AuthorizationException();

    if (username.equals(superuser))
      throw new TException("Cannot query permissions of superuser.");

    SUser user = consensusClient.getStateMachineClient(IdentityStateMachine.class).getUser(GetUser.local(username));
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

    if (!ticket.getClaim().isIsSuperUser())
      throw new AuthorizationException();

    if (username.equals(superuser))
      throw new TException("Superuser permissions cannot be changed.");

    SUser user = new SUser();
    user.setUsername(username);
    user.setEmail(email);
    internalSetUserPassword(user, password);

    consensusClient.getStateMachineClient(IdentityStateMachine.class).setUser(SetUser.local(user));
  }

  @Override
  public void deleteUser(Ticket ticket, String username)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!ticket.getClaim().isIsSuperUser())
      throw new AuthorizationException();

    if (username.equals(superuser))
      throw new TException("Superuser permissions cannot be changed.");

    consensusClient.getStateMachineClient(IdentityStateMachine.class).deleteUser(DeleteUser.local(username));
  }

  private void internalSetUserPassword(SUser user, String newPassword) throws TException {
    BouncyCastleUtil.ensureInitialized();

    byte[] newSalt = new byte[SALT_LENGTH_BYTES];
    try {
      SecureRandom.getInstanceStrong().nextBytes(newSalt);
    } catch (NoSuchAlgorithmException e) {
      logger.error("Internal error when calculating new salt for new password", e);
      throw new TException("Internal error.", e);
    }

    PKCS5S2ParametersGenerator pbkdf2sha256 = new PKCS5S2ParametersGenerator(new SHA256Digest());
    pbkdf2sha256.init(newPassword.getBytes(Charset.forName("UTF-8")), newSalt, PBKDF2_ITERATIONS);
    byte[] newHash = ((KeyParameter) pbkdf2sha256.generateDerivedParameters(HASH_LENGTH_BYTES * 8)).getKey();

    user.getPassword().setHash(newHash);
    user.getPassword().setSalt(newSalt);
  }
}
