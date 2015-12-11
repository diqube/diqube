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
package org.diqube.itest.tests;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.diqube.config.ConfigKey;
import org.diqube.itest.AbstractDiqubeIntegrationTest;
import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.util.IdentityCallbackServiceTestUtil;
import org.diqube.itest.util.IdentityCallbackServiceTestUtil.TestIdentityCallbackService;
import org.diqube.itest.util.Waiter;
import org.diqube.remote.query.thrift.Ticket;
import org.diqube.remote.query.thrift.TicketInfo;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Integration test for login/logout mechanisms.
 *
 * @author Bastian Gloeckle
 */
public class LoginLogoutIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final String ROOT_PASSWORD = "diqube";
  private static final String ROOT_USER = "root";

  private static final String TEST_USER = "test";
  private static final String TEST_PASSWORD = "test";

  @Test
  @NeedsServer(servers = 2, manualStart = true)
  public void addUserAndLoginLogoutTest() {
    serverControl.get(0).start(p -> {
      p.setProperty(ConfigKey.SUPERUSER, ROOT_USER);
      p.setProperty(ConfigKey.SUPERUSER_PASSWORD, ROOT_PASSWORD);
    });
    serverControl.get(1).start(p -> {
      p.setProperty(ConfigKey.SUPERUSER, ROOT_USER);
      p.setProperty(ConfigKey.SUPERUSER_PASSWORD, ROOT_PASSWORD);
    });

    serverControl.get(0).getSerivceTestUtil().identityService(identityService -> {
      Ticket rootTicket = identityService.login(ROOT_USER, ROOT_PASSWORD);
      Assert.assertNotNull(rootTicket, "Expected valid rootTicket");

      identityService.createUser(rootTicket, TEST_USER, "", TEST_PASSWORD);

      Ticket testTicket = identityService.login(TEST_USER, TEST_PASSWORD);
      Assert.assertNotNull(testTicket, "Expected valid testTicket");

      identityService.getPermissions(testTicket, TEST_USER);
      // expected: no authentication exception

      identityService.logout(testTicket);

      try {
        identityService.getPermissions(testTicket, TEST_USER);
        Assert.fail("Expected to get an authentication exception");
      } catch (AuthenticationException e) {
      }
    });
  }

  @Test
  @NeedsServer(servers = 2, manualStart = true)
  public void logoutCallbackIsCalledAndInvalidatedTicketIsAvailable() throws IOException {
    serverControl.get(0).start(p -> {
      p.setProperty(ConfigKey.SUPERUSER, ROOT_USER);
      p.setProperty(ConfigKey.SUPERUSER_PASSWORD, ROOT_PASSWORD);
    });
    serverControl.get(1).start(p -> {
      p.setProperty(ConfigKey.SUPERUSER, ROOT_USER);
      p.setProperty(ConfigKey.SUPERUSER_PASSWORD, ROOT_PASSWORD);
    });

    try (TestIdentityCallbackService callbackServ = IdentityCallbackServiceTestUtil.createIdentityCallbackService()) {
      serverControl.get(0).getSerivceTestUtil().identityService(identityService -> {
        Ticket rootTicket = identityService.login(ROOT_USER, ROOT_PASSWORD);
        Assert.assertNotNull(rootTicket, "Expected valid rootTicket");

        identityService.createUser(rootTicket, TEST_USER, "", TEST_PASSWORD);
        Ticket testTicket = identityService.login(TEST_USER, TEST_PASSWORD);
        Assert.assertNotNull(testTicket, "Expected valid testTicket");

        identityService.registerCallback(callbackServ.getThisServicesAddr().toRNodeAddress());
        identityService.logout(testTicket);

        // we should receive the invalidation event at least twice: Once the quick-send in IdentityHandler#logout and
        // then the one when the logout is being applied to LogoutStateMachine!
        new Waiter().waitUntil("Logout event captured on callback service (twice)", 10, 200,
            () -> callbackServ.getInvalidTickets().size() >= 2);

        // validate correct ticket was invalidated
        Set<TicketInfo> invalidTicketSet = new HashSet<>(callbackServ.getInvalidTickets());
        Assert.assertEquals(invalidTicketSet.size(), 1, "Expected correct number of distinct invalid tickets.");

        TicketInfo invalidInfo = invalidTicketSet.iterator().next();
        Assert.assertEquals(invalidInfo.getTicketId(), testTicket.getClaim().getTicketId(),
            "Expected correct invalidated ticket ID.");
        Assert.assertEquals(invalidInfo.getValidUntil(), testTicket.getClaim().getValidUntil(),
            "Expected correct invalidated 'valid until'.");

        // check that the correct invalidated ticket is returned by IdentityService#getInvalidTickets, too.
        List<TicketInfo> serviceInvalidTickets = identityService.getInvalidTicketInfos();
        Assert.assertEquals(serviceInvalidTickets, invalidTicketSet,
            "Expected to get correc result when calling IdentityService#getInvalidTicketInfos.");
      });
    }
  }
}
