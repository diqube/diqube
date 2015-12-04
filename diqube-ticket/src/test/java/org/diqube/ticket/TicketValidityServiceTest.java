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
package org.diqube.ticket;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.diqube.context.Profiles;
import org.diqube.remote.query.thrift.Ticket;
import org.diqube.remote.query.thrift.TicketClaim;
import org.diqube.ticket.TicketRsaKeyFileProvider.IOExceptionSupplier;
import org.diqube.util.Triple;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author Bastian Gloeckle
 */
public class TicketValidityServiceTest {
  private static final Triple<String, IOExceptionSupplier<InputStream>, String> PRIVATE_PLAIN =
      new Triple<>("private.plain", () -> TicketValidityServiceTest.class
          .getResourceAsStream("/" + TicketValidityServiceTest.class.getSimpleName() + "/private.plain.pem"), null);

  private AnnotationConfigApplicationContext dataContext;
  private TicketSignatureService ticketSignatureService;
  private TicketValidityService ticketValidityService;

  @BeforeMethod
  public void before() {

  }

  @AfterMethod
  public void after() {
    dataContext.close();
    dataContext = null;
    ticketSignatureService = null;
    ticketValidityService = null;
  }

  @Test
  public void invalidated() {
    synchronized (TestTicketRsaKeyFileProvider.class) {
      TestTicketRsaKeyFileProvider.clean();
      TestTicketRsaKeyFileProvider.files.add(PRIVATE_PLAIN);
      TestTicketRsaKeyFileProvider.isFilesWithPrivateKeyAreRequired = true;
      startNewContext(0);

      // GIVEN
      Ticket t = new Ticket();
      t.setClaim(new TicketClaim());
      t.getClaim().setUsername("abc");
      t.getClaim().setValidUntil(123);
      t.getClaim().setIsSuperUser(false);

      ticketSignatureService.signTicket(t);

      // WHEN
      ticketValidityService.markTicketAsInvalid(t);
      boolean isValid = ticketValidityService.isTicketValid(t);

      // THEN
      Assert.assertFalse(isValid, "Ticket should NOT be valid.");
    }
  }

  @Test
  public void tooLate() {
    synchronized (TestTicketRsaKeyFileProvider.class) {
      TestTicketRsaKeyFileProvider.clean();
      TestTicketRsaKeyFileProvider.files.add(PRIVATE_PLAIN);
      TestTicketRsaKeyFileProvider.isFilesWithPrivateKeyAreRequired = true;
      startNewContext(500); // now is "500"

      // GIVEN
      Ticket t = new Ticket();
      t.setClaim(new TicketClaim());
      t.getClaim().setUsername("abc");
      t.getClaim().setValidUntil(123);
      t.getClaim().setIsSuperUser(false);

      ticketSignatureService.signTicket(t);

      // WHEN
      boolean isValid = ticketValidityService.isTicketValid(t);

      // THEN
      Assert.assertFalse(isValid, "Ticket should NOT be valid.");
    }
  }

  @Test
  public void tampered() {
    synchronized (TestTicketRsaKeyFileProvider.class) {
      TestTicketRsaKeyFileProvider.clean();
      TestTicketRsaKeyFileProvider.files.add(PRIVATE_PLAIN);
      TestTicketRsaKeyFileProvider.isFilesWithPrivateKeyAreRequired = true;
      startNewContext(0);

      // GIVEN
      Ticket t = new Ticket();
      t.setClaim(new TicketClaim());
      t.getClaim().setUsername("abc");
      t.getClaim().setValidUntil(123);
      t.getClaim().setIsSuperUser(false);

      ticketSignatureService.signTicket(t);

      // WHEN
      t.getClaim().setUsername("xyz");
      boolean isValid = ticketValidityService.isTicketValid(t);

      // THEN
      Assert.assertFalse(isValid, "Ticket should NOT be valid.");
    }
  }

  private void startNewContext(long now) {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.ALL_BUT_NEW_DATA_WATCHER);
    dataContext.scan("org.diqube");
    dataContext.registerBeanDefinition("a", new RootBeanDefinition(TestTicketRsaKeyFileProvider.class));
    dataContext.refresh();
    ticketSignatureService = dataContext.getBean(TicketSignatureService.class);
    ticketValidityService = dataContext.getBean(TicketValidityService.class);
    ticketValidityService.setTimestampProvider(() -> now);
  }

  public static class TestTicketRsaKeyFileProvider implements TicketRsaKeyFileProvider {
    static boolean isFilesWithPrivateKeyAreRequired;
    static List<Triple<String, IOExceptionSupplier<InputStream>, String>> files;

    static void clean() {
      isFilesWithPrivateKeyAreRequired = false;
      files = new ArrayList<>();
    }

    @Override
    public List<Triple<String, IOExceptionSupplier<InputStream>, String>> getPemFiles() {
      return files;
    }

    @Override
    public boolean filesWithPrivateKeyAreRequired() {
      return isFilesWithPrivateKeyAreRequired;
    }
  }
}
