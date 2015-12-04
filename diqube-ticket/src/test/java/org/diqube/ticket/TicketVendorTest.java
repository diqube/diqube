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
import org.diqube.ticket.TicketRsaKeyFileProvider.IOExceptionSupplier;
import org.diqube.util.Triple;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for {@link TicketVendor}.
 *
 * @author Bastian Gloeckle
 */
public class TicketVendorTest {
  private static final Triple<String, IOExceptionSupplier<InputStream>, String> PRIVATE_PLAIN =
      new Triple<>("private.plain", () -> TicketVendorTest.class
          .getResourceAsStream("/" + TicketVendorTest.class.getSimpleName() + "/private.plain.pem"), null);

  private AnnotationConfigApplicationContext dataContext;
  private TicketVendor ticketVendor;
  private TicketValidityService ticketValidityService;

  @BeforeMethod
  public void before() {

  }

  @AfterMethod
  public void after() {
    dataContext.close();
    dataContext = null;
    ticketVendor = null;
  }

  @Test
  public void freshTicketIsValid() {
    synchronized (TestTicketRsaKeyFileProvider.class) {
      TestTicketRsaKeyFileProvider.clean();
      TestTicketRsaKeyFileProvider.files.add(PRIVATE_PLAIN);
      TestTicketRsaKeyFileProvider.isFilesWithPrivateKeyAreRequired = true;
      startNewContext();

      // GIVEN
      Ticket t = ticketVendor.createDefaultTicketForUser("abc", false);

      // WHEN
      boolean isValid = ticketValidityService.isTicketValid(t);

      // THEN
      Assert.assertTrue(isValid, "Ticket should be valid.");
    }
  }

  private void startNewContext() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.ALL_BUT_NEW_DATA_WATCHER);
    dataContext.scan("org.diqube");
    dataContext.registerBeanDefinition("a", new RootBeanDefinition(TestTicketRsaKeyFileProvider.class));
    dataContext.refresh();
    ticketVendor = dataContext.getBean(TicketVendor.class);
    ticketValidityService = dataContext.getBean(TicketValidityService.class);
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
