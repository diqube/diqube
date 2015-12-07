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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
public class TicketSignatureServiceTest {
  private static final String ENCRYPTED_PASSWORD = "diqube";

  private static final Triple<String, IOExceptionSupplier<InputStream>, String> PRIVATE_ENCRYPTED =
      new Triple<>("private.enc",
          () -> TicketSignatureServiceTest.class
              .getResourceAsStream("/" + TicketSignatureServiceTest.class.getSimpleName() + "/private.enc.pem"),
          ENCRYPTED_PASSWORD);
  private static final Triple<String, IOExceptionSupplier<InputStream>, String> PRIVATE_PLAIN =
      new Triple<>("private.plain",
          () -> TicketSignatureServiceTest.class
              .getResourceAsStream("/" + TicketSignatureServiceTest.class.getSimpleName() + "/private.plain.pem"),
          null);
  private static final Triple<String, IOExceptionSupplier<InputStream>, String> PUBLIC_ENCRYPTED =
      new Triple<>("public.enc",
          () -> TicketSignatureServiceTest.class
              .getResourceAsStream("/" + TicketSignatureServiceTest.class.getSimpleName() + "/public.enc.pem"),
          ENCRYPTED_PASSWORD);
  private static final Triple<String, IOExceptionSupplier<InputStream>, String> PUBLIC_PLAIN =
      new Triple<>("public.plain", () -> TicketSignatureServiceTest.class
          .getResourceAsStream("/" + TicketSignatureServiceTest.class.getSimpleName() + "/public.plain.pem"), null);

  private AnnotationConfigApplicationContext dataContext;
  private TicketSignatureService ticketSignatureService;

  @BeforeMethod
  public void before() {

  }

  @AfterMethod
  public void after() {
    dataContext.close();
    dataContext = null;
    ticketSignatureService = null;
  }

  @Test
  public void simpleSignAndValidateEncryptedKey() {
    synchronized (TestTicketRsaKeyFileProvider.class) {
      TestTicketRsaKeyFileProvider.clean();
      TestTicketRsaKeyFileProvider.files.add(PRIVATE_ENCRYPTED);
      TestTicketRsaKeyFileProvider.isFilesWithPrivateKeyAreRequired = true;
      startNewContext();

      // GIVEN
      Ticket t = new Ticket();
      t.setClaim(new TicketClaim());
      t.getClaim().setUsername("abc");
      t.getClaim().setValidUntil(123);
      t.getClaim().setIsSuperUser(false);

      // WHEN
      ticketSignatureService.signTicket(t);
      boolean isValid = isValid(t);

      // THEN
      Assert.assertTrue(isValid, "Ticket should be valid.");
    }
  }

  @Test
  public void simpleSignAndValidatePlainKey() {
    synchronized (TestTicketRsaKeyFileProvider.class) {
      TestTicketRsaKeyFileProvider.clean();
      TestTicketRsaKeyFileProvider.files.add(PRIVATE_PLAIN);
      TestTicketRsaKeyFileProvider.isFilesWithPrivateKeyAreRequired = true;
      startNewContext();

      // GIVEN
      Ticket t = new Ticket();
      t.setClaim(new TicketClaim());
      t.getClaim().setUsername("abc");
      t.getClaim().setValidUntil(123);
      t.getClaim().setIsSuperUser(false);

      // WHEN
      ticketSignatureService.signTicket(t);
      boolean isValid = isValid(t);

      // THEN
      Assert.assertTrue(isValid, "Ticket should be valid.");
    }
  }

  @Test
  public void signAndValidatePublicPlainKey() {
    synchronized (TestTicketRsaKeyFileProvider.class) {
      TestTicketRsaKeyFileProvider.clean();
      TestTicketRsaKeyFileProvider.files.add(PRIVATE_PLAIN);
      TestTicketRsaKeyFileProvider.isFilesWithPrivateKeyAreRequired = true;
      startNewContext();

      // GIVEN
      Ticket t = new Ticket();
      t.setClaim(new TicketClaim());
      t.getClaim().setUsername("abc");
      t.getClaim().setValidUntil(123);
      t.getClaim().setIsSuperUser(false);

      ticketSignatureService.signTicket(t);

      // WHEN
      TestTicketRsaKeyFileProvider.clean();
      TestTicketRsaKeyFileProvider.files.add(PUBLIC_PLAIN);
      TestTicketRsaKeyFileProvider.isFilesWithPrivateKeyAreRequired = false;
      startNewContext();

      boolean isValid = isValid(t);

      // THEN
      Assert.assertTrue(isValid, "Ticket should be valid.");
    }
  }

  @Test
  public void signAndValidatePublicEncryptedKey() {
    synchronized (TestTicketRsaKeyFileProvider.class) {
      TestTicketRsaKeyFileProvider.clean();
      TestTicketRsaKeyFileProvider.files.add(PRIVATE_ENCRYPTED);
      TestTicketRsaKeyFileProvider.isFilesWithPrivateKeyAreRequired = true;
      startNewContext();

      // GIVEN
      Ticket t = new Ticket();
      t.setClaim(new TicketClaim());
      t.getClaim().setUsername("abc");
      t.getClaim().setValidUntil(123);
      t.getClaim().setIsSuperUser(false);

      ticketSignatureService.signTicket(t);

      // WHEN
      TestTicketRsaKeyFileProvider.clean();
      TestTicketRsaKeyFileProvider.files.add(PUBLIC_ENCRYPTED);
      TestTicketRsaKeyFileProvider.isFilesWithPrivateKeyAreRequired = false;
      startNewContext();

      boolean isValid = isValid(t);

      // THEN
      Assert.assertTrue(isValid, "Ticket should be valid.");
    }
  }

  @Test
  public void signAndValidateEncryptedKeyTampered() {
    synchronized (TestTicketRsaKeyFileProvider.class) {
      TestTicketRsaKeyFileProvider.clean();
      TestTicketRsaKeyFileProvider.files.add(PRIVATE_ENCRYPTED);
      TestTicketRsaKeyFileProvider.isFilesWithPrivateKeyAreRequired = true;
      startNewContext();

      // GIVEN
      Ticket t = new Ticket();
      t.setClaim(new TicketClaim());
      t.getClaim().setUsername("abc");
      t.getClaim().setValidUntil(123);
      t.getClaim().setIsSuperUser(false);

      ticketSignatureService.signTicket(t);

      // WHEN
      byte[] sig = t.getSignature();
      sig[0] = (byte) (sig[0] + 1);
      t.setSignature(sig);
      boolean isValid = isValid(t);

      // THEN
      Assert.assertFalse(isValid, "Ticket should NOT be valid.");
    }
  }

  @Test
  public void signAndValidateEncryptedKeyTampered2() {
    synchronized (TestTicketRsaKeyFileProvider.class) {
      TestTicketRsaKeyFileProvider.clean();
      TestTicketRsaKeyFileProvider.files.add(PRIVATE_ENCRYPTED);
      TestTicketRsaKeyFileProvider.isFilesWithPrivateKeyAreRequired = true;
      startNewContext();

      // GIVEN
      Ticket t = new Ticket();
      t.setClaim(new TicketClaim());
      t.getClaim().setUsername("abc");
      t.getClaim().setValidUntil(123);
      t.getClaim().setIsSuperUser(false);

      ticketSignatureService.signTicket(t);

      // WHEN
      t.getClaim().setUsername("xyz");
      boolean isValid = isValid(t);

      // THEN
      Assert.assertFalse(isValid, "Ticket should NOT be valid.");
    }
  }

  private boolean isValid(Ticket t) {
    return ticketSignatureService
        .isValidTicketSignature(TicketUtil.deserialize(ByteBuffer.wrap(TicketUtil.serialize(t))));
  }

  private void startNewContext() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.ALL_BUT_NEW_DATA_WATCHER);
    dataContext.scan("org.diqube");
    dataContext.registerBeanDefinition("a", new RootBeanDefinition(TestTicketRsaKeyFileProvider.class));
    dataContext.refresh();
    ticketSignatureService = dataContext.getBean(TicketSignatureService.class);
  }

  public static class TestTicketRsaKeyFileProvider implements TicketRsaKeyFileProvider {
    static boolean isFilesWithPrivateKeyAreRequired;
    static List<Triple<String, IOExceptionSupplier<InputStream>, String>> files;

    static void clean() {
      isFilesWithPrivateKeyAreRequired = false;
      files = new ArrayList<>();
    }

    @Override
    public CompletableFuture<List<Triple<String, IOExceptionSupplier<InputStream>, String>>> getPemFiles() {
      return CompletableFuture.completedFuture(files);
    }

    @Override
    public boolean filesWithPrivateKeyAreRequired() {
      return isFilesWithPrivateKeyAreRequired;
    }
  }
}
