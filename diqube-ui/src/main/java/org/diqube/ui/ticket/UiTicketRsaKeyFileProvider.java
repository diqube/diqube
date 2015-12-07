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
package org.diqube.ui.ticket;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.ticket.TicketRsaKeyFileProvider;
import org.diqube.ui.DiqubeServletConfig;
import org.diqube.ui.DiqubeServletConfig.ServletConfigListener;
import org.diqube.util.Triple;

/**
 * Provider of .pem files for validating signed tickets for UI code.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class UiTicketRsaKeyFileProvider implements TicketRsaKeyFileProvider, ServletConfigListener {

  @Inject
  private DiqubeServletConfig config;
  private CompletableFuture<List<Triple<String, IOExceptionSupplier<InputStream>, String>>> pemFileFuture =
      new CompletableFuture<>();

  @Override
  public synchronized CompletableFuture<List<Triple<String, IOExceptionSupplier<InputStream>, String>>> getPemFiles() {
    return pemFileFuture;
  }

  @Override
  public boolean filesWithPrivateKeyAreRequired() {
    // we do not accept private keys here, as this is a configuration error: The UI will never sign new tickets and does
    // not need private keys therefore, but it would actually be harmful: If an attacker gains access to a UI server
    // (which should usually be those servers that are best accessible for users obviously), he should not get his grip
    // on the private keys that sign Tickets!
    return false;
  }

  @Override
  public synchronized void servletConfigurationAvailable() {
    List<Triple<String, IOExceptionSupplier<InputStream>, String>> pemFiles = new ArrayList<>();
    for (String fileName : Arrays.asList(config.getTicketPublicKeyPem(), config.getTicketPublicKeyPemAlt1(),
        config.getTicketPublicKeyPemAlt2())) {
      if (fileName == null)
        continue;

      File f = new File(fileName);
      if (!f.exists())
        throw new RuntimeException("File '" + fileName + "' does not exist!");

      pemFiles.add(
          new Triple<>(f.getAbsolutePath(), () -> new FileInputStream(f), null /* public keys are not encrypted */));
    }

    pemFileFuture.complete(pemFiles);
  }

}
