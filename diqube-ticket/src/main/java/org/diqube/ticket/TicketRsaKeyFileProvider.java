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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.diqube.remote.query.thrift.Ticket;
import org.diqube.util.Triple;

/**
 * Provides information on what RSA keys should be used to sign/validate {@link Ticket}s.
 *
 * @author Bastian Gloeckle
 */
public interface TicketRsaKeyFileProvider {
  /**
   * Returns OpenSSL .pem files which contain either a RSA public key or a RSA public/private key pair. The public key
   * will be used by {@link TicketSignatureService} to validate any tickets.
   * 
   * For validating tickets the public keys of all returned files are inspected, but new tickets will be signed only
   * with the private key (if there is one) of the <b>first</b> returned file.
   * 
   * @return A {@link CompletableFuture} that completes to a list of {@link Triple}s: Left is the string denoting the
   *         source of the .pem file (= file name), middle is a supplier of a new {@link InputStream} to read from it
   *         and right is the password which is needed to decrypt the .pem stream (<code>null</code> if no password).
   */
  public CompletableFuture<List<Triple<String, IOExceptionSupplier<InputStream>, String>>> getPemFiles();

  /**
   * @return If <code>true</code> it is required that {@link #getPemFiles()} returns files that contain a private key,
   *         otherwise {@link TicketRsaKeyManager} will throw a corresponding exception. If <code>false</code>,
   *         {@link TicketRsaKeyManager} will throw an exception if it finds a file that contains a private key.
   */
  public boolean filesWithPrivateKeyAreRequired();

  public static interface IOExceptionSupplier<T> {
    T get() throws IOException;
  }
}
