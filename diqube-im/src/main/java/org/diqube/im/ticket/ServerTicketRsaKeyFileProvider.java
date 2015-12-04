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
package org.diqube.im.ticket;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.ticket.TicketRsaKeyFileProvider;
import org.diqube.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

/**
 * A {@link TicketRsaKeyFileProvider} for diqube-server that relies on private keys to sign any new tickets.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ServerTicketRsaKeyFileProvider implements TicketRsaKeyFileProvider {
  private static final Logger logger = LoggerFactory.getLogger(ServerTicketRsaKeyFileProvider.class);

  private static final String NONE = "none";

  /**
   * {@link ConfigKey#TICKET_RSA_PRIVATE_KEY_PEM_FILE} might have this prefix, then load key file from classpath. This
   * is officially undocumented, because diqube pem files should never be used by users (although there should be none
   * packaged in the jar).
   * 
   * We need this for the tests.
   */
  private static final String CLASSPATH_PREFIX = "classpath:";

  @Config(ConfigKey.TICKET_RSA_PRIVATE_KEY_PEM_FILE)
  private String pemFile1;

  @Config(ConfigKey.TICKET_RSA_PRIVATE_KEY_PASSWORD)
  private String password1;

  @Config(ConfigKey.TICKET_RSA_PRIVATE_KEY_ALTERNATIVE1_PEM_FILE)
  private String pemFile2;

  @Config(ConfigKey.TICKET_RSA_PRIVATE_KEY_ALTERNATIVE1_PASSWORD)
  private String password2;

  @Config(ConfigKey.TICKET_RSA_PRIVATE_KEY_ALTERNATIVE2_PEM_FILE)
  private String pemFile3;

  @Config(ConfigKey.TICKET_RSA_PRIVATE_KEY_ALTERNATIVE2_PASSWORD)
  private String password3;

  private Triple<String, IOExceptionSupplier<InputStream>, String> getPemFile(String fileName, String password) {
    if (fileName == null || "".equals(fileName.trim()) || NONE.equals(fileName))
      return null;

    if (fileName.startsWith(CLASSPATH_PREFIX)) {
      fileName = fileName.substring(CLASSPATH_PREFIX.length());
      try (InputStream classPathStream = getClass().getClassLoader().getResourceAsStream(fileName)) {
        File tmpFile = File.createTempFile("diqube-test-", ".pem");
        logger.debug("Serializing .pem from classpath '{}' to '{}'.", fileName, tmpFile.getAbsolutePath());
        try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
          ByteStreams.copy(classPathStream, fos);
        }
        fileName = tmpFile.getAbsolutePath();
        tmpFile.deleteOnExit();
      } catch (IOException e) {
        throw new RuntimeException("Could not load .pem from classpath.", e);
      }
    }

    File file = new File(fileName);
    if (!file.exists())
      throw new RuntimeException("File '" + file.getAbsolutePath() + "' does not exist.");

    return new Triple<>(file.getAbsolutePath(), () -> new FileInputStream(file),
        ("".equals(password)) ? null : password);
  }

  @Override
  public List<Triple<String, IOExceptionSupplier<InputStream>, String>> getPemFiles() {
    List<Triple<String, IOExceptionSupplier<InputStream>, String>> pemFiles = new ArrayList<>();

    Triple<String, IOExceptionSupplier<InputStream>, String> t = getPemFile(pemFile1, password1);
    if (t == null)
      throw new RuntimeException("Configuration key '" + ConfigKey.TICKET_RSA_PRIVATE_KEY_PEM_FILE + "' must be set.");
    pemFiles.add(t);

    t = getPemFile(pemFile2, password2);
    if (t != null)
      pemFiles.add(t);

    t = getPemFile(pemFile3, password3);
    if (t != null)
      pemFiles.add(t);

    return pemFiles;
  }

  @Override
  public boolean filesWithPrivateKeyAreRequired() {
    return true;
  }

}
