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
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.crypto.params.RSAKeyParameters;
import org.bouncycastle.crypto.params.RSAPrivateCrtKeyParameters;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.crypto.util.PublicKeyFactory;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.diqube.context.AutoInstatiate;
import org.diqube.thrift.base.thrift.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads and provides the RSA keys from {@link TicketRsaKeyFileProvider}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class TicketRsaKeyManager {
  private static final Logger logger = LoggerFactory.getLogger(TicketRsaKeyManager.class);

  @Inject
  private TicketRsaKeyFileProvider provider;

  private RSAPrivateCrtKeyParameters privateSigningKey = null;
  private List<RSAKeyParameters> publicValidationKeys = null;

  private boolean initialized = false;

  /**
   * @return The private key that needs to be used to sign any {@link Ticket}s. May be <code>null</code> in case there
   *         is no private key available.
   */
  public RSAPrivateCrtKeyParameters getPrivateSigningKey() {
    if (!initialized)
      throw new IllegalStateException("Not initialized");
    return privateSigningKey;
  }

  /**
   * @return Unmodifiable list of RSA public keys that can be used to validate signatures of {@link Ticket}s.
   */
  public List<RSAKeyParameters> getPublicValidationKeys() {
    if (!initialized)
      throw new IllegalStateException("Not initialized");
    return publicValidationKeys;
  }

  @PostConstruct
  public void initialize() {
    List<RSAKeyParameters> allPublicKeys = new ArrayList<>();
    publicValidationKeys = Collections.unmodifiableList(allPublicKeys);

    provider.getPemFiles().whenComplete((pemFiles, error) -> {
      if (error != null)
        throw new RuntimeException("Exception while identifying .pem files!", error);

      if (pemFiles.isEmpty())
        throw new RuntimeException("No .pem files configured that can be used to sign/validate tickets!");

      for (int i = 0; i < pemFiles.size(); i++) {
        String pemFileName = pemFiles.get(i).getLeft();
        String pemPassword = pemFiles.get(i).getRight();

        try (InputStream pemStream = pemFiles.get(i).getMiddle().get()) {
          Reader pemReader = new InputStreamReader(pemStream);
          try (PEMParser parser = new PEMParser(pemReader)) {
            Object o = parser.readObject();

            SubjectPublicKeyInfo publicKeyInfo = null;
            PrivateKeyInfo privateKeyInfo = null;

            if (o instanceof PEMEncryptedKeyPair) {
              if (pemPassword == null)
                throw new RuntimeException(
                    "PEM file '" + pemFileName + "' is password protected, but the password is not configured.");

              PEMDecryptorProvider decryptionProvider =
                  new JcePEMDecryptorProviderBuilder().build(pemPassword.toCharArray());

              PEMEncryptedKeyPair encryptedKeyPair = (PEMEncryptedKeyPair) o;
              PEMKeyPair keyPair = encryptedKeyPair.decryptKeyPair(decryptionProvider);
              publicKeyInfo = keyPair.getPublicKeyInfo();
              privateKeyInfo = keyPair.getPrivateKeyInfo();
            } else if (o instanceof PEMKeyPair) {
              PEMKeyPair keyPair = (PEMKeyPair) o;
              publicKeyInfo = keyPair.getPublicKeyInfo();
              privateKeyInfo = keyPair.getPrivateKeyInfo();
            } else if (o instanceof SubjectPublicKeyInfo)
              publicKeyInfo = (SubjectPublicKeyInfo) o;
            else
              throw new RuntimeException(
                  "Could not identify content of pem file '" + pemFileName + "': " + o.toString());

            if (publicKeyInfo == null)
              throw new RuntimeException(
                  "Could not load '" + pemFileName + "' because it did not contain a public key.");

            if (privateKeyInfo == null)
              logger.info("Loading public key from '{}'.", pemFileName);
            else if (!provider.filesWithPrivateKeyAreRequired())
              throw new RuntimeException("File '" + pemFileName + "' contains a private key, but only public keys are "
                  + "accepted. Please extract the public key from the current file and configure diqube to use "
                  + "that new file.");

            allPublicKeys.add((RSAKeyParameters) PublicKeyFactory.createKey(publicKeyInfo));
            if (i == 0 && privateKeyInfo != null) {
              logger.info("Loading private key from '{}' and will use that for signing tickets.", pemFileName);
              privateSigningKey = (RSAPrivateCrtKeyParameters) PrivateKeyFactory.createKey(privateKeyInfo);
            }
          }
        } catch (IOException e) {
          throw new RuntimeException("Could not interact with '" + pemFileName + "'. Correct password?", e);
        }
      }

      if (privateSigningKey == null && provider.filesWithPrivateKeyAreRequired())
        throw new RuntimeException("A .pem file containing a private key for signing tickets is required. "
            + "Make sure that the first configured .pem file contains a private key.");

      initialized = true;
    });
  }

}
