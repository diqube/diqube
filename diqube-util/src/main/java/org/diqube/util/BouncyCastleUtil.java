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
package org.diqube.util;

import java.security.Provider;
import java.security.Security;

/**
 * Util class for using Bouncycastle
 *
 * @author Bastian Gloeckle
 */
public class BouncyCastleUtil {
  private static final String BC_PROVIDER_NAME = "BC";

  /**
   * Ensure BouncyCastle is fully initialized.
   */
  public static void ensureInitialized() {
    getProvider();
  }

  /**
   * @return The java security {@link Provider} of bouncycastle, if available. Make sure to add the dependency to the
   *         projects pom.
   * @throws BouncyCastleUnavailableException
   *           if BC is unavailable.
   */
  public static Provider getProvider() throws BouncyCastleUnavailableException {
    Provider res = Security.getProvider(BC_PROVIDER_NAME);
    if (res == null) {
      synchronized (BouncyCastleUtil.class) {
        res = Security.getProvider(BC_PROVIDER_NAME);
        if (res == null) {
          // disable BC "EC MQV" (patent issues).
          System.setProperty("org.bouncycastle.ec.disable_mqv", "true");

          try {
            @SuppressWarnings("unchecked")
            Class<? extends Provider> providerClass =
                (Class<? extends Provider>) Class.forName("org.bouncycastle.jce.provider.BouncyCastleProvider");

            Provider bcProvider = providerClass.newInstance();
            Security.addProvider(bcProvider);

            res = Security.getProvider(BC_PROVIDER_NAME);
          } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new BouncyCastleUnavailableException(e);
          }
        }
      }
    }
    return res;
  }

  public static class BouncyCastleUnavailableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public BouncyCastleUnavailableException(Throwable cause) {
      super(cause);
    }
  }
}
