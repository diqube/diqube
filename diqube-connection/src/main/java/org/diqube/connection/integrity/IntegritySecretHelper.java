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
package org.diqube.connection.integrity;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;

import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;

/**
 * Helper for the configKeys {@link ConfigKey#MESSAGE_INTEGRITY_SECRET},
 * {@link ConfigKey#MESSAGE_INTEGRITY_SECRET_ALTERNATIVE1} and {@link ConfigKey#MESSAGE_INTEGRITY_SECRET_ALTERNATIVE2}
 * in order to transform them into a form where they can be used for {@link IntegrityCheckingProtocol}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class IntegritySecretHelper {

  private static final String NONE = "none";

  @Config(ConfigKey.MESSAGE_INTEGRITY_SECRET)
  private String messageIntegritySecret;

  @Config(ConfigKey.MESSAGE_INTEGRITY_SECRET_ALTERNATIVE1)
  private String messageIntegritySecretAlternative1;

  @Config(ConfigKey.MESSAGE_INTEGRITY_SECRET_ALTERNATIVE2)
  private String messageIntegritySecretAlternative2;

  @PostConstruct
  public void initialize() {
    if (messageIntegritySecret == null || "".equals(messageIntegritySecret.trim()))
      throw new RuntimeException(
          "No config value set for " + ConfigKey.MESSAGE_INTEGRITY_SECRET + ", but one is needed!");
    messageIntegritySecret = messageIntegritySecret.trim();

    if (messageIntegritySecretAlternative1 != null)
      messageIntegritySecretAlternative1 = messageIntegritySecretAlternative1.trim();
    if ("".equals(messageIntegritySecretAlternative1))
      messageIntegritySecretAlternative1 = NONE;

    if (messageIntegritySecretAlternative2 != null)
      messageIntegritySecretAlternative2 = messageIntegritySecretAlternative2.trim();
    if ("".equals(messageIntegritySecretAlternative2))
      messageIntegritySecretAlternative2 = NONE;
  }

  /**
   * @return The MAC keys to be used with {@link IntegrityCheckingProtocol}.
   */
  public byte[][] provideMessageIntegritySecrets() {
    List<byte[]> res = new ArrayList<>();

    if (messageIntegritySecret.startsWith("0x"))
      res.add(hexStringToBytes(messageIntegritySecret));
    else
      res.add(messageIntegritySecret.getBytes(Charset.forName("UTF-8")));

    for (String k : Arrays.asList(messageIntegritySecretAlternative1, messageIntegritySecretAlternative2)) {
      if (k == null || k.equals(NONE))
        continue;
      if (k.startsWith("0x"))
        res.add(hexStringToBytes(k));
      else
        res.add(k.getBytes(Charset.forName("UTF-8")));
    }

    return res.toArray(new byte[res.size()][]);
  }

  private byte[] hexStringToBytes(String hexString) {
    byte[] res = new byte[(int) Math.ceil((hexString.length() - 2) / 2.)];
    int resPos = 0;
    for (int i = 2; i < hexString.length(); i += 2) {
      String s = hexString.substring(i, i + 2);
      if (s.length() == 1)
        s += "0";
      s = "0x" + s;
      res[resPos++] = Byte.decode(s);
    }

    return res;
  }

  /** for tests */
  /* package */ void setMessageIntegritySecret(String messageIntegritySecret) {
    this.messageIntegritySecret = messageIntegritySecret;
  }
}
