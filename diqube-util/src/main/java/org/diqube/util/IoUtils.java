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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author Bastian Gloeckle
 */
public class IoUtils {
  /**
   * Read {@link InputStream} fully into a ByteBuffer.
   * 
   * An {@link IOException} when reading from {@link InputStream} is wrapped in a {@link RuntimeException}.
   * 
   * Supports InputStreams with up to {@link Integer#MAX_VALUE} bytes.
   */
  public static BigByteBuffer inputStreamToBigByteBuffer(InputStream is) {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    byte[] buf = new byte[4096];
    int read;
    try {
      while ((read = is.read(buf)) >= 0)
        if (read > 0)
          os.write(buf, 0, read);
    } catch (IOException e) {
      throw new RuntimeException("Could not read input stream", e);
    }
    return new BigByteBuffer(os.toByteArray());
  }
}
