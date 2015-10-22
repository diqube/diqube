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
package org.diqube.function.projection;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

import org.diqube.data.ColumnType;
import org.diqube.function.Function;

/**
 * Expects an URI as input and will return the "top level domain" of it or the IP address of the server.
 *
 * @author Bastian Gloeckle
 */
@Function(name = TopLevelDomainStringFunction.NAME)
public class TopLevelDomainStringFunction extends AbstractSingleParamSameColTypeProjectionFunction<String> {
  public static final String NAME = "topleveldomain";

  private static Pattern tldPattern = Pattern.compile("[a-zA-Z]+");

  public TopLevelDomainStringFunction() {
    super(NAME, ColumnType.STRING, TopLevelDomainStringFunction::topLevelDomainFn);
  }

  public static String topLevelDomainFn(String uriString) {
    try {
      URI uri = new URI(uriString);
      String host = uri.getHost();
      if (host == null)
        return uriString;

      int lastDot = host.lastIndexOf('.');
      if (lastDot == -1 || lastDot == host.length() - 1 || lastDot == 0)
        return uriString;

      if (tldPattern.matcher(host.substring(lastDot + 1)).matches()) {
        int prevDot = host.lastIndexOf('.', lastDot - 1);

        if (prevDot == -1)
          return host;

        return host.substring(prevDot + 1);
      }
      return uriString;
    } catch (URISyntaxException e) {
      // no valid URI.
      return uriString;
    }
  }
}
