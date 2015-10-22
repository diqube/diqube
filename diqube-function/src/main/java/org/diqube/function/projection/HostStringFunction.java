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

import org.diqube.data.ColumnType;
import org.diqube.function.Function;

/**
 * Converts an URI-style input string col into a string col containing only the "host" part of the URI. The host can
 * either be the full domain name or an IP address.
 *
 * @author Bastian Gloeckle
 */
@Function(name = HostStringFunction.NAME)
public class HostStringFunction extends AbstractSingleParamSameColTypeProjectionFunction<String> {

  public static final String NAME = "host";

  public HostStringFunction() {
    super(NAME, ColumnType.STRING, HostStringFunction::domainFn);
  }

  public static String domainFn(String uriString) {
    try {
      URI uri = new URI(uriString);
      String host = uri.getHost();
      if (host == null)
        return uriString;
      return host;
    } catch (URISyntaxException e) {
      return uriString; // apparently not a valid URI, return string just as it is.
    }
  }

}
