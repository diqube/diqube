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
package org.diqube.thrift.base.util;

import org.diqube.thrift.base.thrift.RNodeAddress;
import org.diqube.thrift.base.thrift.RNodeDefaultAddress;
import org.diqube.util.Pair;

/**
 * Util class for {@link RNodeAddress}.
 *
 * @author Bastian Gloeckle
 */
public class RNodeAddressUtil {
  public static RNodeAddress buildDefault(String host, short port) {
    RNodeAddress res = new RNodeAddress();
    res.setDefaultAddr(new RNodeDefaultAddress());
    res.getDefaultAddr().setHost(host);
    res.getDefaultAddr().setPort(port);
    return res;
  }

  public static RNodeAddress buildDefault(Pair<String, Short> addr) {
    RNodeAddress res = new RNodeAddress();
    res.setDefaultAddr(new RNodeDefaultAddress());
    res.getDefaultAddr().setHost(addr.getLeft());
    res.getDefaultAddr().setPort(addr.getRight());
    return res;
  }

  public static Pair<String, Short> readDefault(RNodeAddress addr) {
    if (!addr.isSetDefaultAddr())
      return null;
    String host = addr.getDefaultAddr().getHost();
    short port = addr.getDefaultAddr().getPort();
    return new Pair<>(host, port);
  }
}
