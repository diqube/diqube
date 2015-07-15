package org.diqube.remote.base.util;

import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RNodeDefaultAddress;
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
