package org.diqube.remote.base.util;

import org.diqube.remote.base.thrift.RValue;

/**
 *
 * @author Bastian Gloeckle
 */
public class RValueUtil {
  public static RValue createRValue(Object value) {
    RValue res = new RValue();
    if (value instanceof String)
      res.setStrValue((String) value);
    else if (value instanceof Long)
      res.setLongValue((Long) value);
    else if (value instanceof Double)
      res.setDoubleValue((Double) value);
    else
      return null;
    return res;
  }
}
