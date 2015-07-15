package org.diqube.remote.cluster;

import org.diqube.data.ColumnType;
import org.diqube.function.IntermediaryResult;
import org.diqube.remote.base.util.RValueUtil;
import org.diqube.remote.cluster.thrift.RColumnType;
import org.diqube.remote.cluster.thrift.RIntermediateAggregationResult;

/**
 * Util for {@link RIntermediateAggregationResult}
 *
 * @author Bastian Gloeckle
 */
public class RIntermediateAggregationResultUtil {
  public static IntermediaryResult<Object, Object, Object> buildIntermediateAggregationResult(
      RIntermediateAggregationResult input) {
    Object left = RValueUtil.createValue(input.getValue1());
    Object middle = null;
    if (input.isSetValue2())
      middle = RValueUtil.createValue(input.getValue2());
    Object right = null;
    if (input.isSetValue3())
      right = RValueUtil.createValue(input.getValue3());

    ColumnType type;
    switch (input.getInputColumnType()) {
    case LONG:
      type = ColumnType.LONG;
      break;
    case DOUBLE:
      type = ColumnType.DOUBLE;
      break;
    default:
      type = ColumnType.STRING;
      break;
    }

    IntermediaryResult<Object, Object, Object> res =
        new IntermediaryResult<Object, Object, Object>(left, middle, right, type);
    return res;
  }

  public static RIntermediateAggregationResult buildRIntermediateAggregationResult(
      IntermediaryResult<Object, Object, Object> input) {
    RIntermediateAggregationResult res = new RIntermediateAggregationResult();
    res.setValue1(RValueUtil.createRValue(input.getLeft()));
    if (input.getMiddle() != null)
      res.setValue2(RValueUtil.createRValue(input.getMiddle()));
    if (input.getRight() != null)
      res.setValue3(RValueUtil.createRValue(input.getRight()));

    switch (input.getInputColumnType()) {
    case STRING:
      res.setInputColumnType(RColumnType.STRING);
      break;
    case LONG:
      res.setInputColumnType(RColumnType.LONG);
      break;
    case DOUBLE:
      res.setInputColumnType(RColumnType.DOUBLE);
      break;
    }

    return res;
  }

}
