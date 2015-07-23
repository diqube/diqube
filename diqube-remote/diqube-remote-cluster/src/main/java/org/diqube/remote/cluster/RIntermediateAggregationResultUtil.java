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

    ColumnType type = null;
    if (input.isSetInputColumnType()) {
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
    }

    IntermediaryResult<Object, Object, Object> res =
        new IntermediaryResult<Object, Object, Object>(left, middle, right, type);
    res.setOutputColName(input.getOutputColName());
    return res;
  }

  public static RIntermediateAggregationResult buildRIntermediateAggregationResult(
      IntermediaryResult<Object, Object, Object> input) {
    RIntermediateAggregationResult res = new RIntermediateAggregationResult();
    res.setOutputColName(input.getOutputColName());
    res.setValue1(RValueUtil.createRValue(input.getLeft()));
    if (input.getMiddle() != null)
      res.setValue2(RValueUtil.createRValue(input.getMiddle()));
    if (input.getRight() != null)
      res.setValue3(RValueUtil.createRValue(input.getRight()));

    if (input.getInputColumnType() != null) {
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
    }

    return res;
  }

}
