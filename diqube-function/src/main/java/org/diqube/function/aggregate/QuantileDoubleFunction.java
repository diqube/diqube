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
package org.diqube.function.aggregate;

import java.nio.ByteBuffer;

import org.diqube.data.column.ColumnType;
import org.diqube.function.AggregationFunction;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;
import org.diqube.function.aggregate.result.IntermediaryResultValueIterator;
import org.diqube.function.aggregate.result.IntermediaryResultValueSink;
import org.diqube.function.aggregate.util.SerializedAVLTreeDigest;

import com.tdunning.math.stats.AVLTreeDigest;

/**
 * Approximates a custom quantile using t-digest.
 *
 * @author Bastian Gloeckle
 */
@Function(name = QuantileDoubleFunction.NAME)
public class QuantileDoubleFunction implements AggregationFunction<Double, Double> {
  public static final String NAME = "quantile";

  private Double quantile = null;

  private AVLTreeDigest tdigest = new AVLTreeDigest(100.);

  private boolean complete = false;

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void provideConstantParameter(int idx, Object value) throws FunctionException {
    if (!(value instanceof Double))
      throw new FunctionException("Parameter to " + NAME + " function can be a DOUBLE only.");

    if (idx == 0)
      quantile = (Double) value;
  }

  @Override
  public void addValues(ValueProvider<Double> valueProvider) {
    for (Double value : valueProvider.getValues())
      tdigest.add(value);

    complete = valueProvider.isFinalSetOfValues();
  }

  @Override
  public void addIntermediary(IntermediaryResultValueIterator intermediary) {
    Long otherComplete = (Long) intermediary.next();
    if (otherComplete == 1L) {
      // only add state if we received some state where the other instance was "complete", i.e. this is the final result
      // of a single other instance of this function. We will not have to remove this state again (we wouldn't be able
      // to anyway).
      SerializedAVLTreeDigest serialized = (SerializedAVLTreeDigest) intermediary.next();
      AVLTreeDigest other = AVLTreeDigest.fromBytes(ByteBuffer.wrap(serialized.getSerialized()));
      tdigest.add(other);
    }
  }

  @Override
  public void removeIntermediary(IntermediaryResultValueIterator intermediary) {
    // noop, tdigest does not support removing state.
  }

  @Override
  public void populateIntermediary(IntermediaryResultValueSink res) throws FunctionException {
    ensureQuantilePresent();

    if (!complete) {
      res.pushValue(Long.valueOf(0L));
    } else {
      res.pushValue(Long.valueOf(1L));

      tdigest.compress();

      ByteBuffer buf = ByteBuffer.allocate(tdigest.byteSize());
      tdigest.asSmallBytes(buf);
      int len = buf.position();
      byte[] b = new byte[len];
      buf.rewind();
      buf.get(b);

      res.pushValue(new SerializedAVLTreeDigest(b));
    }
  }

  @Override
  public Double calculate() throws FunctionException {
    ensureQuantilePresent();

    double res = tdigest.quantile(quantile);
    if (Double.isNaN(res))
      return -1.;
    return res;
  }

  private void ensureQuantilePresent() throws FunctionException {
    if (quantile == null)
      throw new FunctionException("Which quantile should be calculated? Use constant paramater to specify. "
          + "Example: quantile(0.25, X) to calculate the 25% quantile.");
  }

  @Override
  public ColumnType getOutputType() {
    return ColumnType.DOUBLE;
  }

  @Override
  public ColumnType getInputType() {
    return ColumnType.LONG;
  }

  @Override
  public boolean needsActualValues() {
    return true;
  }
}
