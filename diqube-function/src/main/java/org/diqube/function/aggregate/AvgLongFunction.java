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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.Supplier;

import org.diqube.data.column.ColumnType;
import org.diqube.function.AggregationFunction;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;
import org.diqube.function.aggregate.result.IntermediaryResultValueIterator;
import org.diqube.function.aggregate.result.IntermediaryResultValueSink;
import org.diqube.function.aggregate.util.BigDecimalHelper;

/**
 * Average function that takes Longs as input.
 *
 * @author Bastian Gloeckle
 */
@Function(name = AvgLongFunction.NAME)
public class AvgLongFunction implements AggregationFunction<Long, Double> {

  public static final String NAME = "avg";

  private static final Supplier<BigDecimal> ZERO_DECIMAL = () -> new BigDecimal("0.000000");
  private static final Supplier<BigInteger> ZERO_INT = () -> BigInteger.valueOf(0l);

  private BigInteger sum = ZERO_INT.get();
  private long count = 0L;

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void addIntermediary(IntermediaryResultValueIterator intermediary) {
    BigInteger otherSum = (BigInteger) intermediary.next();
    long otherCount = (Long) intermediary.next();

    if (otherCount == 0)
      return;

    sum = sum.add(otherSum);
    count += otherCount;
  }

  @Override
  public void removeIntermediary(IntermediaryResultValueIterator intermediary) {
    BigInteger otherSum = (BigInteger) intermediary.next();
    long otherCount = (Long) intermediary.next();

    if (otherCount == 0)
      return;

    if (otherCount == count) {
      sum = ZERO_INT.get();
      count = 0;
      return;
    }

    sum = sum.subtract(otherSum);
    count -= otherCount;
  }

  @Override
  public void addValues(ValueProvider<Long> valueProvider) {
    Long[] values = valueProvider.getValues();

    for (Long value : values) {
      sum = sum.add(BigInteger.valueOf(value));
      count++;
    }
  }

  @Override
  public void populateIntermediary(IntermediaryResultValueSink res) throws FunctionException {
    res.pushValue(sum);
    res.pushValue(count);
  }

  @Override
  public Double calculate() throws FunctionException {
    BigDecimal sumDec = BigDecimalHelper.zeroCreate().add(new BigDecimal(sum));
    return sumDec.divide(new BigDecimal(count), BigDecimalHelper.defaultMathContext()).doubleValue();
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
  public void provideConstantParameter(int idx, Object value) throws FunctionException {
    // noop.
  }

  @Override
  public boolean needsActualValues() {
    return true;
  }

}
