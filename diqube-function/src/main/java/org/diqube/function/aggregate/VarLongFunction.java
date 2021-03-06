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
 * Calculates the population variance.
 * 
 * <p>
 * 1/N * sum(x_i^2) - avg(x_i)^2.
 *
 * @author Bastian Gloeckle
 */
@Function(name = VarLongFunction.NAME)
public class VarLongFunction implements AggregationFunction<Long, Double> {
  public static final String NAME = "var";

  private static final Supplier<BigInteger> ZERO_SUM = () -> BigInteger.valueOf(0l);

  private AvgLongFunction avgFn;
  private long count = 0l;
  private BigInteger squaredSum = ZERO_SUM.get();

  public VarLongFunction() {
    avgFn = new AvgLongFunction();
  }

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void provideConstantParameter(int idx, Object value) throws FunctionException {
    // noop
  }

  @Override
  public void addValues(ValueProvider<Long> valueProvider) {
    Long[] values = valueProvider.getValues();

    for (Long value : values) {
      squaredSum = squaredSum.add(BigInteger.valueOf(value).pow(2));
      count++;
    }

    avgFn.addValues(new ValueProvider<Long>() {
      @Override
      public long size() {
        return values.length;
      }

      @Override
      public Long[] getValues() {
        return values;
      }

      @Override
      public boolean isFinalSetOfValues() {
        return valueProvider.isFinalSetOfValues();
      }
    });
  }

  @Override
  public void addIntermediary(IntermediaryResultValueIterator intermediary) {
    long otherCount = (Long) intermediary.next();
    BigInteger otherSquaredSum = (BigInteger) intermediary.next();

    avgFn.addIntermediary(intermediary);
    count += otherCount;
    squaredSum = squaredSum.add(otherSquaredSum);
  }

  @Override
  public void removeIntermediary(IntermediaryResultValueIterator intermediary) {
    long otherCount = (Long) intermediary.next();
    BigInteger otherSquaredSum = (BigInteger) intermediary.next();
    avgFn.removeIntermediary(intermediary);

    if (otherCount == count) {
      count = 0;
      squaredSum = ZERO_SUM.get();
      return;
    }

    count -= otherCount;
    squaredSum = squaredSum.subtract(otherSquaredSum);
  }

  @Override
  public void populateIntermediary(IntermediaryResultValueSink res) throws FunctionException {
    res.pushValue(count);
    res.pushValue(squaredSum);
    avgFn.populateIntermediary(res);
  }

  @Override
  public Double calculate() throws FunctionException {
    double avg = avgFn.calculate();

    BigDecimal res = BigDecimalHelper.zeroCreate().add(new BigDecimal(squaredSum));
    res = res.divide(BigDecimal.valueOf(count), BigDecimalHelper.defaultMathContext());
    res = res.subtract(BigDecimal.valueOf(avg).pow(2));
    return res.doubleValue();
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
