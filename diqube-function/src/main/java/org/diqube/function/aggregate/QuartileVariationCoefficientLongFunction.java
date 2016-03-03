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

import org.diqube.data.column.ColumnType;
import org.diqube.function.AggregationFunction;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;
import org.diqube.function.aggregate.result.IntermediaryResultValueIterator;
import org.diqube.function.aggregate.result.IntermediaryResultValueSink;

/**
 * Calculates the Quartile Variantion Coefficient (= Quartile coefficient of dispersion).
 * 
 * <p>
 * (Q3 - Q1) / (Q3 + Q1) = (quantile(0.75) - quantile(0.25)) / (quantile(0.75) + quantile(0.25))
 *
 * @author Bastian Gloeckle
 */
@Function(name = QuartileVariationCoefficientLongFunction.NAME)
public class QuartileVariationCoefficientLongFunction implements AggregationFunction<Long, Double> {
  public static final String NAME = "qvc";

  private QuantileLongFunction q1;
  private QuantileLongFunction q3;

  public QuartileVariationCoefficientLongFunction() {
    q1 = new QuantileLongFunction();
    q1.provideConstantParameter(0, 0.25);

    q3 = new QuantileLongFunction();
    q3.provideConstantParameter(0, 0.75);
  }

  @Override
  public String getNameLowerCase() {
    return NAME;
  }

  @Override
  public void provideConstantParameter(int idx, Object value) throws FunctionException {
    // noop.
  }

  @Override
  public void addValues(ValueProvider<Long> valueProvider) {
    Long[] values = valueProvider.getValues();

    ValueProvider<Long> childValueProvider = new ValueProvider<Long>() {
      @Override
      public long size() {
        return values.length;
      }

      @Override
      public boolean isFinalSetOfValues() {
        return valueProvider.isFinalSetOfValues();
      }

      @Override
      public Long[] getValues() {
        return values;
      }
    };

    q1.addValues(childValueProvider);
    q3.addValues(childValueProvider);
  }

  @Override
  public void addIntermediary(IntermediaryResultValueIterator intermediary) {
    q1.addIntermediary(intermediary);
    q3.addIntermediary(intermediary);
  }

  @Override
  public void removeIntermediary(IntermediaryResultValueIterator intermediary) {
    q1.removeIntermediary(intermediary);
    q3.removeIntermediary(intermediary);
  }

  @Override
  public void populateIntermediary(IntermediaryResultValueSink res) throws FunctionException {
    q1.populateIntermediary(res);
    q3.populateIntermediary(res);
  }

  @Override
  public Double calculate() throws FunctionException {
    double q1Res = q1.calculate();
    double q3Res = q3.calculate();

    if (q1Res + q3Res == 0.)
      return -1.;

    return (q3Res - q1Res) / (q3Res + q1Res);
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
