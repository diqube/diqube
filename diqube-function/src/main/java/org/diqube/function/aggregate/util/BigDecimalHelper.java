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
package org.diqube.function.aggregate.util;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.function.Supplier;

/**
 * Helper class for {@link BigDecimal}s.
 *
 * @author Bastian Gloeckle
 */
public class BigDecimalHelper {

  private static final MathContext DEFAULT_MATH_CONTEXT = new MathContext(34, RoundingMode.HALF_UP);

  /**
   * @return a new {@link BigDecimal} with the correct scale and value 0.
   */
  public static BigDecimal zeroCreate() {
    return new BigDecimal(0., defaultMathContext());
  }

  /**
   * @return {@link Supplier} for {@link BigDecimal}s with correct scale and value 0.
   */
  public static Supplier<BigDecimal> zeroSupplier() {
    return () -> zeroCreate();
  }

  /**
   * @return The default {@link MathContext} to use.
   */
  public static MathContext defaultMathContext() {
    return DEFAULT_MATH_CONTEXT;
  }
}
