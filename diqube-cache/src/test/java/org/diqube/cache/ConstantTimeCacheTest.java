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
package org.diqube.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author Bastian Gloeckle
 */
public class ConstantTimeCacheTest {
  @Test
  public void simpleAdd() throws InterruptedException {
    // GIVEN
    ConstantTimeCache<Long, Long, Long> cache = new ConstantTimeCache<>(200);
    cache.offer(1L, 2L, 3L);

    Thread.sleep(50);

    Long value = cache.get(1L, 2L);
    Collection<Long> values = cache.getAll(1L);
    int size = cache.size();

    Assert.assertEquals((long) value, 3L, "Expected to get value.");
    Assert.assertEquals(values, new HashSet<>(Arrays.asList(3L)), "Expected to get values.");
    Assert.assertEquals(size, 1, "Expected correct size");

    Thread.sleep(200);

    value = cache.get(1L, 2L);
    values = cache.getAll(1L);
    size = cache.size();

    Assert.assertNull(value, "Expected to NOT get value.");
    Assert.assertEquals(values, new HashSet<>(), "Expected to NOT get values.");
    Assert.assertEquals(size, 0, "Expected correct size");
  }
}
