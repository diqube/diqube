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
package org.diqube.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * A HashingBatchCollector is a {@link Collector} that tries to collect batches of specific size before sending them to
 * a consumer.
 * 
 * <p>
 * Please be aware that input objects (of type T) will be <b>hashed</b> and must therefore be non-colliding. If they are
 * colliding, <b>values may be lost</b>. It is fine to use this collector with classes that do not have equals() and
 * hashCode() methods overridden from {@link Object}, but only have standard identity implementations provided by
 * {@link Object}.
 * 
 * <p>
 * This method can be used in a multi-threaded way, but when calling the provided consumer, it will be synchronized.
 * 
 * <p>
 * It is not guaranteed that batches of exact batchSize are provided to the Consumer.
 * 
 * @param <T>
 *          The type of input objects.
 * 
 * @author Bastian Gloeckle
 */
public final class HashingBatchCollector<T> implements Collector<T, ConcurrentHashMap<T, Object>, Void> {
  private final Object EMPTY = new Object();
  private final Object SYNC = new Object();
  private int batchSize;
  private Consumer<T[]> consumer;
  private Factory<T> factory;

  /**
   * Create a new {@link HashingBatchCollector}.
   * 
   * @param batchSize
   *          The size of batches that should be tried to be used when calling the consumer.
   * @param factory
   *          Factory that creates an array of the correct result type with the given length. The arrays created by this
   *          factory will be provided to the consumer.
   * @param consumer
   *          The consumer that will be called as soon as a batch is approximately full (or the finisher identifies some
   *          objects are left).
   */
  public HashingBatchCollector(int batchSize, Factory<T> factory, Consumer<T[]> consumer) {
    this.batchSize = batchSize;
    this.consumer = consumer;
    this.factory = factory;
  }

  @Override
  public Supplier<ConcurrentHashMap<T, Object>> supplier() {
    return () -> new ConcurrentHashMap<T, Object>();
  }

  @Override
  public BiConsumer<ConcurrentHashMap<T, Object>, T> accumulator() {
    return (map, a) -> {
      if (map.size() >= batchSize) {
        synchronized (SYNC) {
          int size = map.size();
          if (size > 0) {
            Set<T> keysToWorkOn = new HashSet<T>(Collections.list(map.keys()));
            consumer.accept(createDataArray(keysToWorkOn));
            for (T k : keysToWorkOn)
              map.remove(k);
          }
        }
      }
      map.put(a, EMPTY);
    };
  }

  @Override
  public BinaryOperator<ConcurrentHashMap<T, Object>> combiner() {
    return (x, y) -> {
      x.putAll(y);
      if (x.size() >= batchSize) {
        synchronized (SYNC) {
          int size = x.size();
          if (size > 0) {
            Set<T> keysToWorkOn = new HashSet<T>(Collections.list(x.keys()));
            consumer.accept(createDataArray(keysToWorkOn));
          }
        }
        return supplier().get();
      }
      return x;
    };
  }

  @Override
  public Function<ConcurrentHashMap<T, Object>, Void> finisher() {
    return (map) -> {
      synchronized (SYNC) {
        int size = map.size();
        if (size > 0) {
          Set<T> keysToWorkOn = new HashSet<T>(Collections.list(map.keys()));
          consumer.accept(createDataArray(keysToWorkOn));
        }
      }
      return null;
    };
  }

  @Override
  public Set<java.util.stream.Collector.Characteristics> characteristics() {
    return new HashSet<Collector.Characteristics>(Arrays.asList(new Characteristics[] { Characteristics.CONCURRENT,
        Characteristics.UNORDERED }));
  }

  /**
   * Transform the values of the enumeration into a result array.
   */
  private T[] createDataArray(Set<T> values) {
    T[] res = factory.create(values.size());
    Iterator<T> setIt = values.iterator();
    for (int i = 0; i < res.length; i++) {
      res[i] = setIt.next();
    }
    return res;
  }

  public static interface Factory<T> {
    /**
     * Creates and returns an array of the result type with the given length.
     */
    public T[] create(int len);
  }
}