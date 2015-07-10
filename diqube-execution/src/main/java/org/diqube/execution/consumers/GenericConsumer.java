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
package org.diqube.execution.consumers;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;

import org.diqube.execution.ExecutablePlanStep;

/**
 * Generic super-interface for all consumers. See sub-interfaces.
 *
 * Typically, for each {@link GenericConsumer} there will be one source that fills the consumer with data, but there
 * might be multiple.
 *
 * @author Bastian Gloeckle
 */
public interface GenericConsumer {
  /**
   * Will be called when a source that provides data to this consumer is fully processed.
   */
  public void sourceIsDone();

  /**
   * @return <code>null</code> or, if available, the ID of the {@link ExecutablePlanStep} this GenericConsumer belongs
   *         to.
   */
  public Integer getDestinationPlanStepId();

  /**
   * Method will be called as soon as the consumer was wired once. Might be called multiple times according to step.
   */
  public void recordOneWiring();

  /**
   * @return Number of times {@link #recordOneWiring()} was called.
   */
  public int getNumberOfTimesWired();

  /**
   * @return Number of active wirings, that is {@link #getNumberOfTimesWired()} minus the number of times
   *         {@link #sourceIsDone()} was called.
   */
  public int getNumberOfActiveWirings();

  /**
   * @return A string representation of the type of consumer
   */
  default public String getType() {
    Deque<Class<?>> clazzes = new LinkedList<Class<?>>();
    clazzes.add(this.getClass());
    while (!clazzes.isEmpty()) {
      Class<?> clazz = clazzes.poll();

      IdentifyingConsumerClass info = clazz.getDeclaredAnnotation(IdentifyingConsumerClass.class);
      if (info != null)
        return info.value().getSimpleName();

      if (clazz.getSuperclass() != null)
        clazzes.add(clazz.getSuperclass());
      clazzes.addAll(Arrays.asList(clazz.getInterfaces()));
    }

    return "?";
  }

  /**
   * Used solely for producing readable results for {@link GenericConsumer#getType()}.
   */
  @Target({ ElementType.TYPE })
  @Retention(RetentionPolicy.RUNTIME)
  public static @interface IdentifyingConsumerClass {
    Class<? extends GenericConsumer> value();
  }
}
