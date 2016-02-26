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
package org.diqube.consensus.test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import javax.inject.Inject;

import org.diqube.consensus.ConsensusClient;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.Profiles;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;

/**
 * Implementation of {@link ConsensusClient} for unit tests: Forwards method calls to the {@link ConsensusClient} to
 * locally instantiated beans, without actually running a consensus cluster.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
@Profile(Profiles.TEST_CONSENSUS)
public class TestConsensusClient implements ConsensusClient {

  @Inject
  private ApplicationContext context;

  @Override
  public <T> ClosableProvider<T> getStateMachineClient(Class<T> stateMachineInterface) throws IllegalStateException {
    T serviceBean = context.getBean(stateMachineInterface);
    if (serviceBean == null)
      throw new IllegalStateException("Bean not found for " + stateMachineInterface);

    InvocationHandler h = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Method beanMethod = serviceBean.getClass().getMethod(method.getName(), method.getParameterTypes());
        if (beanMethod == null)
          throw new RuntimeException("Method " + method.getName() + " not found on " + serviceBean);

        return beanMethod.invoke(serviceBean, args);
      }
    };

    @SuppressWarnings("unchecked")
    T stateMachineProxy = (T) Proxy.newProxyInstance(TestConsensusClient.class.getClassLoader(),
        new Class<?>[] { stateMachineInterface }, h);

    return new ClosableProvider<T>() {
      @Override
      public void close() {
        // noop.
      }

      @Override
      public T getClient() {
        return stateMachineProxy;
      }
    };
  }

  @Override
  public void contextAboutToShutdown() {
    // noop
  }

}
