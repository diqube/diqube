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
package org.diqube.testutil;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.support.GenericApplicationContext;

/**
 * Helper class for the bean context that is started in tests.
 *
 * @author Bastian Gloeckle
 */
public class TestContextOverrideBean {
  /**
   * Override a {@link BeanDefinition} in the given context, effectively replacing the bean that will be created.
   * 
   * This has to be called before the context is {@link GenericApplicationContext#refresh()}ed.
   * 
   * @param ctx
   *          The context to work on.
   * @param overriddenBeanClass
   *          Class of the bean that should be overridden.
   * @param newBeanClass
   *          Class of bean that should be used instead.
   */
  public static void overrideBeanClass(GenericApplicationContext ctx, Class<?> overriddenBeanClass, Class<?> newBeanClass) {
    String overrideBeanDefName = null;
    for (String beanDefinitionName : ctx.getBeanFactory().getBeanDefinitionNames()) {
      if (ctx.getBeanFactory().getBeanDefinition(beanDefinitionName).getBeanClassName()
          .equals(overriddenBeanClass.getName())) {
        overrideBeanDefName = beanDefinitionName;
        break;
      }
    }

    GenericBeanDefinition overridingBeanDef =
        (GenericBeanDefinition) ((GenericBeanDefinition) ctx.getBeanFactory().getBeanDefinition(overrideBeanDefName))
            .cloneBeanDefinition();
    overridingBeanDef.setBeanClass(newBeanClass);
    ctx.registerBeanDefinition(overrideBeanDefName, overridingBeanDef);
  }
}
