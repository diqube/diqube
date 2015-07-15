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
package org.diqube.config;

import java.lang.reflect.Field;
import java.util.Deque;
import java.util.LinkedList;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.ReflectionUtils;

/**
 * A {@link BeanPostProcessor} that will fill the fields annotated with {@link Config} correctly.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ConfigurationPostProcessor implements BeanPostProcessor {

  private static final Logger logger = LoggerFactory.getLogger(ConfigurationPostProcessor.class);

  @Inject
  private ConfigurationManager configManager;

  @Override
  public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {

    Deque<Class<?>> classes = new LinkedList<>();
    classes.add(bean.getClass());
    while (!classes.isEmpty()) {
      Class<?> clazz = classes.poll();

      if (clazz.equals(Object.class))
        break;

      for (Field f : clazz.getDeclaredFields()) {
        Config[] c = f.getAnnotationsByType(Config.class);
        if (c.length > 0) {
          String configKey = c[0].value();
          if (configManager.getValue(configKey) != null) {
            String valueString = configManager.getValue(configKey);

            Object value;
            if (f.getType().equals(Long.class) || f.getType().equals(Long.TYPE))
              value = Long.valueOf(valueString);
            else if (f.getType().equals(Double.class) || f.getType().equals(Double.TYPE))
              value = Double.valueOf(valueString);
            else if (f.getType().equals(Integer.class) || f.getType().equals(Integer.TYPE))
              value = Integer.valueOf(valueString);
            else if (f.getType().equals(String.class))
              value = valueString;
            else
              throw new FatalBeanException("Cannot wire config value for '" + bean.getClass().getName() + "."
                  + f.getName() + " as the datatype is not supported.");

            ReflectionUtils.makeAccessible(f);
            try {
              logger.debug("Wiring config value of '{}' to '{}.{}'", configKey, bean.getClass().getName(), f.getName());
              f.set(bean, value);
            } catch (IllegalArgumentException | IllegalAccessException e) {
              throw new FatalBeanException("Could not wire configManager value to " + f);
            }
          } else
            throw new FatalBeanException(bean.getClass().getName() + "." + f.getName()
                + " requires configManager of which no value is available: " + configKey);
        }
      }
      classes.add(clazz.getSuperclass());
    }

    return bean;
  }

  @Override
  public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
    return bean;
  }

}
