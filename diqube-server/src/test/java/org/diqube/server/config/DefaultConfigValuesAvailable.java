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
package org.diqube.server.config;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.diqube.config.ConfigKey;
import org.diqube.config.ConfigurationManager;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

/**
 * Validates that for each constant in {@link ConfigKey} there is a default value in server.properties.
 *
 * @author Bastian Gloeckle
 */
public class DefaultConfigValuesAvailable {
  @Test
  public void defaultValuesForAllConfigsAvailable() throws IOException {
    Properties properties = new Properties();
    properties.load(this.getClass().getResourceAsStream(ConfigurationManager.DEFAULT_CONFIG_CLASSPATH_FILENAME));

    Set<String> neededConfigKeys = Stream.of(ConfigKey.class.getFields()). //
        filter(f -> f.getType().equals(String.class)). // String fields
        filter(f -> Modifier.isStatic(f.getModifiers())). // static fields
        map(f -> {
          try {
            return (String) f.get(null); // get value of field.
          } catch (Exception e) {
            throw new RuntimeException("Could not access value.");
          }
        }).collect(Collectors.toSet());

    Set<String> notAvailableKeys = Sets.difference(neededConfigKeys, properties.keySet());

    Assert.assertEquals(notAvailableKeys, new HashSet<String>(),
        "Expected that for each constant in " + ConfigKey.class.getSimpleName() + " there is a default value in "
            + ConfigurationManager.DEFAULT_CONFIG_CLASSPATH_FILENAME
            + ". For the following keys there is no default value: ");
  }
}
