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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.diqube.context.AutoInstatiate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads configuration values from both, a user supplied file and a file on the classpath which contains the default
 * values - this class then provides the active configuration values.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ConfigurationManager {
  private static final Logger logger = LoggerFactory.getLogger(ConfigurationManager.class);

  public static final String DEFAULT_CONFIG_CLASSPATH_FILENAME = "/server.properties";

  public static final String CUSTOM_PROPERTIES_SYSTEM_PROPERTY = "diqube.properties";

  private Properties defaultProperties;
  private Properties customProperties;

  @PostConstruct
  private void initialize() {
    String customFileName = System.getProperty(CUSTOM_PROPERTIES_SYSTEM_PROPERTY);
    if (customFileName != null) {
      customProperties = new Properties();
      try {
        customProperties.load(new InputStreamReader(new FileInputStream(customFileName), Charset.forName("UTF-8")));
        // TODO validate that properties have valid names!
        logger.info("Loaded custom properties from {}", customFileName);
      } catch (IOException e) {
        throw new RuntimeException("Could not load custom properties from " + customFileName);
      }
    } else {
      customProperties = null;
      logger.info(
          "Did not load custom properties. Use system property '{}' to point to a file containing custom properties.",
          CUSTOM_PROPERTIES_SYSTEM_PROPERTY);
    }

    defaultProperties = new Properties();
    try {
      defaultProperties.load(new InputStreamReader(
          this.getClass().getResourceAsStream(DEFAULT_CONFIG_CLASSPATH_FILENAME), Charset.forName("UTF-8")));
    } catch (IOException e) {
      throw new RuntimeException("Could not load default config.");
    }
  }

  /**
   * @return The value of the given config key (see {@link ConfigKey}), either the one provided by the user or the
   *         default one.
   */
  public String getValue(String configKey) {
    if (customProperties != null && customProperties.getProperty(configKey) != null)
      return customProperties.getProperty(configKey);

    return getDefaultValue(configKey);
  }

  /**
   * @return The default value for the given config key.
   */
  public String getDefaultValue(String configKey) {
    return defaultProperties.getProperty(configKey);
  }
}
