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
package org.diqube.ui.db;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.ui.DiqubeServletConfig;
import org.diqube.ui.DiqubeServletConfig.ServletConfigListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider of a {@link UiDatabase}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class UiDbProvider implements ServletConfigListener {
  private static final Logger logger = LoggerFactory.getLogger(UiDbProvider.class);

  @Inject
  private DiqubeServletConfig config;

  @Inject
  private UiDatabaseFactory factory;

  private UiDatabase db;

  @Override
  public void servletConfigurationAvailable() {
    if (DiqubeServletConfig.UI_DB_TYPE_HSQLDB.equals(config.getUiDbType())) {
      logger.info("Using HSQLDB as UI database.");
      db = factory.createHsql(config.getUiDbLocation(), config.getUiDbUser(), config.getUiDbPassword());
    } else
      throw new RuntimeException("Unknown DB type.");
  }

  @PreDestroy
  public void cleanup() {
    if (db != null)
      db.shutdown();
  }

  /**
   * @return The {@link UiDatabase} to be used. Can be <code>null</code> in case the config is not yet loaded.
   */
  public UiDatabase getDb() {
    return db;
  }
}
