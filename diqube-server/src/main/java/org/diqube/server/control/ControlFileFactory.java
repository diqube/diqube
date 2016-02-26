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
package org.diqube.server.control;

import java.io.File;
import java.util.List;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.table.TableFactory;
import org.diqube.executionenv.TableRegistry;
import org.diqube.loader.CsvLoader;
import org.diqube.loader.DiqubeLoader;
import org.diqube.loader.JsonLoader;
import org.diqube.metadata.TableMetadataManager;
import org.diqube.server.metadata.ServerTableMetadataPublisher;
import org.diqube.server.queryremote.flatten.ClusterFlattenServiceHandler;
import org.diqube.util.Pair;

/**
 * Factory for {@link ControlFileLoader} and {@link ControlFileUnloader}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ControlFileFactory {
  @Inject
  private TableRegistry tableRegistry;

  @Inject
  private TableFactory tableFactory;

  @Inject
  private CsvLoader csvLoader;

  @Inject
  private JsonLoader jsonLoader;

  @Inject
  private DiqubeLoader diqubeLoader;

  @Inject
  private ClusterFlattenServiceHandler clusterFlattenServiceHandler;

  @Inject
  private ServerTableMetadataPublisher serverTableMetadataPublisher;

  @Inject
  private TableMetadataManager tableMetadataManager;

  public ControlFileLoader createControlFileLoader(File controlFile) {
    return new ControlFileLoader(tableRegistry, tableFactory, csvLoader, jsonLoader, diqubeLoader,
        clusterFlattenServiceHandler, serverTableMetadataPublisher, controlFile);
  }

  public ControlFileUnloader createControlFileUnloader(File controlFile, Pair<String, List<Long>> tableInfo) {
    return new ControlFileUnloader(tableRegistry, tableMetadataManager, controlFile, tableInfo);
  }
}
