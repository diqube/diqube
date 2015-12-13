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
package org.diqube.consensus;

import org.diqube.consensus.ConsensusServer.ConsensusStorageProvider;

import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;

/**
 *
 * @author Bastian Gloeckle
 */
public class ConsensusServerTestUtil {
  public static void configureMemoryOnlyStorage(ConsensusServer server) {
    server.setConsensusStorageProvider(new ConsensusStorageProvider(null) {
      @Override
      public Storage createStorage() {
        return Storage.builder().withStorageLevel(StorageLevel.MEMORY).build();
      }
    });
  }
}
