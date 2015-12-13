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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import javax.annotation.PostConstruct;

import org.apache.thrift.TBase;
import org.diqube.config.Config;
import org.diqube.config.DerivedConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.file.internaldb.InternalDbFileReader;
import org.diqube.file.internaldb.InternalDbFileReader.ReadException;
import org.diqube.file.internaldb.InternalDbFileWriter;
import org.diqube.file.internaldb.InternalDbFileWriter.WriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.copycat.server.Commit;

/**
 * Abstract base class for consensus state machine implementations that have a
 * {@link ConsensusStateMachineImplementation} annotation and want to provide storing any state on disk using
 * "internalDb" files.
 * 
 * <p>
 * Note that the subclassing class should be {@link AutoInstatiate}d, which is the case for all
 * {@link ConsensusStateMachineImplementation}s.
 *
 * @param <T>
 *          Thrift type used to serialize data to internalDb files.
 * @author Bastian Gloeckle
 */
public abstract class AbstractConsensusStateMachine<T extends TBase<?, ?>> {
  private static final Logger logger = LoggerFactory.getLogger(AbstractConsensusStateMachine.class);

  private String internalDbFilePrefix;
  private String internalDbDataType;
  private Supplier<T> factory;

  @Config(DerivedConfigKey.FINAL_INTERNAL_DB_DIR)
  private String internalDbDir;

  private InternalDbFileWriter<T> internalDbFileWriter;

  public AbstractConsensusStateMachine(String internalDbFilePrefix, String internalDbDataType, Supplier<T> factory) {
    this.internalDbFilePrefix = internalDbFilePrefix;
    this.internalDbDataType = internalDbDataType;
    this.factory = factory;
  }

  @PostConstruct
  public void initialize() {
    File internalDbDirFile = new File(internalDbDir);
    if (!internalDbDirFile.exists())
      if (!internalDbDirFile.mkdirs())
        throw new RuntimeException("Could not create directory " + internalDbDir);

    try {
      InternalDbFileReader<T> internalDbFileReader =
          new InternalDbFileReader<>(internalDbDataType, internalDbFilePrefix, internalDbDirFile, factory);
      List<T> entries = internalDbFileReader.readNewest();
      if (entries == null)
        logger.info("No internaldb for {} available", internalDbDataType);
      doInitialize(entries);
    } catch (ReadException e) {
      throw new RuntimeException("Could not load " + internalDbDataType + " file", e);
    }

    internalDbFileWriter = new InternalDbFileWriter<>(internalDbDataType, internalDbFilePrefix, internalDbDirFile);
  }

  /**
   * Load the initial state of the state machine which was loaded from internalDb.
   * 
   * @param entriesLoadedFromInternalDb
   *          The entries that were stored in internalDb. May be <code>null</code> if no data is available.
   */
  protected abstract void doInitialize(List<T> entriesLoadedFromInternalDb);

  /**
   * Writes current state of the state machine to internalDb.
   * 
   * @param consensusIndex
   *          The {@link Commit#index()} of the commit that was commited last.
   * @param entries
   *          The entries to store (= the state of the state machine).
   */
  protected void writeCurrentStateToInternalDb(long consensusIndex, Collection<T> entries) {
    List<T> entryList;
    if (entries instanceof List)
      entryList = (List<T>) entries;
    else
      entryList = new ArrayList<>(entries);

    try {
      internalDbFileWriter.write(consensusIndex, entryList);
    } catch (WriteException e1) {
      logger.error("Could not write {} internaldb file!", internalDbDataType, e1);
      // this is an error, but we try to continue anyway. When the file is missing, the node might not be able to
      // recover correctly, but for now we can keep working. The admin might want to copy a internaldb file from a
      // different node.
    }
  }
}
