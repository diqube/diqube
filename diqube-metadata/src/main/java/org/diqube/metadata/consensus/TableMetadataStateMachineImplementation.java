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
package org.diqube.metadata.consensus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.diqube.consensus.AbstractConsensusStateMachine;
import org.diqube.consensus.ConsensusStateMachineImplementation;
import org.diqube.data.metadata.FieldMetadata;
import org.diqube.data.metadata.FieldMetadata.FieldType;
import org.diqube.data.metadata.TableMetadata;
import org.diqube.metadata.thrift.v1.SFieldMetadata;
import org.diqube.metadata.thrift.v1.SFieldType;
import org.diqube.metadata.thrift.v1.SMetadataEntry;
import org.diqube.metadata.thrift.v1.STableMetadata;
import org.diqube.util.Pair;

import io.atomix.copycat.server.Commit;

/**
 * Implementation of {@link TableMetadataStateMachine}
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachineImplementation
public class TableMetadataStateMachineImplementation extends AbstractConsensusStateMachine<SMetadataEntry>
    implements TableMetadataStateMachine {

  private static final String INTERNALDB_FILE_PREFIX = "metadata-";
  private static final String INTERNALDB_DATA_TYPE = "metadata_v1";

  /**
   * Map from table name to pair of current table metatdata and the current version number.
   */
  private Map<String, Pair<TableMetadata, Long>> metadata = new HashMap<>();

  private Map<String, Commit<?>> previousCommit = new HashMap<>();

  private Consumer<String> recomputeConsumer = null;

  public TableMetadataStateMachineImplementation() {
    super(INTERNALDB_FILE_PREFIX, INTERNALDB_DATA_TYPE, () -> new SMetadataEntry());
  }

  @Override
  protected void doInitialize(List<SMetadataEntry> entriesLoadedFromInternalDb) {
    if (entriesLoadedFromInternalDb != null) {
      for (SMetadataEntry serializedMetadataEntry : entriesLoadedFromInternalDb) {
        long version = serializedMetadataEntry.getVersionNumber();
        if (serializedMetadataEntry.isSetMetadata()) {
          metadata.put(serializedMetadataEntry.getMetadata().getTableName(),
              new Pair<>(deserializeTable(serializedMetadataEntry.getMetadata()), version));
        } else
          // Table was about to be recomputed
          metadata.put(serializedMetadataEntry.getMetadata().getTableName(), new Pair<>(null, version));
      }
    }
  }

  @Override
  public boolean compareAndSetTableMetadata(Commit<CompareAndSetTableMetadata> commit) {
    long expectedVersion = commit.operation().getPreviousMetadataVersion();
    String tableName = commit.operation().getNewMetadata().getTableName();

    if ((expectedVersion != Long.MIN_VALUE ^ metadata.containsKey(tableName)) || //
        (metadata.containsKey(tableName) && metadata.get(tableName).getRight() != expectedVersion)) {
      commit.clean();
      return false;
    }

    Commit<?> prev = previousCommit.put(commit.operation().getNewMetadata().getTableName(), commit);

    metadata.put(tableName, new Pair<>(commit.operation().getNewMetadata(), expectedVersion + 1));

    writeCurrentStateToInternalDb(commit.index());

    if (prev != null)
      prev.clean();

    return true;
  }

  @Override
  public Pair<TableMetadata, Long> getTableMetadata(Commit<GetTableMetadata> commit) {
    Pair<TableMetadata, Long> res = null;
    if (metadata.containsKey(commit.operation().getTable()))
      res = new Pair<>(metadata.get(commit.operation().getTable()));

    commit.close();

    return res;
  }

  @Override
  public void recomputeTableMetadata(Commit<RecomputeTableMetadata> commit) {
    String tableName = commit.operation().getTableName();

    if (metadata.containsKey(tableName)) {
      metadata.put(tableName, new Pair<>(null, metadata.get(tableName).getRight() + 1));

      writeCurrentStateToInternalDb(commit.index());
      commit.clean();

      if (recomputeConsumer != null)
        recomputeConsumer.accept(tableName);
    } else
      commit.clean();

  }

  private void writeCurrentStateToInternalDb(long consensusIndex) {
    List<SMetadataEntry> m = new ArrayList<>();
    for (Pair<TableMetadata, Long> p : metadata.values()) {
      SMetadataEntry newEntry = new SMetadataEntry();
      if (p.getLeft() != null)
        newEntry.setMetadata(serializeTable(p.getLeft()));

      newEntry.setVersionNumber(p.getRight());

      m.add(newEntry);
    }
    writeCurrentStateToInternalDb(consensusIndex, m);
  }

  private TableMetadata deserializeTable(STableMetadata serializedMetadata) {
    Map<String, FieldMetadata> fieldMetadata = new HashMap<>();
    for (SFieldMetadata serializedFieldMetadata : serializedMetadata.getFields()) {
      fieldMetadata.put(serializedFieldMetadata.getFieldName(), deserializeField(serializedFieldMetadata));
    }

    return new TableMetadata(serializedMetadata.getTableName(), fieldMetadata);
  }

  private FieldMetadata deserializeField(SFieldMetadata serializedMetadata) {
    FieldType type;
    switch (serializedMetadata.getFieldType()) {
    case STRING:
      type = FieldType.STRING;
      break;
    case LONG:
      type = FieldType.LONG;
      break;
    case DOUBLE:
      type = FieldType.DOUBLE;
      break;
    default:
      type = FieldType.CONTAINER;
      break;
    }
    return new FieldMetadata(serializedMetadata.getFieldName(), type, serializedMetadata.isRepeated());
  }

  private STableMetadata serializeTable(TableMetadata metadata) {
    STableMetadata res = new STableMetadata();
    res.setTableName(metadata.getTableName());
    List<SFieldMetadata> fields = new ArrayList<>();
    for (FieldMetadata f : metadata.getFields().values())
      fields.add(serializeField(f));
    res.setFields(fields);
    return res;
  }

  private SFieldMetadata serializeField(FieldMetadata metadata) {
    SFieldType type;
    switch (metadata.getFieldType()) {
    case STRING:
      type = SFieldType.STRING;
      break;
    case LONG:
      type = SFieldType.LONG;
      break;
    case DOUBLE:
      type = SFieldType.DOUBLE;
      break;
    default:
      type = SFieldType.CONTAINER;
      break;
    }

    SFieldMetadata res = new SFieldMetadata();
    res.setFieldName(metadata.getFieldName());
    res.setRepeated(metadata.isRepeated());
    res.setFieldType(type);

    return res;
  }

  /**
   * Consumer will consume the table name of a table whose metadata should be recomputed. Note that this will be called
   * on the state-machine thread!
   */
  public void setRecomputeConsumer(Consumer<String> recomputeConsumer) {
    this.recomputeConsumer = recomputeConsumer;
  }

}
