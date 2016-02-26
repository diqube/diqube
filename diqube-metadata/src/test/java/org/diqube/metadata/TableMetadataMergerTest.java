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
package org.diqube.metadata;

import java.util.Arrays;
import java.util.List;

import org.diqube.data.metadata.FieldMetadata.FieldType;
import org.diqube.data.metadata.TableMetadata;
import org.diqube.metadata.TableMetadataMerger.IllegalTableLayoutException;
import org.diqube.util.Triple;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests {@link TableMetadataMerger}.
 *
 * @author Bastian Gloeckle
 */
public class TableMetadataMergerTest {
  private TableMetadataMerger merger;

  @BeforeMethod
  public void before() {
    merger = new TableMetadataMerger();
  }

  @Test
  public void singleTest() throws IllegalTableLayoutException {
    // GIVEN
    List<TableMetadata> inputMetadata = Arrays.asList(new TableMetadata[] { //
        TableMetadataTestUtil.createMetadata( //
            new Triple<>("a", FieldType.LONG, false), //
            new Triple<>("b", FieldType.STRING, false) //
        ) //
    });

    // WHEN
    TableMetadata metadata = merger.of(inputMetadata).merge();

    // THEN
    Assert.assertEquals(metadata.getTableName(), inputMetadata.get(0).getTableName(), "Expected correct table name");
    TableMetadataTestUtil.assertField(metadata, "a", FieldType.LONG, false);
    TableMetadataTestUtil.assertField(metadata, "b", FieldType.STRING, false);
    Assert.assertEquals(metadata.getFields().size(), 2, "Expected correct number of fields");
  }

  @Test
  public void twoEqualTest() throws IllegalTableLayoutException {
    // GIVEN
    List<TableMetadata> inputMetadata = Arrays.asList(new TableMetadata[] { //
        TableMetadataTestUtil.createMetadata( //
            new Triple<>("a", FieldType.LONG, false), //
            new Triple<>("b", FieldType.STRING, false) //
        ), //
        TableMetadataTestUtil.createMetadata( //
            new Triple<>("a", FieldType.LONG, false), //
            new Triple<>("b", FieldType.STRING, false) //
        ) //
    });

    // WHEN
    TableMetadata metadata = merger.of(inputMetadata).merge();

    // THEN
    TableMetadataTestUtil.assertField(metadata, "a", FieldType.LONG, false);
    TableMetadataTestUtil.assertField(metadata, "b", FieldType.STRING, false);
    Assert.assertEquals(metadata.getFields().size(), 2, "Expected correct number of fields");
  }

  @Test
  public void twoInequalTest() throws IllegalTableLayoutException {
    // GIVEN
    List<TableMetadata> inputMetadata = Arrays.asList(new TableMetadata[] { //
        TableMetadataTestUtil.createMetadata( //
            new Triple<>("a", FieldType.LONG, false), //
            new Triple<>("b", FieldType.STRING, false) //
        ), //
        TableMetadataTestUtil.createMetadata( //
            new Triple<>("a", FieldType.LONG, false), //
            new Triple<>("c", FieldType.DOUBLE, true) //
        ) //
    });

    // WHEN
    TableMetadata metadata = merger.of(inputMetadata).merge();

    // THEN
    TableMetadataTestUtil.assertField(metadata, "a", FieldType.LONG, false);
    TableMetadataTestUtil.assertField(metadata, "b", FieldType.STRING, false);
    TableMetadataTestUtil.assertField(metadata, "c", FieldType.DOUBLE, true);
    Assert.assertEquals(metadata.getFields().size(), 3, "Expected correct number of fields");
  }

  @Test(expectedExceptions = IllegalTableLayoutException.class)
  public void incompatibleTypesTest() throws IllegalTableLayoutException {
    // GIVEN
    List<TableMetadata> inputMetadata = Arrays.asList(new TableMetadata[] { //
        TableMetadataTestUtil.createMetadata( //
            new Triple<>("a", FieldType.LONG, false), //
            new Triple<>("b", FieldType.STRING, false) //
        ), //
        TableMetadataTestUtil.createMetadata( //
            new Triple<>("a", FieldType.STRING, false), //
            new Triple<>("c", FieldType.DOUBLE, false) //
        ) //
    });

    // WHEN
    merger.of(inputMetadata).merge();

    // THEN
    // exception
  }

  @Test(expectedExceptions = IllegalTableLayoutException.class)
  public void incompatibleRepetitionTest() throws IllegalTableLayoutException {
    // GIVEN
    List<TableMetadata> inputMetadata = Arrays.asList(new TableMetadata[] { //
        TableMetadataTestUtil.createMetadata( //
            new Triple<>("a", FieldType.LONG, false), //
            new Triple<>("b", FieldType.STRING, false) //
        ), //
        TableMetadataTestUtil.createMetadata( //
            new Triple<>("a", FieldType.LONG, false), //
            new Triple<>("b", FieldType.DOUBLE, true) //
        ) //
    });

    // WHEN
    merger.of(inputMetadata).merge();

    // THEN
    // exception
  }
}
