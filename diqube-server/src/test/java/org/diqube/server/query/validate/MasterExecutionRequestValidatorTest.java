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
package org.diqube.server.query.validate;

import java.util.Arrays;
import java.util.function.Function;

import org.diqube.context.Profiles;
import org.diqube.metadata.DefaultTableMetadataManager;
import org.diqube.metadata.TableMetadataManager;
import org.diqube.plan.ExecutionPlanBuilder;
import org.diqube.plan.ExecutionPlanBuilderFactory;
import org.diqube.plan.exception.ValidationException;
import org.diqube.server.querymaster.query.validate.MasterExecutionRequestValidator;
import org.diqube.testutil.TestContextOverrideBean;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.FieldMetadata;
import org.diqube.thrift.base.thrift.FieldType;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.mockito.Mockito;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests validation of various diql queries that is implemented by {@link MasterExecutionRequestValidator}.
 *
 * @author Bastian Gloeckle
 */
public class MasterExecutionRequestValidatorTest {

  private static final String TABLE = "tab";

  private AnnotationConfigApplicationContext dataContext;
  private ExecutionPlanBuilder executionPlanBuilder;
  private TableMetadataManager metadataManagerMock;

  @BeforeMethod
  public void before() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.UNIT_TEST);
    dataContext.scan("org.diqube");
    TestContextOverrideBean.overrideBeanClass(dataContext, DefaultTableMetadataManager.class,
        DelegatingTableMetadataManager.class);
    dataContext.refresh();

    metadataManagerMock = Mockito.mock(TableMetadataManager.class);
    dataContext.getBean(DelegatingTableMetadataManager.class).setDelegate(metadataManagerMock);

    ExecutionPlanBuilderFactory executionPlanBuilderFactory = dataContext.getBean(ExecutionPlanBuilderFactory.class);
    executionPlanBuilder = executionPlanBuilderFactory.createExecutionPlanBuilder();
    executionPlanBuilder.withAdditionalRequestValidator(dataContext.getBean(MasterExecutionRequestValidator.class));
  }

  @AfterMethod
  public void after() throws Throwable {
    dataContext.close();
  }

  @Test
  public void simpleTest() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.LONG, false));

    // WHEN
    executionPlanBuilder.fromDiql("select a from " + TABLE + " where a = 1").build();

    // THEN
    // no validation exception
  }

  @Test(expectedExceptions = ValidationException.class)
  public void simpleFailingTest() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.STRING, false));

    // WHEN
    executionPlanBuilder.fromDiql("select a from " + TABLE + " where a = 1").build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because a is compared to a LONG");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void projectionFailingTest() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.LONG, false));

    // WHEN
    executionPlanBuilder.fromDiql("select a from " + TABLE + " where log(a) = 1").build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because log(a) is compared to a LONG");
  }

  @Test
  public void projectionTest() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.LONG, false));

    // WHEN
    executionPlanBuilder.fromDiql("select a from " + TABLE + " where log(a) = 1.").build();

    // THEN
    // no exception
  }

  @Test(expectedExceptions = ValidationException.class)
  public void projectionUnavailableTest() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.LONG, false));

    // WHEN
    executionPlanBuilder.fromDiql("select a from " + TABLE + " where concat(a, a) = 'b'").build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because concat is called on two LONG cols");
  }

  @Test(expectedExceptions = ValidationException.class, enabled = false) // TODO #111
  public void projectionUnavailable2Test() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.STRING, false));

    // WHEN
    executionPlanBuilder.fromDiql("select a from " + TABLE + " where concat(a, 5) = 'b'").build();

    // THEN
    Assert.fail(
        "Expected a ValidationException to be thrown because concat is called on a STRING col and a constant LONG");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void projectionUnavailable3Test() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.LONG, false));

    // WHEN
    executionPlanBuilder.fromDiql("select concat(a, a) from " + TABLE).build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because concat is called on two LONG cols");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void projectionUnavailable4Test() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.DOUBLE, false), //
        new FieldMetadata("b", FieldType.LONG, false));

    // WHEN
    executionPlanBuilder.fromDiql("select add(a, b) from " + TABLE).build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because add is called on different types of cols");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void projectionUnavailable5Test() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.DOUBLE, false));

    // WHEN
    executionPlanBuilder.fromDiql("select log('4') from " + TABLE).build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because log is called on a literal STRING");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void projectionUnavailable6Test() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.DOUBLE, false));

    // WHEN
    executionPlanBuilder.fromDiql("select concat('b', log(a)) from " + TABLE).build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because concat is called with a projected DOUBLE col");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void rootColUnvailableTest() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.DOUBLE, false));

    // WHEN
    executionPlanBuilder.fromDiql("select b from " + TABLE).build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because col b does not exist");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void rootColUnvailable2Test() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.DOUBLE, false));

    // WHEN
    executionPlanBuilder.fromDiql("select log(b) from " + TABLE).build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because col b does not exist");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void flattenedFieldUnavailableTest() throws AuthorizationException {
    // GIVEN
    Mockito.when(metadataManagerMock.getCurrentTableMetadata(TABLE)).then(invocation -> {
      return new TableMetadata(TABLE, Arrays.asList(new FieldMetadata("a", FieldType.DOUBLE, false)));
    });

    // WHEN
    executionPlanBuilder.fromDiql("select b from flatten(" + TABLE + ", c[*])").build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because col c[*] does not exist");
  }

  @Test
  public void flattenedFieldAvailableTest() throws AuthorizationException {
    // GIVEN
    Mockito.when(metadataManagerMock.getCurrentTableMetadata(TABLE)).then(invocation -> {
      return new TableMetadata(TABLE, Arrays.asList(new FieldMetadata("a", FieldType.DOUBLE, true)));
    });

    // WHEN
    executionPlanBuilder.fromDiql("select b from flatten(" + TABLE + ", a[*])").build();

    // THEN
    // no exception
  }

  @Test(expectedExceptions = ValidationException.class)
  public void flattenedFieldNotRepeatedAvailableTest() throws AuthorizationException {
    // GIVEN
    Mockito.when(metadataManagerMock.getCurrentTableMetadata(TABLE)).then(invocation -> {
      return new TableMetadata(TABLE, Arrays.asList(new FieldMetadata("a", FieldType.DOUBLE, false)));
    });

    // WHEN
    executionPlanBuilder.fromDiql("select b from flatten(" + TABLE + ", a)").build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because col a is not repeated");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void flattenedFieldNotRepeatedButReferencedRepeatedAvailableTest() throws AuthorizationException {
    // GIVEN
    Mockito.when(metadataManagerMock.getCurrentTableMetadata(TABLE)).then(invocation -> {
      return new TableMetadata(TABLE, Arrays.asList(new FieldMetadata("a", FieldType.DOUBLE, false)));
    });

    // WHEN
    executionPlanBuilder.fromDiql("select b from flatten(" + TABLE + ", a[*])").build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because col a is not repeated");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void repeatedFieldReferencedUnrepeatedTest() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.DOUBLE, true));

    // WHEN
    executionPlanBuilder.fromDiql("select a from " + TABLE).build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because col a is repeated");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void unrepeatedFieldReferencedRepeatedTest() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.DOUBLE, false));

    // WHEN
    executionPlanBuilder.fromDiql("select avg(a[*]) from " + TABLE).build();

    // THEN
    Assert.fail("Expected a ValidationException to be thrown because col a is not repeated");
  }

  @Test
  public void validColAggregationTest() throws AuthorizationException {
    // GIVEN
    prepareMetadata(new FieldMetadata("a", FieldType.DOUBLE, true));

    // WHEN
    executionPlanBuilder.fromDiql("select avg(a[*]) from " + TABLE).build();

    // THEN
    // no excpetion
  }

  private void prepareMetadata(FieldMetadata... fields) throws AuthorizationException {
    Mockito.when(metadataManagerMock.getCurrentTableMetadata(Mockito.anyString())).then(invocation -> {
      return new TableMetadata(TABLE, Arrays.asList(fields));
    });
  }

  public static class DelegatingTableMetadataManager implements TableMetadataManager {
    private TableMetadataManager delegate;

    public TableMetadataManager getDelegate() {
      return delegate;
    }

    public void setDelegate(TableMetadataManager delegate) {
      this.delegate = delegate;
    }

    @Override
    public TableMetadata getCurrentTableMetadata(String tableName) throws AuthorizationException {
      return delegate.getCurrentTableMetadata(tableName);
    }

    @Override
    public void adjustTableMetadata(String tableName, Function<TableMetadata, TableMetadata> adjustFunction) {
      delegate.adjustTableMetadata(tableName, adjustFunction);
    }

    @Override
    public void startRecomputingTableMetadata(String tableName) {
      delegate.startRecomputingTableMetadata(tableName);
    }
  }

}
