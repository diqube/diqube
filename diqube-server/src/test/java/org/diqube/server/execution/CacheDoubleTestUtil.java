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
package org.diqube.server.execution;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.diqube.execution.cache.ColumnShardCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;

/**
 * Utility to execute test methods of a test twice by only using a single bean context.
 * 
 * <p>
 * This is meaningful to Test the {@link ColumnShardCache} - the first execution of a test method fills the cache, the
 * second is then executed with intermediate results taken from the cache.
 * 
 * <p>
 * Use this utility in a method annotated with TestNGs {@link Factory} annotation.
 * 
 * <p>
 * The test classes that are generated dynamically will have a name that ends with {@link #CACHE_DOUBLE_IDENTIFIER}.
 *
 * @author Bastian Gloeckle
 */
public class CacheDoubleTestUtil {

  /** Identifier for dynamically created classes and methods (suffix). */
  private static final String CACHE_DOUBLE_IDENTIFIER = "CacheDouble";

  /**
   * Creates a object of a dynamically created class for each test method.
   * 
   * When executing the tests on the returned objects, this will result in the test methods being called twice:
   * 
   * <pre>
   * {@link AbstractDiqlExecutionTest#setUp()};
   * testMethod();
   * testMethod();
   * {@link AbstractDiqlExecutionTest#cleanup()};
   * </pre>
   * 
   * @param baseTest
   *          The test class object which defines the test methods.
   * @return For each test method a object which will, when executed by TestNG execute that test method twice.
   */
  public static Object[] createTestObjects(AbstractDiqlExecutionTest<?> baseTest) {
    List<Object> res = new ArrayList<>();
    for (Method m : baseTest.getClass().getMethods()) {
      if (m.isAnnotationPresent(Test.class) && !m.isAnnotationPresent(IgnoreInCacheDoubleTestUtil.class)) {
        res.add(createTestClassObject(baseTest, m));
      }
    }
    return res.toArray();
  }

  /**
   * Creates a test class dynamically for a single test method and returns an instance thereof.
   * 
   * @param baseTest
   * @param testMethod
   * @return
   */
  private static Object createTestClassObject(AbstractDiqlExecutionTest<?> baseTest, Method testMethod) {
    // define the method that the returned class should have
    MethodDescription.Latent testMethodDescription = new MethodDescription.Latent( //
        testMethod.getName() + CACHE_DOUBLE_IDENTIFIER, //
        null, // not important, as this is ignored by ByteBuddy (defineMethod(.) call below).
        TypeDescription.VOID, //
        new ArrayList<>(), //
        Modifier.PUBLIC, //
        new ArrayList<>());

    Test testAnnotation = testMethod.getAnnotation(Test.class);
    String newClassName = baseTest.getClass().getName() + CACHE_DOUBLE_IDENTIFIER;

    Class<?> testClass = new ByteBuddy() //
        .subclass(Object.class) //
        .name(newClassName) // class name
        .defineMethod(testMethodDescription) // define our method
        // intercept calls of our method and call the method in CacheDoubleTestInterceptor instead.
        .intercept(MethodDelegation.to(new CacheDoubleTestInterceptor(baseTest, testMethod, newClassName))) //
        // let the new method have a Test annotation - use the original annotation to preserve all annotation properties
        .annotateMethod(testAnnotation) //
        .make() // make class
        .load(CacheDoubleTestUtil.class.getClassLoader(), ClassLoadingStrategy.Default.WRAPPER) //
        .getLoaded();
    try {
      return testClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Cannot instantiate dynamic test class", e);
    }
  }

  /**
   * Intercepts calls to the method of the dynamically created test classes.
   */
  protected static class CacheDoubleTestInterceptor {
    private Logger logger;

    private AbstractDiqlExecutionTest<?> baseTest;
    private Method testMethod;

    public CacheDoubleTestInterceptor(AbstractDiqlExecutionTest<?> baseTest, Method testMethod, String loggerName) {
      this.baseTest = baseTest;
      this.testMethod = testMethod;
      this.logger = LoggerFactory.getLogger(loggerName);
    }

    /**
     * This method will be executed when the {@link Test}-annotated method of a dynamically created test class is
     * called.
     * 
     * This implements the logic of the test.
     * 
     * Take care when changing the signature of this method that ByteBuddy keeps accepting it as target method of our
     * interception.
     */
    public void testImplementationIntercept() throws Throwable {
      try {
        baseTest.setUp();
        try {
          logger.info("Executing test method first time: {} (see {} for details)", testMethod,
              CacheDoubleTestUtil.class.getName());
          testMethod.invoke(baseTest);

          logger.info(
              "Executing test method second time (this time there should be something in the cache): {} (see {} for details)",
              testMethod, CacheDoubleTestUtil.class.getName());
          testMethod.invoke(baseTest);
        } catch (InvocationTargetException e) {
          logger.error("Exception while double-executing test method {}: {}. Test executed by {}.",
              testMethod.toString(), e.getTargetException().toString(), CacheDoubleTestUtil.class.getName(), e);
          // re-throw inner exception. The test could expect a specific exception to be thrown - take care of that!
          throw e.getTargetException();
        } finally {
          baseTest.cleanup();
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Cannot invoke test method. Test executed by " + CacheDoubleTestUtil.class.getName(),
            e);
      }
    }
  }

  /**
   * Annotate a {@link Test} method with this annotation to <b>NOT</b> let {@link CacheDoubleTestUtil} create a
   * double-test for this method!
   */
  @Target(value = ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  public static @interface IgnoreInCacheDoubleTestUtil {

  }
}
