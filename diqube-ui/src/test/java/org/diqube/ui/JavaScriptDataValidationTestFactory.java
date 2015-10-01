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
package org.diqube.ui;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.diqube.ui.websocket.result.JsonResultDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.google.common.base.CaseFormat;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ResourceInfo;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;

/**
 * Provides Test classes that test correct data objects in JavaScript tests.
 * 
 * <p>
 * This is a TestNG {@link Factory} and will therefore provide instances of other classes which define test methods.
 * 
 * <p>
 * The implementation of this class creates new classes dynamically using byte buddy: For each JavaScript test file
 * (*.test.js) there will be one class created with one testMethod that tests all calls to
 * 
 * <pre>
 * validatedData.data(dataType, object)
 * </pre>
 * 
 * The JavaScript tests should call the method above with (1) the {@link JsonResultDataType} against which the (2)
 * object should be validated. This call will be redirected by the dynamically created classes to
 * {@link JavaScriptDataValidator} / {@link JsonResultJavaScriptDataValidator} which will in turn throw an exception if
 * the object is not valid (see {@link JavaScriptDataValidator#data(String, java.util.Map)}.
 * 
 * <p>
 * The same "data" method will be bound to another method when the JavaScript tests are actually executed in karma (=the
 * JavaScript test logic is executed). This means that each .test.js file will actually be executed twice: First by this
 * test (which validates that the data objects are valid) and then after that by karma in a pure JavaScript environment
 * where the (validated) test objects will be used to actually test JavaScript code.
 * 
 * <p>
 * See also README.md in src/test/js.
 *
 * @author Bastian Gloeckle
 */
public class JavaScriptDataValidationTestFactory {
  private static final Logger logger = LoggerFactory.getLogger(JavaScriptDataValidationTestFactory.class);

  private static final String JS_TEST_FILE_SUFFIX = ".test.js";

  private static final String JS_VALIDATED_DATA_OBJ = "validatedData";

  @Factory
  public Object[] javaScriptDataValidationTests() throws IOException {
    List<Object> res = new ArrayList<>();
    Set<ResourceInfo> resourceInfos = ClassPath.from(this.getClass().getClassLoader()).getResources();
    for (ResourceInfo info : resourceInfos) {
      if (info.getResourceName().endsWith(JS_TEST_FILE_SUFFIX)) {
        res.add(createTestClass(info.getResourceName()));
      }
    }

    return res.toArray();
  }

  /**
   * Create an object of a new dynamic test class which will test the data types defined in the given resource.
   * 
   * @param resourceName
   *          The name of the resouirce with which it can be loaded from the class loader.
   * @return An instance of a class that has 1 {@link Test}-annotated method that will execute the test.
   */
  private Object createTestClass(String resourceName) {
    MethodDescription.Latent testMethodDescription = new MethodDescription.Latent( //
        "javaScriptDataValidationTest", // test method name
        null, // not important, as this is ignored by ByteBuddy (defineMethod(.) call below).
        TypeDescription.VOID, //
        new ArrayList<>(), //
        Modifier.PUBLIC, //
        new ArrayList<>());

    String classNameSuffix = resourceName.replaceAll("\\..*", ""); // remove dot and everything after that
    classNameSuffix = classNameSuffix.replaceAll(".*/", ""); // remove everything until last /
    // the name might still contain "-" as we use that in the names of our test files. Convert to CamelCase.
    classNameSuffix = CaseFormat.LOWER_HYPHEN.to(CaseFormat.UPPER_CAMEL, classNameSuffix);
    String newClassName = JavaScriptDataValidationTestFactory.class.getPackage().getName()
        + ".JavaScriptDataValidationTest" + classNameSuffix;

    Class<?> testClass = new ByteBuddy() //
        .subclass(Object.class) //
        .name(newClassName) // class name
        .defineMethod(testMethodDescription) // define our method
        // intercept calls of our method and call the method in CacheDoubleTestInterceptor instead.
        .intercept(MethodDelegation.to(new JavaScroptDataValidationInterceptor(resourceName))) //
        // let the new method have a Test annotation
        .annotateMethod(AnnotationDescription.Builder.forType(Test.class).make()) //
        .make() // make class
        .load(this.getClass().getClassLoader(), ClassLoadingStrategy.Default.WRAPPER) //
        .getLoaded();

    try {
      return testClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Could not create instance of dynamic test class", e);
    }
  }

  /**
   * Interceptor class for calls to the test method of dynamically created classes.
   */
  public static class JavaScroptDataValidationInterceptor {
    private String jsResourceName;

    public JavaScroptDataValidationInterceptor(String jsResourceName) {
      this.jsResourceName = jsResourceName;
    }

    /**
     * This is the test method that will be called on the interceptor when the actual test is executed. Basically this
     * is the generic implementation of the test method.
     * 
     * Be very careful when changing this that ByteBuddy will still use this method as the target method of intercepted
     * calls!
     */
    public void testMethod() throws Throwable {
      logger.info(
          "Executing test for validity of data object that is specified in JavaScript test file '{}'. This "
              + "test is run from a dynamically created class. For the implementation, see {}",
          jsResourceName, JavaScriptDataValidationTestFactory.class);

      try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(jsResourceName)) {
        Reader jsReader = new InputStreamReader(is, Charset.forName("UTF-8"));

        ScriptEngine jsEngine = new ScriptEngineManager().getEngineByName("nashorn");
        jsEngine.eval("function describe() { };"); // ignore calls to "describe"
        jsEngine.eval(
            "var ValidatedDataGenerator = Java.type('" + JsonResultJavaScriptDataValidator.class.getName() + "');");
        jsEngine.eval("var " + JS_VALIDATED_DATA_OBJ + " = new ValidatedDataGenerator('" + jsResourceName + "');");

        // If the jsResource specified an access to validatedData, that call will be sent to
        // JsonResultJavaScriptDataValidator, which in turn will throw an exception if an object is invalid.
        jsEngine.eval(jsReader);
      }
    }
  }
}
