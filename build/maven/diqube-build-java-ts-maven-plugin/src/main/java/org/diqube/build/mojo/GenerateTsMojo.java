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
package org.diqube.build.mojo;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.diqube.util.Pair;
import org.diqube.util.TopologicalSort;
import org.sonatype.plexus.build.incremental.BuildContext;

import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.ClassPath;

/**
 * Generates a typescript file from java class files.
 * 
 * <p>
 * If this mojo should use input java classes of the current project where it is used, it needs to be executed in the
 * {@link LifecyclePhase#COMPILE}, because it relies on the classes being compiled already.
 * 
 * <p>
 * If this mojo should not use any java classes of the current project, it can safely be used in the
 * {@link LifecyclePhase#GENERATE_SOURCES}.
 * 
 * <p>
 * Obviously, be sure to execute the typescript compiler after this mojo, since the output typescript file should be
 * used as input for the typescript compiler.
 * 
 * <p>
 * The mojo will search for all classes in {@link GenerateTsMojo#rootPackages} where at least one property holds the
 * {@link TypeScriptProperty} annotation. For each of these classes and all classes referenced from those (which in turn
 * again have at least one property with {@link TypeScriptProperty}), one class in .ts will be created.
 */
@Mojo(name = "ts", defaultPhase = LifecyclePhase.COMPILE,
    requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class GenerateTsMojo extends AbstractMojo {

  /** Java classes transformed to a typescript "number" */
  private static final Set<Class<?>> NUMBER_CLASSES =
      new HashSet<>(Arrays.asList(Integer.TYPE, Integer.class, Long.TYPE, Long.class, Byte.TYPE, Byte.class, Short.TYPE,
          Short.class, Float.TYPE, Float.class, Double.TYPE, Double.class));

  /** Java classes transformed to a typescript "string" */
  private static final Set<Class<?>> STRING_CLASSES =
      new HashSet<>(Arrays.asList(String.class, Character.TYPE, Character.class));

  /** Java classes transformed to a typescript "boolean" */
  private static final Set<Class<?>> BOOLEAN_CLASSES = new HashSet<>(Arrays.asList(Boolean.TYPE, Boolean.class));

  /**
   * Classes we will not inspect.
   * 
   * The class objects are from the initial class loader, therefore this must only contain classes that are NOT present
   * in the topmost classloader returned by {@link #createClassLoaderWithCompiledProject()}.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static final Set<Class<?>> STOP_CLASSES = new HashSet(Arrays.asList(Iterables.toArray(
      Iterables.concat(NUMBER_CLASSES, STRING_CLASSES, BOOLEAN_CLASSES, Arrays.asList(Object.class)), Class.class)));

  @Component
  private MavenProject project;

  @Component
  private BuildContext buildContext;

  /**
   * Those packages that will be searched for {@link TypeScriptProperty} annotations first. All of the sub-packages will
   * be searched, too.
   */
  @Parameter(required = true)
  private String[] rootPackages;

  /**
   * Filename of the result .ts file.
   */
  @Parameter(required = true)
  private String resultFile;

  /**
   * Encoding used to write the output file.
   */
  @Parameter(defaultValue = "${project.build.sourceEncoding}")
  private String resultFileEncoding;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    ClassLoader ourClassLoader = createClassLoaderWithCompiledProject();
    ClassLoader oldContextClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(ourClassLoader);

    try {
      Map<String, Pair<Class<?>, Set<String>>> typescriptClasses = findClassesWithTypeScriptAnnotation(ourClassLoader);
      if (typescriptClasses == null)
        return;
      if (typescriptClasses.isEmpty()) {
        getLog().warn(
            "Found no classes where a property has the " + TypeScriptProperty.class.getSimpleName() + " annotation.");
        return;
      }

      List<String> sortedClassNames = topSortClassNames(typescriptClasses);
      getLog().debug("Topologically sorted classnames to be created: " + sortedClassNames);

      StringBuilder sb = new StringBuilder();
      for (String className : sortedClassNames) {
        String classString = generateTypeScript(typescriptClasses, className);
        sb.append(classString);
        sb.append("\n");
      }

      File outputFile = new File(resultFile);
      if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists())
        if (!outputFile.getParentFile().mkdirs())
          throw new MojoExecutionException(
              "Could not create directory " + outputFile.getParentFile().getAbsolutePath());

      byte[] newBytes;
      try {
        newBytes = sb.toString().getBytes(resultFileEncoding);
      } catch (UnsupportedEncodingException e) {
        throw new MojoExecutionException("Error serializing result", e);
      }

      if (outputFile.exists()) {
        try (FileInputStream fis = new FileInputStream(outputFile)) {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          ByteStreams.copy(fis, baos);

          if (Arrays.equals(baos.toByteArray(), newBytes)) {
            // file has exactly the same contents as our output. Do not re-write the output, as we would then change the
            // modification date and we might end up in an endless re-creation loop when using eclipse.
            getLog().info("Not rewriting output file, since it contains the correct contents already.");
            return;
          }
        } catch (IOException e) {
          // swallow, just re-write the file
          getLog().debug("Ignoring error while reading output file, will write results to the file!", e);
        }
      }

      try (OutputStream os = buildContext.newFileOutputStream(outputFile)) {
        os.write(newBytes);
      } catch (IOException e) {
        throw new MojoExecutionException("Could not write output file " + resultFile, e);
      }

      getLog().info("Generated " + sortedClassNames.size() + " interfaces to " + outputFile.getAbsolutePath());
    } finally {
      Thread.currentThread().setContextClassLoader(oldContextClassLoader);
    }
  }

  /**
   * Generates the .ts typescript for one specific java class.
   * 
   * @return String containing the typescript.
   */
  private String generateTypeScript(Map<String, Pair<Class<?>, Set<String>>> typescriptClasses, String className)
      throws MojoExecutionException {
    Class<?> clazz = typescriptClasses.get(className).getLeft();
    StringBuilder res = new StringBuilder();

    List<Pair<Field, TypeScriptProperty>> fields = findTypeScriptFields(clazz);

    if (fields.stream().anyMatch(p -> isPublicStaticFinal(p.getLeft()))) {
      // there is at least one public static final field annotated. Create "Constants" class.

      res.append("export class ");
      res.append(clazz.getSimpleName());
      res.append("Constants {\n");

      for (Pair<Field, TypeScriptProperty> p : fields) {
        Field f = p.getLeft();
        if (!isPublicStaticFinal(f))
          // only work on public static final fields.
          continue;

        String type = getTypescriptNativeType(f.getType());
        if (type == null)
          throw new MojoExecutionException(
              "Field " + className + "#" + f.getName() + " does not have a typescript native type.");

        res.append("  static ");
        res.append(f.getName());
        res.append(": ");
        res.append(type);
        res.append(" = ");

        Object value;
        try {
          value = f.get(null);
        } catch (IllegalArgumentException | IllegalAccessException e) {
          throw new MojoExecutionException(
              "Could not read value of public static final field " + className + "#" + f.getName(), e);
        }

        if (STRING_CLASSES.contains(f.getType())) {
          res.append("\"");
          res.append(((String) value));
          res.append("\"");
        } else if (BOOLEAN_CLASSES.contains(f.getType())) {
          res.append(((Boolean) value) ? "true" : "false");
        } else if (NUMBER_CLASSES.contains(f.getType())) {
          res.append(((Number) value).toString());
        } else
          throw new MojoExecutionException("Could not serialize value of " + className + "#" + f.getName());
        res.append(";\n");
      }
      res.append("}\n");
    }

    res.append("export interface ");
    res.append(clazz.getSimpleName());
    if (typescriptClasses.containsKey(clazz.getSuperclass().getName())) {
      res.append(" extends ");
      res.append(clazz.getSuperclass().getSimpleName());
    }
    res.append(" {\n");

    for (Pair<Field, TypeScriptProperty> p : fields) {
      Field f = p.getLeft();
      if (isPublicStaticFinal(f))
        // do not create anything in the interface for public static final.
        continue;
      TypeScriptProperty annotation = p.getRight();
      String type = getTypescriptType(typescriptClasses, className, f.getGenericType());
      res.append("  ");
      res.append(f.getName());
      if (annotation.optional())
        res.append("?");
      res.append(": ");
      res.append(type);
      res.append(";\n");
    }
    res.append("}\n");

    return res.toString();
  }

  /**
   * Calculates the string identifying the typescript type of a specific field.
   * 
   * @param typescriptClasses
   *          All class infos.
   * @param className
   *          The name of the class the field belongs to.
   * @param type
   *          The {@link Field#getGenericType()} of the field whose type should be turned into a typescript definition.
   * @return A String which is a valid typescript type definition.
   */
  private String getTypescriptType(Map<String, Pair<Class<?>, Set<String>>> typescriptClasses, String fieldName,
      Type type) throws MojoExecutionException {
    if (type instanceof ParameterizedType) {
      ParameterizedType pType = (ParameterizedType) type;
      Type rawType = pType.getRawType();
      Class<?> rawClass = (Class<?>) rawType;
      if (Collection.class.isAssignableFrom(rawClass)) {
        if (pType.getActualTypeArguments().length != 1)
          throw new MojoExecutionException("Unexpected number of type parameters while looking into " + fieldName);

        String detailTsType = getTypescriptType(typescriptClasses, fieldName, pType.getActualTypeArguments()[0]);

        return "Array<" + detailTsType + ">";
      } else if (Map.class.isAssignableFrom(rawClass)) {
        if (pType.getActualTypeArguments().length != 2)
          throw new MojoExecutionException("Unexpected number of type parameters while looking into " + fieldName);

        String keyTsType = getTypescriptType(typescriptClasses, fieldName, pType.getActualTypeArguments()[0]);
        String valueTsType = getTypescriptType(typescriptClasses, fieldName, pType.getActualTypeArguments()[1]);

        return "{ [ key: " + keyTsType + "]: " + valueTsType + " }";
      }

      if (typescriptClasses.containsKey(rawClass.getName()))
        return rawClass.getSimpleName();

      getLog().warn("Field " + fieldName + " has a parameterized type that is not supported/not fully specified "
          + "by classes with " + TypeScriptProperty.class.getSimpleName() + " annotations. Using 'any'.");
      return "any";
    } else if (type instanceof Class) {
      Class<?> clazz = (Class<?>) type;
      String nativeType = getTypescriptNativeType(clazz);
      if (nativeType != null)
        return nativeType;
      if (typescriptClasses.containsKey(clazz.getName()))
        return clazz.getSimpleName();

      getLog().warn("Field " + fieldName + " has a type that is not fully specified by classes with "
          + TypeScriptProperty.class.getSimpleName() + " annotations. Using 'any'.");
      return "any";
    } else
      throw new MojoExecutionException("Could not identify type while looking into " + fieldName);
  }

  /**
   * Return all classes referenced by the generic type.
   */
  private Set<Class<?>> getAllReferencedClasses(Type type) {
    Set<Class<?>> res = new HashSet<>();

    if (type instanceof ParameterizedType) {
      res.add((Class<?>) ((ParameterizedType) type).getRawType());
      for (Type t : ((ParameterizedType) type).getActualTypeArguments())
        res.addAll(getAllReferencedClasses(t));
    } else
      res.add((Class<?>) type);

    return res;
  }

  /**
   * Identifies if the clazz (= the type of a property) corresponds directly to a Typescript native data type and
   * returns that. <code>null</code> if not.
   */
  private String getTypescriptNativeType(Class<?> clazz) {
    if (NUMBER_CLASSES.contains(clazz))
      return "number";
    if (STRING_CLASSES.contains(clazz))
      return "string";
    if (BOOLEAN_CLASSES.contains(clazz))
      return "boolean";
    return null;
  }

  /**
   * Checks whether the field is defined "public static final".
   */
  private boolean isPublicStaticFinal(Field f) {
    return Modifier.isPublic(f.getModifiers()) && Modifier.isStatic(f.getModifiers())
        && Modifier.isFinal(f.getModifiers());
  }

  /**
   * Scan classpath and identify and load all transitive classes that have a {@link TypeScriptProperty} annotation on
   * one of their properties.
   * 
   * @return null if no classes were scanned. Otherwise map from class name to pair of loaded class and list of other
   *         class names the class depends on (only the ones whose classname is present as key in the map!).
   */
  private Map<String, Pair<Class<?>, Set<String>>> findClassesWithTypeScriptAnnotation(ClassLoader ourClassLoader)
      throws MojoExecutionException {
    ClassPath classPath;
    try {
      classPath = ClassPath.from(ourClassLoader);
    } catch (IOException e) {
      throw new MojoExecutionException("Could not scan classpath", e);
    }
    Deque<String> classNamesToVisit = new LinkedList<>();
    for (String pkg : rootPackages)
      classNamesToVisit.addAll(classPath.getTopLevelClassesRecursive(pkg).stream().map(classInfo -> classInfo.getName())
          .collect(Collectors.toList()));

    if (classNamesToVisit.isEmpty()) {
      getLog().info("No classes found.");
      return null;
    }

    getLog().debug("Found following classes in first scan: " + classNamesToVisit);

    Map<String, Pair<Class<?>, Set<String>>> typescriptClasses = new HashMap<>();
    Set<String> classesVisited = new HashSet<>();
    while (!classNamesToVisit.isEmpty()) {
      String className = classNamesToVisit.pop();
      if (!classesVisited.add(className))
        continue;

      Class<?> clazz;
      try {
        clazz = ourClassLoader.loadClass(className);
      } catch (ClassNotFoundException e) {
        getLog().warn("Could not load class " + className);
        continue;
      }

      // check valid, since the ourClassLoader must not overwrite the STOP_CLASSES.
      if (STOP_CLASSES.contains(clazz))
        continue;

      for (Pair<Field, TypeScriptProperty> p : findTypeScriptFields(clazz)) {
        Field f = p.getLeft();

        typescriptClasses.putIfAbsent(clazz.getName(), new Pair<>(clazz, new HashSet<>()));
        Set<String> dependsSet = typescriptClasses.get(clazz.getName()).getRight();

        // check valid, since the ourClassLoader must not overwrite the STOP_CLASSES.
        if (!STOP_CLASSES.contains(f.getType())) {
          Set<Class<?>> allReferencedClasses = getAllReferencedClasses(f.getGenericType());
          Set<String> allReferencedClassNames =
              allReferencedClasses.stream().map(c -> c.getName()).collect(Collectors.toSet());
          classNamesToVisit.addAll(allReferencedClassNames);
          dependsSet.addAll(allReferencedClassNames);
        }
        classNamesToVisit.add(clazz.getSuperclass().getName());
        dependsSet.add(clazz.getSuperclass().getName());
      }
    }

    // cleanup result map, remove all the "depends on" classes that did not contain the annotation
    for (Pair<Class<?>, Set<String>> p : typescriptClasses.values()) {
      p.getRight().retainAll(typescriptClasses.keySet());
    }

    return typescriptClasses;
  }

  private List<Pair<Field, TypeScriptProperty>> findTypeScriptFields(Class<?> clazz) {
    List<Pair<Field, TypeScriptProperty>> res = new ArrayList<>();
    for (Field f : clazz.getDeclaredFields()) {
      if (f.isAnnotationPresent(TypeScriptProperty.class))
        res.add(new Pair<>(f, f.getAnnotation(TypeScriptProperty.class)));
    }
    return res;
  }

  private List<String> topSortClassNames(Map<String, Pair<Class<?>, Set<String>>> typescriptClasses) {
    List<String> classNames = new ArrayList<>(typescriptClasses.keySet());
    // sort before doing topsort, then topsort will output the same order each time with the same input. If based on
    // ordering of keyset, we can't be sure.
    classNames.sort(Comparator.naturalOrder());

    Map<String, List<String>> successors = new HashMap<>();
    for (String className : classNames) {
      getLog().debug("Class " + className + " depends on " + typescriptClasses.get(className).getRight());
      for (String dependsOn : typescriptClasses.get(className).getRight()) {
        successors.putIfAbsent(dependsOn, new ArrayList<>());
        successors.get(dependsOn).add(className);
      }
      successors.putIfAbsent(className, new ArrayList<>());
    }

    getLog().debug("TopSort successors: " + successors);

    TopologicalSort<String> topSort = new TopologicalSort<>(className -> successors.get(className),
        className -> (long) classNames.indexOf(className), null);

    return topSort.sort(classNames);
  }

  /**
   * Creates and returns a {@link ClassLoader} that loads data from the compiled project sources and the dependencies of
   * the project. THe parent of the returned classLoader is this' object classloader.
   * 
   * <p>
   * We expect that the project does not itself implement {@link Collection}, {@link Map}, {@link Object} or any class
   * in {@link #STOP_CLASSES}, or that any transitive dependency of it does.
   */
  private ClassLoader createClassLoaderWithCompiledProject() throws MojoFailureException {
    try {
      List<String> curProjectClasspath = project.getRuntimeClasspathElements();
      List<String> curProjectCompileClasspath = project.getCompileClasspathElements();

      Set<String> usedClasspath = new HashSet<>(curProjectClasspath);
      for (String cp : curProjectCompileClasspath)
        if (!usedClasspath.contains(cp))
          curProjectClasspath.add(cp);

      getLog().debug("Using following classpath elements: " + curProjectClasspath);
      URL[] thisProjectClasspathUrls = new URL[curProjectClasspath.size()];
      for (int i = 0; i < curProjectClasspath.size(); i++) {
        thisProjectClasspathUrls[i] = new File(curProjectClasspath.get(i)).toURI().toURL();
      }
      ClassLoader ourClassLoader = new URLClassLoader(thisProjectClasspathUrls, this.getClass().getClassLoader());
      return ourClassLoader;
    } catch (DependencyResolutionRequiredException | MalformedURLException e) {
      throw new MojoFailureException("Could not create classloader", e);
    }
  }
}