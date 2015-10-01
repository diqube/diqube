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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;

import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.JsonResult;
import org.diqube.ui.websocket.result.JsonResultDataType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

/**
 * Validates data objects that are present in JavaScript test-cases to be valid {@link JsonResult}s and
 * {@link JsonCommand}s.
 *
 * @author Bastian Gloeckle
 */
@SuppressWarnings("restriction") // ScriptObjectMirror is public Nashorn API, but eclipse thinks it's restricted.
public class JsonResultJavaScriptDataValidator implements JavaScriptDataValidator {
  private static final Map<String, Class<? extends JsonResult>> dataTypeToClass = new HashMap<>();
  private static final Map<String, Class<? extends JsonCommand>> commandNameToClass = new HashMap<>();
  private static final ObjectMapper mapper = new ObjectMapper();

  private static final ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();

  private String fileName;

  /**
   * @param fileName
   *          JavaScript filename to make it easier to understand what went wrong in case of an error.
   */
  public JsonResultJavaScriptDataValidator(String fileName) {
    this.fileName = fileName;
  }

  @Override
  public String data(String dataName, ScriptObjectMirror origValue) {
    Class<? extends JsonResult> deserializationResultClass = dataTypeToClass.get(dataName);

    if (deserializationResultClass == null)
      throw new RuntimeException("dataName '" + dataName + "' unknown in file '" + fileName + "'");

    validate(dataName, deserializationResultClass, origValue);

    // there was no exception, data object is valid! wohoo!
    return null;
  }

  @Override
  public String commandData(String commandName, ScriptObjectMirror origValue) {
    Class<? extends JsonCommand> deserializationCommandClass = commandNameToClass.get(commandName);

    if (deserializationCommandClass == null)
      throw new RuntimeException("Command '" + commandName + "' unknown in file '" + fileName + "'");

    validate(commandName, deserializationCommandClass, origValue);

    // there was no exception, data object is valid! wohoo!
    return null;
  }

  private void validate(String desc, Class<?> targetClass, ScriptObjectMirror origValue) throws RuntimeException {
    Object value = clean(origValue);
    if (!(value instanceof Map))
      throw new RuntimeException("Data type " + value.getClass().getName() + " not supported.");

    // convert map to JsonNode
    JsonNode node = mapper.convertValue(value, JsonNode.class);

    try {
      // try to deserialize JsonCommand to actual object, this will throw an exception if data is invalid.
      Object obj = mapper.treeToValue(node, targetClass);

      // And to be absolutely sure, we validate the resulting object using a JSR-349 validator.
      Set<ConstraintViolation<Object>> violations = validatorFactory.getValidator().validate(obj);

      if (!violations.isEmpty())
        throw new RuntimeException("Constraint violations for '" + desc + "' in file '" + fileName + "': "
            + violations.stream().map(violation -> violation.getPropertyPath() + " " + violation.getMessage())
                .collect(Collectors.toList()));

    } catch (JsonProcessingException e) {
      throw new RuntimeException("Invalid object for '" + desc + "' in file '" + fileName + "': " + e.getMessage(), e);
    }

  }

  /**
   * Cleans the {@link ScriptObjectMirror} object and returns a clean {@link Map} or {@link List} of objects. Recursive.
   * 
   * <p>
   * The JS objects are encapsulated and the Jackson deserialization might return for example a {"0": "abc", "1":"def"}
   * {@link JsonNode} for a JS object which is actually an array: ["abc", "def"]. Therefore we "clean" that data here
   * and identify arrays and return clean {@link List}s for them.
   */
  private Object clean(ScriptObjectMirror jsObject) {
    if (jsObject.isArray()) {
      List<Object> res = new ArrayList<>();
      for (Object value : jsObject.values()) {
        if (value instanceof ScriptObjectMirror)
          res.add(clean((ScriptObjectMirror) value));
        else
          res.add(value);
      }
      return res;
    } else {
      Map<String, Object> res = new HashMap<>();
      for (Entry<String, Object> jsEntry : jsObject.entrySet()) {
        Object value;
        if (jsEntry.getValue() instanceof ScriptObjectMirror)
          value = clean((ScriptObjectMirror) jsEntry.getValue());
        else
          value = jsEntry.getValue();

        res.put(jsEntry.getKey(), value);
      }

      return res;
    }
  }

  /** initialization logic: load all JsonResults. */
  static {
    Set<ClassInfo> classInfos;
    try {
      classInfos = ClassPath.from(JsonResultJavaScriptDataValidator.class.getClassLoader())
          .getTopLevelClassesRecursive("org.diqube.ui");
    } catch (IOException e) {
      throw new RuntimeException("Coluld not load class infos.", e);
    }

    for (ClassInfo classInfo : classInfos) {
      Class<?> clazz = classInfo.load();
      if (clazz.getAnnotation(JsonResultDataType.class) != null && JsonResult.class.isAssignableFrom(clazz)) {
        String dataType = clazz.getAnnotation(JsonResultDataType.class).value();
        @SuppressWarnings("unchecked")
        Class<? extends JsonResult> jsonResultClazz = (Class<? extends JsonResult>) clazz;
        dataTypeToClass.put(dataType, jsonResultClazz);
      } else if (clazz.getAnnotation(CommandInformation.class) != null && JsonCommand.class.isAssignableFrom(clazz)) {
        String commandName = clazz.getAnnotation(CommandInformation.class).name();
        @SuppressWarnings("unchecked")
        Class<? extends JsonCommand> commandClazz = (Class<? extends JsonCommand>) clazz;
        commandNameToClass.put(commandName, commandClazz);
      }
    }
  }

}
