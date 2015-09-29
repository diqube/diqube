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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.diqube.ui.websocket.result.JsonResult;
import org.diqube.ui.websocket.result.JsonResultDataType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

/**
 * Validates data objects that are present in JavaScript test-cases to be valid {@link JsonResult}s.
 *
 * @author Bastian Gloeckle
 */
public class JsonResultJavaScriptDataValidator implements JavaScriptDataValidator {
  private static final Map<String, Class<? extends JsonResult>> dataTypeToClass = new HashMap<>();
  private static final ObjectMapper mapper = new ObjectMapper();

  private String fileName;

  /**
   * @param fileName
   *          JavaScript filename to make it easier to understand what went wrong in case of an error.
   */
  public JsonResultJavaScriptDataValidator(String fileName) {
    this.fileName = fileName;
  }

  @Override
  public String data(String dataName, Map<String, Object> values) {
    Class<? extends JsonResult> deserializationResultClass = dataTypeToClass.get(dataName);

    if (deserializationResultClass == null)
      throw new RuntimeException("dataName '" + dataName + "' unknown in file '" + fileName + "'");

    // convert map to JsonNode
    JsonNode node = mapper.convertValue(values, JsonNode.class);

    try {
      // try to deserialize JsonNode to actual object, this will throw an exception if data is invalid.
      mapper.treeToValue(node, dataTypeToClass.get(dataName));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          "Invalid object for type '" + dataName + "' in file '" + fileName + "': " + e.getMessage(), e);
    }

    // there was no exception, data object is valid! wohoo!
    return null;
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
      }
    }
  }
}
