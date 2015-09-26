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
package org.diqube.ui.websocket.request;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.websocket.Session;

import org.diqube.context.AutoInstatiate;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.JsonResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ReflectionUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

/**
 * Deserializes arbitrary {@link JsonRequest} objects, including their {@link JsonCommand}s.
 * 
 * <p>
 * In addition to deserializing the {@link JsonResult} from JSON and the {@link JsonCommand} from JSON, the
 * {@link JsonCommand} may contain additional fields that have both annotations: {@link Inject} and {@link JsonIgnore}.
 * These fields are then wired to instances of matching types from the bean context.
 * 
 * <p>
 * If the command has a method annotated with {@link PostConstruct}, it will be called accordingly.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class JsonRequestDeserializer {
  private static Logger logger = LoggerFactory.getLogger(JsonRequestDeserializer.class);
  private static final String JSON_REQUEST_ID = "requestId";
  private static final String JSON_COMMAND_NAME = "command";
  private static final String JSON_COMMAND_DATA = "commandData";

  private Map<String, Class<? extends JsonCommand>> commandClasses;

  @Inject
  private ApplicationContext beanContext;

  @Inject
  private JsonRequestRegistry jsonRequestRegistry;

  private JsonFactory jsonFactory = new JsonFactory();
  private ObjectMapper mapper = new ObjectMapper(jsonFactory);

  /**
   * Deserialize an arbitrary {@link JsonRequest}
   * 
   * @param clientJson
   *          The JSON provided by the client which contains information about the request to be built.
   * @param websocketSession
   *          the {@link Session} to which the request belongs.
   * @return the new object
   * @throws JsonPayloadDeserializerException
   *           if anything went wrong.
   */
  public JsonRequest deserialize(String clientJson, Session websocketSession) throws JsonPayloadDeserializerException {
    try {
      JsonNode requestTreeRoot = mapper.readTree(clientJson);
      String requestId = requestTreeRoot.get(JSON_REQUEST_ID).textValue();
      String commandName = requestTreeRoot.get(JSON_COMMAND_NAME).textValue();

      if (!commandClasses.containsKey(commandName))
        throw new JsonPayloadDeserializerException("Unknown command: " + commandName);

      Class<? extends JsonCommand> cmdClass = commandClasses.get(commandName);

      JsonCommand cmd;
      if (requestTreeRoot.get(JSON_COMMAND_DATA) != null && !requestTreeRoot.get(JSON_COMMAND_DATA).isNull())
        cmd = mapper.treeToValue(requestTreeRoot.get(JSON_COMMAND_DATA), cmdClass);
      else
        try {
          cmd = cmdClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          logger.error("Could not instantiate command class", e);
          throw new JsonPayloadDeserializerException("Could not instantiate command class");
        }

      wireInjectFieldsAndCallPostConstruct(cmd);
      JsonRequest request = new JsonRequest(websocketSession, requestId, cmd, jsonRequestRegistry);
      wireInjectFieldsAndCallPostConstruct(request);

      return request;
    } catch (IOException e) {
      throw new JsonPayloadDeserializerException("Invalid JSON");
    }
  }

  private void wireInjectFieldsAndCallPostConstruct(Object o) throws JsonPayloadDeserializerException {
    for (Field f : o.getClass().getDeclaredFields()) {
      Inject[] injects = f.getAnnotationsByType(Inject.class);
      JsonIgnore[] jsonIgnores = f.getAnnotationsByType(JsonIgnore.class);
      if (injects.length > 0 && jsonIgnores.length > 0) {
        try {
          Object value = beanContext.getBean(f.getType());
          ReflectionUtils.makeAccessible(f);
          try {
            f.set(o, value);
            logger.trace("Wired object to {}#{}", o.getClass().getName(), f.getName());
          } catch (IllegalArgumentException | IllegalAccessException e) {
            logger.debug("Could not wire object to {}#{}", o.getClass(), f.getName(), e);
          }
        } catch (NoSuchBeanDefinitionException e) {
          logger.debug("Not wiring object to {}#{} because no corresponding bean available", o.getClass(), f.getName());
        }
      }
    }

    for (Method m : o.getClass().getMethods()) {
      if (Modifier.isPublic(m.getModifiers()) && m.isAnnotationPresent(PostConstruct.class)) {
        if (m.getParameterCount() == 0 && m.getReturnType().equals(Void.TYPE)) {
          try {
            m.invoke(o);
          } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new JsonPayloadDeserializerException("Could not invoke PostConstruct.", e);
          }
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  @PostConstruct
  public void initialize() throws IOException {
    commandClasses = new HashMap<>();
    Set<ClassInfo> classInfos =
        ClassPath.from(getClass().getClassLoader()).getTopLevelClassesRecursive("org.diqube.ui");
    for (ClassInfo classInfo : classInfos) {
      Class<?> clazz = classInfo.load();
      if (clazz.isAnnotationPresent(CommandInformation.class) && JsonCommand.class.isAssignableFrom(clazz)) {
        CommandInformation annotation = clazz.getAnnotation(CommandInformation.class);

        commandClasses.put(annotation.name(), (Class<? extends JsonCommand>) clazz);
      }
    }
  }

  public static class JsonPayloadDeserializerException extends Exception {
    private static final long serialVersionUID = 1L;

    public JsonPayloadDeserializerException(String msg) {
      super(msg);
    }

    public JsonPayloadDeserializerException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}