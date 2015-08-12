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
package org.diqube.ui.websocket.json;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;
import javax.websocket.Session;

import org.diqube.context.AutoInstatiate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ReflectionUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Deserializes arbitrary {@link JsonPayload} objects, including {@link JsonCommand}.
 * 
 * In addition to deserializing the {@link JsonPayload}, the {@link JsonPayload} class may contain fields that have both
 * annotationsL: {@link Inject} and {@link JsonIgnore}. In that case, beans matching that field type will be
 * automatically searched in the bean context and wired into those fields.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class JsonPayloadDeserializer {
  private static Map<String, Class<? extends JsonPayload>> payloadClasses;

  private static Logger logger = LoggerFactory.getLogger(JsonPayloadDeserializer.class);

  @Inject
  private ApplicationContext beanContext;

  private JsonFactory jsonFactory = new JsonFactory();
  private ObjectMapper mapper = new ObjectMapper(jsonFactory);

  /**
   * Deserialize an arbitrary {@link JsonPayload}
   * 
   * @param websocketSession
   *          Needed in case the json contains a {@link JsonCommand}, which will be initialized with the given session
   *          automatically.
   * @return the new object
   * @throws JsonPayloadDeserializerException
   *           if anything went wrong.
   */
  public JsonPayload deserialize(String clientJson, Session websocketSession) throws JsonPayloadDeserializerException {
    try {
      JsonParser parser = jsonFactory.createParser(clientJson);

      if (!parser.nextToken().equals(JsonToken.START_OBJECT))
        throw new JsonPayloadDeserializerException("Invalid JSON");
      if (!parser.nextToken().equals(JsonToken.FIELD_NAME))
        throw new JsonPayloadDeserializerException("Invalid JSON");
      String fieldName = parser.getCurrentName();
      if (!"type".equals(fieldName))
        throw new JsonPayloadDeserializerException("Invalid JSON");
      if (!parser.nextToken().equals(JsonToken.VALUE_STRING))
        throw new JsonPayloadDeserializerException("Invalid JSON");
      String jsonPayloadType = parser.getValueAsString();
      if (!parser.nextToken().equals(JsonToken.FIELD_NAME))
        throw new JsonPayloadDeserializerException("Invalid JSON");
      fieldName = parser.getCurrentName();
      if (!"data".equals(fieldName))
        throw new JsonPayloadDeserializerException("Invalid JSON");
      if (!parser.nextToken().equals(JsonToken.START_OBJECT))
        throw new JsonPayloadDeserializerException("Invalid JSON");

      JsonLocation paramObjectLocation = parser.getCurrentLocation();
      int lastBraces = clientJson.lastIndexOf('}');
      if (lastBraces == -1)
        throw new JsonPayloadDeserializerException("Invalid JSON");
      String dataValue = clientJson.substring((int) paramObjectLocation.getCharOffset() - 1, lastBraces);

      if (!payloadClasses.containsKey(jsonPayloadType))
        throw new JsonPayloadDeserializerException("Invalid JSON");

      JsonPayload payload = mapper.readValue(dataValue, payloadClasses.get(jsonPayloadType));

      if (payload instanceof JsonCommand) {
        ((JsonCommand) payload).setWebsocketSession(websocketSession);
      }

      wireInjectFields(payload);

      return payload;
    } catch (IOException e) {
      throw new JsonPayloadDeserializerException("Invalid JSON");
    }
  }

  private void wireInjectFields(Object o) {
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
  }

  public static class JsonPayloadDeserializerException extends Exception {
    private static final long serialVersionUID = 1L;

    public JsonPayloadDeserializerException(String msg) {
      super(msg);
    }
  }

  static {
    payloadClasses = new ConcurrentHashMap<>();
    payloadClasses.put(JsonQueryCommand.PAYLOAD_TYPE, JsonQueryCommand.class);
    payloadClasses.put(JsonExceptionPayload.PAYLOAD_TYPE, JsonExceptionPayload.class);
    payloadClasses.put(JsonQueryResultPayload.PAYLOAD_TYPE, JsonQueryResultPayload.class);
  }
}
