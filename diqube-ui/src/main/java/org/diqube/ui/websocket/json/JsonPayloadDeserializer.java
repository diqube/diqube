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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.websocket.Session;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Deserializes arbitrary {@link JsonPayload} objects, including {@link JsonCommand}.
 *
 * @author Bastian Gloeckle
 */
public class JsonPayloadDeserializer {
  private static Map<String, Class<? extends JsonPayload>> payloadClasses;

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

      if (payload instanceof JsonCommand)
        ((JsonCommand) payload).setWebsocketSession(websocketSession);

      return payload;
    } catch (IOException e) {
      throw new JsonPayloadDeserializerException("Invalid JSON");
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
    payloadClasses.put(JsonQueryExceptionPayload.PAYLOAD_TYPE, JsonQueryExceptionPayload.class);
    payloadClasses.put(JsonQueryResultPayload.PAYLOAD_TYPE, JsonQueryResultPayload.class);
  }
}
