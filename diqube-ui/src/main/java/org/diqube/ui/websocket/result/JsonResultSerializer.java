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
package org.diqube.ui.websocket.result;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import org.diqube.context.AutoInstatiate;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Serializes arbitrary {@link JsonResult} objects to JSON.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class JsonResultSerializer {
  private JsonFactory jsonFactory = new JsonFactory();
  private ObjectMapper mapper = new ObjectMapper(jsonFactory);

  /**
   * Encapsulate the given {@link JsonResult} with a {@link JsonResultEnvelope} and serialize it.
   * 
   * @param requestId
   *          The requestId the client provided when it sent the request (to which this is a result).
   * @param status
   *          One of {@link JsonResultEnvelope#STATUS_DATA}, {@link JsonResultEnvelope#STATUS_DONE} or
   *          {@link JsonResultEnvelope#STATUS_EXCEPTION} depending on what result you want to inform the client of.
   * @param payload
   *          the actual {@link JsonResult}. Can be <code>null</code> if status ==
   *          {@link JsonResultEnvelope#STATUS_DONE}.
   * @throws JsonPayloadSerializerException
   *           If anything goes wrong.
   */
  public String serializeWithEnvelope(String requestId, String status, JsonResult payload)
      throws JsonPayloadSerializerException {
    JsonResultDataType annotation = null;
    JsonNode payloadJson = null;

    if (payload != null) {
      annotation = payload.getClass().getAnnotation(JsonResultDataType.class);
      // serialize payload to tree node
      payloadJson = mapper.valueToTree(payload);
    }

    // serialize envelope to tree with inserted payload node
    JsonResultEnvelope resultEnvelope =
        new JsonResultEnvelope(requestId, status, (annotation != null) ? annotation.value() : "null");
    ObjectNode envelopeJson = mapper.valueToTree(resultEnvelope);

    if (payloadJson != null)
      envelopeJson.set(JsonResultEnvelope.PROPERTY_DATA, payloadJson);

    // serialize the envelope tree to a string.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamWriter osw = new OutputStreamWriter(baos, Charset.forName("UTF-8"));
    try {
      mapper.writeTree(jsonFactory.createGenerator(osw), envelopeJson);
      osw.close();

      return new String(baos.toByteArray(), Charset.forName("UTF-8"));
    } catch (IOException e) {
      throw new JsonPayloadSerializerException("Could not serialize result");
    }
  }

  public static class JsonPayloadSerializerException extends Exception {
    private static final long serialVersionUID = 1L;

    public JsonPayloadSerializerException(String msg) {
      super(msg);
    }
  }
}
