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

import java.util.Map;

import org.diqube.ui.websocket.result.JsonResult;
import org.diqube.ui.websocket.result.JsonResultDataType;

/**
 * Implementing classes can be called from JavaScript tests in order to validate that a specific JavaScript object
 * matches a valid {@link JsonResult}.
 * 
 * <p>
 * Note that this interface will be implemented differently in pure JavaScript when executing the JavaScript tests using
 * karma (see README.md in src/test/js). This class is used in a preliminary run to validate that the test classes use
 * valid data.
 *
 * @author Bastian Gloeckle
 */
public interface JavaScriptDataValidator {

  /**
   * Validates that data is a valid {@link JsonResult}.
   * 
   * @param dataType
   *          The {@link JsonResultDataType} of the target data type.
   * @param values
   *          In JavaScript a normal JavaScript object can be specified for this parameter. This object is then
   *          validated to be a valid JSON serialization of the JsonResult.
   * @return always <code>null</code>.
   * @throws RuntimeException
   *           In case the object is invalid.
   */
  public String data(String dataType, Map<String, Object> values);

}
