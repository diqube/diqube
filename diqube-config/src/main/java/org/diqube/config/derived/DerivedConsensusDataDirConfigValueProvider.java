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
package org.diqube.config.derived;

import java.io.File;

import org.diqube.config.ConfigKey;
import org.diqube.config.DerivedConfigKey;

/**
 * Provider for {@link DerivedConfigKey#FINAL_CONSENSUS_DATA_DIR}
 *
 * @author Bastian Gloeckle
 */
public class DerivedConsensusDataDirConfigValueProvider extends AbstractDerivedConfigValueProvider {

  @Override
  public String derive() {
    String configValue = configurationManager.getValue(ConfigKey.CONSENSUS_DATA_DIR);
    File f = new File(configValue);
    if (!f.isAbsolute())
      f = new File(allProviders.get(DerivedConfigKey.FINAL_INTERNAL_DB_DIR).derive(), configValue);
    return f.getAbsolutePath();
  }

}
