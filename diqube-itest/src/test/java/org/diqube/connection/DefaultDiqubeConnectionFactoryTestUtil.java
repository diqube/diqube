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
package org.diqube.connection;

import org.diqube.connection.integrity.IntegritySecretHelper;
import org.diqube.connection.integrity.IntegritySecretHelperTestUtil;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 *
 * @author Bastian Gloeckle
 */
public class DefaultDiqubeConnectionFactoryTestUtil {

  public static ConnectionFactory createDefaultConnectionFactory(String macKey) {
    ConnectionPool pool = Mockito.mock(ConnectionPool.class);

    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Connection<?> conn = (Connection<?>) invocation.getArguments()[0];
        conn.getTransport().close();
        return null;
      }
    }).when(pool).releaseConnection(Mockito.any());

    IntegritySecretHelper integritySecretHelper = new IntegritySecretHelper();
    IntegritySecretHelperTestUtil.setMessageIntegritySecret(integritySecretHelper, macKey);

    return new DefaultConnectionFactory(pool, integritySecretHelper, 10000);
  }
}
