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
package org.diqube.testutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestResult;

/**
 * A TestNG listener that logs the name of the test method being executed.
 * 
 * This is wired to all TestNG executions in diqube/pom.xml.
 *
 * @author Bastian Gloeckle
 */
public class LoggingTestNgListener implements IInvokedMethodListener {

  private static final Logger logger = LoggerFactory.getLogger(LoggingTestNgListener.class);

  @Override
  public void beforeInvocation(IInvokedMethod method, ITestResult testResult) {
    if (method.getTestMethod().getConstructorOrMethod().getMethod() != null && method.getTestMethod().isTest())
      logger.info("Executing {}#{}", method.getTestMethod().getTestClass().getName(),
          method.getTestMethod().getConstructorOrMethod().getMethod().getName());
  }

  @Override
  public void afterInvocation(IInvokedMethod method, ITestResult testResult) {
  }

}
