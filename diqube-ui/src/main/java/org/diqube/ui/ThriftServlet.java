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
import java.util.function.Function;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.diqube.remote.query.KeepAliveServiceConstants;
import org.diqube.remote.query.QueryResultServiceConstants;
import org.diqube.remote.query.thrift.KeepAliveService;
import org.diqube.remote.query.thrift.QueryResultService;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * Endpoint servlet for calls on the Thrift services this webapp provides. These are usually calls from cluster nodes
 * which have e.g. new result data available.
 *
 * @author Bastian Gloeckle
 */
public class ThriftServlet extends TServlet {
  private static final long serialVersionUID = 1L;

  /** make sure this matches the web.xml. */
  public static final String URL_PATTERN = "/t";

  private static final short NUMBER_OF_PROCESSORS = 2;

  private static ThreadLocal<ApplicationContext> validAppContext = new ThreadLocal<>();
  private static ThreadLocal<Integer> initializedDelegates = new ThreadLocal<>();
  private short numberOfProcessorsInitialized = 0;
  private Object numberOfProcessorsInitializedSync = new Object();

  public ThriftServlet() {
    super(createProcessor(), createProtocolFactory());
  }

  private static TProcessor createProcessor() {
    TMultiplexedProcessor res = new TMultiplexedProcessor();

    res.registerProcessor(QueryResultServiceConstants.SERVICE_NAME,
        new LazyBindingProcessorProvider<>(QueryResultServiceHandler.class,
            handler -> new QueryResultService.Processor<QueryResultService.Iface>(handler)));
    res.registerProcessor(KeepAliveServiceConstants.SERVICE_NAME, new LazyBindingProcessorProvider<>(
        KeepAliveServiceHandler.class, handler -> new KeepAliveService.Processor<KeepAliveService.Iface>(handler)));

    // when adding new processors, update NUMBER_OF_PROCESSORS

    return res;
  }

  private static TProtocolFactory createProtocolFactory() {
    return new TCompactProtocol.Factory();
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    if (numberOfProcessorsInitialized == NUMBER_OF_PROCESSORS) {
      super.doPost(request, response);
      return;
    }

    validAppContext.set(WebApplicationContextUtils.getRequiredWebApplicationContext(request.getServletContext()));
    try {
      super.doPost(request, response);
    } finally {
      validAppContext.remove();

      if (initializedDelegates.get() != null) {
        synchronized (numberOfProcessorsInitializedSync) {
          numberOfProcessorsInitialized += initializedDelegates.get();
        }
        initializedDelegates.remove();
      }
    }
  }

  /**
   * A Processor which will lazily create a delegate processor that is based on a handler object that is available in
   * the bean context.
   * 
   * As all initialization methods of the ThriftServlet need to be static (TServelt does not provide a very nice
   * interface unfortuantely), we cannot fetch the handlers for the services from the bean context when creating the
   * servlet itself. Instead, we install instances of this class as processors. These will then, when first called and
   * when provided with the current bean context in {@link ThriftServlet#validAppContext}, fetch the handler objects
   * from the bean context and initialize the delegate processors. In addition to that, after initializing a delegate
   * processor, this class will set {@link ThriftServlet#initializedDelegates} - using that the enclosing servlet class
   * can check if all delegate processors have already been created - and not set the
   * {@link ThriftServlet#validAppContext} {@link ThreadLocal} for future calls anymore, to save some sync-time.
   */
  private static class LazyBindingProcessorProvider<T> implements TProcessor {
    private Class<? extends T> handlerClass;
    private Function<T, TProcessor> processorFactory;
    private volatile TProcessor delegate = null;

    private LazyBindingProcessorProvider(Class<? extends T> handlerClass, Function<T, TProcessor> processorFactory) {
      this.handlerClass = handlerClass;
      this.processorFactory = processorFactory;
    }

    @Override
    public boolean process(TProtocol in, TProtocol out) throws TException {
      if (delegate != null)
        return delegate.process(in, out);

      synchronized (this) {
        if (delegate == null) {
          T handler = validAppContext.get().getBean(handlerClass);
          delegate = processorFactory.apply(handler);
          initializedDelegates.set(1);
        }
      }

      return delegate.process(in, out);
    }
  }

}
