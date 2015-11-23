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
package org.diqube.remote.base.services;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.thrift.TServiceClient;
import org.diqube.context.AutoInstatiate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

/**
 * Contains informations about services provided by the diqube Thrift interface.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeThriftServiceInfoManager {
  private static final Logger logger = LoggerFactory.getLogger(DiqubeThriftServiceInfoManager.class);

  private static final String BASE_PKG = "org.diqube.remote";

  private Map<Class<?>, DiqubeThriftServiceInfo<?>> annotationByServiceInterface;

  @PostConstruct
  public void initialize() {
    annotationByServiceInterface = new HashMap<>();

    ImmutableSet<ClassInfo> classInfos;
    try {
      classInfos =
          ClassPath.from(DiqubeThriftServiceInfoManager.class.getClassLoader()).getTopLevelClassesRecursive(BASE_PKG);
    } catch (IOException e) {
      throw new RuntimeException("Could not parse ClassPath.");
    }

    for (ClassInfo classInfo : classInfos) {
      Class<?> clazz = classInfo.load();

      DiqubeThriftService annotation = clazz.getAnnotation(DiqubeThriftService.class);
      if (annotation != null)
        annotationByServiceInterface.put(annotation.serviceInterface(), new DiqubeThriftServiceInfo<>(annotation));
    }
    logger.info("Found {} diqube services in {} scanned classes.", annotationByServiceInterface.size(),
        classInfos.size());
  }

  @SuppressWarnings("unchecked")
  public <T> DiqubeThriftServiceInfo<T> getServiceInfo(Class<T> serviceInterfaceClass) {
    return (DiqubeThriftServiceInfo<T>) annotationByServiceInterface.get(serviceInterfaceClass);
  }

  public static class DiqubeThriftServiceInfo<T> {
    private Class<T> serviceInterface;
    private Class<? extends TServiceClient> clientClass;
    private String serviceName;
    private boolean integrityChecked;

    /* package */ @SuppressWarnings("unchecked")
    DiqubeThriftServiceInfo(DiqubeThriftService annotation) {
      serviceInterface = (Class<T>) annotation.serviceInterface();
      clientClass = annotation.clientClass();
      serviceName = annotation.serviceName();
      integrityChecked = annotation.integrityChecked();
    }

    public Class<T> getServiceInterface() {
      return serviceInterface;
    }

    public Class<? extends TServiceClient> getClientClass() {
      return clientClass;
    }

    public String getServiceName() {
      return serviceName;
    }

    public boolean isIntegrityChecked() {
      return integrityChecked;
    }
  }
}
