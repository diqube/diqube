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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.io.ByteStreams;

/**
 *
 * @author Bastian Gloeckle
 */
public class WebServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  private static final String WAR_STATIC_ROOT = "/web";

  private static final String DEFAULT_RESOURCE = "/index.html";

  private volatile byte[] cachedIndexHtmlBytes = null;
  private Object cachedIndexHtmlSync = new Object();

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String path = req.getPathInfo();
    if (path == null || "".equals(path)) {
      resp.sendRedirect("/");
      return;
    }

    if ("/".equals(path))
      path = DEFAULT_RESOURCE;

    try {
      path = new URI(path).normalize().toString();
      if (path.startsWith(".") || path.startsWith(".."))
        path = DEFAULT_RESOURCE;
      if (!path.startsWith("/"))
        path = "/" + path;
    } catch (URISyntaxException e) {
      path = DEFAULT_RESOURCE;
    }

    InputStream is = getClass().getResourceAsStream(WAR_STATIC_ROOT + path);

    if (path.equals("/index.html") || is == null) {
      if (cachedIndexHtmlBytes == null) {
        synchronized (cachedIndexHtmlSync) {
          if (cachedIndexHtmlBytes == null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (InputStream is2 = getClass().getResourceAsStream(WAR_STATIC_ROOT + "/index.html")) {
              ByteStreams.copy(is2, baos);
            }
            String indexHtmlString = baos.toString("UTF-8");
            indexHtmlString = indexHtmlString.replace("{{globalContextPath}}", getServletContext().getContextPath());
            cachedIndexHtmlBytes = indexHtmlString.getBytes("UTF-8");
          }
        }
      }

      is = new ByteArrayInputStream(cachedIndexHtmlBytes);
    }

    if (path.endsWith(".css"))
      resp.setContentType("text/css");

    ByteStreams.copy(is, resp.getOutputStream());
    is.close();
  }

}
