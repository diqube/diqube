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
package org.diqube.ui.db.hsqldb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.db.UiDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Implementation of {@link UiDatabase} for an internally contained HSQLDB.
 * 
 * <p>
 * The "location" provided to this class is
 * <ul>
 * <li>a file name, in which case an internal database will be started and the data will be saved locally. Note that
 * this option will not allow the UI to be served in a cluster.
 * <li>a "full qualified" JDBC connection URL, starting with "jdbc:".
 * </ul>
 *
 * @author Bastian Gloeckle
 */
public class HsqlUiDatabase implements UiDatabase {
  private static final Logger logger = LoggerFactory.getLogger(HsqlUiDatabase.class);

  private static final long VERSION = 1;

  private static final String TABLE_VERSION = "Version";
  private static final String FIELD_VERSION_DB_VERSION = "db_version";

  private static final String TABLE_ANALYSIS = "Analysis";
  private static final String FIELD_ANALYSIS_ID = "analysisId";
  private static final String FIELD_ANALYSIS_VERSION = "analysisVersion";
  private static final String FIELD_ANALYSIS_USER = "analysisUser";
  private static final String FIELD_ANALYSIS_NAME = "analysisName";
  private static final String FIELD_ANALYSIS_ANALYSIS_DATA = "analysisData";

  private Connection connection;

  private JsonFactory jsonFactory = new JsonFactory();
  private ObjectMapper mapper = new ObjectMapper(jsonFactory);

  private Driver jdbcDriver;

  public HsqlUiDatabase(String location, String username, String password) {
    // let the driver register itself.
    try {
      Class<?> driverClass = Class.forName("org.hsqldb.jdbc.JDBCDriver");
      jdbcDriver = (Driver) driverClass.getField("driverInstance").get(null);
    } catch (ClassNotFoundException | IllegalArgumentException | IllegalAccessException | NoSuchFieldException
        | SecurityException e) {
      throw new RuntimeException("Could not instantiate hsqldb driver", e);
    }

    boolean doSetupCalls = false;
    if (location.startsWith("jdbc:")) {
      logger.info("Will connect to '{}' as UI database.", location);
    } else {
      // assume location is filename, startup internal DB.
      logger.info("Using '{}' as storage for the integrated local HSQLDB.", location);
      location = "jdbc:hsqldb:file:" + location + ";hsqldb.script_format=3";
      doSetupCalls = true;
    }

    if (username == null)
      username = "SA";
    if (password == null)
      password = "";

    try {
      connection = DriverManager.getConnection(location, username, password);
      if (doSetupCalls) {
        // see http://www.hsqldb.org/doc/guide/dbproperties-chapt.html
        try (Statement stmt = connection.createStatement()) {
          stmt.execute("SET FILES LOB COMPRESSED TRUE");
        }
        try (Statement stmt = connection.createStatement()) {
          stmt.execute("SET FILES WRITE DELAY FALSE");
        }
      }

      try (Statement stmt = connection.createStatement()) {
        stmt.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE, ISOLATION LEVEL READ COMMITTED");
      }
      validateOrCreateSchema();
    } catch (SQLException e) {
      throw new RuntimeException("Could not initialize database", e);
    }
  }

  @Override
  public void storeAnalysisVersion(UiAnalysis analysis) throws StoreException {
    try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + TABLE_ANALYSIS + "(" + //
        FIELD_ANALYSIS_ID + ", " + //
        FIELD_ANALYSIS_VERSION + ", " + //
        FIELD_ANALYSIS_USER + ", " + //
        FIELD_ANALYSIS_NAME + ", " + //
        FIELD_ANALYSIS_ANALYSIS_DATA + //
        ") VALUES (?, ?, ?, ?, ?)")) {

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      OutputStreamWriter osw = new OutputStreamWriter(baos, Charset.forName("UTF-8"));
      try {
        mapper.writeValue(jsonFactory.createGenerator(osw), analysis);
        osw.close();
      } catch (IOException e) {
        throw new StoreException("Could not serialize analysis");
      }

      stmt.setString(1, analysis.getId());
      stmt.setLong(2, analysis.getVersion());
      stmt.setString(3, analysis.getUser());
      stmt.setString(4, analysis.getName());
      stmt.setClob(5, new InputStreamReader(new ByteArrayInputStream(baos.toByteArray()), Charset.forName("UTF-8")));

      stmt.execute();
    } catch (SQLException e) {
      throw new StoreException("Could not store analysis", e);
    }
  }

  @Override
  public UiAnalysis loadAnalysisVersion(String analysisId, long version) {
    try (PreparedStatement stmt = connection.prepareStatement("SELECT " + //
        FIELD_ANALYSIS_ANALYSIS_DATA + //
        " FROM " + TABLE_ANALYSIS + //
        " WHERE " + FIELD_ANALYSIS_ID + " = ? AND " + FIELD_ANALYSIS_VERSION + " = ?")) {

      stmt.setString(1, analysisId);
      stmt.setLong(2, version);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          Reader jsonReader = rs.getClob(FIELD_ANALYSIS_ANALYSIS_DATA).getCharacterStream();

          try {
            UiAnalysis res = mapper.readValue(jsonReader, UiAnalysis.class);
            return res;
          } catch (IOException e) {
            logger.error("Error deserializing UiAnalysis", e);
            return null;
          }
        }
      }
    } catch (SQLException e) {
      logger.error("Error while reading from UI DB", e);
      return null;
    }
    return null;
  }

  @Override
  public Map<String, Long> findNewestAnalysisVersionsOfUser(String user) {
    try (PreparedStatement stmt = connection.prepareStatement("SELECT " + //
        FIELD_ANALYSIS_ID + " as id, " + //
        "max(" + FIELD_ANALYSIS_VERSION + ") as maxVersion " + //
        " FROM " + TABLE_ANALYSIS + //
        " WHERE " + FIELD_ANALYSIS_USER + " = ?" + //
        " GROUP BY " + FIELD_ANALYSIS_ID //
    )) {

      Map<String, Long> res = new HashMap<>();

      stmt.setString(1, user);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          res.put(rs.getString("id"), rs.getLong("maxVersion"));
        }
      }

      return res;
    } catch (SQLException e) {
      logger.error("Error while reading from UI DB", e);
      return new HashMap<>();
    }
  }

  @Override
  public Map<String, String> findNewestAnalysisNamesOfUser(String user) {
    try (PreparedStatement stmt = connection.prepareStatement("SELECT " + //
        "a." + FIELD_ANALYSIS_ID + " as id, " + //
        "a." + FIELD_ANALYSIS_NAME + " as name " + //
        "FROM " + TABLE_ANALYSIS + " a " + //
        "JOIN (" + //
    /* */"SELECT " + //
    /* */FIELD_ANALYSIS_ID + " as id, " + //
    /* */"max(" + FIELD_ANALYSIS_VERSION + ") as maxVersion " + //
    /* */" FROM " + TABLE_ANALYSIS + //
    /* */" WHERE " + FIELD_ANALYSIS_USER + " = ?" + //
    /* */" GROUP BY " + FIELD_ANALYSIS_ID + //
        ") m " + //
        "ON a." + FIELD_ANALYSIS_ID + " = m.id AND " + //
        "a." + FIELD_ANALYSIS_VERSION + " = m.maxVersion" //
    )) {

      Map<String, String> res = new HashMap<>();

      stmt.setString(1, user);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          res.put(rs.getString("id"), rs.getString("name"));
        }
      }

      return res;
    } catch (SQLException e) {
      logger.error("Error while reading from UI DB", e);
      return new HashMap<>();
    }
  }

  @Override
  public String findOwnerOfAnalysis(String analysisId) {
    try (PreparedStatement stmt = connection.prepareStatement("SELECT " + //
        FIELD_ANALYSIS_USER + //
        " FROM " + TABLE_ANALYSIS + //
        " WHERE " + FIELD_ANALYSIS_ID + " = ?")) {

      stmt.setString(1, analysisId);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          return rs.getString(FIELD_ANALYSIS_USER);
        }
      }

      return null;
    } catch (SQLException e) {
      logger.error("Error while reading from UI DB", e);
      return null;
    }
  }

  @Override
  public Long findNewestAnalysisVersion(String analysisId) {
    try (PreparedStatement stmt = connection.prepareStatement("SELECT " + //
        "max(" + FIELD_ANALYSIS_VERSION + ") as maxVersion " + //
        " FROM " + TABLE_ANALYSIS + //
        " WHERE " + FIELD_ANALYSIS_ID + " = ?" + //
        " GROUP BY " + FIELD_ANALYSIS_ID)) {

      stmt.setString(1, analysisId);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          return rs.getLong("maxVersion");
        }
      }

      return null;
    } catch (SQLException e) {
      logger.error("Error while reading from UI DB", e);
      return null;
    }
  }

  @Override
  public void shutdown() {
    if (connection != null) {
      logger.info("Shutting down hsqldb connection.");
      try (Statement stmt = connection.createStatement()) {
        stmt.executeQuery("SHUTDOWN");
      } catch (SQLException e) {
        logger.warn("Could not execute SHUTDOWN command", e);
      }
      try {
        connection.close();
      } catch (SQLException e) {
        logger.warn("Could not shutdown database cleanly", e);
      }
      connection = null;
      try {
        DriverManager.deregisterDriver(jdbcDriver);
      } catch (SQLException e) {
        logger.warn("Could not deregister JDBC driver", e);
      }
    }
  }

  private void validateOrCreateSchema() throws SQLException {
    boolean createSchema = false;
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("select " + FIELD_VERSION_DB_VERSION + " from " + TABLE_VERSION)) {
        if (!rs.next()) {
          createSchema = true;
        } else {
          long version = rs.getLong(FIELD_VERSION_DB_VERSION);
          if (version != VERSION)
            throw new RuntimeException("UI Db in old version, do not support online update!");
        }
      } catch (SQLException e) {
        createSchema = true;
      }
    }

    if (createSchema) {
      logger.info("Creating new DB schema...");
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("CREATE TABLE " + TABLE_VERSION + " (" + FIELD_VERSION_DB_VERSION + " BIGINT)");
      }
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("CREATE TABLE " + TABLE_ANALYSIS + " (" + //
            FIELD_ANALYSIS_ID + " VARCHAR(50), " + //
            FIELD_ANALYSIS_VERSION + " BIGINT, " + //
            FIELD_ANALYSIS_USER + " VARCHAR(200), " + //
            FIELD_ANALYSIS_NAME + " VARCHAR(1000), " + //
            FIELD_ANALYSIS_ANALYSIS_DATA + " CLOB(100K) " + //
            ", PRIMARY KEY (" + //
            FIELD_ANALYSIS_ID + ", " + FIELD_ANALYSIS_VERSION + ")" + //
            ")");
      }
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("CREATE INDEX idxAnalysisUser ON " + TABLE_ANALYSIS + " (" + //
            FIELD_ANALYSIS_USER + //
            ")");
      }
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("INSERT INTO " + TABLE_VERSION + " (" + //
            FIELD_VERSION_DB_VERSION + //
            ") VALUES (" + VERSION + ")");
      }
      logger.info("DB schema created.");
    }
  }

}
