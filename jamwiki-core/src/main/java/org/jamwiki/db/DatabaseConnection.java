/**
 * Licensed under the GNU LESSER GENERAL PUBLIC LICENSE, version 2.1, dated February 1999.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the latest version of the GNU Lesser General
 * Public License as published by the Free Software Foundation;
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program (LICENSE.txt); if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.jamwiki.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.jamwiki.Environment;
import org.jamwiki.utils.ResourceUtil;
import org.jamwiki.utils.WikiLogger;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * This class provides methods for retrieving database connections, executing queries,
 * and setting up connection pools.
 */
public class DatabaseConnection {

	private static final WikiLogger logger = WikiLogger.getLogger(DatabaseConnection.class.getName());
	private static DataSource dataSource = null;
	private static JdbcTemplate jdbcTemplate = null;
	private static TransactionTemplate transactionTemplate = null;
	private static DataSourceTransactionManager transactionManager = null;

	/**
	 * This class has only static methods and is never instantiated.
	 */
	private DatabaseConnection() {
	}

	/**
	 * Utility method for closing a database connection, a statement and a result set.
	 * This method must ALWAYS be called for any connection retrieved by the
	 * {@link DatabaseConnection#getConnection getConnection()} method, and the
	 * connection SHOULD NOT have already been closed.
	 *
	 * @param conn A database connection, retrieved using DatabaseConnection.getConnection(),
	 *  that is to be closed.  This connection SHOULD NOT have been previously closed.
	 * @param stmt A statement object that is to be closed.  May be <code>null</code>.
	 * @param rs A result set object that is to be closed.  May be <code>null</code>.
	 */
	protected static void closeConnection(Connection conn, Statement stmt, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {}
		}
		DatabaseConnection.closeStatement(stmt);
		if (conn != null) {
			DataSourceUtils.releaseConnection(conn, dataSource);
		}
	}

	/**
	 * Close the connection pool, to be called for example during Servlet shutdown.
	 * <p>
	 * Note that this only applies if the DataSource was created by JAMWiki;
	 * in the case of a container DataSource obtained via JNDI this method does nothing
	 * except clear the static reference to the DataSource.
	 */
	protected static void closeConnectionPool() {
		try {
			DataSource testDataSource = dataSource;
			while (testDataSource instanceof DelegatingDataSource) {
				testDataSource = ((DelegatingDataSource) testDataSource).getTargetDataSource();
			}
			if (testDataSource instanceof BasicDataSource) {
				// required to release any connections e.g. in case of servlet shutdown
				((BasicDataSource) testDataSource).close();
			}
		} catch (SQLException e) {
			// log the connection pool closing failure, but there is no need to propagate
			logger.warn("Unable to close connection pool", e);
		}
		// clear references to prevent them being reused (& allow garbage collection)
		dataSource = null;
		transactionManager = null;
	}

	/**
	 * Utility method for closing a statement that may or may not be <code>null</code>.
	 * The statement SHOULD NOT have already been closed.
	 *
	 * @param stmt A statement object that is to be closed.  May be <code>null</code>.
	 */
	protected static void closeStatement(Statement stmt) {
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {}
		}
	}

	/**
	 * Execute a query to retrieve the next available integer ID for a table,
	 * generally the result of the next available ID after executing SQL such
	 * as "select max(id) from table".
	 *
	 * @param sql The SQL to execute.
	 * @return Returns the result of the query or 1 if no result is found.
	 */
	protected static int executeSequenceQuery(String sql) throws SQLException {
		try {
			Integer result = DatabaseConnection.getJdbcTemplate().queryForObject(sql, Integer.class);
			return (result == null) ? 1 : result.intValue() + 1;
		} catch (IncorrectResultSizeDataAccessException e) {
			// no rows available
			return 1;
		}
	}

	/**
	 *
	 */
	protected static int executeUpdate(String sql, Connection conn) throws SQLException {
		Statement stmt = null;
		try {
			long start = System.currentTimeMillis();
			stmt = conn.createStatement();
			if (logger.isInfoEnabled()) {
				logger.info("Executing SQL: " + sql);
			}
			int result = stmt.executeUpdate(sql);
			if (logger.isDebugEnabled()) {
				long execution = System.currentTimeMillis() - start;
				logger.debug("Executed " + sql + " (" + (execution / 1000.000) + " s.)");
			}
			return result;
		} catch (SQLException e) {
			logger.error("Failure while executing " + sql, e);
			throw e;
		} finally {
			DatabaseConnection.closeStatement(stmt);
		}
	}

	/**
	 * Execute a string representing a SQL statement, suppressing any exceptions.
	 */
	protected static void executeUpdateNoException(String sql, Connection conn) {
		try {
			DatabaseConnection.executeUpdate(sql, conn);
		} catch (SQLException e) {
			// suppress
		}
	}

	/**
	 *
	 */
	protected static Connection getConnection() {
		if (dataSource == null) {
			// DataSource has not yet been created, obtain it now
			configDataSource();
		}
		return DataSourceUtils.getConnection(dataSource);
	}

	/**
	 * Static method that will configure a DataSource based on the Environment setup.
	 */
	private synchronized static void configDataSource() throws IllegalArgumentException {
		if (dataSource != null) {
			// DataSource has already been created so remove it
			closeConnectionPool();
		}
		String url = Environment.getValue(Environment.PROP_DB_URL);
		DataSource targetDataSource = null;
		if (url.startsWith("jdbc:")) {
			try {
				// Use an internal "LocalDataSource" configured from the Environment
				targetDataSource = new LocalDataSource();
			} catch (ClassNotFoundException e) {
				logger.error("Failure while configuring local data source", e);
				throw new IllegalArgumentException("Failure while configuring local data source: " + e.toString());
			}
		} else {
			try {
				// Use a container DataSource obtained via JNDI lookup
				// TODO: Should try prefix java:comp/env/ if not already part of the JNDI name?
				Context ctx = new InitialContext();
				targetDataSource = (DataSource)ctx.lookup(url);
			} catch (NamingException e) {
				logger.error("Failure while configuring JNDI data source with URL: " + url, e);
				throw new IllegalArgumentException("Unable to configure JNDI data source with URL " + url + ": " + e.toString());
			}
		}
		dataSource = new LazyConnectionDataSourceProxy(targetDataSource);
		transactionManager = new DataSourceTransactionManager(targetDataSource);
	}

	/**
	 * Return a Spring JdbcTemplate suitable for querying the database.
	 */
	protected static JdbcTemplate getJdbcTemplate() {
		if (jdbcTemplate == null) {
			if (dataSource == null) {
				// DataSource has not yet been created, obtain it now
				configDataSource();
			}
			jdbcTemplate = new JdbcTemplate(dataSource);
		}
		return jdbcTemplate;
	}

	/**
	 * Return a Spring TransactionTemplate suitable for executing transactional
	 * database logic.
	 */
	protected static TransactionTemplate getTransactionTemplate() {
		if (transactionTemplate == null) {
			if (transactionManager == null) {
				// DataSource has not yet been created, obtain it now
				configDataSource();
			}
			transactionTemplate = new TransactionTemplate(transactionManager);
		}
		return transactionTemplate;
	}

	/**
	 * Test whether the database identified by the given parameters can be connected to.
	 *
	 * @param driver A String indicating the full path for the database driver class.
	 * @param url The JDBC driver URL.
	 * @param user The database user.
	 * @param password The database user password.
	 * @param existence Set to <code>true</code> if a test query should be executed.
	 * @throws SQLException Thrown if any failure occurs while creating a test connection.
	 */
	public static void testDatabase(String driver, String url, String user, String password, boolean existence) throws SQLException, ClassNotFoundException {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = getTestConnection(driver, url, user, password);
			if (existence) {
				stmt = conn.createStatement();
				// test to see if database exists
				AnsiQueryHandler queryHandler = new AnsiQueryHandler();
				stmt.executeQuery(queryHandler.existenceValidationQuery());
			}
		} finally {
			DatabaseConnection.closeConnection(conn, stmt, null);
			// explicitly null the variable to improve garbage collection.
			// with very large loops this can help avoid OOM "GC overhead
			// limit exceeded" errors.
			stmt = null;
			conn = null;
		}
	}

	/**
	 * Return a connection to the database with the specified parameters.
	 * The caller <b>must</b> close this connection when finished!
	 *
	 * @param driver A String indicating the full path for the database driver class.
	 * @param url The JDBC driver URL.
	 * @param user The database user.
	 * @param password The database user password.
	 * @throws SQLException Thrown if any failure occurs while getting the test connection.
	 */
	protected static Connection getTestConnection(String driver, String url, String user, String password) throws SQLException {
		if (url.startsWith("jdbc:")) {
			if (!StringUtils.isBlank(driver)) {
				try {
					// ensure that the Driver class has been loaded
					ResourceUtil.forName(driver);
				} catch (ClassNotFoundException e) {
					throw new SQLException("Unable to instantiate class with name: " + driver);
				}
			}
			return DriverManager.getConnection(url, user, password);
		} else {
			DataSource testDataSource = null;
			try {
				Context ctx = new InitialContext();
				// TODO: Try appending "java:comp/env/" to the JNDI Name if it is missing?
				testDataSource = (DataSource) ctx.lookup(url);
			} catch (NamingException e) {
				logger.error("Failure while configuring JNDI data source with URL: " + url, e);
				throw new SQLException("Unable to configure JNDI data source with URL " + url + ": " + e.toString());
			}
			return testDataSource.getConnection();
		}
	}
}