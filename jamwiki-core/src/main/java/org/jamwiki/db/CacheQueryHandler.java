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

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.jamwiki.Environment;
import org.jamwiki.model.Category;
import org.jamwiki.model.LogItem;
import org.jamwiki.model.RecentChange;
import org.jamwiki.model.TopicType;
import org.jamwiki.model.TopicVersion;
import org.jamwiki.utils.Pagination;
import org.jamwiki.utils.WikiLogger;

/**
 * Caché-specific implementation of the QueryHandler interface.  This class implements
 * Caché-specific methods for instances where Caché does not support the default
 * ASCII SQL syntax.
 * Most of these changes have to do with creating a pagination scheme that will work
 * Caché does not support the limit and offset functionality
 * Also it needs the content to be stored and passed as a clob to avoid default string size limitations
 * 
 * in sql.cache.properties there are 3 changes to upgrade sql statements due to the way
 * Caché handles alter statements.   alter statements are required to do no more than one 
 * operation at a time, and specifying the data type (ie,  VARCHAR(200) NOT NULL) is 
 * considered to be an operation.   Since the datatype and size for the fields in question
 * had not changed,  i removed the datatype declaration to leave the actual "not null" change
 */
public class CacheQueryHandler extends AnsiQueryHandler {

	private static final WikiLogger logger = WikiLogger.getLogger(AnsiQueryHandler.class.getName());
	protected static final String SQL_PROPERTY_FILE_NAME = "sql/sql.cache.properties";

	/**
	 *
	 */
	public CacheQueryHandler() {
		Properties defaults = Environment.loadProperties(AnsiQueryHandler.SQL_PROPERTY_FILE_NAME);
		Properties props = Environment.loadProperties(SQL_PROPERTY_FILE_NAME, defaults);
		super.init(props);
	}

	/**
	 *
	 */
	@Override
	public List<Category> getCategories(int virtualWikiId, String virtualWikiName, Pagination pagination) throws SQLException {
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_CATEGORIES,
				pagination.getNumResults(),
				virtualWikiId,
				pagination.getOffset()
		);
		List<Category> categories = new ArrayList<Category>();
		for (Map<String, Object> result : results) {
			Category category = new Category();
			category.setName((String)result.get("category_name"));
			// child topic name not initialized since it is not needed
			category.setVirtualWiki(virtualWikiName);
			category.setSortKey((String)result.get("sort_key"));
			// topic type not initialized since it is not needed
			categories.add(category);
		}
		return categories;
	}

	/**
	 *
	 */
	@Override
	public List<LogItem> getLogItems(int virtualWikiId, String virtualWikiName, int logType, Pagination pagination, boolean descending) throws SQLException {
		// FIXME - sort order ignored
		String sql = null;
		Object[] args = null;
		int index = 0;
		if (logType == -1) {
			sql = STATEMENT_SELECT_LOG_ITEMS;
			args = new Object[3];
		} else {
			sql = STATEMENT_SELECT_LOG_ITEMS_BY_TYPE;
			args = new Object[4];
			args[index++] = logType;
		}
		args[index++] = pagination.getNumResults();
		args[index++] = virtualWikiId;
		args[index++] = pagination.getOffset();
		return DatabaseConnection.getJdbcTemplate().query(sql, args, new LogItemMapper(virtualWikiName));
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getRecentChanges(String virtualWiki, Pagination pagination, boolean descending) throws SQLException {
		// FIXME - sort order ignored
		Object[] args = {
				pagination.getNumResults(),
				virtualWiki,
				pagination.getOffset()
		};
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_RECENT_CHANGES, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getTopicHistory(int topicId, Pagination pagination, boolean descending, boolean selectDeleted) throws SQLException {
		// FIXME - sort order ignored
		// the SQL contains the syntax "is {0} null", which needs to be formatted as a message.
		Object[] params = {""};
		if (selectDeleted) {
			params[0] = "not";
		}
		String sql = this.formatStatement(STATEMENT_SELECT_TOPIC_HISTORY, params);
		Object[] args = {
				pagination.getNumResults(),
				topicId,
				pagination.getOffset()
		};
		return DatabaseConnection.getJdbcTemplate().query(sql, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public List<String> getTopicsAdmin(int virtualWikiId, Pagination pagination) throws SQLException {
		Object[] args = {
				pagination.getNumResults(),
				virtualWikiId,
				pagination.getOffset()
		};
		return DatabaseConnection.getJdbcTemplate().queryForList(STATEMENT_SELECT_TOPICS_ADMIN, args, String.class);
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getUserContributionsByLogin(String virtualWiki, String login, Pagination pagination, boolean descending) throws SQLException {
		// FIXME - sort order ignored
		Object[] args = {
				pagination.getNumResults(),
				virtualWiki,
				login,
				pagination.getOffset()
		};
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_WIKI_USER_CHANGES_LOGIN, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getUserContributionsByUserDisplay(String virtualWiki, String userDisplay, Pagination pagination, boolean descending) throws SQLException {
		// FIXME - sort order ignored
		Object[] args = {
				pagination.getNumResults(),
				virtualWiki,
				userDisplay,
				pagination.getOffset()
		};
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_WIKI_USER_CHANGES_ANONYMOUS, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getWatchlist(int virtualWikiId, int userId, Pagination pagination) throws SQLException {
		Object[] args = {
				pagination.getNumResults(),
				virtualWikiId,
				userId,
				pagination.getOffset()
		};
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_WATCHLIST_CHANGES, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public Map<Integer, String> lookupTopicByType(int virtualWikiId, TopicType topicType1, TopicType topicType2, int namespaceStart, int namespaceEnd, Pagination pagination) throws SQLException {
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_TOPIC_BY_TYPE,
				pagination.getNumResults(),
				virtualWikiId,
				topicType1.id(),
				topicType2.id(),
				namespaceStart,
				namespaceEnd,
				pagination.getOffset()
		);
		Map<Integer, String> topicMap = new LinkedHashMap<Integer, String>();
		for (Map<String, Object> result : results) {
			topicMap.put((Integer)result.get("topic_id"), (String)result.get("topic_name"));
		}
		return topicMap;
	}

	/**
	 * 
	 */
	@Override
	protected void prepareTopicVersionStatement(TopicVersion topicVersion, PreparedStatement stmt) throws SQLException {
		StringReader sr = null;
		try {
			int index = 1;
			stmt.setInt(index++, topicVersion.getTopicVersionId());
			if (topicVersion.getEditDate() == null) {
				topicVersion.setEditDate(new Timestamp(System.currentTimeMillis()));
			}
			stmt.setInt(index++, topicVersion.getTopicId());
			stmt.setString(index++, topicVersion.getEditComment());
			//pass the content into a stream to be passed to Caché
			sr = new StringReader(topicVersion.getVersionContent());
			stmt.setCharacterStream(index++, sr, topicVersion.getVersionContent().length());
			if (topicVersion.getAuthorId() == null) {
				stmt.setNull(index++, Types.INTEGER);
			} else {
				stmt.setInt(index++, topicVersion.getAuthorId());
			}
			stmt.setInt(index++, topicVersion.getEditType());
			stmt.setString(index++, topicVersion.getAuthorDisplay());
			stmt.setTimestamp(index++, topicVersion.getEditDate());
			if (topicVersion.getPreviousTopicVersionId() == null) {
				stmt.setNull(index++, Types.INTEGER);
			} else {
				stmt.setInt(index++, topicVersion.getPreviousTopicVersionId());
			}
			stmt.setInt(index++, topicVersion.getCharactersChanged());
			stmt.setString(index++, topicVersion.getVersionParamString());
		} finally {
			if (sr != null) {
				sr.close();
			}
		}
	}
}
