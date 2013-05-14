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
import org.jamwiki.utils.Pagination;
import org.jamwiki.utils.WikiLogger;

/**
 * DB2/400-specific implementation of the QueryHandler interface.  This class implements
 * DB2/400-specific methods for instances where DB2/400 does not support the default
 * ASCII SQL syntax.
 */
public class DB2400QueryHandler extends AnsiQueryHandler {

	private static final WikiLogger logger = WikiLogger.getLogger(DB2400QueryHandler.class.getName());
	private static final String SQL_PROPERTY_FILE_NAME = "sql/sql.db2400.properties";

	/**
	 *
	 */
	public DB2400QueryHandler() {
		Properties defaults = Environment.loadProperties(AnsiQueryHandler.SQL_PROPERTY_FILE_NAME);
		Properties props = Environment.loadProperties(SQL_PROPERTY_FILE_NAME, defaults);
		super.init(props);
	}

	/**
	 * DB2/400 will not allow query parameters such as "fetch ? rows only", so
	 * this method provides a way of formatting the query limits without using
	 * query parameters.
	 *
	 * @param sql The SQL statement, with the last result parameter specified as
	 *  {0} and the total number of rows parameter specified as {1}.
	 * @param pagination A Pagination object that specifies the number of results
	 *  and starting result offset for the result set to be retrieved.
	 * @return A formatted SQL string.
	 */
	private String formatStatement(String sql, Pagination pagination) {
		Object[] objects = {pagination.getEnd(), pagination.getNumResults()};
		return this.formatStatement(sql, objects);
	}

	/**
	 * DB2/400 will not allow query parameters such as "fetch ? rows only", so
	 * this method provides a way of formatting the query limits without using
	 * query parameters.
	 *
	 * @param sql The SQL statement, with the last result parameter specified as
	 *  {0} and the total number of rows parameter specified as {1}.
	 * @param pagination A Pagination object that specifies the number of results
	 *  and starting result offset for the result set to be retrieved.
	 * @return A formatted SQL string.
	 */
	private String formatStatement(String sql, int limit) {
		Object[] objects = {limit};
		return this.formatStatement(sql, objects);
	}

	/**
	 *
	 */
	@Override
	public List<Category> getCategories(int virtualWikiId, String virtualWikiName, Pagination pagination) {
		String sql = this.formatStatement(STATEMENT_SELECT_CATEGORIES, pagination);
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				sql,
				virtualWikiId
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
	public List<LogItem> getLogItems(int virtualWikiId, String virtualWikiName, int logType, Pagination pagination, boolean descending) {
		// FIXME - sort order ignored
		String sql = null;
		Object[] args = null;
		int index = 0;
		if (logType == -1) {
			sql = this.formatStatement(STATEMENT_SELECT_LOG_ITEMS, pagination);
			args = new Object[1];
		} else {
			sql = this.formatStatement(STATEMENT_SELECT_LOG_ITEMS_BY_TYPE, pagination);
			args = new Object[2];
			args[index++] = logType;
		}
		args[index++] = virtualWikiId;
		return DatabaseConnection.getJdbcTemplate().query(sql, args, new LogItemMapper(virtualWikiName));
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getRecentChanges(String virtualWiki, Pagination pagination, boolean descending) {
		// FIXME - sort order ignored
		String sql = this.formatStatement(STATEMENT_SELECT_RECENT_CHANGES, pagination);
		Object[] args = {
				virtualWiki
		};
		return DatabaseConnection.getJdbcTemplate().query(sql, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getTopicHistory(int topicId, Pagination pagination, boolean descending, boolean selectDeleted) {
		// FIXME - sort order ignored
		// the SQL contains the syntax "is {0} null", which needs to be formatted as a message.
		Object[] params = {pagination.getEnd(), pagination.getNumResults(), ""};
		if (selectDeleted) {
			params[2] = "not";
		}
		String sql = this.formatStatement(STATEMENT_SELECT_TOPIC_HISTORY, params);
		Object[] args = {
				topicId
		};
		return DatabaseConnection.getJdbcTemplate().query(sql, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public List<String> getTopicsAdmin(int virtualWikiId, Pagination pagination) {
		String sql = this.formatStatement(STATEMENT_SELECT_TOPICS_ADMIN, pagination);
		Object[] args = {
				virtualWikiId
		};
		return DatabaseConnection.getJdbcTemplate().queryForList(sql, args, String.class);
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getUserContributionsByLogin(String virtualWiki, String login, Pagination pagination, boolean descending) {
		// FIXME - sort order ignored
		String sql = this.formatStatement(STATEMENT_SELECT_WIKI_USER_CHANGES_LOGIN, pagination);
		Object[] args = {
				virtualWiki,
				login
		};
		return DatabaseConnection.getJdbcTemplate().query(sql, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getUserContributionsByUserDisplay(String virtualWiki, String userDisplay, Pagination pagination, boolean descending) {
		// FIXME - sort order ignored
		String sql = this.formatStatement(STATEMENT_SELECT_WIKI_USER_CHANGES_ANONYMOUS, pagination);
		Object[] args = {
				virtualWiki,
				userDisplay
		};
		return DatabaseConnection.getJdbcTemplate().query(sql, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getWatchlist(int virtualWikiId, int userId, Pagination pagination) {
		String sql = this.formatStatement(STATEMENT_SELECT_WATCHLIST_CHANGES, pagination);
		Object[] args = {
				virtualWikiId,
				userId
		};
		return DatabaseConnection.getJdbcTemplate().query(sql, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public Map<Integer, String> lookupTopicByType(int virtualWikiId, TopicType topicType1, TopicType topicType2, int namespaceStart, int namespaceEnd, Pagination pagination) {
		String sql = this.formatStatement(STATEMENT_SELECT_TOPIC_BY_TYPE, pagination);
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				sql,
				virtualWikiId,
				topicType1.id(),
				topicType2.id(),
				namespaceStart,
				namespaceEnd
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
	public List<String> lookupWikiUsers(Pagination pagination) {
		String sql = this.formatStatement(STATEMENT_SELECT_WIKI_USERS, pagination);
		return DatabaseConnection.getJdbcTemplate().queryForList(sql, String.class);
	}

	/**
	 *
	 */
	@Override
	public void reloadRecentChanges(int limit) {
		DatabaseConnection.getJdbcTemplate().update(STATEMENT_DELETE_RECENT_CHANGES);
		String sql = this.formatStatement(STATEMENT_INSERT_RECENT_CHANGES_VERSIONS, limit);
		DatabaseConnection.getJdbcTemplate().update(sql, limit);
		DatabaseConnection.getJdbcTemplate().update(STATEMENT_INSERT_RECENT_CHANGES_LOGS);
	}
}
