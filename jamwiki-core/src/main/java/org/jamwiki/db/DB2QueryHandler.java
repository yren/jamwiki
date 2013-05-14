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
 * DB2-specific implementation of the QueryHandler interface.  This class implements
 * DB2-specific methods for instances where DB2 does not support the default
 * ASCII SQL syntax.
 */
public class DB2QueryHandler extends AnsiQueryHandler {

	private static final WikiLogger logger = WikiLogger.getLogger(DB2QueryHandler.class.getName());
	private static final String SQL_PROPERTY_FILE_NAME = "sql/sql.db2.properties";

	/**
	 *
	 */
	public DB2QueryHandler() {
		Properties defaults = Environment.loadProperties(AnsiQueryHandler.SQL_PROPERTY_FILE_NAME);
		Properties props = Environment.loadProperties(SQL_PROPERTY_FILE_NAME, defaults);
		super.init(props);
	}

	/**
	 *
	 */
	@Override
	public List<Category> getCategories(int virtualWikiId, String virtualWikiName, Pagination pagination) {
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_CATEGORIES,
				virtualWikiId,
				pagination.getStart(),
				pagination.getEnd()
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
			sql = STATEMENT_SELECT_LOG_ITEMS;
			args = new Object[3];
		} else {
			sql = STATEMENT_SELECT_LOG_ITEMS_BY_TYPE;
			args = new Object[4];
			args[index++] = logType;
		}
		args[index++] = virtualWikiId;
		args[index++] = pagination.getStart();
		args[index++] = pagination.getEnd();
		return DatabaseConnection.getJdbcTemplate().query(sql, args, new LogItemMapper(virtualWikiName));
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getRecentChanges(String virtualWiki, Pagination pagination, boolean descending) {
		// FIXME - sort order ignored
		Object[] args = {
				virtualWiki,
				pagination.getStart(),
				pagination.getEnd()
		};
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_RECENT_CHANGES, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getTopicHistory(int topicId, Pagination pagination, boolean descending, boolean selectDeleted) {
		// FIXME - sort order ignored
		// the SQL contains the syntax "is {0} null", which needs to be formatted as a message.
		Object[] params = {""};
		if (selectDeleted) {
			params[0] = "not";
		}
		String sql = this.formatStatement(STATEMENT_SELECT_TOPIC_HISTORY, params);
		Object[] args = {
				topicId,
				pagination.getStart(),
				pagination.getEnd()
		};
		return DatabaseConnection.getJdbcTemplate().query(sql, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public List<String> getTopicsAdmin(int virtualWikiId, Pagination pagination) {
		Object[] args = {
				virtualWikiId,
				pagination.getStart(),
				pagination.getEnd()
		};
		return DatabaseConnection.getJdbcTemplate().queryForList(STATEMENT_SELECT_TOPICS_ADMIN, args, String.class);
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getUserContributionsByLogin(String virtualWiki, String login, Pagination pagination, boolean descending) {
		// FIXME - sort order ignored
		Object[] args = {
				virtualWiki,
				login,
				pagination.getStart(),
				pagination.getEnd()
		};
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_WIKI_USER_CHANGES_LOGIN, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getUserContributionsByUserDisplay(String virtualWiki, String userDisplay, Pagination pagination, boolean descending) {
		// FIXME - sort order ignored
		Object[] args = {
				virtualWiki,
				userDisplay,
				pagination.getStart(),
				pagination.getEnd()
		};
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_WIKI_USER_CHANGES_ANONYMOUS, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public List<RecentChange> getWatchlist(int virtualWikiId, int userId, Pagination pagination) {
		Object[] args = {
				virtualWikiId,
				userId,
				pagination.getStart(),
				pagination.getEnd()
		};
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_WATCHLIST_CHANGES, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	@Override
	public Map<Integer, String> lookupTopicByType(int virtualWikiId, TopicType topicType1, TopicType topicType2, int namespaceStart, int namespaceEnd, Pagination pagination) {
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_TOPIC_BY_TYPE,
				virtualWikiId,
				topicType1.id(),
				topicType2.id(),
				namespaceStart,
				namespaceEnd,
				pagination.getStart(),
				pagination.getEnd()
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
		Object[] args = { pagination.getStart(), pagination.getEnd() };
		return DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_WIKI_USERS,
				args,
				String.class
		);
	}
}
