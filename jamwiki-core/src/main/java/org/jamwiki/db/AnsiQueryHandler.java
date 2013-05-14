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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.jamwiki.Environment;
import org.jamwiki.model.Category;
import org.jamwiki.model.GroupMap;
import org.jamwiki.model.ImageData;
import org.jamwiki.model.Interwiki;
import org.jamwiki.model.LogItem;
import org.jamwiki.model.Namespace;
import org.jamwiki.model.RecentChange;
import org.jamwiki.model.Role;
import org.jamwiki.model.RoleMap;
import org.jamwiki.model.Topic;
import org.jamwiki.model.TopicType;
import org.jamwiki.model.TopicVersion;
import org.jamwiki.model.UserBlock;
import org.jamwiki.model.VirtualWiki;
import org.jamwiki.model.WikiFile;
import org.jamwiki.model.WikiFileVersion;
import org.jamwiki.model.WikiGroup;
import org.jamwiki.model.WikiUser;
import org.jamwiki.model.WikiUserDetails;
import org.jamwiki.utils.Pagination;
import org.jamwiki.utils.WikiLogger;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * Default implementation of the QueryHandler implementation for retrieving, inserting,
 * and updating data in the database.  This method uses ANSI SQL and should therefore
 * work with any fully ANSI-compliant database.
 */
public class AnsiQueryHandler implements QueryHandler {

	private static final WikiLogger logger = WikiLogger.getLogger(AnsiQueryHandler.class.getName());
	protected static final String SQL_PROPERTY_FILE_NAME = "sql/sql.ansi.properties";

	protected static String STATEMENT_CONNECTION_VALIDATION_QUERY = null;
	protected static String STATEMENT_CREATE_AUTHORITIES_TABLE = null;
	protected static String STATEMENT_CREATE_CATEGORY_TABLE = null;
	protected static String STATEMENT_CREATE_CATEGORY_INDEX = null;
	protected static String STATEMENT_CREATE_CONFIGURATION_TABLE = null;
	protected static String STATEMENT_CREATE_GROUP_AUTHORITIES_TABLE = null;
	protected static String STATEMENT_CREATE_GROUP_MEMBERS_TABLE = null;
	protected static String STATEMENT_CREATE_GROUP_TABLE = null;
	protected static String STATEMENT_CREATE_INTERWIKI_TABLE = null;
	protected static String STATEMENT_CREATE_LOG_TABLE = null;
	protected static String STATEMENT_CREATE_NAMESPACE_TABLE = null;
	protected static String STATEMENT_CREATE_NAMESPACE_TRANSLATION_TABLE = null;
	protected static String STATEMENT_CREATE_RECENT_CHANGE_TABLE = null;
	protected static String STATEMENT_CREATE_ROLE_TABLE = null;
	protected static String STATEMENT_CREATE_TOPIC_CURRENT_VERSION_CONSTRAINT = null;
	protected static String STATEMENT_CREATE_TOPIC_TABLE = null;
	protected static String STATEMENT_CREATE_TOPIC_LINKS_TABLE = null;
	protected static String STATEMENT_CREATE_TOPIC_LINKS_INDEX = null;
	protected static String STATEMENT_CREATE_TOPIC_PAGE_NAME_INDEX = null;
	protected static String STATEMENT_CREATE_TOPIC_PAGE_NAME_LOWER_INDEX = null;
	protected static String STATEMENT_CREATE_TOPIC_NAMESPACE_INDEX = null;
	protected static String STATEMENT_CREATE_TOPIC_VIRTUAL_WIKI_INDEX = null;
	protected static String STATEMENT_CREATE_TOPIC_CURRENT_VERSION_INDEX = null;
	protected static String STATEMENT_CREATE_TOPIC_VERSION_TABLE = null;
	protected static String STATEMENT_CREATE_TOPIC_VERSION_TOPIC_INDEX = null;
	protected static String STATEMENT_CREATE_TOPIC_VERSION_PREVIOUS_INDEX = null;
	protected static String STATEMENT_CREATE_TOPIC_VERSION_USER_DISPLAY_INDEX = null;
	protected static String STATEMENT_CREATE_TOPIC_VERSION_USER_ID_INDEX = null;
	protected static String STATEMENT_CREATE_USER_BLOCK_TABLE = null;
	protected static String STATEMENT_CREATE_USERS_TABLE = null;
	protected static String STATEMENT_CREATE_VIRTUAL_WIKI_TABLE = null;
	protected static String STATEMENT_CREATE_WATCHLIST_TABLE = null;
	protected static String STATEMENT_CREATE_WIKI_FILE_TABLE = null;
	protected static String STATEMENT_CREATE_WIKI_FILE_VERSION_TABLE = null;
	protected static String STATEMENT_CREATE_WIKI_USER_TABLE = null;
	protected static String STATEMENT_CREATE_WIKI_USER_LOGIN_INDEX = null;
	protected static String STATEMENT_CREATE_USER_PREFERENCES_DEFAULTS_TABLE = null;
	protected static String STATEMENT_CREATE_USER_PREFERENCES_TABLE = null;
	protected static String STATEMENT_CREATE_USER_PREFERENCES_WIKI_USER_INDEX = null;
	protected static String STATEMENT_DELETE_AUTHORITIES = null;
	protected static String STATEMENT_DELETE_CONFIGURATION = null;
	protected static String STATEMENT_DELETE_GROUP_AUTHORITIES = null;
	protected static String STATEMENT_DELETE_GROUP_MAP_GROUP = null;
	protected static String STATEMENT_DELETE_GROUP_MAP_USER = null;
	protected static String STATEMENT_DELETE_INTERWIKI = null;
	protected static String STATEMENT_DELETE_LOG_ITEMS = null;
	protected static String STATEMENT_DELETE_LOG_ITEMS_BY_TOPIC_VERSION = null;
	protected static String STATEMENT_DELETE_NAMESPACE_TRANSLATIONS = null;
	protected static String STATEMENT_DELETE_RECENT_CHANGES = null;
	protected static String STATEMENT_DELETE_RECENT_CHANGES_TOPIC = null;
	protected static String STATEMENT_DELETE_RECENT_CHANGES_TOPIC_VERSION = null;
	protected static String STATEMENT_DELETE_TOPIC_CATEGORIES = null;
	protected static String STATEMENT_DELETE_TOPIC_LINKS = null;
	protected static String STATEMENT_DELETE_TOPIC_VERSION = null;
	protected static String STATEMENT_DELETE_WATCHLIST_ENTRY = null;
	protected static String STATEMENT_DELETE_USER_PREFERENCES = null;
	protected static String STATEMENT_DROP_AUTHORITIES_TABLE = null;
	protected static String STATEMENT_DROP_CATEGORY_TABLE = null;
	protected static String STATEMENT_DROP_CONFIGURATION_TABLE = null;
	protected static String STATEMENT_DROP_GROUP_AUTHORITIES_TABLE = null;
	protected static String STATEMENT_DROP_GROUP_MEMBERS_TABLE = null;
	protected static String STATEMENT_DROP_GROUP_TABLE = null;
	protected static String STATEMENT_DROP_INTERWIKI_TABLE = null;
	protected static String STATEMENT_DROP_LOG_TABLE = null;
	protected static String STATEMENT_DROP_NAMESPACE_TABLE = null;
	protected static String STATEMENT_DROP_NAMESPACE_TRANSLATION_TABLE = null;
	protected static String STATEMENT_DROP_RECENT_CHANGE_TABLE = null;
	protected static String STATEMENT_DROP_ROLE_TABLE = null;
	protected static String STATEMENT_DROP_TOPIC_CURRENT_VERSION_CONSTRAINT = null;
	protected static String STATEMENT_DROP_TOPIC_TABLE = null;
	protected static String STATEMENT_DROP_TOPIC_LINKS_TABLE = null;
	protected static String STATEMENT_DROP_TOPIC_VERSION_TABLE = null;
	protected static String STATEMENT_DROP_USER_BLOCK_TABLE = null;
	protected static String STATEMENT_DROP_USERS_TABLE = null;
	protected static String STATEMENT_DROP_VIRTUAL_WIKI_TABLE = null;
	protected static String STATEMENT_DROP_WATCHLIST_TABLE = null;
	protected static String STATEMENT_DROP_WIKI_FILE_TABLE = null;
	protected static String STATEMENT_DROP_WIKI_FILE_VERSION_TABLE = null;
	protected static String STATEMENT_DROP_WIKI_USER_TABLE = null;
	protected static String STATEMENT_INSERT_AUTHORITY = null;
	protected static String STATEMENT_INSERT_CATEGORY = null;
	protected static String STATEMENT_INSERT_CONFIGURATION = null;
	protected static String STATEMENT_INSERT_GROUP = null;
	protected static String STATEMENT_INSERT_GROUP_AUTO_INCREMENT = null;
	protected static String STATEMENT_INSERT_GROUP_AUTHORITY = null;
	protected static String STATEMENT_INSERT_GROUP_MEMBER = null;
	protected static String STATEMENT_INSERT_GROUP_MEMBER_AUTO_INCREMENT = null;
	protected static String STATEMENT_INSERT_INTERWIKI = null;
	protected static String STATEMENT_INSERT_LOG_ITEM = null;
	protected static String STATEMENT_INSERT_LOG_ITEMS_BLOCK = null;
	protected static String STATEMENT_INSERT_LOG_ITEMS_BY_TOPIC_VERSION_TYPE = null;
	protected static String STATEMENT_INSERT_LOG_ITEMS_IMPORT = null;
	protected static String STATEMENT_INSERT_LOG_ITEMS_MOVE = null;
	protected static String STATEMENT_INSERT_LOG_ITEMS_UNBLOCK = null;
	protected static String STATEMENT_INSERT_LOG_ITEMS_UPLOAD = null;
	protected static String STATEMENT_INSERT_LOG_ITEMS_USER = null;
	protected static String STATEMENT_INSERT_NAMESPACE = null;
	protected static String STATEMENT_INSERT_NAMESPACE_TRANSLATION = null;
	protected static String STATEMENT_INSERT_RECENT_CHANGE = null;
	protected static String STATEMENT_INSERT_RECENT_CHANGES_LOGS = null;
	protected static String STATEMENT_INSERT_RECENT_CHANGES_VERSIONS = null;
	protected static String STATEMENT_INSERT_ROLE = null;
	protected static String STATEMENT_INSERT_TOPIC = null;
	protected static String STATEMENT_INSERT_TOPIC_AUTO_INCREMENT = null;
	protected static String STATEMENT_INSERT_TOPIC_LINKS = null;
	protected static String STATEMENT_INSERT_TOPIC_VERSION = null;
	protected static String STATEMENT_INSERT_TOPIC_VERSION_AUTO_INCREMENT = null;
	protected static String STATEMENT_INSERT_USER = null;
	protected static String STATEMENT_INSERT_USER_BLOCK = null;
	protected static String STATEMENT_INSERT_USER_BLOCK_AUTO_INCREMENT = null;
	protected static String STATEMENT_INSERT_VIRTUAL_WIKI = null;
	protected static String STATEMENT_INSERT_VIRTUAL_WIKI_AUTO_INCREMENT = null;
	protected static String STATEMENT_INSERT_WATCHLIST_ENTRY = null;
	protected static String STATEMENT_INSERT_WIKI_FILE = null;
	protected static String STATEMENT_INSERT_WIKI_FILE_AUTO_INCREMENT = null;
	protected static String STATEMENT_INSERT_WIKI_FILE_VERSION = null;
	protected static String STATEMENT_INSERT_WIKI_FILE_VERSION_AUTO_INCREMENT = null;
	protected static String STATEMENT_INSERT_WIKI_USER = null;
	protected static String STATEMENT_INSERT_WIKI_USER_AUTO_INCREMENT = null;
	protected static String STATEMENT_INSERT_USER_PREFERENCE = null;
	protected static String STATEMENT_INSERT_USER_PREFERENCE_DEFAULTS = null;
	protected static String STATEMENT_SELECT_AUTHORITIES_AUTHORITY = null;
	protected static String STATEMENT_SELECT_AUTHORITIES_AUTHORITY_ALL = null;
	protected static String STATEMENT_SELECT_AUTHORITIES_LOGIN = null;
	protected static String STATEMENT_SELECT_AUTHORITIES_USER = null;
	protected static String STATEMENT_SELECT_CATEGORIES = null;
	protected static String STATEMENT_SELECT_CATEGORY_TOPICS = null;
	protected static String STATEMENT_SELECT_CONFIGURATION = null;
	protected static String STATEMENT_SELECT_GROUP_MAP_GROUP = null;
	protected static String STATEMENT_SELECT_GROUP_MAP_USER = null;
	protected static String STATEMENT_SELECT_GROUP_MAP_AUTHORITIES = null;
	protected static String STATEMENT_SELECT_GROUPS = null;
	protected static String STATEMENT_SELECT_GROUP = null;
	protected static String STATEMENT_SELECT_GROUP_BY_ID = null;
	protected static String STATEMENT_SELECT_GROUP_AUTHORITIES = null;
	protected static String STATEMENT_SELECT_GROUPS_AUTHORITIES = null;
	protected static String STATEMENT_SELECT_GROUP_MEMBERS_SEQUENCE = null;
	protected static String STATEMENT_SELECT_GROUP_SEQUENCE = null;
	protected static String STATEMENT_SELECT_INTERWIKIS = null;
	protected static String STATEMENT_SELECT_LOG_ITEMS = null;
	protected static String STATEMENT_SELECT_LOG_ITEMS_BY_TYPE = null;
	protected static String STATEMENT_SELECT_NAMESPACE_SEQUENCE = null;
	protected static String STATEMENT_SELECT_NAMESPACES = null;
	protected static String STATEMENT_SELECT_PW_RESET_CHALLENGE_DATA = null;
	protected static String STATEMENT_SELECT_RECENT_CHANGES = null;
	protected static String STATEMENT_SELECT_ROLES = null;
	protected static String STATEMENT_SELECT_TOPIC_BY_ID = null;
	protected static String STATEMENT_SELECT_TOPIC_BY_TYPE = null;
	protected static String STATEMENT_SELECT_TOPIC_COUNT = null;
	protected static String STATEMENT_SELECT_TOPIC = null;
	protected static String STATEMENT_SELECT_TOPIC_HISTORY = null;
	protected static String STATEMENT_SELECT_TOPIC_LINK_ORPHANS = null;
	protected static String STATEMENT_SELECT_TOPIC_LINKS = null;
	protected static String STATEMENT_SELECT_TOPIC_LOWER = null;
	protected static String STATEMENT_SELECT_TOPIC_NAME = null;
	protected static String STATEMENT_SELECT_TOPIC_NAME_LOWER = null;
	protected static String STATEMENT_SELECT_TOPIC_NAMES = null;
	protected static String STATEMENT_SELECT_TOPICS_ADMIN = null;
	protected static String STATEMENT_SELECT_TOPIC_SEQUENCE = null;
	protected static String STATEMENT_SELECT_TOPIC_VERSION = null;
	protected static String STATEMENT_SELECT_TOPIC_VERSION_NEXT_ID = null;
	protected static String STATEMENT_SELECT_TOPIC_VERSION_SEQUENCE = null;
	protected static String STATEMENT_SELECT_USER_BLOCKS = null;
	protected static String STATEMENT_SELECT_USER_BLOCK_SEQUENCE = null;
	protected static String STATEMENT_SELECT_USERS_AUTHENTICATION = null;
	protected static String STATEMENT_SELECT_VIRTUAL_WIKIS = null;
	protected static String STATEMENT_SELECT_VIRTUAL_WIKI_SEQUENCE = null;
	protected static String STATEMENT_SELECT_WATCHLIST = null;
	protected static String STATEMENT_SELECT_WATCHLIST_CHANGES = null;
	protected static String STATEMENT_SELECT_WIKI_FILE = null;
	protected static String STATEMENT_SELECT_WIKI_FILE_COUNT = null;
	protected static String STATEMENT_SELECT_WIKI_FILE_SEQUENCE = null;
	protected static String STATEMENT_SELECT_WIKI_FILE_VERSION_SEQUENCE = null;
	protected static String STATEMENT_SELECT_WIKI_FILE_VERSIONS = null;
	protected static String STATEMENT_SELECT_WIKI_USER = null;
	protected static String STATEMENT_SELECT_WIKI_USER_CHANGES_ANONYMOUS = null;
	protected static String STATEMENT_SELECT_WIKI_USER_CHANGES_LOGIN = null;
	protected static String STATEMENT_SELECT_WIKI_USER_COUNT = null;
	protected static String STATEMENT_SELECT_WIKI_USER_DETAILS_PASSWORD = null;
	protected static String STATEMENT_SELECT_WIKI_USER_LOGIN = null;
	protected static String STATEMENT_SELECT_WIKI_USER_SEQUENCE = null;
	protected static String STATEMENT_SELECT_WIKI_USERS = null;
	protected static String STATEMENT_SELECT_USER_PREFERENCES_DEFAULTS = null;
	protected static String STATEMENT_SELECT_USER_PREFERENCES = null;
	protected static String STATEMENT_UPDATE_GROUP = null;
	protected static String STATEMENT_UPDATE_ROLE = null;
	protected static String STATEMENT_UPDATE_NAMESPACE = null;
	protected static String STATEMENT_UPDATE_PW_RESET_CHALLENGE_DATA = null;
	protected static String STATEMENT_UPDATE_RECENT_CHANGES_PREVIOUS_VERSION_ID = null;
	protected static String STATEMENT_UPDATE_TOPIC = null;
	protected static String STATEMENT_UPDATE_TOPIC_NAMESPACE = null;
	protected static String STATEMENT_UPDATE_TOPIC_VERSION = null;
	protected static String STATEMENT_UPDATE_TOPIC_VERSION_PREVIOUS_VERSION_ID = null;
	protected static String STATEMENT_UPDATE_USER = null;
	protected static String STATEMENT_UPDATE_USER_BLOCK = null;
	protected static String STATEMENT_UPDATE_VIRTUAL_WIKI = null;
	protected static String STATEMENT_UPDATE_WIKI_FILE = null;
	protected static String STATEMENT_UPDATE_WIKI_USER = null;
	protected static String STATEMENT_UPDATE_USER_PREFERENCE_DEFAULTS = null;
	protected static String STATEMENT_CREATE_FILE_DATA_TABLE = null;
	protected static String STATEMENT_DROP_FILE_DATA_TABLE = null;
	protected static String STATEMENT_INSERT_FILE_DATA = null;
	protected static String STATEMENT_DELETE_RESIZED_IMAGES = null;
	protected static String STATEMENT_SELECT_FILE_INFO = null;
	protected static String STATEMENT_SELECT_FILE_DATA = null;
	protected static String STATEMENT_SELECT_FILE_VERSION_DATA = null;
	protected static String STATEMENT_CREATE_SEQUENCES = null;
	protected static String STATEMENT_DROP_SEQUENCES = null;
	private Properties props = null;

	/**
	 *
	 */
	protected AnsiQueryHandler() {
		props = Environment.loadProperties(SQL_PROPERTY_FILE_NAME);
		this.init(props);
	}

	/**
	 *
	 */
	public boolean authenticateUser(String username, String encryptedPassword) {
		Object[] args = { username, encryptedPassword };
		try {
			DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_USERS_AUTHENTICATION, args, String.class);
			return true;
		} catch (IncorrectResultSizeDataAccessException e) {
			// invalid username / password
			return false;
		}
	}

	/**
	 *
	 */
	public boolean autoIncrementPrimaryKeys() {
		return false;
	}

	/**
	 *
	 */
	public String connectionValidationQuery() {
		return STATEMENT_CONNECTION_VALIDATION_QUERY;
	}

	/**
	 *
	 */
	public void deleteGroupAuthorities(int groupId) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_GROUP_AUTHORITIES,
				groupId
		);
	}

	/**
	 *
	 */
	public void deleteGroupMap(GroupMap groupMap) {
		if (groupMap.getGroupMapType() == GroupMap.GROUP_MAP_GROUP) {
			DatabaseConnection.getJdbcTemplate().update(
					STATEMENT_DELETE_GROUP_MAP_GROUP,
					groupMap.getGroupId()
			);
		} else if (groupMap.getGroupMapType() == GroupMap.GROUP_MAP_USER) {
			DatabaseConnection.getJdbcTemplate().update(
					STATEMENT_DELETE_GROUP_MAP_USER,
					groupMap.getUserLogin()
			);
		}
	}

	/**
	 *
	 */
	public void deleteInterwiki(Interwiki interwiki) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_INTERWIKI,
				interwiki.getInterwikiPrefix()
		);
	}

	/**
	 *
	 */
	public void deleteRecentChanges(int topicId) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_RECENT_CHANGES_TOPIC,
				topicId
		);
	}

	/**
	 *
	 */
	public void deleteTopicCategories(int childTopicId) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_TOPIC_CATEGORIES,
				childTopicId
		);
	}

	/**
	 *
	 */
	public void deleteTopicLinks(int topicId) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_TOPIC_LINKS,
				topicId
		);
	}

	/**
	 *
	 */
	public void deleteTopicVersion(int topicVersionId, Integer previousTopicVersionId) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_LOG_ITEMS_BY_TOPIC_VERSION,
				topicVersionId
		);
		// delete references to the topic version from the recent changes table
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_RECENT_CHANGES_TOPIC_VERSION,
				topicVersionId
		);
		// update any recent changes that refer to this record as the previous record
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_UPDATE_RECENT_CHANGES_PREVIOUS_VERSION_ID,
				previousTopicVersionId,
				topicVersionId
		);
		// delete the topic version record
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_TOPIC_VERSION,
				topicVersionId
		);
	}

	/**
	 *
	 */
	public void deleteUserAuthorities(String username) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_AUTHORITIES,
				username
		);
	}

	/**
	 *
	 */
	public void deleteWatchlistEntry(int virtualWikiId, String topicName, int userId) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_WATCHLIST_ENTRY,
				virtualWikiId,
				topicName,
				userId
		);
	}

	/**
	 * Execute an insert that returns a generated key value.
	 */
	private int executeGeneratedKeyInsert(String insertSql, Object[] args, int[] types, String primaryKeyColumnName) {
		PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(insertSql, types);
		String[] keys = { primaryKeyColumnName };
		factory.setGeneratedKeysColumnNames(keys);
		factory.setReturnGeneratedKeys(true);
		KeyHolder keyHolder = new GeneratedKeyHolder();
		DatabaseConnection.getJdbcTemplate().update(factory.newPreparedStatementCreator(args), keyHolder);
		return keyHolder.getKey().intValue();
	}

	/**
	 * Return a simple query, that if successfully run indicates that JAMWiki
	 * tables have been initialized in the database.  This method should not
	 * be overridden as it is directly invoked by the
	 * {@link DatabaseConnection#testDatabase} method and should thus be used
	 * in its base class form for all databases.
	 *
	 * @return Returns a simple query that, if successfully run, indicates
	 *  that JAMWiki tables have been set up in the database.
	 */
	public final String existenceValidationQuery() {
		return STATEMENT_SELECT_VIRTUAL_WIKIS;
	}

	/**
	 * In rare cases a single statement cannot easily be used across databases, such
	 * as "date is null" and "date is not null".  Rather than having two separate
	 * SQL statements the statement is instead "date is {0} null", and a Java
	 * MessageFormat object is then used to modify the SQL.
	 *
	 * @param sql The SQL statement in MessageFormat format ("date is {0} null").
	 * @param params An array of objects (which should be strings) to use when
	 *  formatting the message.
	 * @return A formatted SQL string.
	 */
	protected String formatStatement(String sql, Object[] params) {
		if (params == null || params.length == 0) {
			return sql;
		}
		try {
			// replace all single quotes with '' since otherwise MessageFormat
			// will treat the content is a quoted string
			return MessageFormat.format(sql.replaceAll("'", "''"), params);
		} catch (IllegalArgumentException e) {
			String msg = "Unable to format " + sql + " with values: ";
			for (int i = 0; i < params.length; i++) {
				msg += (i > 0) ? " | " + params[i] : params[i];
			}
			logger.warn(msg);
			return null;
		}
	}

	/**
	 *
	 */
	public List<WikiFileVersion> getAllWikiFileVersions(WikiFile wikiFile, boolean descending) {
		// FIXME - sort order ignored
		Object[] args = { wikiFile.getFileId() };
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_WIKI_FILE_VERSIONS, args, new WikiFileVersionMapper());
	}

	/**
	 *
	 */
	public List<Category> getCategories(int virtualWikiId, String virtualWikiName, Pagination pagination) {
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_CATEGORIES,
				virtualWikiId,
				pagination.getNumResults(),
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
		args[index++] = pagination.getNumResults();
		args[index++] = pagination.getOffset();
		return DatabaseConnection.getJdbcTemplate().query(sql, args, new LogItemMapper(virtualWikiName));
	}

	/**
	 *
	 */
	public List<RecentChange> getRecentChanges(String virtualWiki, Pagination pagination, boolean descending) {
		// FIXME - sort order ignored
		Object[] args = {
				virtualWiki,
				pagination.getNumResults(),
				pagination.getOffset()
		};
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_RECENT_CHANGES, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	public List<RoleMap> getRoleMapByLogin(String loginFragment) {
		if (StringUtils.isBlank(loginFragment)) {
			return new ArrayList<RoleMap>();
		}
		loginFragment = '%' + loginFragment.toLowerCase() + '%';
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_AUTHORITIES_LOGIN,
				loginFragment
		);
		LinkedHashMap<Integer, RoleMap> roleMaps = new LinkedHashMap<Integer, RoleMap>();
		for (Map<String, Object> result : results) {
			Integer userId = (Integer)result.get("wiki_user_id");
			RoleMap roleMap = new RoleMap();
			if (roleMaps.containsKey(userId)) {
				roleMap = roleMaps.get(userId);
			} else {
				roleMap.setUserId(userId);
				roleMap.setUserLogin((String)result.get("username"));
			}
			roleMap.addRole((String)result.get("authority"));
			roleMaps.put(userId, roleMap);
		}
		return new ArrayList<RoleMap>(roleMaps.values());
	}

	/**
	 *
	 */
	public List<RoleMap> getRoleMapByRole(String authority,boolean includeInheritedRoles) {
		List<Map<String, Object>> results = null;
		if (includeInheritedRoles) {
			results = DatabaseConnection.getJdbcTemplate().queryForList(
					STATEMENT_SELECT_AUTHORITIES_AUTHORITY_ALL,
					authority,
					authority,
					authority,
					authority
			);
		} else {
			results = DatabaseConnection.getJdbcTemplate().queryForList(
					STATEMENT_SELECT_AUTHORITIES_AUTHORITY,
					authority,
					authority
			);
		}
		LinkedHashMap<String, RoleMap> roleMaps = new LinkedHashMap<String, RoleMap>();
		for (Map<String, Object> result : results) {
			int userId = (Integer)result.get("wiki_user_id");
			int groupId = (Integer)result.get("group_id");
			RoleMap roleMap = new RoleMap();
			String key = userId + "|" + groupId;
			if (roleMaps.containsKey(key)) {
				roleMap = roleMaps.get(key);
			} else {
				if (userId > 0) {
					roleMap.setUserId(userId);
					roleMap.setUserLogin((String)result.get("username"));
				}
				if (groupId > 0) {
					roleMap.setGroupId(groupId);
					roleMap.setGroupName((String)result.get("group_name"));
				}
			}
			String roleName = (String)result.get("authority");
			if (roleName != null) {
				roleMap.addRole(roleName);
			}
			roleMaps.put(key, roleMap);
		}
		return new ArrayList<RoleMap>(roleMaps.values());
	}

	/**
	 *
	 */
	public List<Role> getRoleMapGroup(String groupName) {
		Object[] args = { groupName };
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_GROUP_AUTHORITIES, args, new RoleMapper());
	}

	/**
	 *
	 */
	public List<RoleMap> getRoleMapGroups() {
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_GROUPS_AUTHORITIES
		);
		LinkedHashMap<Integer, RoleMap> roleMaps = new LinkedHashMap<Integer, RoleMap>();
		for (Map<String, Object> result : results) {
			Integer groupId = (Integer)result.get("group_id");
			RoleMap roleMap = new RoleMap();
			if (roleMaps.containsKey(groupId)) {
				roleMap = roleMaps.get(groupId);
			} else {
				roleMap.setGroupId(groupId);
				roleMap.setGroupName((String)result.get("group_name"));
			}
			roleMap.addRole((String)result.get("authority"));
			roleMaps.put(groupId, roleMap);
		}
		return new ArrayList<RoleMap>(roleMaps.values());
	}

	/**
	 *
	 */
	public List<Role> getRoleMapUser(String login) {
		Object[] args = { login, login };
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_AUTHORITIES_USER, args, new RoleMapper());
	}

	/**
	 *
	 */
	public List<Role> getRoles() {
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_ROLES, new RoleMapper());
	}

	/**
	 *
	 */
	public List<WikiGroup> getGroups() {
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_GROUPS, new WikiGroupMapper());
	}

	/**
	 *
	 */
	public LinkedHashMap<String, Map<String, String>> getUserPreferencesDefaults() {
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_USER_PREFERENCES_DEFAULTS
		);
		// the map of groups containing the maps to their preferences
		LinkedHashMap<String, Map<String, String>> groups = new LinkedHashMap<String, Map<String, String>>();
		LinkedHashMap<String, String> defaultPreferences = null;
		String lastGroup = null;
		for (Map<String, Object> result : results) {
			// get the group name
			String group = (String)result.get("pref_group_key");
			// test if we need a new list of items for a new group
			if (group != null && (lastGroup == null || !lastGroup.equals(group))) {
				lastGroup = group;
				defaultPreferences = new LinkedHashMap<String, String>();
			}
			String key = (String)result.get("pref_key");
			String value = (String)result.get("pref_value");
			defaultPreferences.put(key, value);
			groups.put(group, defaultPreferences);
		}
		return groups;
	}

	/**
	 *
	 */
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
				pagination.getNumResults(),
				pagination.getOffset()
		};
		return DatabaseConnection.getJdbcTemplate().query(sql, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	public List<String> getTopicsAdmin(int virtualWikiId, Pagination pagination) {
		Object[] args = {
				virtualWikiId,
				pagination.getNumResults(),
				pagination.getOffset()
		};
		return DatabaseConnection.getJdbcTemplate().queryForList(STATEMENT_SELECT_TOPICS_ADMIN, args, String.class);
	}

	/**
	 *
	 */
	public List<UserBlock> getUserBlocks() {
		Object[] args = {
				new Timestamp(System.currentTimeMillis())
		};
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_USER_BLOCKS, args, new UserBlockMapper());
	}

	/**
	 *
	 */
	public List<RecentChange> getUserContributionsByLogin(String virtualWiki, String login, Pagination pagination, boolean descending) {
		// FIXME - sort order ignored
		Object[] args = {
				virtualWiki,
				login,
				pagination.getNumResults(),
				pagination.getOffset()
		};
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_WIKI_USER_CHANGES_LOGIN, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	public List<RecentChange> getUserContributionsByUserDisplay(String virtualWiki, String userDisplay, Pagination pagination, boolean descending) {
		// FIXME - sort order ignored
		Object[] args = {
				virtualWiki,
				userDisplay,
				pagination.getNumResults(),
				pagination.getOffset()
		};
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_WIKI_USER_CHANGES_ANONYMOUS, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	public List<VirtualWiki> getVirtualWikis() {
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_VIRTUAL_WIKIS, new VirtualWikiMapper());
	}

	/**
	 *
	 */
	public List<String> getWatchlist(int virtualWikiId, int userId) {
		Object[] args = { virtualWikiId, userId };
		return DatabaseConnection.getJdbcTemplate().queryForList(STATEMENT_SELECT_WATCHLIST, args, String.class);
	}

	/**
	 *
	 */
	public List<RecentChange> getWatchlist(int virtualWikiId, int userId, Pagination pagination) {
		Object[] args = {
				virtualWikiId,
				userId,
				pagination.getNumResults(),
				pagination.getOffset()
		};
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_WATCHLIST_CHANGES, args, new RecentChangeMapper());
	}

	/**
	 *
	 */
	protected void init(Properties properties) {
		this.props = properties;
		STATEMENT_CONNECTION_VALIDATION_QUERY    = props.getProperty("STATEMENT_CONNECTION_VALIDATION_QUERY");
		STATEMENT_CREATE_CONFIGURATION_TABLE     = props.getProperty("STATEMENT_CREATE_CONFIGURATION_TABLE");
		STATEMENT_CREATE_GROUP_TABLE             = props.getProperty("STATEMENT_CREATE_GROUP_TABLE");
		STATEMENT_CREATE_INTERWIKI_TABLE         = props.getProperty("STATEMENT_CREATE_INTERWIKI_TABLE");
		STATEMENT_CREATE_NAMESPACE_TABLE         = props.getProperty("STATEMENT_CREATE_NAMESPACE_TABLE");
		STATEMENT_CREATE_NAMESPACE_TRANSLATION_TABLE = props.getProperty("STATEMENT_CREATE_NAMESPACE_TRANSLATION_TABLE");
		STATEMENT_CREATE_ROLE_TABLE              = props.getProperty("STATEMENT_CREATE_ROLE_TABLE");
		STATEMENT_CREATE_VIRTUAL_WIKI_TABLE      = props.getProperty("STATEMENT_CREATE_VIRTUAL_WIKI_TABLE");
		STATEMENT_CREATE_WIKI_USER_TABLE         = props.getProperty("STATEMENT_CREATE_WIKI_USER_TABLE");
		STATEMENT_CREATE_WIKI_USER_LOGIN_INDEX   = props.getProperty("STATEMENT_CREATE_WIKI_USER_LOGIN_INDEX");
		STATEMENT_CREATE_USER_PREFERENCES_DEFAULTS_TABLE = props.getProperty("STATEMENT_CREATE_USER_PREFERENCES_DEFAULTS_TABLE");
		STATEMENT_CREATE_USER_PREFERENCES_TABLE  = props.getProperty("STATEMENT_CREATE_USER_PREFERENCES_TABLE");
		STATEMENT_CREATE_USER_PREFERENCES_WIKI_USER_INDEX = props.getProperty("STATEMENT_CREATE_USER_PREFERENCES_WIKI_USER_INDEX");
		STATEMENT_CREATE_TOPIC_CURRENT_VERSION_CONSTRAINT = props.getProperty("STATEMENT_CREATE_TOPIC_CURRENT_VERSION_CONSTRAINT");
		STATEMENT_CREATE_TOPIC_TABLE             = props.getProperty("STATEMENT_CREATE_TOPIC_TABLE");
		STATEMENT_CREATE_TOPIC_LINKS_TABLE       = props.getProperty("STATEMENT_CREATE_TOPIC_LINKS_TABLE");
		STATEMENT_CREATE_TOPIC_LINKS_INDEX       = props.getProperty("STATEMENT_CREATE_TOPIC_LINKS_INDEX");
		STATEMENT_CREATE_TOPIC_PAGE_NAME_INDEX   = props.getProperty("STATEMENT_CREATE_TOPIC_PAGE_NAME_INDEX");
		STATEMENT_CREATE_TOPIC_PAGE_NAME_LOWER_INDEX = props.getProperty("STATEMENT_CREATE_TOPIC_PAGE_NAME_LOWER_INDEX");
		STATEMENT_CREATE_TOPIC_NAMESPACE_INDEX   = props.getProperty("STATEMENT_CREATE_TOPIC_NAMESPACE_INDEX");
		STATEMENT_CREATE_TOPIC_VIRTUAL_WIKI_INDEX = props.getProperty("STATEMENT_CREATE_TOPIC_VIRTUAL_WIKI_INDEX");
		STATEMENT_CREATE_TOPIC_CURRENT_VERSION_INDEX = props.getProperty("STATEMENT_CREATE_TOPIC_CURRENT_VERSION_INDEX");
		STATEMENT_CREATE_TOPIC_VERSION_TABLE     = props.getProperty("STATEMENT_CREATE_TOPIC_VERSION_TABLE");
		STATEMENT_CREATE_TOPIC_VERSION_TOPIC_INDEX = props.getProperty("STATEMENT_CREATE_TOPIC_VERSION_TOPIC_INDEX");
		STATEMENT_CREATE_TOPIC_VERSION_PREVIOUS_INDEX = props.getProperty("STATEMENT_CREATE_TOPIC_VERSION_PREVIOUS_INDEX");
		STATEMENT_CREATE_TOPIC_VERSION_USER_DISPLAY_INDEX = props.getProperty("STATEMENT_CREATE_TOPIC_VERSION_USER_DISPLAY_INDEX");
		STATEMENT_CREATE_TOPIC_VERSION_USER_ID_INDEX = props.getProperty("STATEMENT_CREATE_TOPIC_VERSION_USER_ID_INDEX");
		STATEMENT_CREATE_USER_BLOCK_TABLE        = props.getProperty("STATEMENT_CREATE_USER_BLOCK_TABLE");
		STATEMENT_CREATE_USERS_TABLE             = props.getProperty("STATEMENT_CREATE_USERS_TABLE");
		STATEMENT_CREATE_WIKI_FILE_TABLE         = props.getProperty("STATEMENT_CREATE_WIKI_FILE_TABLE");
		STATEMENT_CREATE_WIKI_FILE_VERSION_TABLE = props.getProperty("STATEMENT_CREATE_WIKI_FILE_VERSION_TABLE");
		STATEMENT_CREATE_AUTHORITIES_TABLE       = props.getProperty("STATEMENT_CREATE_AUTHORITIES_TABLE");
		STATEMENT_CREATE_CATEGORY_TABLE          = props.getProperty("STATEMENT_CREATE_CATEGORY_TABLE");
		STATEMENT_CREATE_CATEGORY_INDEX          = props.getProperty("STATEMENT_CREATE_CATEGORY_INDEX");
		STATEMENT_CREATE_GROUP_AUTHORITIES_TABLE = props.getProperty("STATEMENT_CREATE_GROUP_AUTHORITIES_TABLE");
		STATEMENT_CREATE_GROUP_MEMBERS_TABLE     = props.getProperty("STATEMENT_CREATE_GROUP_MEMBERS_TABLE");
		STATEMENT_CREATE_LOG_TABLE               = props.getProperty("STATEMENT_CREATE_LOG_TABLE");
		STATEMENT_CREATE_RECENT_CHANGE_TABLE     = props.getProperty("STATEMENT_CREATE_RECENT_CHANGE_TABLE");
		STATEMENT_CREATE_WATCHLIST_TABLE         = props.getProperty("STATEMENT_CREATE_WATCHLIST_TABLE");
		STATEMENT_DELETE_AUTHORITIES             = props.getProperty("STATEMENT_DELETE_AUTHORITIES");
		STATEMENT_DELETE_CONFIGURATION           = props.getProperty("STATEMENT_DELETE_CONFIGURATION");
		STATEMENT_DELETE_GROUP_AUTHORITIES       = props.getProperty("STATEMENT_DELETE_GROUP_AUTHORITIES");
		STATEMENT_DELETE_GROUP_MAP_GROUP         = props.getProperty("STATEMENT_DELETE_GROUP_MAP_GROUP");
		STATEMENT_DELETE_GROUP_MAP_USER          = props.getProperty("STATEMENT_DELETE_GROUP_MAP_USER");
		STATEMENT_DELETE_INTERWIKI               = props.getProperty("STATEMENT_DELETE_INTERWIKI");
		STATEMENT_DELETE_LOG_ITEMS               = props.getProperty("STATEMENT_DELETE_LOG_ITEMS");
		STATEMENT_DELETE_LOG_ITEMS_BY_TOPIC_VERSION = props.getProperty("STATEMENT_DELETE_LOG_ITEMS_BY_TOPIC_VERSION");
		STATEMENT_DELETE_NAMESPACE_TRANSLATIONS  = props.getProperty("STATEMENT_DELETE_NAMESPACE_TRANSLATIONS");
		STATEMENT_DELETE_RECENT_CHANGES          = props.getProperty("STATEMENT_DELETE_RECENT_CHANGES");
		STATEMENT_DELETE_RECENT_CHANGES_TOPIC    = props.getProperty("STATEMENT_DELETE_RECENT_CHANGES_TOPIC");
		STATEMENT_DELETE_RECENT_CHANGES_TOPIC_VERSION = props.getProperty("STATEMENT_DELETE_RECENT_CHANGES_TOPIC_VERSION");
		STATEMENT_DELETE_TOPIC_CATEGORIES        = props.getProperty("STATEMENT_DELETE_TOPIC_CATEGORIES");
		STATEMENT_DELETE_TOPIC_LINKS             = props.getProperty("STATEMENT_DELETE_TOPIC_LINKS");
		STATEMENT_DELETE_TOPIC_VERSION           = props.getProperty("STATEMENT_DELETE_TOPIC_VERSION");
		STATEMENT_DELETE_WATCHLIST_ENTRY         = props.getProperty("STATEMENT_DELETE_WATCHLIST_ENTRY");
		STATEMENT_DELETE_USER_PREFERENCES        = props.getProperty("STATEMENT_DELETE_USER_PREFERENCES");
		STATEMENT_DROP_AUTHORITIES_TABLE         = props.getProperty("STATEMENT_DROP_AUTHORITIES_TABLE");
		STATEMENT_DROP_CATEGORY_TABLE            = props.getProperty("STATEMENT_DROP_CATEGORY_TABLE");
		STATEMENT_DROP_CONFIGURATION_TABLE       = props.getProperty("STATEMENT_DROP_CONFIGURATION_TABLE");
		STATEMENT_DROP_GROUP_AUTHORITIES_TABLE   = props.getProperty("STATEMENT_DROP_GROUP_AUTHORITIES_TABLE");
		STATEMENT_DROP_GROUP_MEMBERS_TABLE       = props.getProperty("STATEMENT_DROP_GROUP_MEMBERS_TABLE");
		STATEMENT_DROP_GROUP_TABLE               = props.getProperty("STATEMENT_DROP_GROUP_TABLE");
		STATEMENT_DROP_INTERWIKI_TABLE           = props.getProperty("STATEMENT_DROP_INTERWIKI_TABLE");
		STATEMENT_DROP_LOG_TABLE                 = props.getProperty("STATEMENT_DROP_LOG_TABLE");
		STATEMENT_DROP_NAMESPACE_TABLE           = props.getProperty("STATEMENT_DROP_NAMESPACE_TABLE");
		STATEMENT_DROP_NAMESPACE_TRANSLATION_TABLE = props.getProperty("STATEMENT_DROP_NAMESPACE_TRANSLATION_TABLE");
		STATEMENT_DROP_RECENT_CHANGE_TABLE       = props.getProperty("STATEMENT_DROP_RECENT_CHANGE_TABLE");
		STATEMENT_DROP_ROLE_TABLE                = props.getProperty("STATEMENT_DROP_ROLE_TABLE");
		STATEMENT_DROP_TOPIC_CURRENT_VERSION_CONSTRAINT = props.getProperty("STATEMENT_DROP_TOPIC_CURRENT_VERSION_CONSTRAINT");
		STATEMENT_DROP_TOPIC_TABLE               = props.getProperty("STATEMENT_DROP_TOPIC_TABLE");
		STATEMENT_DROP_TOPIC_LINKS_TABLE         = props.getProperty("STATEMENT_DROP_TOPIC_LINKS_TABLE");
		STATEMENT_DROP_TOPIC_VERSION_TABLE       = props.getProperty("STATEMENT_DROP_TOPIC_VERSION_TABLE");
		STATEMENT_DROP_USER_BLOCK_TABLE          = props.getProperty("STATEMENT_DROP_USER_BLOCK_TABLE");
		STATEMENT_DROP_USERS_TABLE               = props.getProperty("STATEMENT_DROP_USERS_TABLE");
		STATEMENT_DROP_VIRTUAL_WIKI_TABLE        = props.getProperty("STATEMENT_DROP_VIRTUAL_WIKI_TABLE");
		STATEMENT_DROP_WATCHLIST_TABLE           = props.getProperty("STATEMENT_DROP_WATCHLIST_TABLE");
		STATEMENT_DROP_WIKI_USER_TABLE           = props.getProperty("STATEMENT_DROP_WIKI_USER_TABLE");
		STATEMENT_DROP_WIKI_FILE_TABLE           = props.getProperty("STATEMENT_DROP_WIKI_FILE_TABLE");
		STATEMENT_DROP_WIKI_FILE_VERSION_TABLE   = props.getProperty("STATEMENT_DROP_WIKI_FILE_VERSION_TABLE");
		STATEMENT_INSERT_AUTHORITY               = props.getProperty("STATEMENT_INSERT_AUTHORITY");
		STATEMENT_INSERT_CATEGORY                = props.getProperty("STATEMENT_INSERT_CATEGORY");
		STATEMENT_INSERT_CONFIGURATION           = props.getProperty("STATEMENT_INSERT_CONFIGURATION");
		STATEMENT_INSERT_GROUP                   = props.getProperty("STATEMENT_INSERT_GROUP");
		STATEMENT_INSERT_GROUP_AUTO_INCREMENT    = props.getProperty("STATEMENT_INSERT_GROUP_AUTO_INCREMENT");
		STATEMENT_INSERT_GROUP_AUTHORITY         = props.getProperty("STATEMENT_INSERT_GROUP_AUTHORITY");
		STATEMENT_INSERT_GROUP_MEMBER            = props.getProperty("STATEMENT_INSERT_GROUP_MEMBER");
		STATEMENT_INSERT_GROUP_MEMBER_AUTO_INCREMENT = props.getProperty("STATEMENT_INSERT_GROUP_MEMBER_AUTO_INCREMENT");
		STATEMENT_INSERT_INTERWIKI               = props.getProperty("STATEMENT_INSERT_INTERWIKI");
		STATEMENT_INSERT_LOG_ITEM                = props.getProperty("STATEMENT_INSERT_LOG_ITEM");
		STATEMENT_INSERT_LOG_ITEMS_BLOCK         = props.getProperty("STATEMENT_INSERT_LOG_ITEMS_BLOCK");
		STATEMENT_INSERT_LOG_ITEMS_BY_TOPIC_VERSION_TYPE = props.getProperty("STATEMENT_INSERT_LOG_ITEMS_BY_TOPIC_VERSION_TYPE");
		STATEMENT_INSERT_LOG_ITEMS_IMPORT        = props.getProperty("STATEMENT_INSERT_LOG_ITEMS_IMPORT");
		STATEMENT_INSERT_LOG_ITEMS_MOVE          = props.getProperty("STATEMENT_INSERT_LOG_ITEMS_MOVE");
		STATEMENT_INSERT_LOG_ITEMS_UNBLOCK       = props.getProperty("STATEMENT_INSERT_LOG_ITEMS_UNBLOCK");
		STATEMENT_INSERT_LOG_ITEMS_UPLOAD        = props.getProperty("STATEMENT_INSERT_LOG_ITEMS_UPLOAD");
		STATEMENT_INSERT_LOG_ITEMS_USER          = props.getProperty("STATEMENT_INSERT_LOG_ITEMS_USER");
		STATEMENT_INSERT_NAMESPACE               = props.getProperty("STATEMENT_INSERT_NAMESPACE");
		STATEMENT_INSERT_NAMESPACE_TRANSLATION   = props.getProperty("STATEMENT_INSERT_NAMESPACE_TRANSLATION");
		STATEMENT_INSERT_RECENT_CHANGE           = props.getProperty("STATEMENT_INSERT_RECENT_CHANGE");
		STATEMENT_INSERT_RECENT_CHANGES_LOGS     = props.getProperty("STATEMENT_INSERT_RECENT_CHANGES_LOGS");
		STATEMENT_INSERT_RECENT_CHANGES_VERSIONS = props.getProperty("STATEMENT_INSERT_RECENT_CHANGES_VERSIONS");
		STATEMENT_INSERT_ROLE                    = props.getProperty("STATEMENT_INSERT_ROLE");
		STATEMENT_INSERT_TOPIC                   = props.getProperty("STATEMENT_INSERT_TOPIC");
		STATEMENT_INSERT_TOPIC_AUTO_INCREMENT    = props.getProperty("STATEMENT_INSERT_TOPIC_AUTO_INCREMENT");
		STATEMENT_INSERT_TOPIC_LINKS             = props.getProperty("STATEMENT_INSERT_TOPIC_LINKS");
		STATEMENT_INSERT_TOPIC_VERSION           = props.getProperty("STATEMENT_INSERT_TOPIC_VERSION");
		STATEMENT_INSERT_TOPIC_VERSION_AUTO_INCREMENT = props.getProperty("STATEMENT_INSERT_TOPIC_VERSION_AUTO_INCREMENT");
		STATEMENT_INSERT_USER                    = props.getProperty("STATEMENT_INSERT_USER");
		STATEMENT_INSERT_USER_BLOCK              = props.getProperty("STATEMENT_INSERT_USER_BLOCK");
		STATEMENT_INSERT_USER_BLOCK_AUTO_INCREMENT = props.getProperty("STATEMENT_INSERT_USER_BLOCK_AUTO_INCREMENT");
		STATEMENT_INSERT_VIRTUAL_WIKI            = props.getProperty("STATEMENT_INSERT_VIRTUAL_WIKI");
		STATEMENT_INSERT_VIRTUAL_WIKI_AUTO_INCREMENT = props.getProperty("STATEMENT_INSERT_VIRTUAL_WIKI_AUTO_INCREMENT");
		STATEMENT_INSERT_WATCHLIST_ENTRY         = props.getProperty("STATEMENT_INSERT_WATCHLIST_ENTRY");
		STATEMENT_INSERT_WIKI_FILE               = props.getProperty("STATEMENT_INSERT_WIKI_FILE");
		STATEMENT_INSERT_WIKI_FILE_AUTO_INCREMENT = props.getProperty("STATEMENT_INSERT_WIKI_FILE_AUTO_INCREMENT");
		STATEMENT_INSERT_WIKI_FILE_VERSION       = props.getProperty("STATEMENT_INSERT_WIKI_FILE_VERSION");
		STATEMENT_INSERT_WIKI_FILE_VERSION_AUTO_INCREMENT = props.getProperty("STATEMENT_INSERT_WIKI_FILE_VERSION_AUTO_INCREMENT");
		STATEMENT_INSERT_WIKI_USER               = props.getProperty("STATEMENT_INSERT_WIKI_USER");
		STATEMENT_INSERT_WIKI_USER_AUTO_INCREMENT = props.getProperty("STATEMENT_INSERT_WIKI_USER_AUTO_INCREMENT");
		STATEMENT_INSERT_USER_PREFERENCE_DEFAULTS = props.getProperty("STATEMENT_INSERT_USER_PREFERENCE_DEFAULTS");
		STATEMENT_INSERT_USER_PREFERENCE         = props.getProperty("STATEMENT_INSERT_USER_PREFERENCE");
		STATEMENT_SELECT_AUTHORITIES_AUTHORITY   = props.getProperty("STATEMENT_SELECT_AUTHORITIES_AUTHORITY");
		STATEMENT_SELECT_AUTHORITIES_AUTHORITY_ALL = props.getProperty("STATEMENT_SELECT_AUTHORITIES_AUTHORITY_ALL");
		STATEMENT_SELECT_AUTHORITIES_LOGIN       = props.getProperty("STATEMENT_SELECT_AUTHORITIES_LOGIN");
		STATEMENT_SELECT_AUTHORITIES_USER        = props.getProperty("STATEMENT_SELECT_AUTHORITIES_USER");
		STATEMENT_SELECT_CATEGORIES              = props.getProperty("STATEMENT_SELECT_CATEGORIES");
		STATEMENT_SELECT_CATEGORY_TOPICS         = props.getProperty("STATEMENT_SELECT_CATEGORY_TOPICS");
		STATEMENT_SELECT_CONFIGURATION           = props.getProperty("STATEMENT_SELECT_CONFIGURATION");
		STATEMENT_SELECT_GROUP_MAP_GROUP         = props.getProperty("STATEMENT_SELECT_GROUP_MAP_GROUP");
		STATEMENT_SELECT_GROUP_MAP_USER          = props.getProperty("STATEMENT_SELECT_GROUP_MAP_USER");
		STATEMENT_SELECT_GROUP_MAP_AUTHORITIES   = props.getProperty("STATEMENT_SELECT_GROUP_MAP_AUTHORITIES");
		STATEMENT_SELECT_GROUP                   = props.getProperty("STATEMENT_SELECT_GROUP");
		STATEMENT_SELECT_GROUP_BY_ID             = props.getProperty("STATEMENT_SELECT_GROUP_BY_ID");
		STATEMENT_SELECT_GROUPS                  = props.getProperty("STATEMENT_SELECT_GROUPS");
		STATEMENT_SELECT_GROUP_AUTHORITIES       = props.getProperty("STATEMENT_SELECT_GROUP_AUTHORITIES");
		STATEMENT_SELECT_GROUPS_AUTHORITIES      = props.getProperty("STATEMENT_SELECT_GROUPS_AUTHORITIES");
		STATEMENT_SELECT_GROUP_MEMBERS_SEQUENCE  = props.getProperty("STATEMENT_SELECT_GROUP_MEMBERS_SEQUENCE");
		STATEMENT_SELECT_GROUP_SEQUENCE          = props.getProperty("STATEMENT_SELECT_GROUP_SEQUENCE");
		STATEMENT_SELECT_INTERWIKIS              = props.getProperty("STATEMENT_SELECT_INTERWIKIS");
		STATEMENT_SELECT_LOG_ITEMS               = props.getProperty("STATEMENT_SELECT_LOG_ITEMS");
		STATEMENT_SELECT_LOG_ITEMS_BY_TYPE       = props.getProperty("STATEMENT_SELECT_LOG_ITEMS_BY_TYPE");
		STATEMENT_SELECT_NAMESPACE_SEQUENCE      = props.getProperty("STATEMENT_SELECT_NAMESPACE_SEQUENCE");
		STATEMENT_SELECT_NAMESPACES              = props.getProperty("STATEMENT_SELECT_NAMESPACES");
		STATEMENT_SELECT_PW_RESET_CHALLENGE_DATA = props.getProperty("STATEMENT_SELECT_PW_RESET_CHALLENGE_DATA");
		STATEMENT_SELECT_RECENT_CHANGES          = props.getProperty("STATEMENT_SELECT_RECENT_CHANGES");
		STATEMENT_SELECT_ROLES                   = props.getProperty("STATEMENT_SELECT_ROLES");
		STATEMENT_SELECT_TOPIC_BY_ID             = props.getProperty("STATEMENT_SELECT_TOPIC_BY_ID");
		STATEMENT_SELECT_TOPIC_BY_TYPE           = props.getProperty("STATEMENT_SELECT_TOPIC_BY_TYPE");
		STATEMENT_SELECT_TOPIC_COUNT             = props.getProperty("STATEMENT_SELECT_TOPIC_COUNT");
		STATEMENT_SELECT_TOPIC                   = props.getProperty("STATEMENT_SELECT_TOPIC");
		STATEMENT_SELECT_TOPIC_HISTORY           = props.getProperty("STATEMENT_SELECT_TOPIC_HISTORY");
		STATEMENT_SELECT_TOPIC_LINK_ORPHANS      = props.getProperty("STATEMENT_SELECT_TOPIC_LINK_ORPHANS");
		STATEMENT_SELECT_TOPIC_LINKS             = props.getProperty("STATEMENT_SELECT_TOPIC_LINKS");
		STATEMENT_SELECT_TOPIC_LOWER             = props.getProperty("STATEMENT_SELECT_TOPIC_LOWER");
		STATEMENT_SELECT_TOPIC_NAME              = props.getProperty("STATEMENT_SELECT_TOPIC_NAME");
		STATEMENT_SELECT_TOPIC_NAME_LOWER        = props.getProperty("STATEMENT_SELECT_TOPIC_NAME_LOWER");
		STATEMENT_SELECT_TOPIC_NAMES             = props.getProperty("STATEMENT_SELECT_TOPIC_NAMES");
		STATEMENT_SELECT_TOPICS_ADMIN            = props.getProperty("STATEMENT_SELECT_TOPICS_ADMIN");
		STATEMENT_SELECT_TOPIC_SEQUENCE          = props.getProperty("STATEMENT_SELECT_TOPIC_SEQUENCE");
		STATEMENT_SELECT_TOPIC_VERSION           = props.getProperty("STATEMENT_SELECT_TOPIC_VERSION");
		STATEMENT_SELECT_TOPIC_VERSION_NEXT_ID   = props.getProperty("STATEMENT_SELECT_TOPIC_VERSION_NEXT_ID");
		STATEMENT_SELECT_TOPIC_VERSION_SEQUENCE  = props.getProperty("STATEMENT_SELECT_TOPIC_VERSION_SEQUENCE");
		STATEMENT_SELECT_USER_BLOCKS             = props.getProperty("STATEMENT_SELECT_USER_BLOCKS");
		STATEMENT_SELECT_USER_BLOCK_SEQUENCE     = props.getProperty("STATEMENT_SELECT_USER_BLOCK_SEQUENCE");
		STATEMENT_SELECT_USERS_AUTHENTICATION    = props.getProperty("STATEMENT_SELECT_USERS_AUTHENTICATION");
		STATEMENT_SELECT_VIRTUAL_WIKIS           = props.getProperty("STATEMENT_SELECT_VIRTUAL_WIKIS");
		STATEMENT_SELECT_VIRTUAL_WIKI_SEQUENCE   = props.getProperty("STATEMENT_SELECT_VIRTUAL_WIKI_SEQUENCE");
		STATEMENT_SELECT_WATCHLIST               = props.getProperty("STATEMENT_SELECT_WATCHLIST");
		STATEMENT_SELECT_WATCHLIST_CHANGES       = props.getProperty("STATEMENT_SELECT_WATCHLIST_CHANGES");
		STATEMENT_SELECT_WIKI_FILE               = props.getProperty("STATEMENT_SELECT_WIKI_FILE");
		STATEMENT_SELECT_WIKI_FILE_COUNT         = props.getProperty("STATEMENT_SELECT_WIKI_FILE_COUNT");
		STATEMENT_SELECT_WIKI_FILE_SEQUENCE      = props.getProperty("STATEMENT_SELECT_WIKI_FILE_SEQUENCE");
		STATEMENT_SELECT_WIKI_FILE_VERSION_SEQUENCE = props.getProperty("STATEMENT_SELECT_WIKI_FILE_VERSION_SEQUENCE");
		STATEMENT_SELECT_WIKI_FILE_VERSIONS      = props.getProperty("STATEMENT_SELECT_WIKI_FILE_VERSIONS");
		STATEMENT_SELECT_WIKI_USER               = props.getProperty("STATEMENT_SELECT_WIKI_USER");
		STATEMENT_SELECT_WIKI_USER_CHANGES_ANONYMOUS = props.getProperty("STATEMENT_SELECT_WIKI_USER_CHANGES_ANONYMOUS");
		STATEMENT_SELECT_WIKI_USER_CHANGES_LOGIN = props.getProperty("STATEMENT_SELECT_WIKI_USER_CHANGES_LOGIN");
		STATEMENT_SELECT_WIKI_USER_COUNT         = props.getProperty("STATEMENT_SELECT_WIKI_USER_COUNT");
		STATEMENT_SELECT_WIKI_USER_DETAILS_PASSWORD = props.getProperty("STATEMENT_SELECT_WIKI_USER_DETAILS_PASSWORD");
		STATEMENT_SELECT_WIKI_USER_LOGIN         = props.getProperty("STATEMENT_SELECT_WIKI_USER_LOGIN");
		STATEMENT_SELECT_WIKI_USER_SEQUENCE      = props.getProperty("STATEMENT_SELECT_WIKI_USER_SEQUENCE");
		STATEMENT_SELECT_WIKI_USERS              = props.getProperty("STATEMENT_SELECT_WIKI_USERS");
		STATEMENT_SELECT_USER_PREFERENCES_DEFAULTS = props.getProperty("STATEMENT_SELECT_USER_PREFERENCES_DEFAULTS");
		STATEMENT_SELECT_USER_PREFERENCES        = props.getProperty("STATEMENT_SELECT_USER_PREFERENCES");
		STATEMENT_UPDATE_GROUP                   = props.getProperty("STATEMENT_UPDATE_GROUP");
		STATEMENT_UPDATE_NAMESPACE               = props.getProperty("STATEMENT_UPDATE_NAMESPACE");
		STATEMENT_UPDATE_PW_RESET_CHALLENGE_DATA = props.getProperty("STATEMENT_UPDATE_PW_RESET_CHALLENGE_DATA");
		STATEMENT_UPDATE_RECENT_CHANGES_PREVIOUS_VERSION_ID = props.getProperty("STATEMENT_UPDATE_RECENT_CHANGES_PREVIOUS_VERSION_ID");
		STATEMENT_UPDATE_TOPIC_NAMESPACE         = props.getProperty("STATEMENT_UPDATE_TOPIC_NAMESPACE");
		STATEMENT_UPDATE_ROLE                    = props.getProperty("STATEMENT_UPDATE_ROLE");
		STATEMENT_UPDATE_TOPIC                   = props.getProperty("STATEMENT_UPDATE_TOPIC");
		STATEMENT_UPDATE_TOPIC_VERSION           = props.getProperty("STATEMENT_UPDATE_TOPIC_VERSION");
		STATEMENT_UPDATE_TOPIC_VERSION_PREVIOUS_VERSION_ID = props.getProperty("STATEMENT_UPDATE_TOPIC_VERSION_PREVIOUS_VERSION_ID");
		STATEMENT_UPDATE_USER                    = props.getProperty("STATEMENT_UPDATE_USER");
		STATEMENT_UPDATE_USER_BLOCK              = props.getProperty("STATEMENT_UPDATE_USER_BLOCK");
		STATEMENT_UPDATE_VIRTUAL_WIKI            = props.getProperty("STATEMENT_UPDATE_VIRTUAL_WIKI");
		STATEMENT_UPDATE_WIKI_FILE               = props.getProperty("STATEMENT_UPDATE_WIKI_FILE");
		STATEMENT_UPDATE_WIKI_USER               = props.getProperty("STATEMENT_UPDATE_WIKI_USER");
		STATEMENT_UPDATE_USER_PREFERENCE_DEFAULTS = props.getProperty("STATEMENT_UPDATE_USER_PREFERENCE_DEFAULTS");
		STATEMENT_CREATE_FILE_DATA_TABLE         = props.getProperty("STATEMENT_CREATE_FILE_DATA_TABLE");
		STATEMENT_DROP_FILE_DATA_TABLE           = props.getProperty("STATEMENT_DROP_FILE_DATA_TABLE");
		STATEMENT_INSERT_FILE_DATA               = props.getProperty("STATEMENT_INSERT_FILE_DATA");
		STATEMENT_DELETE_RESIZED_IMAGES          = props.getProperty("STATEMENT_DELETE_RESIZED_IMAGES");
		STATEMENT_SELECT_FILE_INFO               = props.getProperty("STATEMENT_SELECT_FILE_INFO");
		STATEMENT_SELECT_FILE_DATA               = props.getProperty("STATEMENT_SELECT_FILE_DATA");
		STATEMENT_SELECT_FILE_VERSION_DATA       = props.getProperty("STATEMENT_SELECT_FILE_VERSION_DATA");
		STATEMENT_CREATE_SEQUENCES               = props.getProperty("STATEMENT_CREATE_SEQUENCES");
		STATEMENT_DROP_SEQUENCES                 = props.getProperty("STATEMENT_DROP_SEQUENCES");
	}

	/**
	 *
	 */
	public void insertCategories(List<Category> categoryList, int virtualWikiId, int topicId) {
		if (topicId == -1) {
			throw new InvalidDataAccessApiUsageException("Invalid topicId passed to method AnsiQueryHandler.insertCategories");
		}
		List<Object[]> batchArgs = new ArrayList<Object[]>();
		for (Category category : categoryList) {
			Object[] args = { topicId, category.getName(), category.getSortKey() };
			batchArgs.add(args);
		}
		DatabaseConnection.getJdbcTemplate().batchUpdate(STATEMENT_INSERT_CATEGORY, batchArgs);
	}

	/**
	 *
	 */
	public void insertGroupAuthority(int groupId, String authority) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_GROUP_AUTHORITY,
				groupId,
				authority
		);
	}

	/**
	 *
	 */
	public void insertGroupMember(String username, int groupId) {
		int[] types = (this.autoIncrementPrimaryKeys()) ? new int[2] : new int[3];
		Object[] args = (this.autoIncrementPrimaryKeys()) ? new Object[2] : new Object[3];
		int index = 0;
		if (!this.autoIncrementPrimaryKeys()) {
			int groupMemberId = DatabaseConnection.executeSequenceQuery(STATEMENT_SELECT_GROUP_MEMBERS_SEQUENCE);
			types[index] = Types.INTEGER;
			args[index++] = groupMemberId;
		}
		types[index] = Types.VARCHAR;
		args[index++] = username;
		types[index] = Types.INTEGER;
		args[index++] = groupId;
		if (this.autoIncrementPrimaryKeys()) {
			this.executeGeneratedKeyInsert(STATEMENT_INSERT_GROUP_MEMBER_AUTO_INCREMENT, args, types, "id");
		} else {
			PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(STATEMENT_INSERT_GROUP_MEMBER, types);
			DatabaseConnection.getJdbcTemplate().update(factory.newPreparedStatementCreator(args));
		}
	}

	/**
	 *
	 */
	public void insertInterwiki(Interwiki interwiki) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_INTERWIKI,
				interwiki.getInterwikiPrefix(),
				interwiki.getInterwikiPattern(),
				interwiki.getInterwikiDisplay(),
				interwiki.getInterwikiType()
		);
	}

	/**
	 *
	 */
	public void insertLogItem(LogItem logItem, int virtualWikiId) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_LOG_ITEM,
				logItem.getLogDate(),
				virtualWikiId,
				logItem.getUserId(),
				logItem.getUserDisplayName(),
				logItem.getLogType(),
				logItem.getLogSubType(),
				logItem.getLogComment(),
				logItem.getLogParamString(),
				logItem.getTopicId(),
				logItem.getTopicVersionId()
		);
	}

	/**
	 *
	 */
	public void insertRecentChange(RecentChange change, int virtualWikiId) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_RECENT_CHANGE,
				change.getTopicVersionId(),
				change.getPreviousTopicVersionId(),
				change.getTopicId(),
				change.getTopicName(),
				change.getChangeDate(),
				change.getChangeComment(),
				change.getAuthorId(),
				change.getAuthorName(),
				change.getEditType(),
				virtualWikiId,
				change.getVirtualWiki(),
				change.getCharactersChanged(),
				change.getLogType(),
				change.getLogSubType(),
				change.getParamString()
		);
	}

	/**
	 *
	 */
	public void insertRole(Role role) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_ROLE,
				role.getAuthority(),
				role.getDescription()
		);
	}

	/**
	 *
	 */
	public void insertTopic(Topic topic, int virtualWikiId) {
		int[] types = (this.autoIncrementPrimaryKeys()) ? new int[11] : new int[12];
		Object[] args = (this.autoIncrementPrimaryKeys()) ? new Object[11] : new Object[12];
		int index = 0;
		if (!this.autoIncrementPrimaryKeys()) {
			int topicId = DatabaseConnection.executeSequenceQuery(STATEMENT_SELECT_TOPIC_SEQUENCE);
			topic.setTopicId(topicId);
			types[index] = Types.INTEGER;
			args[index++] = topicId;
		}
		types[index] = Types.INTEGER;
		args[index++] = virtualWikiId;
		types[index] = Types.VARCHAR;
		args[index++] = topic.getName();
		types[index] = Types.INTEGER;
		args[index++] = topic.getTopicType().id();
		types[index] = Types.INTEGER;
		args[index++] = (topic.getReadOnly() ? 1 : 0);
		types[index] = Types.INTEGER;
		args[index++] = topic.getCurrentVersionId();
		types[index] = Types.TIMESTAMP;
		args[index++] = topic.getDeleteDate();
		types[index] = Types.INTEGER;
		args[index++] = (topic.getAdminOnly() ? 1 : 0);
		types[index] = Types.VARCHAR;
		args[index++] = topic.getRedirectTo();
		types[index] = Types.INTEGER;
		args[index++] = topic.getNamespace().getId();
		types[index] = Types.VARCHAR;
		args[index++] = topic.getPageName();
		types[index] = Types.VARCHAR;
		args[index++] = topic.getPageName().toLowerCase();
		if (this.autoIncrementPrimaryKeys()) {
			int topicId = this.executeGeneratedKeyInsert(STATEMENT_INSERT_TOPIC_AUTO_INCREMENT, args, types, "topic_id");
			topic.setTopicId(topicId);
		} else {
			PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(STATEMENT_INSERT_TOPIC, types);
			DatabaseConnection.getJdbcTemplate().update(factory.newPreparedStatementCreator(args));
		}
	}

	/**
	 *
	 */
	public void insertTopicLinks(List<Topic> topicLinks, int topicId) {
		if (topicId == -1) {
			throw new InvalidDataAccessApiUsageException("Invalid topicId passed to method AnsiQueryHandler.insertTopicLinks");
		}
		List<Object[]> batchArgs = new ArrayList<Object[]>();
		for (Topic topicLink : topicLinks) {
			Object[] args = { topicId, topicLink.getNamespace().getId(), topicLink.getPageName() };
			batchArgs.add(args);
		}
		DatabaseConnection.getJdbcTemplate().batchUpdate(STATEMENT_INSERT_TOPIC_LINKS, batchArgs);
	}

	/**
	 *
	 */
	private void insertTopicVersion(TopicVersion topicVersion) {
		if (topicVersion.getEditDate() == null) {
			topicVersion.setEditDate(new Timestamp(System.currentTimeMillis()));
		}
		int[] types = (this.autoIncrementPrimaryKeys()) ? new int[10] : new int[11];
		Object[] args = (this.autoIncrementPrimaryKeys()) ? new Object[10] : new Object[11];
		int index = 0;
		if (!this.autoIncrementPrimaryKeys()) {
			int topicVersionId = DatabaseConnection.executeSequenceQuery(STATEMENT_SELECT_TOPIC_VERSION_SEQUENCE);
			topicVersion.setTopicVersionId(topicVersionId);
			types[index] = Types.INTEGER;
			args[index++] = topicVersionId;
		}
		types[index] = Types.INTEGER;
		args[index++] = topicVersion.getTopicId();
		types[index] = Types.VARCHAR;
		args[index++] = topicVersion.getEditComment();
		types[index] = Types.VARCHAR;
		args[index++] = topicVersion.getVersionContent();
		types[index] = Types.INTEGER;
		args[index++] = topicVersion.getAuthorId();
		types[index] = Types.INTEGER;
		args[index++] = topicVersion.getEditType();
		types[index] = Types.VARCHAR;
		args[index++] = topicVersion.getAuthorDisplay();
		types[index] = Types.TIMESTAMP;
		args[index++] = topicVersion.getEditDate();
		types[index] = Types.INTEGER;
		args[index++] = topicVersion.getPreviousTopicVersionId();
		types[index] = Types.INTEGER;
		args[index++] = topicVersion.getCharactersChanged();
		types[index] = Types.VARCHAR;
		args[index++] = topicVersion.getVersionParamString();
		if (this.autoIncrementPrimaryKeys()) {
			int topicVersionId = this.executeGeneratedKeyInsert(STATEMENT_INSERT_TOPIC_VERSION_AUTO_INCREMENT, args, types, "topic_version_id");
			topicVersion.setTopicVersionId(topicVersionId);
		} else {
			PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(STATEMENT_INSERT_TOPIC_VERSION, types);
			DatabaseConnection.getJdbcTemplate().update(factory.newPreparedStatementCreator(args));
		}
	}

	/**
	 *
	 */
	public void insertTopicVersions(List<TopicVersion> topicVersions) {
		if (topicVersions.size() == 1) {
			this.insertTopicVersion(topicVersions.get(0));
			return;
		}
		// manually retrieve next topic version id when using batch
		// mode or when the database doesn't support generated keys.
		int topicVersionId = DatabaseConnection.executeSequenceQuery(STATEMENT_SELECT_TOPIC_VERSION_SEQUENCE);
		List<Object[]> batchArgs = new ArrayList<Object[]>();
		for (TopicVersion topicVersion : topicVersions) {
			if (topicVersion.getEditDate() == null) {
				topicVersion.setEditDate(new Timestamp(System.currentTimeMillis()));
			}
			// FIXME - if two threads update the database simultaneously then
			// it is possible that this code could set the topic version ID
			// to a value that is different from what the database ends up
			// using.
			topicVersion.setTopicVersionId(topicVersionId++);
			int index = 0;
			Object[] args = (this.autoIncrementPrimaryKeys()) ? new Object[10] : new Object[11];
			if (!this.autoIncrementPrimaryKeys()) {
				args[index++] = topicVersion.getTopicVersionId();
			}
			args[index++] = topicVersion.getTopicId();
			args[index++] = topicVersion.getEditComment();
			args[index++] = topicVersion.getVersionContent();
			args[index++] = topicVersion.getAuthorId();
			args[index++] = topicVersion.getEditType();
			args[index++] = topicVersion.getAuthorDisplay();
			args[index++] = topicVersion.getEditDate();
			args[index++] = topicVersion.getPreviousTopicVersionId();
			args[index++] = topicVersion.getCharactersChanged();
			args[index++] = topicVersion.getVersionParamString();
			batchArgs.add(args);
		}
		if (!batchArgs.isEmpty()) {
			// generated keys don't work in batch mode
			if (!this.autoIncrementPrimaryKeys()) {
				DatabaseConnection.getJdbcTemplate().batchUpdate(STATEMENT_INSERT_TOPIC_VERSION, batchArgs);
			} else {
				DatabaseConnection.getJdbcTemplate().batchUpdate(STATEMENT_INSERT_TOPIC_VERSION_AUTO_INCREMENT, batchArgs);
			}
		}
	}

	/**
	 *
	 */
	public void insertUserAuthority(String username, String authority) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_AUTHORITY,
				username,
				authority
		);
	}

	/**
	 *
	 */
	public void insertUserBlock(UserBlock userBlock) {
		int[] types =  (this.autoIncrementPrimaryKeys()) ? new int[9] : new int[10];
		Object[] args = (this.autoIncrementPrimaryKeys()) ? new Object[9] : new Object[10];
		int index = 0;
		if (!this.autoIncrementPrimaryKeys()) {
			int blockId = DatabaseConnection.executeSequenceQuery(STATEMENT_SELECT_USER_BLOCK_SEQUENCE);
			userBlock.setBlockId(blockId);
			types[index] = Types.INTEGER;
			args[index++] = blockId;
		}
		types[index] = Types.INTEGER;
		args[index++] = userBlock.getWikiUserId();
		types[index] = Types.VARCHAR;
		args[index++] = userBlock.getIpAddress();
		types[index] = Types.TIMESTAMP;
		args[index++] = userBlock.getBlockDate();
		types[index] = Types.TIMESTAMP;
		args[index++] = userBlock.getBlockEndDate();
		types[index] = Types.VARCHAR;
		args[index++] = userBlock.getBlockReason();
		types[index] = Types.INTEGER;
		args[index++] = userBlock.getBlockedByUserId();
		types[index] = Types.TIMESTAMP;
		args[index++] = userBlock.getUnblockDate();
		types[index] = Types.VARCHAR;
		args[index++] = userBlock.getUnblockReason();
		types[index] = Types.INTEGER;
		args[index++] = userBlock.getUnblockedByUserId();
		if (this.autoIncrementPrimaryKeys()) {
			int blockId = this.executeGeneratedKeyInsert(STATEMENT_INSERT_USER_BLOCK_AUTO_INCREMENT, args, types, "user_block_id");
			userBlock.setBlockId(blockId);
		} else {
			PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(STATEMENT_INSERT_USER_BLOCK, types);
			DatabaseConnection.getJdbcTemplate().update(factory.newPreparedStatementCreator(args));
		}
	}

	/**
	 *
	 */
	public void insertUserDetails(WikiUserDetails userDetails) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_USER,
				userDetails.getUsername(),
				userDetails.getPassword()
		);
	}

	/**
	 *
	 */
	public void insertVirtualWiki(VirtualWiki virtualWiki) {
		int[] types = (this.autoIncrementPrimaryKeys()) ? new int[5] : new int[6];
		Object[] args = (this.autoIncrementPrimaryKeys()) ? new Object[5] : new Object[6];
		int index = 0;
		if (!this.autoIncrementPrimaryKeys()) {
			int virtualWikiId = DatabaseConnection.executeSequenceQuery(STATEMENT_SELECT_VIRTUAL_WIKI_SEQUENCE);
			virtualWiki.setVirtualWikiId(virtualWikiId);
			types[index] = Types.INTEGER;
			args[index++] = virtualWikiId;
		}
		types[index] = Types.VARCHAR;
		args[index++] = virtualWiki.getName();
		types[index] = Types.VARCHAR;
		args[index++] = (virtualWiki.isDefaultRootTopicName() ? null : virtualWiki.getRootTopicName());
		types[index] = Types.VARCHAR;
		args[index++] = (virtualWiki.isDefaultLogoImageUrl() ? null : virtualWiki.getLogoImageUrl());
		types[index] = Types.VARCHAR;
		args[index++] = (virtualWiki.isDefaultMetaDescription() ? null : virtualWiki.getMetaDescription());
		types[index] = Types.VARCHAR;
		args[index++] = (virtualWiki.isDefaultSiteName() ? null : virtualWiki.getSiteName());
		if (this.autoIncrementPrimaryKeys()) {
			int virtualWikiId = this.executeGeneratedKeyInsert(STATEMENT_INSERT_VIRTUAL_WIKI_AUTO_INCREMENT, args, types, "virtual_wiki_id");
			virtualWiki.setVirtualWikiId(virtualWikiId);
		} else {
			PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(STATEMENT_INSERT_VIRTUAL_WIKI, types);
			DatabaseConnection.getJdbcTemplate().update(factory.newPreparedStatementCreator(args));
		}
	}

	/**
	 *
	 */
	public void insertWatchlistEntry(int virtualWikiId, String topicName, int userId) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_WATCHLIST_ENTRY,
				virtualWikiId,
				topicName,
				userId
		);
	}

	/**
	 *
	 */
	public void insertWikiFile(WikiFile wikiFile, int virtualWikiId) {
		int[] types = (this.autoIncrementPrimaryKeys()) ? new int[9] : new int[10];
		Object[] args = (this.autoIncrementPrimaryKeys()) ? new Object[9] : new Object[10];
		int index = 0;
		if (!this.autoIncrementPrimaryKeys()) {
			int fileId = DatabaseConnection.executeSequenceQuery(STATEMENT_SELECT_WIKI_FILE_SEQUENCE);
			wikiFile.setFileId(fileId);
			types[index] = Types.INTEGER;
			args[index++] = fileId;
		}
		types[index] = Types.INTEGER;
		args[index++] = virtualWikiId;
		types[index] = Types.VARCHAR;
		args[index++] = wikiFile.getFileName();
		types[index] = Types.VARCHAR;
		args[index++] = wikiFile.getUrl();
		types[index] = Types.VARCHAR;
		args[index++] = wikiFile.getMimeType();
		types[index] = Types.INTEGER;
		args[index++] = wikiFile.getTopicId();
		types[index] = Types.TIMESTAMP;
		args[index++] = wikiFile.getDeleteDate();
		types[index] = Types.INTEGER;
		args[index++] = (wikiFile.getReadOnly() ? 1 : 0);
		types[index] = Types.INTEGER;
		args[index++] = (wikiFile.getAdminOnly() ? 1 : 0);
		types[index] = Types.BIGINT;
		args[index++] = wikiFile.getFileSize();
		if (this.autoIncrementPrimaryKeys()) {
			int fileId = this.executeGeneratedKeyInsert(STATEMENT_INSERT_WIKI_FILE_AUTO_INCREMENT, args, types, "file_id");
			wikiFile.setFileId(fileId);
		} else {
			PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(STATEMENT_INSERT_WIKI_FILE, types);
			DatabaseConnection.getJdbcTemplate().update(factory.newPreparedStatementCreator(args));
		}
	}

	/**
	 *
	 */
	public void insertWikiFileVersion(WikiFileVersion wikiFileVersion) {
		if (wikiFileVersion.getUploadDate() == null) {
			Timestamp uploadDate = new Timestamp(System.currentTimeMillis());
			wikiFileVersion.setUploadDate(uploadDate);
		}
		int[] types = (this.autoIncrementPrimaryKeys()) ? new int[8] : new int[9];
		Object[] args = (this.autoIncrementPrimaryKeys()) ? new Object[8] : new Object[9];
		int index = 0;
		if (!this.autoIncrementPrimaryKeys()) {
			int fileVersionId = DatabaseConnection.executeSequenceQuery(STATEMENT_SELECT_WIKI_FILE_VERSION_SEQUENCE);
			wikiFileVersion.setFileVersionId(fileVersionId);
			types[index] = Types.INTEGER;
			args[index++] = fileVersionId;
		}
		types[index] = Types.INTEGER;
		args[index++] = wikiFileVersion.getFileId();
		types[index] = Types.VARCHAR;
		args[index++] = wikiFileVersion.getUploadComment();
		types[index] = Types.VARCHAR;
		args[index++] = wikiFileVersion.getUrl();
		types[index] = Types.INTEGER;
		args[index++] = wikiFileVersion.getAuthorId();
		types[index] = Types.VARCHAR;
		args[index++] = wikiFileVersion.getAuthorDisplay();
		types[index] = Types.TIMESTAMP;
		args[index++] = wikiFileVersion.getUploadDate();
		types[index] = Types.VARCHAR;
		args[index++] = wikiFileVersion.getMimeType();
		types[index] = Types.BIGINT;
		args[index++] = wikiFileVersion.getFileSize();
		if (this.autoIncrementPrimaryKeys()) {
			int fileVersionId = this.executeGeneratedKeyInsert(STATEMENT_INSERT_WIKI_FILE_VERSION_AUTO_INCREMENT, args, types, "file_version_id");
			wikiFileVersion.setFileVersionId(fileVersionId);
		} else {
			PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(STATEMENT_INSERT_WIKI_FILE_VERSION, types);
			DatabaseConnection.getJdbcTemplate().update(factory.newPreparedStatementCreator(args));
		}
	}

	/**
	 *
	 */
	public void insertWikiGroup(WikiGroup group) {
		int[] types = (this.autoIncrementPrimaryKeys()) ? new int[2] : new int[3];
		Object[] args = (this.autoIncrementPrimaryKeys()) ? new Object[2] : new Object[3];
		int index = 0;
		if (!this.autoIncrementPrimaryKeys()) {
			int groupId = DatabaseConnection.executeSequenceQuery(STATEMENT_SELECT_GROUP_SEQUENCE);
			group.setGroupId(groupId);
			types[index] = Types.INTEGER;
			args[index++] = groupId;
		}
		types[index] = Types.VARCHAR;
		args[index++] = group.getName();
		types[index] = Types.VARCHAR;
		args[index++] = group.getDescription();
		if (this.autoIncrementPrimaryKeys()) {
			int groupId = this.executeGeneratedKeyInsert(STATEMENT_INSERT_GROUP_AUTO_INCREMENT, args, types, "group_id");
			group.setGroupId(groupId);
		} else {
			PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(STATEMENT_INSERT_GROUP, types);
			DatabaseConnection.getJdbcTemplate().update(factory.newPreparedStatementCreator(args));
		}
	}

	/**
	 *
	 */
	public void insertWikiUser(WikiUser user) {
		int[] types = (this.autoIncrementPrimaryKeys()) ? new int[7] : new int[8];
		Object[] args = (this.autoIncrementPrimaryKeys()) ? new Object[7] : new Object[8];
		int index = 0;
		if (!this.autoIncrementPrimaryKeys()) {
			int userId = DatabaseConnection.executeSequenceQuery(STATEMENT_SELECT_WIKI_USER_SEQUENCE);
			user.setUserId(userId);
			types[index] = Types.INTEGER;
			args[index++] = userId;
		}
		types[index] = Types.VARCHAR;
		args[index++] = user.getUsername();
		types[index] = Types.VARCHAR;
		args[index++] = user.getDisplayName();
		types[index] = Types.TIMESTAMP;
		args[index++] = user.getCreateDate();
		types[index] = Types.TIMESTAMP;
		args[index++] = user.getLastLoginDate();
		types[index] = Types.VARCHAR;
		args[index++] = user.getCreateIpAddress();
		types[index] = Types.VARCHAR;
		args[index++] = user.getLastLoginIpAddress();
		types[index] = Types.VARCHAR;
		args[index++] = user.getEmail();
		if (this.autoIncrementPrimaryKeys()) {
			int userId = this.executeGeneratedKeyInsert(STATEMENT_INSERT_WIKI_USER_AUTO_INCREMENT, args, types, "wiki_user_id");
			user.setUserId(userId);
		} else {
			PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(STATEMENT_INSERT_WIKI_USER, types);
			DatabaseConnection.getJdbcTemplate().update(factory.newPreparedStatementCreator(args));
		}
	}

	/**
	 *
	 */
	public void insertWikiUserPreferences(WikiUser user, Map<String, String> preferenceDefaults) {
		// Store user preferences
		Map<String, String> preferences = user.getPreferences();
		// Only store preferences that are not default
		for (String key : preferenceDefaults.keySet()) {
			String defVal = preferenceDefaults.get(key);
			String cusVal = preferences.get(key);
			if (StringUtils.isBlank(cusVal)) {
				user.setPreference(key, defVal);
			} else if (StringUtils.isBlank(defVal) || !preferenceDefaults.get(key).equals(preferences.get(key))) {
				DatabaseConnection.getJdbcTemplate().update(
						STATEMENT_INSERT_USER_PREFERENCE,
						user.getUserId(),
						key,
						cusVal
				);
			}
		}
	}

	/**
	 *
	 */
	public void insertUserPreferenceDefault(String userPreferenceKey, String userPreferenceDefaultValue, String userPreferenceGroupKey, int sequenceNr) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_USER_PREFERENCE_DEFAULTS,
				userPreferenceKey,
				userPreferenceDefaultValue,
				userPreferenceGroupKey,
				sequenceNr
		);
	}

	/**
	 *
	 */
	public List<Category> lookupCategoryTopics(int virtualWikiId, String virtualWikiName, String categoryName) {
		// category name must be lowercase since search is case-insensitive
		categoryName = categoryName.toLowerCase();
		Object[] args = { virtualWikiId, categoryName };
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_CATEGORY_TOPICS, args, new CategoryMapper(virtualWikiName));
	}

	/**
	 *
	 */
	public Map<String, String> lookupConfiguration() {
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_CONFIGURATION
		);
		Map<String, String> configuration = new HashMap<String, String>();
		for (Map<String, Object> result : results) {
			// note that the value must be trimmed since Oracle cannot store empty
			// strings (it converts them to NULL) so empty config values are stored
			// as " ".
			String key = (String)result.get("config_key");
			String value = (String)result.get("config_value");
			configuration.put(key, value.trim());
		}
		return configuration;
	}

	/**
	 *
	 */
	public List<Interwiki> lookupInterwikis() {
		return DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_INTERWIKIS, new InterwikiMapper());
	}

	/**
	 *
	 */
	public List<Namespace> lookupNamespaces() {
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_NAMESPACES
		);
		Map<Integer, Namespace> namespaces = new TreeMap<Integer, Namespace>();
		// because there is no consistent way to sort null keys, get all data and then
		// create Namespace objects by initializing main namespaces first, then the talk
		// namespaces that reference the main namespace.
		Map<Integer, Namespace> talkNamespaces = new HashMap<Integer, Namespace>();
		for (Map<String, Object> result : results) {
			int namespaceId = (Integer)result.get("namespace_id");
			Namespace namespace = namespaces.get(namespaceId);
			if (namespace == null) {
				String namespaceLabel = (String)result.get("namespace");
				namespace = new Namespace(namespaceId, namespaceLabel);
			}
			String virtualWiki = (String)result.get("virtual_wiki_name");
			String namespaceTranslation = (String)result.get("namespace_translation");
			if (virtualWiki != null) {
				namespace.getNamespaceTranslations().put(virtualWiki, namespaceTranslation);
			}
			namespaces.put(namespaceId, namespace);
			Integer mainNamespaceId = (Integer)result.get("main_namespace_id");
			if (mainNamespaceId != null) {
				talkNamespaces.put(mainNamespaceId, namespace);
			}
		}
		for (Map.Entry<Integer, Namespace> entry : talkNamespaces.entrySet()) {
			Namespace mainNamespace = namespaces.get(entry.getKey());
			if (mainNamespace == null) {
				logger.warn("Invalid namespace reference - bad database data.  Namespace references invalid main namespace with ID " + entry.getKey());
			}
			Namespace talkNamespace = entry.getValue();
			talkNamespace.setMainNamespaceId(mainNamespace.getId());
			namespaces.put(talkNamespace.getId(), talkNamespace);
		}
		return new ArrayList<Namespace>(namespaces.values());
	}

	/**
	 *
	 */
	public Topic lookupTopic(int virtualWikiId, Namespace namespace, String pageName) {
		if (namespace.getId().equals(Namespace.SPECIAL_ID)) {
			// invalid namespace
			return null;
		}
		Object[] args = {
				pageName,
				virtualWikiId,
				namespace.getId()
		};
		Topic topic = null;
		List<Topic> topics = DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_TOPIC, args, new TopicMapper());
		if (topics != null && !topics.isEmpty()) {
			// if there are deleted topics then multiple results are returned,
			// so use the last (non-deleted) result
			topic = topics.get(topics.size() - 1);
		}
		if (topic == null && !namespace.isCaseSensitive() && !pageName.toLowerCase().equals(pageName)) {
			args[0] = pageName.toLowerCase();
			topics = DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_TOPIC_LOWER, args, new TopicMapper());
			if (topics != null && !topics.isEmpty()) {
				// if there are deleted topics then multiple results are returned,
				// so use the last (non-deleted) result
				topic = topics.get(topics.size() - 1);
			}
		}
		return topic;
	}

	/**
	 *
	 */
	public Topic lookupTopicById(int topicId) {
		Object[] args = { topicId };
		Topic topic = null;
		List<Topic> topics = DatabaseConnection.getJdbcTemplate().query(STATEMENT_SELECT_TOPIC_BY_ID, args, new TopicMapper());
		if (topics != null && !topics.isEmpty()) {
			// if there are deleted topics then multiple results are returned,
			// so use the last (non-deleted) result
			topic = topics.get(topics.size() - 1);
		}
		return topic;
	}

	/**
	 *
	 */
	public Map<Integer, String> lookupTopicByType(int virtualWikiId, TopicType topicType1, TopicType topicType2, int namespaceStart, int namespaceEnd, Pagination pagination) {
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_TOPIC_BY_TYPE,
				virtualWikiId,
				topicType1.id(),
				topicType2.id(),
				namespaceStart,
				namespaceEnd,
				pagination.getNumResults(),
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
	public int lookupTopicCount(int virtualWikiId, int namespaceStart, int namespaceEnd) {
		Object[] args = { virtualWikiId, namespaceStart, namespaceEnd, TopicType.REDIRECT.id() };
		try {
			return DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_TOPIC_COUNT, args, Integer.class);
		} catch (IncorrectResultSizeDataAccessException e) {
			return 0;
		}
	}

	/**
	 *
	 */
	public String lookupTopicName(int virtualWikiId, String virtualWikiName, Namespace namespace, String pageName) {
		if (namespace.getId().equals(Namespace.SPECIAL_ID)) {
			// invalid namespace
			return null;
		}
		String topicName = null;
		Object[] args = { pageName, virtualWikiId, namespace.getId() };
		try {
			topicName = DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_TOPIC_NAME, args, String.class);
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
		}
		if (topicName == null && !namespace.isCaseSensitive() && !pageName.toLowerCase().equals(pageName)) {
			args[0] = pageName.toLowerCase();
			try {
				topicName = DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_TOPIC_NAME_LOWER, args, String.class);
			} catch (IncorrectResultSizeDataAccessException e) {
				// no matching result
			}
		}
		return topicName;
	}

	/**
	 *
	 */
	public List<String[]> lookupTopicLinks(int virtualWikiId, Topic topic) {
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_TOPIC_LINKS,
				virtualWikiId,
				topic.getNamespace().getId(),
				topic.getPageName(),
				virtualWikiId,
				topic.getName()
		);
		List<String[]> topicLinks = new ArrayList<String[]>();
		for (Map<String, Object> result : results) {
			String[] element = new String[2];
			element[0] = (String)result.get("topic_name");
			element[1] = (String)result.get("child_topic_name");
			topicLinks.add(element);
		}
		return topicLinks;
	}

	/**
	 *
	 */
	public List<String> lookupTopicLinkOrphans(int virtualWikiId, int namespaceId){
		Object[] args = { virtualWikiId, namespaceId, TopicType.REDIRECT.id() };
		return DatabaseConnection.getJdbcTemplate().queryForList(STATEMENT_SELECT_TOPIC_LINK_ORPHANS, args, String.class);
	}

	/**
	 *
	 */
	public Map<Integer, String> lookupTopicNames(int virtualWikiId, boolean includeDeleted) {
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_TOPIC_NAMES,
				virtualWikiId
		);
		Map<Integer, String> topicNames = new LinkedHashMap<Integer, String>();
		for (Map<String, Object> result : results) {
			if (includeDeleted || (Timestamp)result.get("delete_date") == null) {
				topicNames.put((Integer)result.get("topic_id"), (String)result.get("topic_name"));
			}
		}
		return topicNames;
	}

	/**
	 *
	 */
	public TopicVersion lookupTopicVersion(int topicVersionId) {
		Object[] args = { topicVersionId };
		try {
			return DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_TOPIC_VERSION, args, new TopicVersionMapper());
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
			return null;
		}
	}

	/**
	 *
	 */
	public Integer lookupTopicVersionNextId(int topicVersionId) {
		Object[] args = { topicVersionId };
		try {
			return DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_TOPIC_VERSION_NEXT_ID, args, Integer.class);
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
			return null;
		}
	}

	/**
	 *
	 */
	private Map<String, String> lookupUserPreferencesDefaults() {
		List<Map<String, Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(STATEMENT_SELECT_USER_PREFERENCES_DEFAULTS);
		Map<String, String> defaults = new HashMap<String, String>();
		for (Map<String, Object> row : results) {
			defaults.put((String)row.get("pref_key"), (String)row.get("pref_value"));
		}
		return defaults;
	}

	/**
	 *
	 */
	public WikiFile lookupWikiFile(int virtualWikiId, String virtualWikiName, int topicId) {
		Object[] args = { virtualWikiId, topicId };
		try {
			return DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_WIKI_FILE, args, new WikiFileMapper(virtualWikiName));
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
			return null;
		}
	}

	/**
	 * Return a count of all wiki files currently available on the Wiki.  This
	 * method excludes deleted files.
	 *
	 * @param virtualWikiId The virtual wiki id for the virtual wiki of the files
	 *  being retrieved.
	 */
	public int lookupWikiFileCount(int virtualWikiId) {
		Object[] args = { virtualWikiId };
		try {
			return DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_WIKI_FILE_COUNT, args, Integer.class);
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
			return 0;
		}
	}

	/**
	 *
	 */
	public GroupMap lookupGroupMapGroup(int groupId) {
		if (lookupWikiGroupById(groupId) == null) {
			return null;
		}
		GroupMap groupMap = new GroupMap(groupId);
		Object[] args = { groupId };
		List<String> userLogins = DatabaseConnection.getJdbcTemplate().queryForList(STATEMENT_SELECT_GROUP_MAP_GROUP, args, String.class);
		groupMap.setGroupMembers(userLogins);
		return groupMap;
	}

	/**
	 *
	 */
	public GroupMap lookupGroupMapUser(String userLogin) {
		Object[] args = { userLogin };
		List<Integer> groupIds = DatabaseConnection.getJdbcTemplate().queryForList(STATEMENT_SELECT_GROUP_MAP_USER, args, Integer.class);
		GroupMap groupMap = new GroupMap(userLogin);
		groupMap.setGroupIds(groupIds);
		// retrieve roles assigned through group assignment
		List<String> roleNames = DatabaseConnection.getJdbcTemplate().queryForList(STATEMENT_SELECT_GROUP_MAP_AUTHORITIES, args, String.class);
		groupMap.setRoleNames(roleNames);
		return groupMap;
	}

	/**
	 *
	 */
	public WikiGroup lookupWikiGroup(String groupName) {
		Object[] args = { groupName };
		try {
			return DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_GROUP, args, new WikiGroupMapper());
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
			return null;
		}
	}

	/**
	 *
	 */
	public WikiGroup lookupWikiGroupById(int groupId) {
		Object[] args = { groupId };
		try {
			return DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_GROUP_BY_ID, args, new WikiGroupMapper());
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
			return null;
		}
	}

	/**
	 *
	 */
	public WikiUser lookupWikiUser(int userId) {
		WikiUser user = null;
		Object[] args = { userId };
		try {
			user = DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_WIKI_USER, args, new WikiUserMapper());
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
			return null;
		}
		// get the default user preferences
		Map<String, String> preferences = this.lookupUserPreferencesDefaults();
		// overwrite the defaults with any user-specific preferences
		List<Map<String,Object>> results = DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_USER_PREFERENCES,
				userId
		);
		for (Map<String, Object> result : results) {
			preferences.put((String)result.get("pref_key"), (String)result.get("pref_value"));
		}
		user.setPreferences(preferences);
		return user;
	}

	/**
	 *
	 */
	public int lookupWikiUser(String username) {
		try {
			return DatabaseConnection.getJdbcTemplate().queryForObject(
					STATEMENT_SELECT_WIKI_USER_LOGIN,
					Integer.class,
					username
			);
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
			return -1;
		}
	}

	public WikiUser lookupPwResetChallengeData(String username) {
		WikiUser user = this.lookupWikiUser(this.lookupWikiUser(username));
		if (user == null) {
			return null;
		}
		try {
			Map<String,Object> result = DatabaseConnection.getJdbcTemplate().queryForMap(
					STATEMENT_SELECT_PW_RESET_CHALLENGE_DATA,
					user.getUsername()
			);
			user.setChallengeValue((String)result.get("challenge_value"));
			user.setChallengeDate((Timestamp)result.get("challenge_date"));
			user.setChallengeIp((String)result.get("challenge_ip"));
			user.setChallengeTries((Integer)result.get("challenge_tries"));
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
		}
		return user;
	}

	/**
	 * Return a count of all wiki users.
	 */
	public int lookupWikiUserCount() {
		try {
			return DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_WIKI_USER_COUNT, Integer.class);
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
			return 0;
		}
	}

	/**
	 *
	 */
	public String lookupWikiUserEncryptedPassword(String username) {
		Object[] args = { username };
		try {
			return DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_WIKI_USER_DETAILS_PASSWORD, args, String.class);
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
			return null;
		}
	}

	/**
	 *
	 */
	public List<String> lookupWikiUsers(Pagination pagination) {
		Object[] args = { pagination.getNumResults(), pagination.getOffset() };
		return DatabaseConnection.getJdbcTemplate().queryForList(
				STATEMENT_SELECT_WIKI_USERS,
				args,
				String.class
		);
	}

	/**
	 *
	 */
	public void reloadLogItems(int virtualWikiId) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_LOG_ITEMS,
				virtualWikiId
		);
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_LOG_ITEMS_BY_TOPIC_VERSION_TYPE,
				LogItem.LOG_TYPE_DELETE,
				"",
				virtualWikiId,
				TopicVersion.EDIT_DELETE
		);
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_LOG_ITEMS_BY_TOPIC_VERSION_TYPE,
				LogItem.LOG_TYPE_DELETE,
				"|" + TopicVersion.EDIT_UNDELETE,
				virtualWikiId,
				TopicVersion.EDIT_UNDELETE
		);
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_LOG_ITEMS_BY_TOPIC_VERSION_TYPE,
				LogItem.LOG_TYPE_PERMISSION,
				"",
				virtualWikiId,
				TopicVersion.EDIT_PERMISSION
		);
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_LOG_ITEMS_IMPORT,
				LogItem.LOG_TYPE_IMPORT,
				TopicVersion.EDIT_IMPORT,
				virtualWikiId,
				TopicVersion.EDIT_IMPORT
		);
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_LOG_ITEMS_MOVE,
				LogItem.LOG_TYPE_MOVE,
				virtualWikiId,
				TopicVersion.EDIT_MOVE
		);
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_LOG_ITEMS_UPLOAD,
				LogItem.LOG_TYPE_UPLOAD,
				virtualWikiId,
				TopicVersion.EDIT_NORMAL
		);
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_LOG_ITEMS_USER,
				virtualWikiId,
				LogItem.LOG_TYPE_USER_CREATION
		);
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_LOG_ITEMS_BLOCK,
				virtualWikiId,
				LogItem.LOG_TYPE_BLOCK,
				LogItem.LOG_SUBTYPE_BLOCK_BLOCK
		);
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_LOG_ITEMS_UNBLOCK,
				virtualWikiId,
				LogItem.LOG_TYPE_BLOCK,
				LogItem.LOG_SUBTYPE_BLOCK_UNBLOCK
		);
	}

	/**
	 *
	 */
	public void orderTopicVersions(Topic topic, int virtualWikiId, List<Integer> topicVersionIdList) {
		List<Object[]> batchArgs = new ArrayList<Object[]>();
		Integer previousTopicVersionId = null;
		for (int topicVersionId : topicVersionIdList) {
			if (previousTopicVersionId != null) {
				Object[] args = { previousTopicVersionId, topicVersionId };
				batchArgs.add(args);
			}
			previousTopicVersionId = topicVersionId;
		}
		if (!batchArgs.isEmpty()) {
			DatabaseConnection.getJdbcTemplate().batchUpdate(STATEMENT_UPDATE_TOPIC_VERSION_PREVIOUS_VERSION_ID, batchArgs);
		}
		TopicVersion topicVersion = this.lookupTopicVersion(previousTopicVersionId);
		topic.setCurrentVersionId(previousTopicVersionId);
		topic.setTopicContent(topicVersion.getVersionContent());
		this.updateTopic(topic, virtualWikiId);
	}

	/**
	 *
	 */
	public void reloadRecentChanges(int limit) {
		DatabaseConnection.getJdbcTemplate().update(STATEMENT_DELETE_RECENT_CHANGES);
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_RECENT_CHANGES_VERSIONS,
				limit
		);
		DatabaseConnection.getJdbcTemplate().update(STATEMENT_INSERT_RECENT_CHANGES_LOGS);
	}

	/**
	 * Given a property name that holds a SQL query, return the database-specific
	 * SQL for that property.
	 *
	 * @param property The property name that holds the SQL query.
	 * @return The database-specific SQL for that property.
	 * @throws IllegalArgumentException if there is no SQL associated with the
	 *  property.
	 */
	public String sql(String property) throws IllegalArgumentException {
		String sql = this.props.getProperty(property);
		if (sql == null) {
			throw new IllegalArgumentException("No property found for " + property);
		}
		return sql;
	}

	/**
	 *
	 */
	public void updateConfiguration(Map<String, String> configuration) {
		DatabaseConnection.getJdbcTemplate().update(STATEMENT_DELETE_CONFIGURATION);
		List<Object[]> batchArgs = new ArrayList<Object[]>();
		for (Map.Entry<String, String> entry : configuration.entrySet()) {
			// FIXME - Oracle cannot store an empty string - it converts them
			// to null - so add a hack to work around the problem.
			String value = entry.getValue();
			if (StringUtils.isBlank(value)) {
				value = " ";
			}
			Object[] args = { entry.getKey(), value };
			batchArgs.add(args);
		}
		DatabaseConnection.getJdbcTemplate().batchUpdate(STATEMENT_INSERT_CONFIGURATION, batchArgs);
	}

	/**
	 *
	 */
	public void updateNamespace(Namespace namespace) {
		// update if the ID is specified AND a namespace with the same ID already exists
		boolean isUpdate = (namespace.getId() != null && this.lookupNamespaces().indexOf(namespace) != -1);
		// if adding determine the namespace ID(s)
		if (!isUpdate && namespace.getId() == null) {
			// note - this returns the last id in the system, so add one
			int nextId = DatabaseConnection.executeSequenceQuery(STATEMENT_SELECT_NAMESPACE_SEQUENCE);
			if (nextId < 200) {
				// custom namespaces start with IDs of 200 or more to leave room for future expansion
				nextId = 200;
			}
			namespace.setId(nextId);
		}
		// execute the adds/updates
		String sql = (isUpdate) ? STATEMENT_UPDATE_NAMESPACE : STATEMENT_INSERT_NAMESPACE;
		DatabaseConnection.getJdbcTemplate().update(
				sql,
				namespace.getDefaultLabel(),
				namespace.getMainNamespaceId(),
				namespace.getId()
		);
	}

	/**
	 *
	 */
	public void updateNamespaceTranslations(List<Namespace> namespaces, String virtualWiki, int virtualWikiId) {
		// delete any existing translation then add the new one
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_NAMESPACE_TRANSLATIONS,
				virtualWikiId
		);
		List<Object[]> batchArgs = new ArrayList<Object[]>();
		String translatedNamespace;
		for (Namespace namespace : namespaces) {
			translatedNamespace = namespace.getLabel(virtualWiki);
			if (translatedNamespace.equals(namespace.getDefaultLabel())) {
				continue;
			}
			Object[] args = { namespace.getId(), virtualWikiId, translatedNamespace };
			batchArgs.add(args);
		}
		if (!batchArgs.isEmpty()) {
			DatabaseConnection.getJdbcTemplate().batchUpdate(STATEMENT_INSERT_NAMESPACE_TRANSLATION, batchArgs);
		}
	}

	/**
	 *
	 */
	public void updateRole(Role role) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_UPDATE_ROLE,
				role.getDescription(),
				role.getAuthority()
		);
	}

	/**
	 *
	 */
	public void updateTopic(Topic topic, int virtualWikiId) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_UPDATE_TOPIC,
				virtualWikiId,
				topic.getName(),
				topic.getTopicType().id(),
				(topic.getReadOnly() ? 1 : 0),
				topic.getCurrentVersionId(),
				topic.getDeleteDate(),
				(topic.getAdminOnly() ? 1 : 0),
				topic.getRedirectTo(),
				topic.getNamespace().getId(),
				topic.getPageName(),
				topic.getPageName().toLowerCase(),
				topic.getTopicId()
		);
	}

	/**
	 *
	 */
	public void updateTopicNamespaces(List<Topic> topics) {
		List<Object[]> batchArgs = new ArrayList<Object[]>();
		for (Topic topic : topics) {
			Object[] args = {
					topic.getNamespace().getId(),
					topic.getPageName(),
					topic.getPageName().toLowerCase(),
					topic.getTopicId()
			};
		}
		if (!batchArgs.isEmpty()) {
			DatabaseConnection.getJdbcTemplate().batchUpdate(STATEMENT_UPDATE_TOPIC_NAMESPACE, batchArgs);
		}
	}

	/**
	 *
	 */
	public void updateTopicVersion(TopicVersion topicVersion) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_UPDATE_TOPIC_VERSION,
				topicVersion.getTopicId(),
				topicVersion.getEditComment(),
				topicVersion.getVersionContent(),
				topicVersion.getAuthorId(),
				topicVersion.getEditType(),
				topicVersion.getAuthorDisplay(),
				topicVersion.getEditDate(),
				topicVersion.getPreviousTopicVersionId(),
				topicVersion.getCharactersChanged(),
				topicVersion.getVersionParamString(),
				topicVersion.getTopicVersionId()
		);
	}

	/**
	 *
	 */
	public void updateUserBlock(UserBlock userBlock) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_UPDATE_USER_BLOCK,
				userBlock.getWikiUserId(),
				userBlock.getIpAddress(),
				userBlock.getBlockDate(),
				userBlock.getBlockEndDate(),
				userBlock.getBlockReason(),
				userBlock.getBlockedByUserId(),
				userBlock.getUnblockDate(),
				userBlock.getUnblockReason(),
				userBlock.getUnblockedByUserId(),
				userBlock.getBlockId()
		);
	}

	/**
	 *
	 */
	public void updateUserDetails(WikiUserDetails userDetails) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_UPDATE_USER,
				userDetails.getPassword(),
				1,
				userDetails.getUsername()
		);
	}

	/**
	 *
	 */
	public void updateVirtualWiki(VirtualWiki virtualWiki) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_UPDATE_VIRTUAL_WIKI,
				(virtualWiki.isDefaultRootTopicName() ? null : virtualWiki.getRootTopicName()),
				(virtualWiki.isDefaultLogoImageUrl() ? null : virtualWiki.getLogoImageUrl()),
				(virtualWiki.isDefaultMetaDescription() ? null : virtualWiki.getMetaDescription()),
				(virtualWiki.isDefaultSiteName() ? null : virtualWiki.getSiteName()),
				virtualWiki.getVirtualWikiId()
		);
	}

	/**
	 *
	 */
	public void updateWikiFile(WikiFile wikiFile, int virtualWikiId) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_UPDATE_WIKI_FILE,
				virtualWikiId,
				wikiFile.getFileName(),
				wikiFile.getUrl(),
				wikiFile.getMimeType(),
				wikiFile.getTopicId(),
				wikiFile.getDeleteDate(),
				(wikiFile.getReadOnly() ? 1 : 0),
				(wikiFile.getAdminOnly() ? 1 : 0),
				wikiFile.getFileSize(),
				wikiFile.getFileId()
		);
	}

	/**
	 *
	 */
	public void updateWikiGroup(WikiGroup group) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_UPDATE_GROUP,
				group.getName(),
				group.getDescription(),
				group.getGroupId()
		);
	}

	/**
	 *
	 */
	public void updateWikiUser(WikiUser user) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_UPDATE_WIKI_USER,
				user.getUsername(),
				user.getDisplayName(),
				user.getLastLoginDate(),
				user.getLastLoginIpAddress(),
				user.getEmail(),
				user.getUserId()
		);
	}

	/**
	 *
	 */
	public void updateWikiUserPreferences(WikiUser user) {
		Map<String, String> preferenceDefaults = this.lookupUserPreferencesDefaults();
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_USER_PREFERENCES,
				user.getUserId()
		);
		Map<String, String> preferences = user.getPreferences();
		// Only store preferences that are not default
		List<Object[]> batchArgs = new ArrayList<Object[]>();
		for (String key : preferences.keySet()) {
			String defVal = preferenceDefaults.get(key);
			String cusVal = preferences.get(key);
			if (StringUtils.isBlank(cusVal) || StringUtils.equals(defVal, cusVal)) {
				continue;
			}
			Object[] args = { user.getUserId(), key, cusVal };
			batchArgs.add(args);
		}
		if (!batchArgs.isEmpty()) {
			DatabaseConnection.getJdbcTemplate().batchUpdate(STATEMENT_INSERT_USER_PREFERENCE, batchArgs);
		}
	}

	/**
	 *
	 */
	public void updateUserPreferenceDefault(String userPreferenceKey, String userPreferenceDefaultValue, String userPreferenceGroupKey, int sequenceNr) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_UPDATE_USER_PREFERENCE_DEFAULTS,
				userPreferenceDefaultValue,
				userPreferenceGroupKey,
				sequenceNr,
				userPreferenceKey
		);
	}

	/**
	 *
	 */
	public boolean existsUserPreferenceDefault(String userPreferenceKey) {
		HashMap<String, Map<String, String>> defaultPrefs = this.getUserPreferencesDefaults();
		for (Map<String, String> group: defaultPrefs.values()) {
			if (group.containsKey(userPreferenceKey)) {
				return true;
			}
		}
		return false;
	}

	/**
	 *
	 */
	public void updatePwResetChallengeData(WikiUser user) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_UPDATE_PW_RESET_CHALLENGE_DATA,
				user.getChallengeValue(),
				user.getChallengeDate(),
				user.getChallengeIp(),
				user.getChallengeTries(),
				user.getUsername()
		);
	}
	/**
	 *
	 */
	public void insertImage(ImageData imageData, boolean isResized) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_INSERT_FILE_DATA,
				imageData.fileVersionId,
				(isResized ? imageData.width : 0),
				imageData.width,
				imageData.height,
				imageData.data
		);
	}

	/**
	 *
	 */
	public void deleteResizedImages(int fileId) {
		DatabaseConnection.getJdbcTemplate().update(
				STATEMENT_DELETE_RESIZED_IMAGES,
				fileId
		);
	}

	/**
	 *
	 */
	public ImageData getImageInfo(int fileId, int resized) {
		Object[] args = { fileId, resized };
		try {
			return DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_FILE_INFO, args, new ImageDataMapper(false));
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
			return null;
		}
	}

	/**
	 *
	 */
	public ImageData getImageData(int fileId, int resized) {
		Object[] args = { fileId, resized };
		try {
			return DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_FILE_DATA, args, new ImageDataMapper(true));
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
			return null;
		}
	}

	/**
	 *
	 */
	public ImageData getImageVersionData(int fileVersionId, int resized) {
		Object[] args = { fileVersionId, resized };
		try {
			return DatabaseConnection.getJdbcTemplate().queryForObject(STATEMENT_SELECT_FILE_VERSION_DATA, args, new ImageDataMapper(true));
		} catch (IncorrectResultSizeDataAccessException e) {
			// no matching result
			return null;
		}
	}

	/**
	 * Inner class for converting result set to category.
	 */
	static final class CategoryMapper implements RowMapper<Category> {

		private final String virtualWikiName;

		/**
		 *
		 */
		CategoryMapper(String virtualWikiName) {
			this.virtualWikiName = virtualWikiName;
		}

		/**
		 *
		 */
		public Category mapRow(ResultSet rs, int rowNum) throws SQLException {
			Category category = new Category();
			category.setName(rs.getString("category_name"));
			category.setVirtualWiki(this.virtualWikiName);
			category.setChildTopicName(rs.getString("topic_name"));
			category.setSortKey(rs.getString("sort_key"));
			category.setTopicType(TopicType.findTopicType(rs.getInt("topic_type")));
			return category;
		}
	}

	/**
	 * Inner class for converting result set to interwiki.
	 */
	static final class ImageDataMapper implements RowMapper<ImageData> {

		private final boolean isFileVersion;

		/**
		 *
		 */
		ImageDataMapper(boolean isFileVersion) {
			this.isFileVersion = isFileVersion;
		}

		/**
		 *
		 */
		public ImageData mapRow(ResultSet rs, int rowNum) throws SQLException {
			String mimeType = rs.getString("mime_type");
			int height = rs.getInt("image_height");
			int width = rs.getInt("image_width");
			byte[] data = (this.isFileVersion) ? rs.getBytes("file_data") : null;
			ImageData imageData = new ImageData(mimeType, width, height, data);
			if (this.isFileVersion) {
				imageData.fileVersionId = rs.getInt("file_version_id");
			}
			return imageData;
		}
	}

	/**
	 * Inner class for converting result set to interwiki.
	 */
	static final class InterwikiMapper implements RowMapper<Interwiki> {

		/**
		 *
		 */
		public Interwiki mapRow(ResultSet rs, int rowNum) throws SQLException {
			String interwikiPrefix = rs.getString("interwiki_prefix");
			String interwikiPattern = rs.getString("interwiki_pattern");
			String interwikiDisplay = rs.getString("interwiki_display");
			int interwikiType = rs.getInt("interwiki_type");
			Interwiki interwiki = new Interwiki(interwikiPrefix, interwikiPattern, interwikiDisplay);
			interwiki.setInterwikiType(interwikiType);
			return interwiki;
		}
	}

	/**
	 * Inner class for converting result set to log item.
	 */
	static final class LogItemMapper implements RowMapper<LogItem> {

		private final String virtualWikiName;

		/**
		 *
		 */
		LogItemMapper(String virtualWikiName) {
			this.virtualWikiName = virtualWikiName;
		}

		/**
		 *
		 */
		public LogItem mapRow(ResultSet rs, int rowNum) throws SQLException {
			LogItem logItem = new LogItem();
			int userId = rs.getInt("wiki_user_id");
			if (userId > 0) {
				logItem.setUserId(userId);
			}
			logItem.setUserDisplayName(rs.getString("display_name"));
			int topicId = rs.getInt("topic_id");
			if (topicId > 0) {
				logItem.setTopicId(topicId);
			}
			int topicVersionId = rs.getInt("topic_version_id");
			if (topicVersionId > 0) {
				logItem.setTopicVersionId(topicVersionId);
			}
			logItem.setLogDate(rs.getTimestamp("log_date"));
			logItem.setLogComment(rs.getString("log_comment"));
			logItem.setLogParamString(rs.getString("log_params"));
			logItem.setLogType(rs.getInt("log_type"));
			logItem.setLogSubType(rs.getInt("log_sub_type"));
			logItem.setVirtualWiki(virtualWikiName);
			return logItem;
		}
	}

	/**
	 * Inner class for converting result set to recent change.
	 */
	static final class RecentChangeMapper implements RowMapper<RecentChange> {

		/**
		 *
		 */
		public RecentChange mapRow(ResultSet rs, int rowNum) throws SQLException {
			RecentChange change = new RecentChange();
			int topicVersionId = rs.getInt("topic_version_id");
			if (topicVersionId > 0) {
				change.setTopicVersionId(topicVersionId);
			}
			int previousTopicVersionId = rs.getInt("previous_topic_version_id");
			if (previousTopicVersionId > 0) {
				change.setPreviousTopicVersionId(previousTopicVersionId);
			}
			int topicId = rs.getInt("topic_id");
			if (topicId > 0) {
				change.setTopicId(topicId);
			}
			change.setTopicName(rs.getString("topic_name"));
			change.setCharactersChanged(rs.getInt("characters_changed"));
			change.setChangeDate(rs.getTimestamp("change_date"));
			change.setChangeComment(rs.getString("change_comment"));
			int userId = rs.getInt("wiki_user_id");
			if (userId > 0) {
				change.setAuthorId(userId);
			}
			change.setAuthorName(rs.getString("display_name"));
			int editType = rs.getInt("edit_type");
			if (editType > 0) {
				change.setEditType(editType);
				change.initChangeWikiMessageForVersion(editType, rs.getString("log_params"));
			}
			int logType = rs.getInt("log_type");
			Integer logSubType = (rs.getInt("log_sub_type") <= 0) ? null : rs.getInt("log_sub_type");
			if (logType > 0) {
				change.setLogType(logType);
				change.setLogSubType(logSubType);
				change.initChangeWikiMessageForLog(rs.getString("virtual_wiki_name"), logType, logSubType, rs.getString("log_params"), change.getTopicVersionId());
			}
			change.setVirtualWiki(rs.getString("virtual_wiki_name"));
			return change;
		}
	}

	/**
	 * Inner class for converting result set to role.
	 */
	static final class RoleMapper implements RowMapper<Role> {

		/**
		 *
		 */
		public Role mapRow(ResultSet rs, int rowNum) throws SQLException {
			Role role = new Role(rs.getString("role_name"));
			role.setDescription(rs.getString("role_description"));
			return role;
		}
	}

	/**
	 * Inner class for converting result set to topic.
	 */
	static final class TopicMapper implements RowMapper<Topic> {

		/**
		 *
		 */
		public Topic mapRow(ResultSet rs, int rowNum) throws SQLException {
			Topic topic = new Topic(rs.getString("virtual_wiki_name"), Namespace.namespace(rs.getInt("namespace_id")), rs.getString("page_name"));
			topic.setAdminOnly(rs.getInt("topic_admin_only") != 0);
			int currentVersionId = rs.getInt("current_version_id");
			if (currentVersionId > 0) {
				topic.setCurrentVersionId(currentVersionId);
			}
			topic.setTopicContent(rs.getString("version_content"));
			// FIXME - Oracle cannot store an empty string - it converts them
			// to null - so add a hack to work around the problem.
			if (topic.getTopicContent() == null) {
				topic.setTopicContent("");
			}
			topic.setTopicId(rs.getInt("topic_id"));
			topic.setReadOnly(rs.getInt("topic_read_only") != 0);
			topic.setDeleteDate(rs.getTimestamp("delete_date"));
			topic.setTopicType(TopicType.findTopicType(rs.getInt("topic_type")));
			topic.setRedirectTo(rs.getString("redirect_to"));
			return topic;
		}
	}

	/**
	 * Inner class for converting result set to topic version.
	 */
	static final class TopicVersionMapper implements RowMapper<TopicVersion> {

		/**
		 *
		 */
		public TopicVersion mapRow(ResultSet rs, int rowNum) throws SQLException {
			TopicVersion topicVersion = new TopicVersion();
			topicVersion.setTopicVersionId(rs.getInt("topic_version_id"));
			topicVersion.setTopicId(rs.getInt("topic_id"));
			topicVersion.setEditComment(rs.getString("edit_comment"));
			topicVersion.setVersionContent(rs.getString("version_content"));
			// FIXME - Oracle cannot store an empty string - it converts them
			// to null - so add a hack to work around the problem.
			if (topicVersion.getVersionContent() == null) {
				topicVersion.setVersionContent("");
			}
			int previousTopicVersionId = rs.getInt("previous_topic_version_id");
			if (previousTopicVersionId > 0) {
				topicVersion.setPreviousTopicVersionId(previousTopicVersionId);
			}
			int userId = rs.getInt("wiki_user_id");
			if (userId > 0) {
				topicVersion.setAuthorId(userId);
			}
			topicVersion.setCharactersChanged(rs.getInt("characters_changed"));
			topicVersion.setVersionParamString(rs.getString("version_params"));
			topicVersion.setEditDate(rs.getTimestamp("edit_date"));
			topicVersion.setEditType(rs.getInt("edit_type"));
			topicVersion.setAuthorDisplay(rs.getString("wiki_user_display"));
			return topicVersion;
		}
	}

	/**
	 * Inner class for converting result set to user block.
	 */
	static final class UserBlockMapper implements RowMapper<UserBlock> {

		/**
		 *
		 */
		public UserBlock mapRow(ResultSet rs, int rowNum) throws SQLException {
			Integer wikiUserId = (rs.getInt("wiki_user_id") > 0) ? rs.getInt("wiki_user_id") : null;
			String ipAddress = rs.getString("ip_address");
			Timestamp blockEndDate = rs.getTimestamp("block_end_date");
			int blockedByUserId = rs.getInt("blocked_by_user_id");
			UserBlock userBlock = new UserBlock(wikiUserId, ipAddress, blockEndDate, blockedByUserId);
			userBlock.setBlockId(rs.getInt("user_block_id"));
			userBlock.setBlockDate(rs.getTimestamp("block_date"));
			userBlock.setBlockReason(rs.getString("block_reason"));
			userBlock.setUnblockDate(rs.getTimestamp("unblock_date"));
			userBlock.setUnblockReason(rs.getString("unblock_reason"));
			int unblockedByUserId = rs.getInt("unblocked_by_user_id");
			if (unblockedByUserId > 0) {
				userBlock.setUnblockedByUserId(unblockedByUserId);
			}
			return userBlock;
		}
	}

	/**
	 * Inner class for converting result set to virtual wiki.
	 */
	static final class VirtualWikiMapper implements RowMapper<VirtualWiki> {

		/**
		 *
		 */
		public VirtualWiki mapRow(ResultSet rs, int rowNum) throws SQLException {
			VirtualWiki virtualWiki = new VirtualWiki(rs.getString("virtual_wiki_name"));
			virtualWiki.setVirtualWikiId(rs.getInt("virtual_wiki_id"));
			virtualWiki.setRootTopicName(rs.getString("default_topic_name"));
			virtualWiki.setLogoImageUrl(rs.getString("logo_image_url"));
			virtualWiki.setMetaDescription(rs.getString("meta_description"));
			virtualWiki.setSiteName(rs.getString("site_name"));
			return virtualWiki;
		}
	}

	/**
	 * Inner class for converting result set to wiki file.
	 */
	static final class WikiFileMapper implements RowMapper<WikiFile> {

		private final String virtualWikiName;

		/**
		 *
		 */
		WikiFileMapper(String virtualWikiName) {
			this.virtualWikiName = virtualWikiName;
		}

		/**
		 *
		 */
		public WikiFile mapRow(ResultSet rs, int rowNum) throws SQLException {
			WikiFile wikiFile = new WikiFile();
			wikiFile.setFileId(rs.getInt("file_id"));
			wikiFile.setAdminOnly(rs.getInt("file_admin_only") != 0);
			wikiFile.setFileName(rs.getString("file_name"));
			wikiFile.setVirtualWiki(this.virtualWikiName);
			wikiFile.setUrl(rs.getString("file_url"));
			wikiFile.setTopicId(rs.getInt("topic_id"));
			wikiFile.setReadOnly(rs.getInt("file_read_only") != 0);
			wikiFile.setDeleteDate(rs.getTimestamp("delete_date"));
			wikiFile.setMimeType(rs.getString("mime_type"));
			wikiFile.setFileSize(rs.getInt("file_size"));
			return wikiFile;
		}
	}

	/**
	 * Inner class for converting result set to wiki file version.
	 */
	static final class WikiFileVersionMapper implements RowMapper<WikiFileVersion> {

		/**
		 *
		 */
		public WikiFileVersion mapRow(ResultSet rs, int rowNum) throws SQLException {
			WikiFileVersion wikiFileVersion = new WikiFileVersion();
			wikiFileVersion.setFileVersionId(rs.getInt("file_version_id"));
			wikiFileVersion.setFileId(rs.getInt("file_id"));
			wikiFileVersion.setUploadComment(rs.getString("upload_comment"));
			wikiFileVersion.setUrl(rs.getString("file_url"));
			int userId = rs.getInt("wiki_user_id");
			if (userId > 0) {
				wikiFileVersion.setAuthorId(userId);
			}
			wikiFileVersion.setUploadDate(rs.getTimestamp("upload_date"));
			wikiFileVersion.setMimeType(rs.getString("mime_type"));
			wikiFileVersion.setAuthorDisplay(rs.getString("wiki_user_display"));
			wikiFileVersion.setFileSize(rs.getInt("file_size"));
			return wikiFileVersion;
		}
	}

	/**
	 * Inner class for converting result set to wiki group.
	 */
	static final class WikiGroupMapper implements RowMapper<WikiGroup> {

		/**
		 *
		 */
		public WikiGroup mapRow(ResultSet rs, int rowNum) throws SQLException {
			WikiGroup wikiGroup = new WikiGroup(rs.getString("group_name"));
			wikiGroup.setGroupId(rs.getInt("group_id"));
			wikiGroup.setDescription(rs.getString("group_description"));
			return wikiGroup;
		}
	}

	/**
	 * Inner class for converting result set to wiki user.
	 */
	static final class WikiUserMapper implements RowMapper<WikiUser> {

		/**
		 *
		 */
		public WikiUser mapRow(ResultSet rs, int rowNum) throws SQLException {
			String username = rs.getString("login");
			WikiUser user = new WikiUser(username);
			user.setDisplayName(rs.getString("display_name"));
			user.setUserId(rs.getInt("wiki_user_id"));
			user.setCreateDate(rs.getTimestamp("create_date"));
			user.setLastLoginDate(rs.getTimestamp("last_login_date"));
			user.setCreateIpAddress(rs.getString("create_ip_address"));
			user.setLastLoginIpAddress(rs.getString("last_login_ip_address"));
			user.setEmail(rs.getString("email"));
			return user;
		}
	}
}
