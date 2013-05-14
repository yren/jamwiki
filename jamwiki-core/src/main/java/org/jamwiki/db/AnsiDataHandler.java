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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.jamwiki.Environment;
import org.jamwiki.WikiBase;
import org.jamwiki.WikiException;
import org.jamwiki.WikiMessage;
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
import org.jamwiki.model.Watchlist;
import org.jamwiki.model.WikiFile;
import org.jamwiki.model.WikiFileVersion;
import org.jamwiki.model.WikiGroup;
import org.jamwiki.model.WikiUser;
import org.jamwiki.model.WikiUserDetails;
import org.jamwiki.parser.LinkUtil;
import org.jamwiki.parser.ParserException;
import org.jamwiki.parser.ParserOutput;
import org.jamwiki.parser.ParserUtil;
import org.jamwiki.utils.Encryption;
import org.jamwiki.utils.Pagination;
import org.jamwiki.utils.ResourceUtil;
import org.jamwiki.utils.WikiCache;
import org.jamwiki.utils.WikiLogger;
import org.jamwiki.utils.WikiUtil;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.NonTransientDataAccessResourceException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;

/**
 * Default handler for ANSI SQL compatible databases.
 */
public class AnsiDataHandler {

	/** Any topic lookup that takes longer than the specified time (in ms) will trigger a log message. */
	private static final int TIME_LIMIT_TOPIC_LOOKUP = 20;
	private static final WikiCache<String, List<Interwiki>> CACHE_INTERWIKI_LIST = new WikiCache<String, List<Interwiki>>("org.jamwiki.db.AnsiDataHandler.CACHE_INTERWIKI_LIST");
	private static final WikiCache<String, List<Namespace>> CACHE_NAMESPACE_LIST = new WikiCache<String, List<Namespace>>("org.jamwiki.db.AnsiDataHandler.CACHE_NAMESPACE_LIST");
	private static final WikiCache<String, List<RoleMap>> CACHE_ROLE_MAP_GROUP = new WikiCache<String, List<RoleMap>>("org.jamwiki.db.AnsiDataHandler.CACHE_ROLE_MAP_GROUP");
	/**
	 * Cache a topic name lookup to the actual topic name, useful for cases where
	 * a topic name may vary by case.  This cache should not include deleted topics.
	 */
	private static final WikiCache<String, String> CACHE_TOPIC_NAMES_BY_NAME = new WikiCache<String, String>("org.jamwiki.db.AnsiDataHandler.CACHE_TOPIC_NAMES_BY_NAME");
	/** Cache a topic object by its ID value.  This cache may include deleted topics. */
	private static final WikiCache<Integer, Topic> CACHE_TOPICS_BY_ID = new WikiCache<Integer, Topic>("org.jamwiki.db.AnsiDataHandler.CACHE_TOPICS_BY_ID");
	/** Cache topic IDs by the topic name.  This cache may include deleted topics. */
	private static final WikiCache<String, Integer> CACHE_TOPIC_IDS_BY_NAME = new WikiCache<String, Integer>("org.jamwiki.db.AnsiDataHandler.CACHE_TOPIC_IDS_BY_NAME");
	private static final WikiCache<Integer, TopicVersion> CACHE_TOPIC_VERSIONS = new WikiCache<Integer, TopicVersion>("org.jamwiki.db.AnsiDataHandler.CACHE_TOPIC_VERSIONS");
	private static final WikiCache<String, Map<Object, UserBlock>> CACHE_USER_BLOCKS_ACTIVE = new WikiCache<String, Map<Object, UserBlock>>("org.jamwiki.db.AnsiDataHandler.CACHE_USER_BLOCKS_ACTIVE");
	private static final WikiCache<Integer, WikiUser> CACHE_USER_BY_USER_ID = new WikiCache<Integer, WikiUser>("org.jamwiki.db.AnsiDataHandler.CACHE_USER_BY_USER_ID");
	private static final WikiCache<String, WikiUser> CACHE_USER_BY_USER_NAME = new WikiCache<String, WikiUser>("org.jamwiki.db.AnsiDataHandler.CACHE_USER_BY_USER_NAME");
	private static final WikiCache<String, List<VirtualWiki>> CACHE_VIRTUAL_WIKI_LIST = new WikiCache<String, List<VirtualWiki>>("org.jamwiki.db.AnsiDataHandler.CACHE_VIRTUAL_WIKI_LIST");
	private static final WikiLogger logger = WikiLogger.getLogger(AnsiDataHandler.class.getName());

	// TODO - remove when the ability to upgrade to 1.3 is deprecated
	private static final Map<String, String> LEGACY_DATA_HANDLER_MAP = new HashMap<String, String>();
	static {
		LEGACY_DATA_HANDLER_MAP.put("org.jamwiki.db.AnsiDataHandler", QueryHandler.QUERY_HANDLER_ANSI);
		LEGACY_DATA_HANDLER_MAP.put("org.jamwiki.db.CacheDataHandler", QueryHandler.QUERY_HANDLER_CACHE);
		LEGACY_DATA_HANDLER_MAP.put("org.jamwiki.db.DB2DataHandler", QueryHandler.QUERY_HANDLER_DB2);
		LEGACY_DATA_HANDLER_MAP.put("org.jamwiki.db.DB2400DataHandler", QueryHandler.QUERY_HANDLER_DB2400);
		LEGACY_DATA_HANDLER_MAP.put("org.jamwiki.db.H2DataHandler", QueryHandler.QUERY_HANDLER_H2);
		LEGACY_DATA_HANDLER_MAP.put("org.jamwiki.db.HSqlDataHandler", QueryHandler.QUERY_HANDLER_HSQL);
		LEGACY_DATA_HANDLER_MAP.put("org.jamwiki.db.MSSqlDataHandler", QueryHandler.QUERY_HANDLER_MSSQL);
		LEGACY_DATA_HANDLER_MAP.put("org.jamwiki.db.MySqlDataHandler", QueryHandler.QUERY_HANDLER_MYSQL);
		LEGACY_DATA_HANDLER_MAP.put("org.jamwiki.db.OracleDataHandler", QueryHandler.QUERY_HANDLER_ORACLE);
		LEGACY_DATA_HANDLER_MAP.put("org.jamwiki.db.PostgresDataHandler", QueryHandler.QUERY_HANDLER_POSTGRES);
		LEGACY_DATA_HANDLER_MAP.put("org.jamwiki.db.SybaseASADataHandler", QueryHandler.QUERY_HANDLER_SYBASE);
	}

	protected final QueryHandler queryHandler;
	protected AnsiDataValidator dataValidator = new AnsiDataValidator();

	/**
	 *
	 */
	public AnsiDataHandler() {
		this.queryHandler = this.queryHandlerInstance();
	}

	/**
	 *
	 */
	private void addTopicLinks(List<String> links, String virtualWiki, int topicId) {
		// strip any links longer than 200 characters and any duplicates
		Map<String, Topic> linksMap = new HashMap<String, Topic>();
		for (String link : links) {
			if (link.length() <= 200) {
				Namespace namespace = LinkUtil.retrieveTopicNamespace(virtualWiki, link);
				String pageName = LinkUtil.retrieveTopicPageName(namespace, virtualWiki, link);
				// FIXE - link to records are always capitalized, which will cause problems for the
				// rare case of two topics such as "eBay" and "EBay".
				pageName = StringUtils.capitalize(pageName);
				Topic topic = new Topic(virtualWiki, namespace, pageName);
				linksMap.put(topic.getName(), topic);
			}
		}
		List<Topic> topicLinks = new ArrayList<Topic>(linksMap.values());
		this.queryHandler().insertTopicLinks(topicLinks, topicId);
	}

	/**
	 * Determine if a value matching the given username and password exists in
	 * the data store.
	 *
	 * @param username The username that is being validated against.
	 * @param password The password that is being validated against.
	 * @return <code>true</code> if the username / password combination matches
	 *  an existing record in the data store, <code>false</code> otherwise.
	 */
	public boolean authenticate(String username, String password) {
		if (StringUtils.isBlank(password)) {
			return false;
		}
		// password is stored encrypted, so encrypt password
		String encryptedPassword = Encryption.encrypt(password);
		return this.queryHandler().authenticateUser(username, encryptedPassword);
	}

	/**
	 * Utility method for retrieving a user display name.
	 */
	private String authorName(Integer authorId, String authorName) {
		if (authorId != null) {
			WikiUser user = this.lookupWikiUser(authorId);
			authorName = user.getUsername();
		}
		return authorName;
	}

	/**
	 * Given a virtual wiki and topic name, generate the key used for caching
	 * the corresponding topic information.
	 */
	private String cacheTopicKey(String virtualWiki, Namespace namespace, String pageName) {
		StringBuilder cacheKey = new StringBuilder(virtualWiki);
		cacheKey.append('/');
		if (namespace.getLabel(virtualWiki).length() != 0) {
			cacheKey.append(namespace.getLabel(virtualWiki));
			cacheKey.append(Namespace.SEPARATOR);
		}
		cacheKey.append(pageName);
		return cacheKey.toString();
	}

	/**
	 * Call this method whenever a topic is updated to update all relevant caches
	 * for the topic.
	 *
	 * @param topic The topic being added/updated in the cache.
	 * @param removeExisting Set to <code>true</code> when data has been updated,
	 *  such as when adding or updating a topic.  If a topic is simply being cached
	 *  after a lookup then set to <code>false</code> to avoid any unnecessary
	 *  performance overhead.
	 * @param altKey Specifies an alternative key to use for caching, such as when
	 *  using a shared virtual wiki.  May also be <code>null</code>.
	 */
	private void cacheTopicRefresh(Topic topic, boolean removeExisting, String altKey) {
		String key = this.cacheTopicKey(topic.getVirtualWiki(), topic.getNamespace(), topic.getPageName());
		boolean useAltKey = (altKey != null && !key.equals(altKey));
		if (removeExisting) {
			// because some topics may be cached in a case-insensitive manner remove
			// all possible cache keys for the topic, regardless of case
			WikiBase.CACHE_PARSED_TOPIC_CONTENT.removeFromCacheCaseInsensitive(key);
			CACHE_TOPIC_NAMES_BY_NAME.removeFromCacheCaseInsensitive(key);
			CACHE_TOPIC_IDS_BY_NAME.removeFromCacheCaseInsensitive(key);
			if (useAltKey && !key.equalsIgnoreCase(altKey)) {
				// if the two keys differ only by case then the previous remove
				// will have already removed the alt version, otherwise perform
				// a second remove
				WikiBase.CACHE_PARSED_TOPIC_CONTENT.removeFromCacheCaseInsensitive(altKey);
				CACHE_TOPIC_NAMES_BY_NAME.removeFromCacheCaseInsensitive(altKey);
				CACHE_TOPIC_IDS_BY_NAME.removeFromCacheCaseInsensitive(altKey);
			}
		}
		if (topic.getDeleteDate() == null) {
			// topic name cache does not include deleted topics
			CACHE_TOPIC_NAMES_BY_NAME.addToCache(key, topic.getName());
			if (useAltKey) {
				CACHE_TOPIC_NAMES_BY_NAME.addToCache(altKey, topic.getName());
			}
		}
		CACHE_TOPIC_IDS_BY_NAME.addToCache(key, topic.getTopicId());
		if (useAltKey) {
			CACHE_TOPIC_IDS_BY_NAME.addToCache(altKey, topic.getTopicId());
		}
		CACHE_TOPICS_BY_ID.addToCache(topic.getTopicId(), new Topic(topic));
	}

	/**
	 * Determine if a topic can be moved to a new location.  If the
	 * destination is not an existing topic, is a topic that has been deleted,
	 * or is a topic that redirects to the source topic then this method
	 * should return <code>true</code>.
	 *
	 * @param fromTopic The Topic that is being moved.
	 * @param destination The new name for the topic.
	 * @return <code>true</code> if the topic can be moved to the destination,
	 *  <code>false</code> otherwise.
	 */
	public boolean canMoveTopic(Topic fromTopic, String destination) {
		Topic toTopic = this.lookupTopic(fromTopic.getVirtualWiki(), destination, false);
		if (toTopic == null || toTopic.getDeleteDate() != null) {
			// destination doesn't exist or is deleted, so move is OK
			return true;
		}
		if (!toTopic.getVirtualWiki().equals(fromTopic.getVirtualWiki())) {
			// topics are on different virtual wikis (can happen with shared images) so move is not allowed
			return false;
		}
		if (toTopic.getRedirectTo() != null && toTopic.getRedirectTo().equals(fromTopic.getName())) {
			// source redirects to destination, so move is OK
			return true;
		}
		return false;
	}

	/**
	 * Delete an interwiki record from the interwiki table.
	 *
	 * @param interwiki The Interwiki record to be deleted.
	 */
	public void deleteInterwiki(Interwiki interwiki) {
		this.queryHandler().deleteInterwiki(interwiki);
		CACHE_INTERWIKI_LIST.removeAllFromCache();
	}

	/**
	 * Mark a topic deleted by setting its delete date to a non-null value.
	 * Prior to calling this method the topic content should also be set
	 * empty.  This method will also delete recent changes for the topic,
	 * and a new TopicVersion should be supplied reflecting the topic deletion
	 * event.
	 *
	 * @param topic The Topic object that is being deleted.
	 * @param topicVersion A TopicVersion object that indicates the delete
	 *  date, author, and other parameters for the topic.  If this value is
	 *  <code>null</code> then no version is saved, nor is any recent change
	 *  entry created.
	 * @throws WikiException Thrown if the topic information is invalid.
	 */
	public void deleteTopic(final Topic topic, final TopicVersion topicVersion) throws WikiException {
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					try {
						if (topicVersion != null) {
							// delete old recent changes
							queryHandler().deleteRecentChanges(topic.getTopicId());
						}
						// update topic to indicate deleted, add delete topic version.  parser output
						// should be empty since no links or categories to update.
						ParserOutput parserOutput = new ParserOutput();
						topic.setDeleteDate(new Timestamp(System.currentTimeMillis()));
						writeTopic(topic, topicVersion, parserOutput.getCategories(), parserOutput.getLinks());
					} catch (WikiException e) {
						status.setRollbackOnly();
						throw new TransactionRuntimeException(e);
					}
				}
			}
		);
	}

	/**
	 * Determine the largest namespace ID for all current defined namespaces.
	 */
	private int findMaxNamespaceId() {
		int namespaceEnd = 0;
		for (Namespace namespace : this.lookupNamespaces()) {
			namespaceEnd = (namespace.getId() > namespaceEnd) ? namespace.getId() : namespaceEnd;
		}
		return namespaceEnd;
	}

	/**
	 * Return a List of all Category objects for a given virtual wiki.
	 *
	 * @param virtualWiki The virtual wiki for which categories are being
	 *  retrieved.
	 * @param pagination A Pagination object indicating the total number of
	 *  results and offset for the results to be retrieved.
	 * @return A List of all Category objects for a given virutal wiki.
	 */
	public List<Category> getAllCategories(String virtualWiki, Pagination pagination) {
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		return this.queryHandler().getCategories(virtualWikiId, virtualWiki, pagination);
	}

	/**
	 * Return a List of all Role objects for the wiki.
	 *
	 * @return A List of all Role objects for the wiki.
	 */
	public List<Role> getAllRoles() {
		return this.queryHandler().getRoles();
	}

	/**
	 * Return a List of all custom WikiGroup objects for the wiki, i.e. all groups except the groups
	 * GROUP_ANONYMOUS and GROUP_REGISTERED_USER. These are managed only internally
	 * through the application.
	 *
	 * @return A List of all custom WikiGroups objects for the wiki
	 */
	public List<WikiGroup> getAllWikiGroups() {
		return this.queryHandler().getGroups();
	}

	/**
	 * Retrieve the GroupMap for the group identified by groupId. The GroupMap contains
	 * a list of all its members (logins)
	 * @param groupId The group to retrieve
	 * @return The GroupMap for the group identified by groupId.
	 */
	public GroupMap getGroupMapGroup(int groupId) {
		return this.queryHandler().lookupGroupMapGroup(groupId);
	}

	/**
	 * Retrieve the GroupMap for the user identified by login. The GroupMap contains
	 * a list of all groups that this login belongs to.
	 *
	 * @param userLogin The user, whose groups must be looked up
	 * @return The GroupMap of the user identified by login
	 */
	public GroupMap getGroupMapUser(String userLogin) {
		return this.queryHandler().lookupGroupMapUser(userLogin);
	}

	/**
	 * Return a List of all topic names for all topics that exist for
	 * the virtual wiki.
	 *
	 * @param virtualWiki The virtual wiki for which topics are being
	 *  retrieved.
	 * @param includeDeleted Set to <code>true</code> if deleted topics
	 *  should be included in the results.
	 * @return A List of all topic names for all non-deleted topics that
	 *  exist for the virtual wiki.
	 */
	public List<String> getAllTopicNames(String virtualWiki, boolean includeDeleted) {
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		return new ArrayList<String>(this.queryHandler().lookupTopicNames(virtualWikiId, includeDeleted).values());
	}

	/**
	 * Retrieve a List of all TopicVersions for a given topic, sorted
	 * chronologically.
	 *
	 * @param virtualWiki The virtual wiki for the topic being queried.
	 * @param topicName The name of the topic being queried.
	 * @param descending Set to <code>true</code> if the results should be
	 *  sorted with the most recent version first, <code>false</code> if the
	 *  results should be sorted with the oldest versions first.
	 * @return A List of all TopicVersion objects for the given topic.
	 *  If no matching topic exists then an exception is thrown.
	 */
	public List<WikiFileVersion> getAllWikiFileVersions(String virtualWiki, String topicName, boolean descending) {
		WikiFile wikiFile = lookupWikiFile(virtualWiki, topicName);
		if (wikiFile == null) {
			throw new InvalidDataAccessApiUsageException("No topic exists for " + virtualWiki + " / " + topicName);
		}
		return this.queryHandler().getAllWikiFileVersions(wikiFile, descending);
	}

	/**
	 * Return a map of key/map(key/value) pairs containing the defined user preferences
	 * defaults.  The map returned has the following structure:
	 * pref_group_key -> Map(pref_key -> pref_value)
	 * the pref_group_key points to a map of pref key/value pairs that belong to it.
	 */
	public Map<String, Map<String, String>> getUserPreferencesDefaults() {
		return this.queryHandler().getUserPreferencesDefaults();
	}

	/**
	 * Retrieve a List of all LogItem objects for a given virtual wiki, sorted
	 * chronologically.
	 *
	 * @param virtualWiki The virtual wiki for which log items are being
	 *  retrieved.
	 * @param logType Set to <code>-1</code> if all log items should be returned,
	 *  otherwise set the log type for items to retrieve.
	 * @param pagination A Pagination object indicating the total number of
	 *  results and offset for the results to be retrieved.
	 * @param descending Set to <code>true</code> if the results should be
	 *  sorted with the most recent log items first, <code>false</code> if the
	 *  results should be sorted with the oldest items first.
	 * @return A List of LogItem objects for a given virtual wiki, sorted
	 *  chronologically.
	 */
	public List<LogItem> getLogItems(String virtualWiki, int logType, Pagination pagination, boolean descending) {
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		return this.queryHandler().getLogItems(virtualWikiId, virtualWiki, logType, pagination, descending);
	}

	/**
	 * Retrieve a List of all RecentChange objects for a given virtual
	 * wiki, sorted chronologically.
	 *
	 * @param virtualWiki The virtual wiki for which recent changes are being
	 *  retrieved.
	 * @param pagination A Pagination object indicating the total number of
	 *  results and offset for the results to be retrieved.
	 * @param descending Set to <code>true</code> if the results should be
	 *  sorted with the most recent changes first, <code>false</code> if the
	 *  results should be sorted with the oldest changes first.
	 * @return A List of all RecentChange objects for a given virtual
	 *  wiki, sorted chronologically.
	 */
	public List<RecentChange> getRecentChanges(String virtualWiki, Pagination pagination, boolean descending) {
		return this.queryHandler().getRecentChanges(virtualWiki, pagination, descending);
	}

	/**
	 * Retrieve a List of RoleMap objects for all users whose login
	 * contains the given login fragment.
	 *
	 * @param loginFragment A value that must be contained with the user's
	 *  login.  This method will return partial matches, so "name" will
	 *  match "name", "firstname" and "namesake".
	 * @return A list of RoleMap objects containing all roles for all
	 *  users whose login contains the login fragment.  If no matches are
	 *  found then this method returns an empty List.  This method will
	 *  never return <code>null</code>.
	 */
	public List<RoleMap> getRoleMapByLogin(String loginFragment) {
		return this.queryHandler().getRoleMapByLogin(loginFragment);
	}

	/**
	 * Retrieve a list of RoleMap objects for all users and groups who
	 * have been assigned the specified role.
	 *
	 * @param authority The name of the role being queried against.
	 * @return A list of RoleMap objects containing all roles for all
	 *  users and groups who have been assigned the specified role.  If no
	 *  matches are found then this method returns an empty List.  This
	 *  method will never return <code>null</code>.
	 */
	public List<RoleMap> getRoleMapByRole(String authority) {
		return getRoleMapByRole(authority,false);
	}

	/**
	 * Retrieve a list of RoleMap objects for all users and groups who
	 * have been assigned the specified role.
	 *
	 * @param authority The name of the role being queried against.
	 * @param includeInheritedRoles Set to false return only roles that are assigned
	 *  directly 
	 * @return A list of RoleMap objects containing all roles for all
	 *  users and groups who have been assigned the specified role.  If no
	 *  matches are found then this method returns an empty List.  This
	 *  method will never return <code>null</code>.
	 */
	public List<RoleMap> getRoleMapByRole(String authority, boolean includeInheritedRoles) {
		// first check the cache
		List<RoleMap> roleMapList = CACHE_ROLE_MAP_GROUP.retrieveFromCache(authority + includeInheritedRoles);
		if (roleMapList != null || CACHE_ROLE_MAP_GROUP.isKeyInCache(authority + includeInheritedRoles)) {
			return roleMapList;
		}
		// if not in the cache, go to the database
		roleMapList = this.queryHandler().getRoleMapByRole(authority,includeInheritedRoles);
		CACHE_ROLE_MAP_GROUP.addToCache(authority + includeInheritedRoles, roleMapList);
		return roleMapList;
	}

	/**
	 * Retrieve all roles assigned to a given group.
	 *
	 * @param groupName The name of the group for whom roles are being retrieved.
	 * @return An array of Role objects for the given group, or an empty
	 *  array if no roles are assigned to the group.  This method will
	 *  never return <code>null</code>.
	 */
	public List<Role> getRoleMapGroup(String groupName) {
		return this.queryHandler().getRoleMapGroup(groupName);
	}

	/**
	 * Retrieve a list of RoleMap objects for all groups.
	 *
	 * @return A list of RoleMap objects containing all roles for all
	 *  groups.  If no matches are found then this method returns an empty
	 *  List.  This method will never return <code>null</code>.
	 */
	public List<RoleMap> getRoleMapGroups() {
		return this.queryHandler().getRoleMapGroups();
	}

	/**
	 * Retrieve all roles assigned to a given user.
	 *
	 * @param login The login of the user for whom roles are being retrieved.
	 * @return A list of Role objects for the given user, or an empty
	 *  array if no roles are assigned to the user.  This method will
	 *  never return <code>null</code>.
	 */
	public List<Role> getRoleMapUser(String login) {
		return this.queryHandler().getRoleMapUser(login);
	}

	/**
	 * Retrieve a List of RecentChange objects representing a topic's history,
	 * sorted chronologically.
	 *
	 * @param topic The topic whose history is being retrieved.  Note that revisions
	 *  will be returned even if the topic is currently deleted.
	 * @param pagination A Pagination object indicating the total number of
	 *  results and offset for the results to be retrieved.
	 * @param descending Set to <code>true</code> if the results should be
	 *  sorted with the most recent changes first, <code>false</code> if the
	 *  results should be sorted with the oldest changes first.
	 * @return A List of all RecentChange objects representing a topic's history,
	 *  sorted chronologically.
	 */
	public List<RecentChange> getTopicHistory(Topic topic, Pagination pagination, boolean descending) {
		if (topic == null) {
			return new ArrayList<RecentChange>();
		}
		return this.queryHandler().getTopicHistory(topic.getTopicId(), pagination, descending, topic.getDeleted());
	}

	/**
	 * Retrieve a List of topic names for all admin-only topics, sorted
	 * alphabetically.
	 *
	 * @param virtualWiki The virtual wiki for which admin-only topics are
	 *  being retrieved.
	 * @param pagination A Pagination object indicating the total number of
	 *  results and offset for the results to be retrieved.
	 * @return A List of topic names for all admin-only topics, sorted
	 *  alphabetically.
	 */
	public List<String> getTopicsAdmin(String virtualWiki, Pagination pagination) {
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		return this.queryHandler().getTopicsAdmin(virtualWikiId, pagination);
	}

	/**
	 * Return a map of all active user blocks, where the key is the ip or user id
	 * of the blocked user and the value is the UserBlock object.
	 *
	 * @return A map of all active user blocks, where the key is the ip or user id
	 * of the blocked user and the value is the UserBlock object.
	 */
	public Map<Object, UserBlock> getUserBlocks() {
		// for performance reasons cache all active blocks.  in general there
		// shouldn't be a huge number of active blocks at any given time, so
		// rather than hit the database for every page request to verify whether
		// or not the user is blocked it is far more efficient to cache the few
		// active blocks and query against that cached list.
		Map<Object, UserBlock> userBlockMap = CACHE_USER_BLOCKS_ACTIVE.retrieveFromCache(CACHE_USER_BLOCKS_ACTIVE.getCacheName());
		if (userBlockMap != null || CACHE_USER_BLOCKS_ACTIVE.isKeyInCache(CACHE_USER_BLOCKS_ACTIVE.getCacheName())) {
			// note that due to caching some blocks may have expired, so the caller
			// should be sure to check whether a result is still active or not
			return userBlockMap;
		}
		List<UserBlock> userBlocks = this.queryHandler().getUserBlocks();
		userBlockMap = new LinkedHashMap<Object, UserBlock>();
		if (userBlocks != null) {
			for (UserBlock userBlock : userBlocks) {
				if (userBlock.getWikiUserId() != null) {
					userBlockMap.put(userBlock.getWikiUserId(), userBlock);
				}
				if (userBlock.getIpAddress() != null) {
					userBlockMap.put(userBlock.getIpAddress(), userBlock);
				}
			}
		}
		CACHE_USER_BLOCKS_ACTIVE.addToCache(CACHE_USER_BLOCKS_ACTIVE.getCacheName(), userBlockMap);
		return userBlockMap;
	}

	/**
	 * Retrieve a List of RecentChange objects corresponding to all
	 * changes made by a particular user.
	 *
	 * @param virtualWiki The virtual wiki for which changes are being
	 *  retrieved.
	 * @param userString Either a user display, which is typically an IP
	 *  address (for anonymous users) or the user login corresponding to
	 *  the user for whom contributions are being retrieved.
	 * @param pagination A Pagination object indicating the total number of
	 *  results and offset for the results to be retrieved.
	 * @param descending Set to <code>true</code> if the results should be
	 *  sorted with the most recent changes first, <code>false</code> if the
	 *  results should be sorted with the oldest changes first.
	 * @return A List of RecentChange objects corresponding to all
	 *  changes made by a particular user.
	 */
	public List<RecentChange> getUserContributions(String virtualWiki, String userString, Pagination pagination, boolean descending) {
		if (this.lookupWikiUser(userString) != null) {
			return this.queryHandler().getUserContributionsByLogin(virtualWiki, userString, pagination, descending);
		} else {
			return this.queryHandler().getUserContributionsByUserDisplay(virtualWiki, userString, pagination, descending);
		}
	}

	/**
	 * Return a List of all VirtualWiki objects that exist for the wiki.
	 *
	 * @return A List of all VirtualWiki objects that exist for the
	 *  wiki.
	 */
	public List<VirtualWiki> getVirtualWikiList() {
		List<VirtualWiki> virtualWikis = CACHE_VIRTUAL_WIKI_LIST.retrieveFromCache(CACHE_VIRTUAL_WIKI_LIST.getCacheName());
		if (virtualWikis != null || CACHE_VIRTUAL_WIKI_LIST.isKeyInCache(CACHE_VIRTUAL_WIKI_LIST.getCacheName())) {
			return virtualWikis;
		}
		virtualWikis = this.queryHandler().getVirtualWikis();
		CACHE_VIRTUAL_WIKI_LIST.addToCache(CACHE_VIRTUAL_WIKI_LIST.getCacheName(), virtualWikis);
		return virtualWikis;
	}

	/**
	 * Retrieve a user's watchlist.
	 *
	 * @param virtualWiki The virtual wiki for which a watchlist is being
	 *  retrieved.
	 * @param userId The ID of the user whose watchlist is being retrieved.
	 * @return The Watchlist object for the user.
	 */
	public Watchlist getWatchlist(String virtualWiki, int userId) {
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		List<String> watchedTopicNames = this.queryHandler().getWatchlist(virtualWikiId, userId);
		return new Watchlist(virtualWiki, watchedTopicNames);
	}

	/**
	 * Retrieve a List of RecentChange objects corresponding to a user's
	 * watchlist.  This method is primarily used to display a user's watchlist
	 * on the Special:Watchlist page.
	 *
	 * @param virtualWiki The virtual wiki for which a watchlist is being
	 *  retrieved.
	 * @param userId The ID of the user whose watchlist is being retrieved.
	 * @param pagination A Pagination object indicating the total number of
	 *  results and offset for the results to be retrieved.
	 * @return A List of RecentChange objects corresponding to a user's
	 *  watchlist.
	 */
	public List<RecentChange> getWatchlist(String virtualWiki, int userId, Pagination pagination) {
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		return this.queryHandler().getWatchlist(virtualWikiId, userId, pagination);
	}

	/**
	 * Retrieve a List of Category objects corresponding to all topics
	 * that belong to the category, sorted by either the topic name, or
	 * category sort key (if specified).
	 *
	 * @param virtualWiki The virtual wiki for the category being queried.
	 * @param categoryName The name of the category being queried.
	 * @return A List of all Category objects corresponding to all
	 *  topics that belong to the category, sorted by either the topic name,
	 *  or category sort key (if specified).
	 */
	public List<Category> lookupCategoryTopics(String virtualWiki, String categoryName) {
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		return this.queryHandler().lookupCategoryTopics(virtualWikiId, virtualWiki, categoryName);
	}

	/**
	 * Return a map of key-value pairs corresponding to all configuration values
	 * currently set up for the system.
	 *
	 * @return A map of key-value pairs corresponding to all configuration values
	 * currently set up for the system.
	 */
	public Map<String, String> lookupConfiguration() {
		return this.queryHandler().lookupConfiguration();
	}

	/**
	 * Given an interwiki prefix, return the Interwiki that corresponds to that prefix,
	 * or <code>null</code> if no match exists.
	 *
	 * @param interwikiPrefix The value to query to see if a matching interwiki record
	 *  exists.
	 * @return The matching Interwiki object, or <code>null</code> if no match is found.
	 */
	public Interwiki lookupInterwiki(String interwikiPrefix) {
		if (interwikiPrefix == null) {
			return null;
		}
		for (Interwiki interwiki : this.lookupInterwikis()) {
			if (interwiki.getInterwikiPrefix().equalsIgnoreCase(interwikiPrefix.trim())) {
				// found a match, return it
				return interwiki;
			}
		}
		// no result found
		return null;
	}

	/**
	 * Return all interwiki records currently available for the wiki.
	 *
	 * @return A list of all Interwiki records currently available for the wiki.
	 */
	public List<Interwiki> lookupInterwikis() {
		// first check the cache
		List<Interwiki> interwikis = CACHE_INTERWIKI_LIST.retrieveFromCache(CACHE_INTERWIKI_LIST.getCacheName());
		if (interwikis != null || CACHE_INTERWIKI_LIST.isKeyInCache(CACHE_INTERWIKI_LIST.getCacheName())) {
			return interwikis;
		}
		// if not in the cache, go to the database
		interwikis = this.queryHandler().lookupInterwikis();
		if (interwikis != null) {
			Collections.sort(interwikis);
		}
		CACHE_INTERWIKI_LIST.addToCache(CACHE_INTERWIKI_LIST.getCacheName(), interwikis);
		return interwikis;
	}

	/**
	 * Given a namespace string, return the namespace that corresponds to that string,
	 * or <code>null</code> if no match exists.
	 *
	 * @param virtualWiki The virtual wiki for the namespace being queried.
	 * @param namespaceString The value to query to see if a matching namespace exists.
	 * @return The matching Namespace object, or <code>null</code> if no match is found.
	 */
	public Namespace lookupNamespace(String virtualWiki, String namespaceString) {
		if (namespaceString == null) {
			return null;
		}
		for (Namespace namespace : this.lookupNamespaces()) {
			if (namespace.getDefaultLabel().equals(namespaceString) || namespace.getLabel(virtualWiki).equalsIgnoreCase(namespaceString)) {
				// found a match, return it
				return namespace;
			}
		}
		// no result found
		return null;
	}

	/**
	 * Given a namespace ID return the corresponding namespace, or <code>null</code>
	 * if no match exists.
	 *
	 * @param namespaceId The ID for the namespace being retrieved.
	 * @return The matching Namespace object, or <code>null</code> if no match is found.
	 */
	public Namespace lookupNamespaceById(int namespaceId) {
		for (Namespace namespace : this.lookupNamespaces()) {
			if (namespace.getId() != null && namespace.getId().intValue() == namespaceId) {
				// found a match, return it
				return namespace;
			}
		}
		// no result found
		return null;
	}

	/**
	 * Return all namespaces currently available for the wiki.
	 *
	 * @return A list of all Namespace objects currently available for the wiki.
	 */
	public List<Namespace> lookupNamespaces() {
		// first check the cache
		List<Namespace> namespaces = CACHE_NAMESPACE_LIST.retrieveFromCache(CACHE_NAMESPACE_LIST.getCacheName());
		if (namespaces != null || CACHE_NAMESPACE_LIST.isKeyInCache(CACHE_NAMESPACE_LIST.getCacheName())) {
			return namespaces;
		}
		// if not in the cache, go to the database
		namespaces = this.queryHandler().lookupNamespaces();
		CACHE_NAMESPACE_LIST.addToCache(CACHE_NAMESPACE_LIST.getCacheName(), namespaces);
		return namespaces;
	}

	/**
	 * Retrieve a Topic object that matches the given virtual wiki and topic
	 * name.  Note that when a shared image repository is in use this method
	 * should first try to retrieve images from the specified virtual wiki,
	 * but if that search fails then a second search should be performed
	 * against the shared repository.
	 *
	 * @param virtualWiki The virtual wiki for the topic being queried.
	 * @param topicName The name of the topic being queried.
	 * @param deleteOK Set to <code>true</code> if deleted topics can be
	 *  retrieved, <code>false</code> otherwise.
	 * @return A Topic object that matches the given virtual wiki and topic
	 *  name, or <code>null</code> if no matching topic exists.
	 */
	public Topic lookupTopic(String virtualWiki, String topicName, boolean deleteOK) {
		return this.lookupTopic(virtualWiki, topicName, deleteOK, true);
	}

	/**
	 * Retrieve a Topic object that matches the given virtual wiki, namespace
	 * and page name.  Note that when a shared image repository is in use this
	 * method should first try to retrieve images from the specified virtual
	 * wiki, but if that search fails then a second search should be performed
	 * against the shared repository.
	 *
	 * @param virtualWiki The virtual wiki for the topic being queried.
	 * @param namespace The namespace of the topic being queried.
	 * @param pageName The page name of the topic being queried.
	 * @param deleteOK Set to <code>true</code> if deleted topics can be
	 *  retrieved, <code>false</code> otherwise.
	 * @return A Topic object that matches the given virtual wiki, namespace
	 *  and page name, or <code>null</code> if no matching topic exists.
	 */
	public Topic lookupTopic(String virtualWiki, Namespace namespace, String pageName, boolean deleteOK) {
		return this.lookupTopic(virtualWiki, namespace, pageName, deleteOK, true);
	}

	/**
	 *
	 */
	private Topic lookupTopic(String virtualWiki, String topicName, boolean deleteOK, boolean useCache) {
		if (StringUtils.isBlank(virtualWiki) || StringUtils.isBlank(topicName)) {
			return null;
		}
		Namespace namespace = LinkUtil.retrieveTopicNamespace(virtualWiki, topicName);
		String pageName = LinkUtil.retrieveTopicPageName(namespace, virtualWiki, topicName);
		return this.lookupTopic(virtualWiki, namespace, pageName, deleteOK, useCache);
	}

	/**
	 *
	 */
	private Topic lookupTopic(String virtualWiki, Namespace namespace, String pageName, boolean deleteOK, boolean useCache) {
		long start = System.currentTimeMillis();
		String key = this.cacheTopicKey(virtualWiki, namespace, pageName);
		if (useCache) {
			// retrieve topic from the cache only if this call is not currently a part
			// of a transaction to avoid retrieving data that might have been updated
			// as part of this transaction and would thus now be out of date
			Integer cacheTopicId = CACHE_TOPIC_IDS_BY_NAME.retrieveFromCache(key);
			if (cacheTopicId != null || CACHE_TOPIC_IDS_BY_NAME.isKeyInCache(key)) {
				Topic cacheTopic = (cacheTopicId != null) ? this.lookupTopicById(cacheTopicId.intValue()) : null;
				return (cacheTopic == null || (!deleteOK && cacheTopic.getDeleteDate() != null)) ? null : cacheTopic;
			}
		}
		boolean checkSharedVirtualWiki = this.useSharedVirtualWiki(virtualWiki, namespace);
		String sharedVirtualWiki = Environment.getValue(Environment.PROP_SHARED_UPLOAD_VIRTUAL_WIKI);
		if (useCache && checkSharedVirtualWiki) {
			String sharedKey = this.cacheTopicKey(sharedVirtualWiki, namespace, pageName);
			Integer cacheTopicId = CACHE_TOPIC_IDS_BY_NAME.retrieveFromCache(sharedKey);
			if (cacheTopicId != null || CACHE_TOPIC_IDS_BY_NAME.isKeyInCache(sharedKey)) {
				Topic cacheTopic = (cacheTopicId != null) ? this.lookupTopicById(cacheTopicId.intValue()) : null;
				return (cacheTopic == null || (!deleteOK && cacheTopic.getDeleteDate() != null)) ? null : cacheTopic;
			}
		}
		Topic topic = null;
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		topic = this.queryHandler().lookupTopic(virtualWikiId, namespace, pageName);
		if (topic == null && Environment.getBooleanValue(Environment.PROP_PARSER_ALLOW_CAPITALIZATION)) {
			String alternativePageName = (StringUtils.equals(pageName, StringUtils.capitalize(pageName))) ? StringUtils.lowerCase(pageName) : StringUtils.capitalize(pageName);
			topic = this.queryHandler().lookupTopic(virtualWikiId, namespace, alternativePageName);
		}
		if (topic == null && checkSharedVirtualWiki) {
			topic = this.lookupTopic(sharedVirtualWiki, namespace, pageName, deleteOK, useCache);
		}
		if (useCache) {
			// add topic to the cache only if it is not currently a part of a transaction
			// to avoid caching something that might need to be rolled back.
			if (topic == null) {
				CACHE_TOPIC_IDS_BY_NAME.addToCache(key, null);
				CACHE_TOPIC_NAMES_BY_NAME.addToCache(key, null);
			} else {
				this.cacheTopicRefresh(topic, false, key);
			}
		}
		if (logger.isDebugEnabled()) {
			long execution = (System.currentTimeMillis() - start);
			if (execution > TIME_LIMIT_TOPIC_LOOKUP) {
				logger.debug("Slow topic lookup for: " + Topic.buildTopicName(virtualWiki, namespace, pageName) + " (" + (execution / 1000.000) + " s)");
			}
		}
		return (topic == null || (!deleteOK && topic.getDeleteDate() != null)) ? null : topic;
	}

	/**
	 *
	 */
	public Topic lookupTopicById(int topicId) {
		Topic result = CACHE_TOPICS_BY_ID.retrieveFromCache(topicId);
		if (result != null || CACHE_TOPICS_BY_ID.isKeyInCache(topicId)) {
			return (result == null) ? null : new Topic(result);
		}
		result = this.queryHandler().lookupTopicById(topicId);
		if (result == null) {
			logger.info("Attempt to look up topic with non-existent ID: " + topicId + ".  This may indicate a code error");
		} else {
			this.cacheTopicRefresh(result, false, null);
		}
		return result;
	}

	/**
	 * Return a count of all topics, including redirects, comments pages and
	 * templates, for the given virtual wiki.  Deleted topics are not included
	 * in the count.
	 *
	 * @param virtualWiki The virtual wiki for which the total topic count is
	 *  being returned.
	 * @param namespaceId An optional parameter to specify that results should only
	 *  be from the specified namespace.  If this value is <code>null</code> then
	 *  results will be returned from all namespaces.
	 * @return A count of all topics, including redirects, comments pages and
	 *  templates, for the given virtual wiki and (optionally) namespace.  Deleted
	 *  topics are not included in the count.
	 */
	public int lookupTopicCount(String virtualWiki, Integer namespaceId) {
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		int namespaceStart = (namespaceId != null) ? namespaceId : 0;
		int namespaceEnd = (namespaceId != null) ? namespaceId : this.findMaxNamespaceId();
		return this.queryHandler().lookupTopicCount(virtualWikiId, namespaceStart, namespaceEnd);
	}

	/**
	 * Return a List of topic names for all non-deleted topics in the
	 * virtual wiki that match a specific topic type.
	 *
	 * @param virtualWiki The virtual wiki for the topics being queried.
	 * @param topicType1 The type of topics to return.
	 * @param topicType2 The type of topics to return.  Set to the same value
	 *  as topicType1 if only one type is needed.
	 * @param namespaceId An optional parameter to specify that results should only
	 *  be from the specified namespace.  If this value is <code>null</code> then
	 *  results will be returned from all namespaces.
	 * @param pagination A Pagination object indicating the total number of
	 *  results and offset for the results to be retrieved.
	 * @return A map of topic id and topic name for all non-deleted topics in the
	 *  virtual wiki that match a specific topic type.
	 */
	public Map<Integer, String> lookupTopicByType(String virtualWiki, TopicType topicType1, TopicType topicType2, Integer namespaceId, Pagination pagination) {
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		int namespaceStart = (namespaceId != null) ? namespaceId : 0;
		int namespaceEnd = (namespaceId != null) ? namespaceId : this.findMaxNamespaceId();
		return this.queryHandler().lookupTopicByType(virtualWikiId, topicType1, topicType2, namespaceStart, namespaceEnd, pagination);
	}

	/**
	 * This method is used primarily to determine if a topic with a given name exists,
	 * taking as input a topic name and virtual wiki and returning the corresponding
	 * topic name, or <code>null</code> if no matching topic exists.  This method will
	 * return only non-deleted topics and performs better for cases where a caller only
	 * needs to know if a topic exists, but does not need a full Topic object.
	 *
	 * @param virtualWiki The virtual wiki for the topic being queried.
	 * @param namespace The Namespace for the topic being retrieved.
	 * @param pageName The topic pageName (topic name without the namespace) for
	 *  the topic being retrieved.
	 * @return The name of the Topic object that matches the given virtual wiki and topic
	 *  name, or <code>null</code> if no matching topic exists.
	 */
	public String lookupTopicName(String virtualWiki, Namespace namespace, String pageName) {
		if (StringUtils.isBlank(virtualWiki) || StringUtils.isBlank(pageName)) {
			return null;
		}
		long start = System.currentTimeMillis();
		String key = this.cacheTopicKey(virtualWiki, namespace, pageName);
		String topicName = CACHE_TOPIC_NAMES_BY_NAME.retrieveFromCache(key);
		if (topicName != null || CACHE_TOPIC_NAMES_BY_NAME.isKeyInCache(key)) {
			return topicName;
		}
		boolean checkSharedVirtualWiki = this.useSharedVirtualWiki(virtualWiki, namespace);
		String sharedVirtualWiki = Environment.getValue(Environment.PROP_SHARED_UPLOAD_VIRTUAL_WIKI);
		if (checkSharedVirtualWiki) {
			String sharedKey = this.cacheTopicKey(sharedVirtualWiki, namespace, pageName);
			topicName = CACHE_TOPIC_NAMES_BY_NAME.retrieveFromCache(sharedKey);
			if (topicName != null || CACHE_TOPIC_NAMES_BY_NAME.isKeyInCache(sharedKey)) {
				return topicName;
			}
		}
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		topicName = this.queryHandler().lookupTopicName(virtualWikiId, virtualWiki, namespace, pageName);
		if (topicName == null && checkSharedVirtualWiki) {
			topicName = this.lookupTopicName(sharedVirtualWiki, namespace, pageName);
		}
		CACHE_TOPIC_NAMES_BY_NAME.addToCache(key, topicName);
		if (logger.isDebugEnabled()) {
			long execution = (System.currentTimeMillis() - start);
			if (execution > TIME_LIMIT_TOPIC_LOOKUP) {
				logger.debug("Slow topic existence lookup for: " + Topic.buildTopicName(virtualWiki, namespace, pageName) + " (" +  (execution / 1000.000) + " s)");
			}
		}
		return topicName;
	}

	/**
	 * Find the names for all topics that link to a specified topic.
	 *
	 * @param virtualWiki The virtual wiki for the topic.
	 * @param topicName The name of the topic.
	 * @return A list of topic name and (for redirects) the redirect topic
	 *  name for all topics that link to the specified topic.  If no results
	 *  are found then an empty list is returned.
	 */
	public List<String[]> lookupTopicLinks(String virtualWiki, String topicName) {
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		Namespace namespace = LinkUtil.retrieveTopicNamespace(virtualWiki, topicName);
		String pageName = LinkUtil.retrieveTopicPageName(namespace, virtualWiki, topicName);
		// FIXE - link to records are always capitalized, which will cause problems for the
		// rare case of two topics such as "eBay" and "EBay".
		pageName = StringUtils.capitalize(pageName);
		Topic topic = new Topic(virtualWiki, namespace, pageName);
		return this.queryHandler().lookupTopicLinks(virtualWikiId, topic);
	}

	/**
	 * Find the names for all un-linked topics in the main namespace.
	 *
	 * @param virtualWiki The virtual wiki to query against.
	 * @param namespaceId The ID for the namespace being retrieved.
	 * @return A list of topic names for all topics that are not linked to by
	 *  any other topic.
	 */
	public List<String> lookupTopicLinkOrphans(String virtualWiki, int namespaceId) {
		// FIXME - caching needed
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		return this.queryHandler().lookupTopicLinkOrphans(virtualWikiId, namespaceId);
	}

	/**
	 * Retrieve a TopicVersion object for a given topic version ID.
	 *
	 * @param topicVersionId The ID of the topic version being retrieved.
	 * @return A TopicVersion object matching the given topic version ID,
	 *  or <code>null</code> if no matching topic version is found.
	 */
	public TopicVersion lookupTopicVersion(int topicVersionId) {
		TopicVersion topicVersion = CACHE_TOPIC_VERSIONS.retrieveFromCache(topicVersionId);
		if (topicVersion != null || CACHE_TOPIC_VERSIONS.isKeyInCache(topicVersionId)) {
			return topicVersion;
		}
		topicVersion = this.queryHandler().lookupTopicVersion(topicVersionId);
		CACHE_TOPIC_VERSIONS.addToCache(topicVersionId, topicVersion);
		return topicVersion;
	}

	/**
	 * Retrieve the next topic version ID chronologically for a given topic
	 * version, or <code>null</code> if there is no next topic version ID.
	 *
	 * @param topicVersionId The ID of the topic version whose next topic version
	 *  ID is being retrieved.
	 * @return The next topic version ID chronologically for a given topic
	 * version, or <code>null</code> if there is no next topic version ID.
	 */
	public Integer lookupTopicVersionNextId(int topicVersionId) {
		return this.queryHandler().lookupTopicVersionNextId(topicVersionId);
	}

	/**
	 * Find any active user block for the given user or IP address.
	 *
	 * @param wikiUserId The wiki user ID, or <code>null</code> if the search is
	 *  by IP address.
	 * @param ipAddress The IP address, or <code>null</code> if the search is by
	 *  user ID.
	 * @return A currently-active user block for the ID or IP address, or
	 *  <code>null</code> if no block is currently active.
	 */
	public UserBlock lookupUserBlock(Integer wikiUserId, String ipAddress) {
		Map<Object, UserBlock> userBlocks = this.getUserBlocks();
		UserBlock userBlock = null;
		if (wikiUserId != null) {
			userBlock = userBlocks.get(wikiUserId);
		}
		if (userBlock == null && ipAddress != null) {
			userBlock = userBlocks.get(ipAddress);
		}
		// verify that the block has not expired since being cached
		return (userBlock != null && userBlock.isExpired()) ? null : userBlock;
	}

	/**
	 * Given a virtual wiki name, return the corresponding VirtualWiki object.
	 *
	 * @param virtualWikiName The name of the VirtualWiki object that is
	 *  being retrieved.
	 * @return The VirtualWiki object that corresponds to the virtual wiki
	 *  name being queried, or <code>null</code> if no matching VirtualWiki
	 *  can be found.
	 */
	public VirtualWiki lookupVirtualWiki(String virtualWikiName) {
		List<VirtualWiki> virtualWikis = this.getVirtualWikiList();
		for (VirtualWiki virtualWiki : virtualWikis) {
			if (virtualWiki.getName().equals(virtualWikiName)) {
				// found a match, return it
				return virtualWiki;
			}
		}
		// no result found
		return null;
	}

	/**
	 *
	 */
	private int lookupVirtualWikiId(String virtualWikiName) {
		VirtualWiki virtualWiki = this.lookupVirtualWiki(virtualWikiName);
		return (virtualWiki == null) ? -1 : virtualWiki.getVirtualWikiId();
	}

	/**
	 * Retrieve a WikiFile object for a given virtual wiki and topic name.
	 *
	 * @param virtualWiki The virtual wiki for the file being queried.
	 * @param topicName The topic name for the file being queried.
	 * @return The WikiFile object for the given virtual wiki and topic name,
	 *  or <code>null</code> if no matching WikiFile exists.
	 */
	public WikiFile lookupWikiFile(String virtualWiki, String topicName) {
		if (StringUtils.isBlank(virtualWiki) || StringUtils.isBlank(topicName)) {
			return null;
		}
		Namespace namespace = LinkUtil.retrieveTopicNamespace(virtualWiki, topicName);
		String pageName = LinkUtil.retrieveTopicPageName(namespace, virtualWiki, topicName);
		return this.lookupWikiFile(virtualWiki, namespace, pageName);
	}

	/**
	 *
	 */
	private WikiFile lookupWikiFile(String virtualWiki, Namespace namespace, String pageName) {
		Topic topic = this.lookupTopic(virtualWiki, namespace, pageName, false, true);
		if (topic == null) {
			return null;
		}
		int virtualWikiId = this.lookupVirtualWikiId(topic.getVirtualWiki());
		WikiFile wikiFile = this.queryHandler().lookupWikiFile(virtualWikiId, topic.getVirtualWiki(), topic.getTopicId());
		if (wikiFile == null && this.useSharedVirtualWiki(topic.getVirtualWiki(), topic.getNamespace())) {
			// this is a weird corner case.  if there is a shared virtual wiki
			// then someone might have uploaded the image to the shared virtual
			// wiki but then created a description on the non-shared image page,
			// so check for a file on the shared virtual wiki.
			String sharedVirtualWiki = Environment.getValue(Environment.PROP_SHARED_UPLOAD_VIRTUAL_WIKI);
			wikiFile = this.lookupWikiFile(sharedVirtualWiki, namespace, pageName);
		}
		return wikiFile;
	}

	/**
	 * Return a count of all wiki files for the given virtual wiki.  Deleted
	 * files are not included in the count.
	 *
	 * @param virtualWiki The virtual wiki for which the total file count is
	 *  being returned.
	 * @return A count of all wiki files for the given virtual wiki.  Deleted
	 *  files are not included in the count.
	 */
	public int lookupWikiFileCount(String virtualWiki) {
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		return this.queryHandler().lookupWikiFileCount(virtualWikiId);
	}

	/**
	 * Retrieve a WikiGroup object for a given group name.
	 *
	 * @param groupName The group name for the group being queried.
	 * @return The WikiGroup object for the given group name, or
	 *  <code>null</code> if no matching group exists.
	 */
	public WikiGroup lookupWikiGroup(String groupName) {
		return this.queryHandler().lookupWikiGroup(groupName);
	}

	/**
	 * Retrieve a WikiUser object matching a given user ID.
	 *
	 * @param userId The ID of the WikiUser being retrieved.
	 * @return The WikiUser object matching the given user ID, or
	 *  <code>null</code> if no matching WikiUser exists.
	 */
	public WikiUser lookupWikiUser(int userId) {
		WikiUser user = CACHE_USER_BY_USER_ID.retrieveFromCache(userId);
		if (user != null || CACHE_USER_BY_USER_ID.isKeyInCache(userId)) {
			return user;
		}
		user = this.queryHandler().lookupWikiUser(userId);
		CACHE_USER_BY_USER_ID.addToCache(userId, user);
		return user;
	}

	/**
	 * Retrieve a WikiUser object matching a given username.
	 *
	 * @param username The username of the WikiUser being retrieved.
	 * @return The WikiUser object matching the given username, or
	 *  <code>null</code> if no matching WikiUser exists.
	 */
	public WikiUser lookupWikiUser(String username) {
		WikiUser result = CACHE_USER_BY_USER_NAME.retrieveFromCache(username);
		if (result != null || CACHE_USER_BY_USER_NAME.isKeyInCache(username)) {
			return result;
		}
		int userId = this.queryHandler().lookupWikiUser(username);
		if (userId != -1) {
			result = lookupWikiUser(userId);
		}
		CACHE_USER_BY_USER_NAME.addToCache(username, result);
		return result;
	}

	/**
	 *
	 */
	public WikiUser lookupPwResetChallengeData(String username) {
		return this.queryHandler().lookupPwResetChallengeData(username);
	}
	
	/**
	 * Return a count of all wiki users.
	 *
	 * @return A count of all wiki users.
	 */
	public int lookupWikiUserCount() {
		return this.queryHandler().lookupWikiUserCount();
	}

	/**
	 * Retrieve a WikiUser object matching a given username.
	 *
	 * @param username The username of the WikiUser being retrieved.
	 * @return The encrypted password for the given user name, or
	 *  <code>null</code> if no matching WikiUser exists.
	 */
	public String lookupWikiUserEncryptedPassword(String username) {
		return this.queryHandler().lookupWikiUserEncryptedPassword(username);
	}

	/**
	 * Return a List of user logins for all wiki users.
	 *
	 * @param pagination A Pagination object indicating the total number of
	 *  results and offset for the results to be retrieved.
	 * @return A List of user logins for all wiki users.
	 */
	public List<String> lookupWikiUsers(Pagination pagination) {
		return this.queryHandler().lookupWikiUsers(pagination);
	}

	/**
	 * Move a topic to a new name, creating a redirect topic in the old
	 * topic location.  An exception will be thrown if the topic cannot be
	 * moved for any reason.
	 *
	 * @param fromTopic The Topic object that is being moved.
	 * @param destination The new name for the topic.
	 * @param user The WikiUser who will be credited in the topic version
	 *  associated with this topic move as having performed the move.
	 * @param ipAddress The IP address of the user making the topic move.
	 * @param moveComment The edit comment to associate with the topic
	 *  move.
	 * @throws WikiException Thrown if the topic information is invalid.
	 */
	public void moveTopic(final Topic fromTopic, final String destination, WikiUser user, String ipAddress, String moveComment) throws WikiException {
		// set up the version record to record the topic move
		final TopicVersion fromVersion = new TopicVersion(user, ipAddress, moveComment, fromTopic.getTopicContent(), 0);
		fromVersion.setEditType(TopicVersion.EDIT_MOVE);
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					try {
						if (!canMoveTopic(fromTopic, destination)) {
							throw new WikiException(new WikiMessage("move.exception.destinationexists", destination));
						}
						Topic toTopic = lookupTopic(fromTopic.getVirtualWiki(), destination, false, false);
						boolean detinationExistsFlag = (toTopic != null && toTopic.getDeleteDate() == null);
						if (detinationExistsFlag) {
							// if the target topic is a redirect to the source topic then the
							// target must first be deleted.
							deleteTopic(toTopic, null);
						}
						// first rename the source topic with the new destination name
						String fromTopicName = fromTopic.getName();
						fromTopic.setName(destination);
						// only one version needs to create a recent change entry, so do not create a log entry
						// for the "from" version
						fromVersion.setRecentChangeAllowed(false);
						// handle categories
						ParserOutput fromParserOutput = ParserUtil.parserOutput(fromTopic.getTopicContent(), fromTopic.getVirtualWiki(), fromTopic.getName());
						writeTopic(fromTopic, fromVersion, fromParserOutput.getCategories(), fromParserOutput.getLinks());
						// now either create a new topic that is a redirect with the
						// source topic's old name, or else undelete the new topic and
						// rename.
						if (detinationExistsFlag) {
							// target topic was deleted, so rename and undelete
							toTopic.setName(fromTopicName);
							writeTopic(toTopic, null, null, null);
							undeleteTopic(toTopic, null);
						} else {
							// create a new topic that redirects to the destination
							toTopic = new Topic(fromTopic);
							toTopic.setTopicId(-1);
							toTopic.setName(fromTopicName);
						}
						String content = ParserUtil.parserRedirectContent(destination);
						toTopic.setRedirectTo(destination);
						toTopic.setTopicType(TopicType.REDIRECT);
						toTopic.setTopicContent(content);
						TopicVersion toVersion = fromVersion;
						toVersion.setTopicVersionId(-1);
						toVersion.setVersionContent(content);
						toVersion.setRecentChangeAllowed(true);
						ParserOutput toParserOutput = ParserUtil.parserOutput(toTopic.getTopicContent(), toTopic.getVirtualWiki(), toTopic.getName());
						writeTopic(toTopic, toVersion, toParserOutput.getCategories(), toParserOutput.getLinks());
					} catch (ParserException e) {
						status.setRollbackOnly();
						throw new TransactionRuntimeException(e);
					} catch (WikiException e) {
						status.setRollbackOnly();
						throw new TransactionRuntimeException(e);
					}
				}
			}
		);
	}

	/**
	 * Utility method used when importing to updating the previous topic version ID field
	 * of topic versions, as well as the current version ID field for the topic record.
	 *
	 * @param topic The topic record to update.
	 * @param topicVersionIdList A list of all topic version IDs for the topic, sorted
	 *  chronologically from oldest to newest.
	 */
	public void orderTopicVersions(final Topic topic, final List<Integer> topicVersionIdList) {
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					int virtualWikiId = lookupVirtualWikiId(topic.getVirtualWiki());
					queryHandler().orderTopicVersions(topic, virtualWikiId, topicVersionIdList);
					cacheTopicRefresh(topic, true, null);
				}
			}
		);
	}

	/**
	 * Remove a topic version from the database.  This action deletes the record
	 * entirely, including references in other tables, and cannot be undone.
	 *
	 * @param topic The topic record for which a version is being purged.
	 * @param topicVersionId The ID of the topic version being deleted.
	 * @param user The WikiUser who will be credited in the log record
	 *  associated with this action.
	 * @param ipAddress The IP address of the user deleting the topic version.
	 */
	public void purgeTopicVersion(final Topic topic, final int topicVersionId, final WikiUser user, final String ipAddress) throws WikiException {
		// 1. get the topic version record.  if no such record exists
		// throw an exception.
		final TopicVersion topicVersion = this.lookupTopicVersion(topicVersionId);
		if (topicVersion == null) {
			throw new WikiException(new WikiMessage("purge.error.noversion", Integer.toString(topicVersionId)));
		}
		// 2. get the current version's previous_topic_version_id
		// record.  if there is no such record get the topic version
		// with the current version as its previous_topic_version_id.
		// if there is still no such record throw an exception.
		Integer previousTopicVersionId = topicVersion.getPreviousTopicVersionId();
		final Integer nextTopicVersionId = this.lookupTopicVersionNextId(topicVersionId);
		if (previousTopicVersionId == null && nextTopicVersionId == null) {
			throw new WikiException(new WikiMessage("purge.error.onlyversion", Integer.toString(topicVersionId), topic.getName()));
		}
		final Integer replacementTopicVersionId = (previousTopicVersionId != null) ? previousTopicVersionId : nextTopicVersionId;
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					try {
						// 3. get a reference to any topic which has this topic as its
						// current_version_id, and update with the value from #2.
						if (topicVersionId == topic.getCurrentVersionId().intValue()) {
							topic.setCurrentVersionId(replacementTopicVersionId);
							int virtualWikiId = lookupVirtualWikiId(topic.getVirtualWiki());
							dataValidator.validateTopic(topic);
							queryHandler().updateTopic(topic, virtualWikiId);
						}
						// 4. if there is a topic version with this version as its
						// previous_topic_version_id update it with the value from #2
						if (nextTopicVersionId != null) {
							TopicVersion nextTopicVersion = lookupTopicVersion(nextTopicVersionId);
							nextTopicVersion.setPreviousTopicVersionId(topicVersion.getPreviousTopicVersionId());
							queryHandler().updateTopicVersion(nextTopicVersion);
						}
						// 5. delete the topic version record from all tables
						queryHandler().deleteTopicVersion(topicVersionId, topicVersion.getPreviousTopicVersionId());
						// 6. create a log record
						LogItem logItem = LogItem.initLogItemPurge(topic, topicVersion, user, ipAddress);
						int logVirtualWikiId = lookupVirtualWikiId(logItem.getVirtualWiki());
						dataValidator.validateLogItem(logItem);
						queryHandler().insertLogItem(logItem, logVirtualWikiId);
						RecentChange change = RecentChange.initRecentChange(logItem);
						int changeVirtualWikiId = lookupVirtualWikiId(change.getVirtualWiki());
						dataValidator.validateRecentChange(change);
						queryHandler().insertRecentChange(change, changeVirtualWikiId);
						CACHE_TOPIC_VERSIONS.removeFromCache(topicVersionId);
						CACHE_TOPIC_VERSIONS.removeFromCache(nextTopicVersionId);
						CACHE_TOPICS_BY_ID.removeFromCache(topic.getTopicId());
					} catch (WikiException e) {
						status.setRollbackOnly();
						throw new TransactionRuntimeException(e);
					}
				}
			}
		);
	}

	/**
	 *
	 */
	protected final QueryHandler queryHandler() {
		return this.queryHandler;
	}

	/**
	 * Utility method to retrieve an instance of the current query handler.
	 *
	 * @return An instance of the current query handler.
	 * @throws IllegalStateException Thrown if a data handler instance can not be
	 *  instantiated.
	 */
	private QueryHandler queryHandlerInstance() {
		if (StringUtils.isBlank(Environment.getValue(Environment.PROP_DB_TYPE))) {
			// this is a problem, but it should never occur
			logger.warn("AnsiDataHandler.queryHandlerInstance called without a valid PROP_DB_TYPE value");
		}
		String queryHandlerClass = Environment.getValue(Environment.PROP_DB_TYPE);
		// TODO - remove when the ability to upgrade to 1.3 is removed
		String dataHandlerClass = LEGACY_DATA_HANDLER_MAP.get(queryHandlerClass);
		if (dataHandlerClass != null) {
			queryHandlerClass = dataHandlerClass;
			Environment.setValue(Environment.PROP_DB_TYPE, queryHandlerClass);
			try {
				Environment.saveConfiguration();
			} catch (WikiException e) {
				throw new IllegalStateException("Failure while updating properties", e);
			}
		}
		try {
			return (QueryHandler)ResourceUtil.instantiateClass(queryHandlerClass);
		} catch (ClassCastException e) {
			throw new IllegalStateException("Query handler specified in jamwiki.properties does not implement org.jamwiki.db.QueryHandler: " + dataHandlerClass);
		}
	}

	/**
	 * Delete all existing log entries and reload the log item table based
	 * on the most recent topic versions, uploads, and user signups.
	 */
	public void reloadLogItems() {
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					for (VirtualWiki virtualWiki : getVirtualWikiList()) {
						queryHandler().reloadLogItems(virtualWiki.getVirtualWikiId());
					}
				}
			}
		);
	}

	/**
	 * Delete all existing recent changes and reload the recent changes based
	 * on the most recent topic versions.
	 */
	public void reloadRecentChanges() {
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					int limit = Environment.getIntValue(Environment.PROP_MAX_RECENT_CHANGES);
					queryHandler().reloadRecentChanges(limit);
				}
			}
		);
	}

	/**
	 * Perform any required setup steps for the DataHandler instance.
	 *
	 * @param locale The locale to be used when setting up the data handler
	 *  instance.  This parameter will affect any messages or defaults used
	 *  for the DataHandler.
	 * @param user The admin user to use when creating default topics and
	 *  other DataHandler parameters.
	 * @param username The admin user's username (login).
	 * @param encryptedPassword The admin user's encrypted password.  This value
	 *  is only required when creating a new admin user.
	 * @throws WikiException Thrown if a setup failure occurs.
	 */
	public void setup(Locale locale, WikiUser user, String username, String encryptedPassword) throws WikiException {
		WikiDatabase.initialize();
		// determine if database exists
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = DatabaseConnection.getConnection();
			stmt = conn.createStatement();
			stmt.executeQuery(this.queryHandler().existenceValidationQuery());
			return;
		} catch (SQLException e) {
			// database not yet set up
		} finally {
			DatabaseConnection.closeConnection(conn, stmt, null);
			// explicitly null the variable to improve garbage collection.
			// with very large loops this can help avoid OOM "GC overhead
			// limit exceeded" errors.
			stmt = null;
			conn = null;
		}
		WikiDatabase.setup(locale, user, username, encryptedPassword);
	}

	/**
	 * Create the special pages used on the wiki, such as the left menu and
	 * default stylesheet.
	 *
	 * @param locale The locale to be used when setting up special pages such
	 *  as the left menu and default stylesheet.  This parameter will affect
	 *  the language used when setting up these pages.
	 * @param user The admin user to use when creating the special pages.
	 * @param virtualWiki The VirtualWiki for which special pages are being
	 *  created.
	 * @throws WikiException Thrown if a setup failure occurs.
	 */
	public void setupSpecialPages(final Locale locale, final WikiUser user, final VirtualWiki virtualWiki) throws WikiException {
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					try {
						// create the default topics
						WikiDatabase.setupSpecialPage(locale, virtualWiki.getName(), WikiBase.SPECIAL_PAGE_STARTING_POINTS, user, false, false);
						WikiDatabase.setupSpecialPage(locale, virtualWiki.getName(), WikiBase.SPECIAL_PAGE_SIDEBAR, user, true, false);
						WikiDatabase.setupSpecialPage(locale, virtualWiki.getName(), WikiBase.SPECIAL_PAGE_FOOTER, user, true, false);
						WikiDatabase.setupSpecialPage(locale, virtualWiki.getName(), WikiBase.SPECIAL_PAGE_HEADER, user, true, false);
						WikiDatabase.setupSpecialPage(locale, virtualWiki.getName(), WikiBase.SPECIAL_PAGE_SYSTEM_CSS, user, true, true);
						WikiDatabase.setupSpecialPage(locale, virtualWiki.getName(), WikiBase.SPECIAL_PAGE_CUSTOM_CSS, user, true, false);
					} catch (IOException e) {
						status.setRollbackOnly();
						throw new TransactionRuntimeException(e);
					} catch (WikiException e) {
						status.setRollbackOnly();
						throw new TransactionRuntimeException(e);
					}
				}
			}
		);
	}

	/**
	 * Undelete a previously deleted topic by setting its delete date to a
	 * null value.  Prior to calling this method the topic content should be
	 * restored to its previous value.  A new TopicVersion should be supplied
	 * reflecting the topic undeletion event.
	 *
	 * @param topic The Topic object that is being undeleted.
	 * @param topicVersion A TopicVersion object that indicates the undelete
	 *  date, author, and other parameters for the topic.  If this value is
	 *  <code>null</code> then no version is saved, nor is any recent change
	 *  entry created.
	 * @throws WikiException Thrown if the topic information is invalid.
	 */
	public void undeleteTopic(Topic topic, TopicVersion topicVersion) throws WikiException {
		try {
			// update topic to indicate deleted, add delete topic version.  if
			// topic has categories or other metadata then parser document is
			// also needed.
			ParserOutput parserOutput = ParserUtil.parserOutput(topic.getTopicContent(), topic.getVirtualWiki(), topic.getName());
			topic.setDeleteDate(null);
			this.writeTopic(topic, topicVersion, parserOutput.getCategories(), parserOutput.getLinks());
		} catch (ParserException e) {
			throw new InvalidDataAccessApiUsageException("Failure while parsing topic " + topic.getName(), e);
		}
	}

	/**
	 * Update a special page used on the wiki, such as the left menu or
	 * default stylesheet.
	 *
	 * @param locale The locale to be used when updating a special page such
	 *  as the left menu and default stylesheet.  This parameter will affect
	 *  the language used when updating up the page.
	 * @param virtualWiki The VirtualWiki for which the special page are being
	 *  updated.
	 * @param topicName The name of the special page topic that is being
	 *  updated.
	 * @param userDisplay A display name for the user updating special pages,
	 *  typically the IP address.
	 * @throws WikiException Thrown if the topic information is invalid.
	 */
	public void updateSpecialPage(Locale locale, String virtualWiki, String topicName, String userDisplay) throws WikiException {
		logger.info("Updating special page " + virtualWiki + " / " + topicName);
		try {
			String contents = WikiDatabase.readSpecialPage(locale, topicName);
			Topic topic = this.lookupTopic(virtualWiki, topicName, false, false);
			int charactersChanged = StringUtils.length(contents) - StringUtils.length(topic.getTopicContent());
			topic.setTopicContent(contents);
			// FIXME - hard coding
			TopicVersion topicVersion = new TopicVersion(null, userDisplay, "Automatically updated by system upgrade", contents, charactersChanged);
			ParserOutput parserOutput = ParserUtil.parserOutput(topic.getTopicContent(), virtualWiki, topicName);
			writeTopic(topic, topicVersion, parserOutput.getCategories(), parserOutput.getLinks());
		} catch (ParserException e) {
			throw new InvalidDataAccessApiUsageException("Failure while parsing topic " + topicName, e);
		} catch (IOException e) {
			throw new NonTransientDataAccessResourceException("I/O exception accessing special page for " + virtualWiki + " / " + topicName, e);
		}
	}

	public void updatePwResetChallengeData(WikiUser user) {
		this.queryHandler().updatePwResetChallengeData(user);
	}
	
	/**
	 * Utility method to determine whether to check a shared virtual wiki when
	 * performing a topic lookup.
	 *
	 * @param virtualWiki The current virtual wiki being used for a lookup.
	 * @param namespace The namespace for the current topic being retrieved.
	 */
	private boolean useSharedVirtualWiki(String virtualWiki, Namespace namespace) {
		String sharedVirtualWiki = Environment.getValue(Environment.PROP_SHARED_UPLOAD_VIRTUAL_WIKI);
		if (!StringUtils.isBlank(sharedVirtualWiki) && !StringUtils.equals(virtualWiki, sharedVirtualWiki)) {
			return (namespace.getId().equals(Namespace.FILE_ID) || namespace.getId().equals(Namespace.MEDIA_ID));
		}
		return false;
	}

	/**
	 * Replace the existing configuration records with a new set of values.  This
	 * method will delete all existing records and replace them with the records
	 * specified.
	 *
	 * @param configuration A map of key-value pairs corresponding to the new
	 *  configuration information.  These values will replace all existing
	 *  configuration values in the system.
	 * @throws WikiException Thrown if the configuration information is invalid.
	 */
	public void writeConfiguration(final Map<String, String> configuration) throws WikiException {
		this.dataValidator.validateConfiguration(configuration);
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					queryHandler().updateConfiguration(configuration);
				}
			}
		);
	}

	/**
	 * Add or update a WikiFile object.  This method will add a new record if
	 * the WikiFile does not have a file ID, otherwise it will perform an update.
	 * A WikiFileVersion object will also be created to capture the author, date,
	 * and other parameters for the file.
	 *
	 * @param wikiFile The WikiFile to add or update.  If the WikiFile does not
	 *  have a file ID then a new record is created, otherwise an update is
	 *  performed.
	 * @param wikiFileVersion A WikiFileVersion containing the author, date, and
	 *  other information about the version being added.
	 * @param imageData If images are stored in the database then this field holds the
	 *  image data information, otherwise it will be <code>null</code>.
	 * @throws WikiException Thrown if the file information is invalid.
	 */
	public void writeFile(final WikiFile wikiFile, final WikiFileVersion wikiFileVersion, final ImageData imageData) throws WikiException {
		this.dataValidator.validateWikiFile(wikiFile);
		final int virtualWikiId = this.lookupVirtualWikiId(wikiFile.getVirtualWiki());
		LinkUtil.validateTopicName(wikiFile.getVirtualWiki(), wikiFile.getFileName(), false);
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					try {
						if (wikiFile.getFileId() <= 0) {
							queryHandler().insertWikiFile(wikiFile, virtualWikiId);
						} else {
							queryHandler().updateWikiFile(wikiFile, virtualWikiId);
						}
						wikiFileVersion.setFileId(wikiFile.getFileId());
						// write version
						dataValidator.validateWikiFileVersion(wikiFileVersion);
						queryHandler().insertWikiFileVersion(wikiFileVersion);
						if (imageData != null) {
							// No more needs of old resized images
							queryHandler().deleteResizedImages(wikiFile.getFileId());
							imageData.fileVersionId = wikiFileVersion.getFileVersionId();
							queryHandler().insertImage(imageData, false);
						}
					} catch (WikiException e) {
						status.setRollbackOnly();
						throw new TransactionRuntimeException(e);
					}
				}
			}
		);
	}

	/**
	 * Add or update an Interwiki record.  This method will first delete any
	 * existing method with the same prefix and then add the new record.
	 *
	 * @param interwiki The Interwiki record to add or update.  If a record
	 *  already exists with the same prefix then that record will be deleted.
	 * @throws WikiException Thrown if the interwiki information is invalid.
	 */
	public void writeInterwiki(final Interwiki interwiki) throws WikiException {
		interwiki.validate();
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					queryHandler().deleteInterwiki(interwiki);
					queryHandler().insertInterwiki(interwiki);
					// only update the cache if no errors
					CACHE_INTERWIKI_LIST.removeAllFromCache();
				}
			}
		);
	}

	/**
	 * Add or update a namespace.  This method will add a new record if the
	 * namespace does not already exist, otherwise it will update the existing
	 * record.
	 *
	 * @param namespace The namespace object to add to the database.
	 * @throws WikiException Thrown if the namespace information is invalid.
	 */
	public void writeNamespace(Namespace namespace) throws WikiException {
		this.dataValidator.validateNamespace(namespace);
		this.queryHandler().updateNamespace(namespace);
		CACHE_NAMESPACE_LIST.removeAllFromCache();
	}

	/**
	 * Add or update virtual-wiki specific labels for a namespace.  This method will
	 * remove existing records for the virtual wiki and add the new ones.
	 *
	 * @param namespaces The namespace translation records to add/update.
	 * @param virtualWiki The virtual wiki for which namespace translations are
	 *  being added or updated.
	 * @throws WikiException Thrown if the namespace information is invalid.
	 */
	public void writeNamespaceTranslations(List<Namespace> namespaces, String virtualWiki) throws WikiException {
		int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		for (Namespace namespace : namespaces) {
			this.dataValidator.validateNamespaceTranslation(namespace, virtualWiki);
		}
		this.queryHandler().updateNamespaceTranslations(namespaces, virtualWiki, virtualWikiId);
		CACHE_NAMESPACE_LIST.removeAllFromCache();
	}

	/**
	 * Add or update a Role object.  This method will add a new record if
	 * the role does not yet exist, otherwise the role will be updated.
	 *
	 * @param role The Role to add or update.  If the Role does not yet
	 *  exist then a new record is created, otherwise an update is
	 *  performed.
	 * @param update A boolean value indicating whether this transaction is
	 *  updating an existing role or not.
	 * @throws WikiException Thrown if the role information is invalid.
	 */
	public void writeRole(Role role, boolean update) throws WikiException {
		this.dataValidator.validateRole(role);
		if (update) {
			this.queryHandler().updateRole(role);
		} else {
			this.queryHandler().insertRole(role);
		}
		// FIXME - add caching
	}

	/**
	 * Add a set of group role mappings.  This method will first delete all
	 * existing role mappings for the specified group, and will then create
	 * a mapping for each specified role.
	 *
	 * @param groupId The group id for whom role mappings are being modified.
	 * @param roles A List of String role names for all roles that are
	 *  to be assigned to this group.
	 * @throws WikiException Thrown if the role information is invalid.
	 */
	public void writeRoleMapGroup(final int groupId, final List<String> roles) throws WikiException {
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					try {
						queryHandler().deleteGroupAuthorities(groupId);
						for (String authority : roles) {
							dataValidator.validateAuthority(authority);
							queryHandler().insertGroupAuthority(groupId, authority);
						}
						// flush the cache
						CACHE_ROLE_MAP_GROUP.removeAllFromCache();
					} catch (WikiException e) {
						status.setRollbackOnly();
						throw new TransactionRuntimeException(e);
					}
				}
			}
		);
	}

	/**
	 * Add a set of user role mappings.  This method will first delete all
	 * existing role mappings for the specified user, and will then create
	 * a mapping for each specified role.
	 *
	 * @param username The username for whom role mappings are being modified.
	 * @param roles A List of String role names for all roles that are
	 *  to be assigned to this user.
	 * @throws WikiException Thrown if the role information is invalid.
	 */
	public void writeRoleMapUser(final String username, final List<String> roles) throws WikiException {
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					try {
						queryHandler().deleteUserAuthorities(username);
						for (String authority : roles) {
							dataValidator.validateAuthority(authority);
							queryHandler().insertUserAuthority(username, authority);
						}
						// flush the cache
						CACHE_ROLE_MAP_GROUP.removeAllFromCache();
					} catch (WikiException e) {
						status.setRollbackOnly();
						throw new TransactionRuntimeException(e);
					}
				}
			}
		);
	}

	/**
	 * Add or update a Topic object.  This method will add a new record if
	 * the Topic does not have a topic ID, otherwise it will perform an update.
	 * A TopicVersion object will also be created to capture the author, date,
	 * and other parameters for the topic.
	 *
	 * @param topic The Topic to add or update.  If the Topic does not have
	 *  a topic ID then a new record is created, otherwise an update is
	 *  performed.
	 * @param topicVersion A TopicVersion containing the author, date, and
	 *  other information about the version being added.  If this value is <code>null</code>
	 *  then no version is saved and no recent change record is created.
	 * @param categories A mapping of categories and their associated sort keys (if any)
	 *  for all categories that are associated with the current topic.
	 * @param links A List of all topic names that are linked to from the
	 *  current topic.  These will be passed to the search engine to create
	 *  searchable metadata.
	 * @throws WikiException Thrown if the topic information is invalid.
	 */
	public void writeTopic(final Topic topic, final TopicVersion topicVersion, final Map<String, String> categories, final List<String> links) throws WikiException {
		long start = System.currentTimeMillis();
		LinkUtil.validateTopicName(topic.getVirtualWiki(), topic.getName(), false);
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					try {
						if (topic.getTopicId() <= 0) {
							// create the initial topic record
							int virtualWikiId = lookupVirtualWikiId(topic.getVirtualWiki());
							dataValidator.validateTopic(topic);
							queryHandler().insertTopic(topic, virtualWikiId);
						} else if (topicVersion == null) {
							// if there is no version record then update the topic.  if there is a version
							// record then the topic will be updated AFTER the version record is created.
							int virtualWikiId = lookupVirtualWikiId(topic.getVirtualWiki());
							dataValidator.validateTopic(topic);
							queryHandler().updateTopic(topic, virtualWikiId);
						}
						if (topicVersion != null) {
							// write version
							if (topicVersion.getPreviousTopicVersionId() == null && topic.getCurrentVersionId() != null) {
								topicVersion.setPreviousTopicVersionId(topic.getCurrentVersionId());
							}
							List<TopicVersion> topicVersions = new ArrayList<TopicVersion>();
							topicVersions.add(topicVersion);
							topicVersion.setTopicId(topic.getTopicId());
							topicVersion.initializeVersionParams(topic);
							dataValidator.validateTopicVersion(topicVersion);
							queryHandler().insertTopicVersions(topicVersions);
							// update the topic AFTER creating the version so that the current_topic_version_id parameter is set properly
							topic.setCurrentVersionId(topicVersion.getTopicVersionId());
							int virtualWikiId = lookupVirtualWikiId(topic.getVirtualWiki());
							dataValidator.validateTopic(topic);
							queryHandler().updateTopic(topic, virtualWikiId);
							String authorName = authorName(topicVersion.getAuthorId(), topicVersion.getAuthorDisplay());
							LogItem logItem = LogItem.initLogItem(topic, topicVersion, authorName);
							RecentChange change = null;
							if (logItem != null) {
								int logVirtualWikiId = lookupVirtualWikiId(logItem.getVirtualWiki());
								dataValidator.validateLogItem(logItem);
								queryHandler().insertLogItem(logItem, logVirtualWikiId);
								change = RecentChange.initRecentChange(logItem);
							} else {
								change = RecentChange.initRecentChange(topic, topicVersion, authorName);
							}
							if (topicVersion.isRecentChangeAllowed()) {
								int changeVirtualWikiId = lookupVirtualWikiId(change.getVirtualWiki());
								dataValidator.validateRecentChange(change);
								queryHandler().insertRecentChange(change, changeVirtualWikiId);
							}
						}
						if (categories != null) {
							// add / remove categories associated with the topic
							queryHandler().deleteTopicCategories(topic.getTopicId());
							if (topic.getDeleteDate() == null && !categories.isEmpty()) {
								List<Category> categoryList = new ArrayList<Category>();
								for (Map.Entry<String, String> entry : categories.entrySet()) {
									Category category = new Category();
									category.setName(entry.getKey());
									category.setSortKey(entry.getValue());
									category.setVirtualWiki(topic.getVirtualWiki());
									category.setChildTopicName(topic.getName());
									categoryList.add(category);
								}
								int virtualWikiId = -1;
								for (Category category : categoryList) {
									virtualWikiId = lookupVirtualWikiId(category.getVirtualWiki());
									dataValidator.validateCategory(category);
								}
								queryHandler().insertCategories(categoryList, virtualWikiId, topic.getTopicId());
							}
						}
						if (links != null) {
							// add / remove links associated with the topic
							queryHandler().deleteTopicLinks(topic.getTopicId());
							if (topic.getDeleteDate() == null && !links.isEmpty()) {
								addTopicLinks(links, topic.getVirtualWiki(), topic.getTopicId());
							}
						}
						if (topicVersion != null) {
							// topic version is only null during changes that aren't user visible
							WikiBase.getSearchEngine().updateInIndex(topic);
						}
						// update the cache only if update successful
						cacheTopicRefresh(topic, true, null);
					} catch (WikiException e) {
						status.setRollbackOnly();
						throw new TransactionRuntimeException(e);
					}
				}
			}
		);
		if (logger.isDebugEnabled()) {
			logger.debug("Wrote topic " + topic.getVirtualWiki() + ':' + topic.getName() + " with params [categories is null: " + (categories == null) + "] / [links is null: " + (links == null) + "] in " + ((System.currentTimeMillis() - start) / 1000.000) + " s.");
		}
	}

	/**
	 * This method exists for performance reasons for scenarios such as topic
	 * imports where many versions may be added without the need to update the
	 * topic record.  In general {@link #writeTopic} should be used instead.
	 *
	 * @param topic The Topic for the versions being added.  The topic must already
	 *  exist.
	 * @param topicVersions A list of TopicVersion objects, each containing the
	 *  author, date, and other information about the version being added.  If
	 *  this value is <code>null</code> or empty then no versions are saved.
	 * @throws WikiException Thrown if the topic version information is invalid.
	 */
	public void writeTopicVersions(Topic topic, List<TopicVersion> topicVersions) throws WikiException {
		if (topic == null || topicVersions == null) {
			logger.warn("Attempt to call writeTopicVersions() with null topic or topic version list");
			return;
		}
		for (TopicVersion topicVersion : topicVersions) {
			topicVersion.setTopicId(topic.getTopicId());
			topicVersion.initializeVersionParams(topic);
			this.dataValidator.validateTopicVersion(topicVersion);
		}
		this.queryHandler().insertTopicVersions(topicVersions);
	}

	/**
	 * Add or update a user block.  This method will add a new record if the
	 * UserBlock object does not have an ID, otherwise it will perform an
	 * update.
	 *
	 * @param userBlock The UserBlock record to add or update.  If the
	 *  UserBlock does not have an ID then a new record is created, otherwise
	 *  an update is performed.
	 * @throws WikiException Thrown if the user block information is invalid.
	 */
	public void writeUserBlock(final UserBlock userBlock) throws WikiException {
		this.dataValidator.validateUserBlock(userBlock);
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					try {
						if (userBlock.getBlockId() <= 0) {
							queryHandler().insertUserBlock(userBlock);
						} else {
							queryHandler().updateUserBlock(userBlock);
						}
						// FIXME - reconsider this approach of separate entries for every virtual wiki
						List<VirtualWiki> virtualWikis = getVirtualWikiList();
						for (VirtualWiki virtualWiki : virtualWikis) {
							LogItem logItem = LogItem.initLogItem(userBlock, virtualWiki.getName());
							int logVirtualWikiId = lookupVirtualWikiId(logItem.getVirtualWiki());
							dataValidator.validateLogItem(logItem);
							queryHandler().insertLogItem(logItem, logVirtualWikiId);
							RecentChange change = RecentChange.initRecentChange(logItem);
							int changeVirtualWikiId = lookupVirtualWikiId(change.getVirtualWiki());
							dataValidator.validateRecentChange(change);
							queryHandler().insertRecentChange(change, changeVirtualWikiId);
						}
						// flush the cache if no errors
						CACHE_USER_BLOCKS_ACTIVE.removeAllFromCache();
					} catch (WikiException e) {
						status.setRollbackOnly();
						throw new TransactionRuntimeException(e);
					}
				}
			}
		);
	}

	/**
	 * Add or update a VirtualWiki object.  This method will add a new record
	 * if the VirtualWiki does not have a virtual wiki ID, otherwise it will
	 * perform an update.
	 *
	 * @param virtualWiki The VirtualWiki to add or update.  If the
	 *  VirtualWiki does not have a virtual wiki ID then a new record is
	 *  created, otherwise an update is performed.
	 * @throws WikiException Thrown if the virtual wiki information is invalid.
	 */
	public void writeVirtualWiki(final VirtualWiki virtualWiki) throws WikiException {
		WikiUtil.validateVirtualWikiName(virtualWiki.getName());
		this.dataValidator.validateVirtualWiki(virtualWiki);
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					if (virtualWiki.getVirtualWikiId() <= 0) {
						queryHandler().insertVirtualWiki(virtualWiki);
					} else {
						queryHandler().updateVirtualWiki(virtualWiki);
					}
					// flush the cache if there were no errors
					CACHE_VIRTUAL_WIKI_LIST.removeAllFromCache();
				}
			}
		);
	}

	/**
	 * Add or delete an item from a user's watchlist.  If the topic is
	 * already in the user's watchlist it will be deleted, otherwise it will
	 * be added.
	 *
	 * @param watchlist The user's current Watchlist.
	 * @param virtualWiki The virtual wiki name for the current virtual wiki.
	 * @param topicName The name of the topic being added or removed from
	 *  the watchlist.
	 * @param userId The ID of the user whose watchlist is being updated.
	 * @throws WikiException Thrown if the watchlist information is invalid.
	 */
	public void writeWatchlistEntry(final Watchlist watchlist, final String virtualWiki, final String topicName, final int userId) throws WikiException {
		final String article = LinkUtil.extractTopicLink(virtualWiki, topicName);
		final String comments = LinkUtil.extractCommentsLink(virtualWiki, topicName);
		final int virtualWikiId = this.lookupVirtualWikiId(virtualWiki);
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					if (watchlist.containsTopic(topicName)) {
						// remove from watchlist
						queryHandler().deleteWatchlistEntry(virtualWikiId, article, userId);
						queryHandler().deleteWatchlistEntry(virtualWikiId, comments, userId);
						watchlist.remove(article);
						watchlist.remove(comments);
					} else {
						// add to watchlist
						queryHandler().insertWatchlistEntry(virtualWikiId, article, userId);
						queryHandler().insertWatchlistEntry(virtualWikiId, comments, userId);
						watchlist.add(article);
						watchlist.add(comments);
					}
				}
			}
		);
	}

	/**
	 * Add or update a WikiGroup object.  This method will add a new record if
	 * the group does not have a group ID, otherwise it will perform an update.
	 *
	 * @param group The WikiGroup to add or update.  If the group does not have
	 *  a group ID then a new record is created, otherwise an update is
	 *  performed.
	 * @throws WikiException Thrown if the group information is invalid.
	 */
	public void writeWikiGroup(final WikiGroup group) throws WikiException {
		this.dataValidator.validateWikiGroup(group);
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					if (group.getGroupId() <= 0) {
						queryHandler().insertWikiGroup(group);
					} else {
						queryHandler().updateWikiGroup(group);
					}
				}
			}
		);
	}

	/**
	 * Write the groupMap to the database. Notice that the method will first delete all entries
	 * referring to the group or the user (depending on groupMapType) and will then add an
	 * entry for each group membership.
	 *
	 * @param groupMap The GroupMap to store
	 */
	public void writeGroupMap(final GroupMap groupMap) {
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					queryHandler().deleteGroupMap(groupMap);
					if (groupMap.getGroupMapType() == GroupMap.GROUP_MAP_GROUP) {
						int groupId = groupMap.getGroupId();
						List<String> groupMembers = groupMap.getGroupMembers();
						for (String groupMember : groupMembers) {
							queryHandler().insertGroupMember(groupMember, groupId);
						}
					} else if (groupMap.getGroupMapType() == GroupMap.GROUP_MAP_USER) {
						String userLogin = groupMap.getUserLogin();
						List<Integer> groupIds = groupMap.getGroupIds();
						for (Integer groupId : groupIds) {
							queryHandler().insertGroupMember(userLogin, groupId.intValue());
						}
					}
				}
			}
		);
	}

	/**
	 * Add or update a WikiUser object.  This method will add a new record
	 * if the WikiUser does not have a user ID, otherwise it will perform an
	 * update.
	 *
	 * @param user The WikiUser being added or updated.  If the WikiUser does
	 *  not have a user ID then a new record is created, otherwise an update
	 *  is performed.
	 * @param username The user's username (login).
	 * @param encryptedPassword The user's encrypted password.  Required only when the
	 *  password is being updated.
	 * @throws WikiException Thrown if the user information is invalid.
	 */
	public void writeWikiUser(final WikiUser user, final String username, final String encryptedPassword) throws WikiException {
		WikiUtil.validateUserName(user.getUsername());
		DatabaseConnection.getTransactionTemplate().execute(
			new TransactionCallbackWithoutResult() {
				protected void doInTransactionWithoutResult(TransactionStatus status) {
					try {
						if (user.getUserId() <= 0) {
							WikiUserDetails userDetails = new WikiUserDetails(username, encryptedPassword);
							dataValidator.validateUserDetails(userDetails);
							queryHandler().insertUserDetails(userDetails);
							dataValidator.validateWikiUser(user);
							queryHandler().insertWikiUser(user);
							queryHandler().updateWikiUserPreferences(user);
							// add all users to the registered user group
							queryHandler().insertGroupMember(user.getUsername(), WikiBase.getGroupRegisteredUser().getGroupId());
							// Flush cache to force reading from database for next search
							// This should be more efficient than looping over the authorities of the
							// group and update them individually
							CACHE_ROLE_MAP_GROUP.removeAllFromCache();
							// FIXME - reconsider this approach of separate entries for every virtual wiki
							List<VirtualWiki> virtualWikis = getVirtualWikiList();
							for (VirtualWiki virtualWiki : virtualWikis) {
								LogItem logItem = LogItem.initLogItem(user, virtualWiki.getName());
								dataValidator.validateLogItem(logItem);
								queryHandler().insertLogItem(logItem, virtualWiki.getVirtualWikiId());
								RecentChange change = RecentChange.initRecentChange(logItem);
								dataValidator.validateRecentChange(change);
								queryHandler().insertRecentChange(change, virtualWiki.getVirtualWikiId());
							}
						} else {
							if (!StringUtils.isBlank(encryptedPassword)) {
								WikiUserDetails userDetails = new WikiUserDetails(username, encryptedPassword);
								dataValidator.validateUserDetails(userDetails);
								queryHandler().updateUserDetails(userDetails);
							}
							dataValidator.validateWikiUser(user);
							queryHandler().updateWikiUser(user);
							queryHandler().updateWikiUserPreferences(user);
						}
						// update the cache only if everything else is successful
						CACHE_USER_BY_USER_ID.addToCache(user.getUserId(), user);
						CACHE_USER_BY_USER_NAME.addToCache(user.getUsername(), user);
					} catch (WikiException e) {
						status.setRollbackOnly();
						throw new TransactionRuntimeException(e);
					}
				}
			}
		);
	}

	/**
	 * Insert or update a user preference default value.
	 *
	 * @param userPreferenceKey The key (or name) of the preference 
	 * @param userPreferenceDefaultValue The default value for this preference
	 * @throws WikiException Thrown if the parameter information is invalid.
	 */
	public void writeUserPreferenceDefault(String userPreferenceKey, String userPreferenceDefaultValue, String userPreferenceGroupKey, int sequenceNr) {
		if (this.queryHandler().existsUserPreferenceDefault(userPreferenceKey)) {
			this.queryHandler().updateUserPreferenceDefault(userPreferenceKey, userPreferenceDefaultValue, userPreferenceGroupKey, sequenceNr);
		} else {
			this.queryHandler().insertUserPreferenceDefault(userPreferenceKey, userPreferenceDefaultValue, userPreferenceGroupKey, sequenceNr);
		}
	}

	/**
	 * Add new image or other data to database.
	 *
	 * @param imageData The image and it's arrtibutes to store.
	 * @param resized Must be true when inserting resized version of image and false otherwise.
	 */
	public void insertImage(ImageData imageData, boolean resized) {
		this.queryHandler().insertImage(imageData, resized);
	}

	/**
	 * Get info of latest version of image.
	 *
	 * @param fileId File identifier.
	 * @param resized Image width or zero for original.
	 * @return The image info or null if image not found. Result's width and height components must
	 * be negative when data are not an image. Result's data component may be null.
	 */
	public ImageData getImageInfo(int fileId, int resized) {
		return this.queryHandler().getImageInfo(fileId, resized);
	}

	/**
	 * Get latest version of image.
	 *
	 * @param fileId File identifier.
	 * @param resized Image width or zero for original.
	 * @return The image data or null if image not found.
	 */
	public ImageData getImageData(int fileId, int resized) {
		return this.queryHandler().getImageData(fileId, resized);
	}

	/**
	 * Get desired version of image.
	 *
	 * @param fileVersionId File identifier.
	 * @param resized Image width or zero for original.
	 * @return The image data or null if image version not found.
	 */
	public ImageData getImageVersionData(int fileVersionId, int resized) {
		return this.queryHandler().getImageVersionData(fileVersionId, resized);
	}
}
