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
package org.jamwiki.utils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.DiskStoreConfiguration;
import org.jamwiki.DataAccessException;
import org.jamwiki.Environment;

/**
 * Implement utility functions that interact with the cache and provide the
 * infrastructure for storing and retrieving items from the cache.
 */
public class WikiCache<K, V> {

	private static final WikiLogger logger = WikiLogger.getLogger(WikiCache.class.getName());
	private static CacheManager CACHE_MANAGER = null;
	private static boolean INITIALIZED = false;
	// track whether this instance was instantiated from an ehcache.xml file or using configured properties.
	private static boolean USES_XML_CONFIG;
	private static final String EHCACHE_XML_CONFIG_FILENAME = "ehcache.xml";
	/** Directory for cache files. */
	private static final String CACHE_DIR = "cache";
	private final String cacheName;

	/**
	 * Initialize a new cache with the given name.
	 *
	 * @param cacheName The name of the cache being created.  This name should not
	 *  be re-used, otherwise unexpected results could be returned.
	 */
	public WikiCache(String cacheName) {
		this.cacheName = cacheName;
	}

	/**
	 * Add an object to the cache.
	 *
	 * @param key A String, Integer, or other object to use as the key for
	 *  storing and retrieving this object from the cache.
	 * @param value The object that is being stored in the cache.
	 */
	public void addToCache(K key, V value) {
		this.getCache().put(new Element(key, value));
	}

	/**
	 * Internal method used to retrieve the Cache object created for this
	 * instance's cache name.  If no cache exists with the given name then
	 * a new cache will be created.
	 *
	 * @return The existing cache object, or a new cache if no existing cache
	 *  exists.
	 * @throws IllegalStateException if an attempt is made to retrieve a cache
	 *  using XML configuration and the cache is not configured.
	 */
	private Cache getCache() throws CacheException {
		if (!WikiCache.INITIALIZED) {
			WikiCache.initialize();
		}
		if (!WikiCache.CACHE_MANAGER.cacheExists(this.cacheName)) {
			if (USES_XML_CONFIG) {
				// all caches should be configured from ehcache.xml
				throw new IllegalStateException("No cache named " + this.cacheName + " is configured in the ehcache.xml file");
			}
			int maxSize = Environment.getIntValue(Environment.PROP_CACHE_INDIVIDUAL_SIZE);
			int maxAge = Environment.getIntValue(Environment.PROP_CACHE_MAX_AGE);
			int maxIdleAge = Environment.getIntValue(Environment.PROP_CACHE_MAX_IDLE_AGE);
			Cache cache = new Cache(this.cacheName, maxSize, true, false, maxAge, maxIdleAge);
			WikiCache.CACHE_MANAGER.addCache(cache);
		}
		return WikiCache.CACHE_MANAGER.getCache(this.cacheName);
	}

	/**
	 * Return the name of the cache that this instance was configured with.
	 */
	public String getCacheName() {
		return this.cacheName;
	}

	/**
	 * Initialize the cache, clearing any existing cache instances and loading
	 * a new cache instance.
	 */
	public static void initialize() {
		boolean xmlConfig = false;
		try {
			ResourceUtil.getClassLoaderFile(EHCACHE_XML_CONFIG_FILENAME);
			logger.info("Initializing cache configuration from " + EHCACHE_XML_CONFIG_FILENAME + " file");
			xmlConfig = true;
		} catch (IOException e) {
			logger.info("No " + EHCACHE_XML_CONFIG_FILENAME + " file found, using default cache configuration");
		}
		WikiCache.USES_XML_CONFIG = xmlConfig;
		try {
			if (WikiCache.CACHE_MANAGER != null) {
				if (USES_XML_CONFIG) {
					WikiCache.CACHE_MANAGER.removalAll();
				}
				WikiCache.CACHE_MANAGER.shutdown();
				WikiCache.CACHE_MANAGER = null;
			}
			File directory = new File(Environment.getValue(Environment.PROP_BASE_FILE_DIR), CACHE_DIR);
			if (!directory.exists()) {
				directory.mkdir();
			}
			if (USES_XML_CONFIG) {
				WikiCache.CACHE_MANAGER = CacheManager.create();
			} else {
				Configuration configuration = new Configuration();
				CacheConfiguration defaultCacheConfiguration = new CacheConfiguration("jamwikiCache", Environment.getIntValue(Environment.PROP_CACHE_TOTAL_SIZE));
				defaultCacheConfiguration.setDiskPersistent(false);
				defaultCacheConfiguration.setEternal(false);
				defaultCacheConfiguration.setOverflowToDisk(true);
				configuration.addDefaultCache(defaultCacheConfiguration);
				DiskStoreConfiguration diskStoreConfiguration = new DiskStoreConfiguration();
				diskStoreConfiguration.setPath(directory.getPath());
				configuration.addDiskStore(diskStoreConfiguration);
				WikiCache.CACHE_MANAGER = new CacheManager(configuration);
			}
		} catch (Exception e) {
			logger.error("Failure while initializing cache", e);
			throw new RuntimeException(e);
		}
		logger.info("Initializing cache");
		WikiCache.INITIALIZED = true;
	}

	/**
	 * Return <code>true</code> if the key is in the specified cache, even
	 * if the value associated with that key is <code>null</code>.
	 */
	public boolean isKeyInCache(K key) {
		return this.getCache().isKeyInCache(key);
	}

	/**
	 * Close the cache manager.
	 */
	public static void shutdown() {
		WikiCache.INITIALIZED = false;
		if (WikiCache.CACHE_MANAGER != null) {
			WikiCache.CACHE_MANAGER.shutdown();
			WikiCache.CACHE_MANAGER = null;
		}
	}

	/**
	 * Given two string values, generate a unique key value that can be used to
	 * store and retrieve cache objects.
	 *
	 * @param value1 The first value to use in the key name.
	 * @param value2 The second value to use in the key name.
	 * @return The generated key value.
	 */
	public static String key(String value1, String value2) {
		if (value1 == null && value2 == null) {
			throw new IllegalArgumentException("WikiCache.key cannot be called with two null values");
		}
		if (value1 == null) {
			value1 = "";
		}
		if (value2 == null) {
			value2 = "";
		}
		return value1 + "/" + value2;
	}

	/**
	 * Remove all values from the cache.
	 */
	public void removeAllFromCache() {
		this.getCache().removeAll();
	}

	/**
	 * Remove a value from the cache with the given key.
	 *
	 * @param key The key for the record that is being removed from the cache.
	 */
	public void removeFromCache(K key) {
		this.getCache().remove(key);
	}

	/**
	 * Remove a key from the cache in a case-insensitive manner.  This method
	 * is significantly slower than removeFromCache and should only be used when
	 * the key values may not be exactly known.
	 */
	public void removeFromCacheCaseInsensitive(String key) {
		List cacheKeys = this.getCache().getKeys();
		for (Object cacheKey : cacheKeys) {
			// with the upgrade to ehcache 2.4.2 it seems that null cache keys are possible...
			if (cacheKey != null && cacheKey.toString().equalsIgnoreCase(key)) {
				this.getCache().remove(cacheKey);
			}
		}
	}

	/**
	 * Retrieve an object from the cache.  IMPORTANT: this method will return
	 * <code>null</code> if no matching element is cached OR if the cached
	 * object has a value of <code>null</code>.  Callers should call
	 * {@link isKeyInCache} if a <code>null</code> value is returned to
	 * determine whether a <code>null</code> was cached or if the value does
	 * not exist in the cache.
	 *
	 * @param key The key for the record that is being retrieved from the
	 *  cache.
	 * @return The cached object if one is found, <code>null</code> otherwise.
	 */
	public V retrieveFromCache(K key) {
		Element element = this.getCache().get(key);
		return (element != null) ? (V)element.getObjectValue() : null;
	}
}
