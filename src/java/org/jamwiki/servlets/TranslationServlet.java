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
package org.jamwiki.servlets;

import java.io.File;
import java.util.Enumeration;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.jamwiki.Environment;
import org.jamwiki.WikiBase;
import org.jamwiki.WikiMessage;
import org.jamwiki.model.Topic;
import org.jamwiki.model.TopicVersion;
import org.jamwiki.model.WikiUser;
import org.jamwiki.utils.SortedProperties;
import org.jamwiki.utils.Utilities;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.ModelAndView;

/**
 *
 */
public class TranslationServlet extends JAMWikiServlet {

	private static Logger logger = Logger.getLogger(TranslationServlet.class.getName());
	private SortedProperties translations = new SortedProperties();

	/**
	 * This method handles the request after its parent class receives control.
	 *
	 * @param request - Standard HttpServletRequest object.
	 * @param response - Standard HttpServletResponse object.
	 * @return A <code>ModelAndView</code> object to be handled by the rest of the Spring framework.
	 */
	public ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response) {
		ModelAndView next = new ModelAndView("wiki");
		WikiPageInfo pageInfo = new WikiPageInfo();
		try {
			if (!Utilities.isAdmin(request)) {
				String redirect = "Special:Translation";
				WikiMessage errorMessage = new WikiMessage("admin.message.loginrequired");
				return viewLogin(request, redirect, errorMessage);
			}
			String function = request.getParameter("function");
			if (!StringUtils.hasText(function)) {
				view(request, next, pageInfo);
			} else {
				translate(request, next, pageInfo);
			}
			next.addObject("translations", new TreeMap(this.translations));
			next.addObject("codes", this.retrieveTranslationCodes());
			if (request.getParameter("language") != null) next.addObject("language", request.getParameter("language"));
		} catch (Exception e) {
			return viewError(request, e);
		}
		loadDefaults(request, next, pageInfo);
		return next;
	}

	/**
	 *
	 */
	private String filename(HttpServletRequest request) {
		String filename = "ApplicationResources.properties";
		String language = request.getParameter("language");
		if (StringUtils.hasText(language)) {
			// FIXME - should also check for valid language code
			filename = "ApplicationResources_" + language + ".properties";
		}
		return filename;
	}

	/**
	 *
	 */
	private TreeSet retrieveTranslationCodes() throws Exception {
		TreeSet codes = new TreeSet();
		File propertyRoot = Utilities.getClassLoaderRoot();
		File[] files = propertyRoot.listFiles();
		File file;
		String filename;
		for (int i = 0; i < files.length; i++) {
			file = files[i];
			if (!file.isFile()) continue;
			filename = file.getName();
			if (!StringUtils.hasText(filename)) continue;
			if (!filename.startsWith("ApplicationResources_") || !filename.endsWith(".properties")) continue;
			String code = filename.substring("ApplicationResources_".length(), filename.length() - ".properties".length());
			if (StringUtils.hasText(code)) codes.add(code);
		}
		return codes;
	}

	/**
	 *
	 */
	private void translate(HttpServletRequest request, ModelAndView next, WikiPageInfo pageInfo) throws Exception {
		pageInfo.setAction(WikiPageInfo.ACTION_ADMIN_TRANSLATION);
		pageInfo.setAdmin(true);
		pageInfo.setPageTitle(new WikiMessage("translation.title"));
		Enumeration names = request.getParameterNames();
		String name;
		while (names.hasMoreElements()) {
			name = (String)names.nextElement();
			if (!name.startsWith("translations[") || !name.endsWith("]")) {
				continue;
			}
			String key = name.substring("translations[".length(), name.length() - "]".length());
			String value = request.getParameter(name);
			this.translations.setProperty(key, value);
		}
		Environment.saveProperties(filename(request), this.translations, null);
		this.writeTopic(request, null);
	}

	/**
	 *
	 */
	private void view(HttpServletRequest request, ModelAndView next, WikiPageInfo pageInfo) throws Exception {
		String language = request.getParameter("language");
		String filename = filename(request);
		this.translations = new SortedProperties(Environment.loadProperties("ApplicationResources.properties"));
		if (StringUtils.hasText(language)) {
			filename = filename(request);
			this.translations.putAll(Environment.loadProperties(filename));
		}
		pageInfo.setAction(WikiPageInfo.ACTION_ADMIN_TRANSLATION);
		pageInfo.setAdmin(true);
		pageInfo.setPageTitle(new WikiMessage("translation.title"));
	}

	/**
	 *
	 */
	protected void writeTopic(HttpServletRequest request, String editComment) throws Exception {
		String virtualWiki = JAMWikiServlet.getVirtualWikiFromURI(request);
		String topicName = filename(request);
		String contents = Utilities.readFile(topicName);
		Topic topic = WikiBase.getHandler().lookupTopic(virtualWiki, topicName);
		if (topic == null) {
			topic = new Topic();
			topic.setVirtualWiki(virtualWiki);
			topic.setName(topicName);
		}
		topic.setTopicContent(contents);
		topic.setReadOnly(true);
		topic.setTopicType(Topic.TYPE_SYSTEM_FILE);
		Integer authorId = null;
		WikiUser user = Utilities.currentUser(request);
		if (user != null) {
			authorId = new Integer(user.getUserId());
		}
		TopicVersion topicVersion = new TopicVersion(authorId, request.getRemoteAddr(), editComment, contents);
		WikiBase.getHandler().writeTopic(topic, topicVersion, null);
	}
}
