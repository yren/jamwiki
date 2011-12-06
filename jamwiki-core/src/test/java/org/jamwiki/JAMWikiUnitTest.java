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
 *
 * Based on code generated by Agitar build: Agitator Version 1.0.2.000071 (Build date: Jan 12, 2007) [1.0.2.000071]
 */
package org.jamwiki;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import org.apache.commons.io.FileUtils;
import org.jamwiki.db.WikiDatabase;
import org.jamwiki.model.Topic;
import org.jamwiki.model.TopicType;
import org.jamwiki.model.TopicVersion;
import org.jamwiki.model.VirtualWiki;
import org.jamwiki.model.WikiFileVersion;
import org.jamwiki.model.WikiUser;
import org.jamwiki.parser.WikiLink;
import org.jamwiki.parser.image.ImageUtil;
import org.junit.Before;

/**
 * JAMWiki parent class for unit tests.  This class will verify that a test
 * JAMWiki instance is available and that is has been loaded with sample topics,
 * virtual wikis, and a default user account.
 */
public abstract class JAMWikiUnitTest {

	/**
	 * If a test JAMWiki instance does not yet exist, create one to allow running
	 * of unit tests that require a working JAMWiki instance.  Note that this
	 * method is run only once per target, since if the test instance already
	 * exists a new one will not be set up.
	 */
	@Before
	public void setup() throws Exception {
		File rootDirectory = new File("target", "data");
		if (!rootDirectory.exists()) {
			rootDirectory.mkdir();
			Environment.setValue(Environment.PROP_BASE_FILE_DIR, rootDirectory.getAbsolutePath());
			File databaseDirectory = new File(rootDirectory, "database");
			if (!databaseDirectory.exists()) {
				this.setupDatabase();
			}
			File filesDirectory = new File(rootDirectory, "files");
			File testFilesDirectory = new File("src/test/resources/data/files");
			if (!filesDirectory.exists() && testFilesDirectory.exists()) {
				// copy everything from src/test/resources/data/files to this directory
				FileUtils.copyDirectory(testFilesDirectory, filesDirectory);
			}
		}
	}

	/**
	 * Initialize a test HSQL database for JAMWiki including two virtual wikis
	 * and a default user account.
	 */
	private void setupDatabase() throws Exception {
		WikiDatabase.setupDefaultDatabase(Environment.getInstance());
		Locale locale = new Locale("en-US");
		String username = "user";
		String password = "password";
		WikiUser wikiUser = new WikiUser(username);
		WikiBase.reset(locale, wikiUser, username, password);
		// set up a second "test" virtual wiki
		VirtualWiki virtualWiki = new VirtualWiki("test");
		virtualWiki.setRootTopicName("StartingPoints");
		WikiBase.getDataHandler().writeVirtualWiki(virtualWiki);
		WikiBase.getDataHandler().setupSpecialPages(locale, wikiUser, virtualWiki);
	}

	/**
	 * Read and load a test topic from the file system.
	 */
	protected Topic setupTopic(VirtualWiki virtualWiki, String fileName) throws DataAccessException, IOException, WikiException {
		String contents = TestFileUtil.retrieveFileContent(TestFileUtil.TEST_TOPICS_DIR, fileName);
		String topicName = TestFileUtil.decodeTopicName(fileName);
		return this.setupTopic(virtualWiki, topicName, contents);
	}

	/**
	 * Crate a test topic.
	 */
	protected Topic setupTopic(VirtualWiki virtualWiki, String topicName, String contents) throws DataAccessException, IOException, WikiException {
		if (virtualWiki == null) {
			virtualWiki = WikiBase.getDataHandler().lookupVirtualWiki("en");
		}
		WikiLink wikiLink = new WikiLink(null, virtualWiki.getName(), topicName);
		Topic topic = new Topic(virtualWiki.getName(), wikiLink.getNamespace(), wikiLink.getArticle());
		topic.setTopicContent(contents);
		if (topicName.toLowerCase().startsWith("image:")) {
			this.setupImage(virtualWiki, topic);
			return topic;
		}
		this.setupTopic(topic);
		return topic;
	}

	/**
	 * Crate a test topic.  Cannot be used for images.
	 */
	protected void setupTopic(Topic topic) throws DataAccessException, WikiException {
		TopicVersion topicVersion = new TopicVersion(null, "127.0.0.1", null, topic.getTopicContent(), topic.getTopicContent().length());
		WikiBase.getDataHandler().writeTopic(topic, topicVersion, null, null);
	}

	/**
	 * Set up images separately - one image is created in both virtual wikis, the
	 * second image is set up in only the shared virtual wiki.
	 */
	private void setupImage(VirtualWiki virtualWiki, Topic topic) throws DataAccessException, IOException, WikiException {
		if (!topic.getName().toLowerCase().startsWith("image:")) {
			throw new IllegalArgumentException("Cannot call JAMWikiUtilTest.setupImage for non-image topics");
		}
		TopicVersion topicVersion = new TopicVersion(null, "127.0.0.1", null, topic.getTopicContent(), topic.getTopicContent().length());
		topic.setTopicType(TopicType.IMAGE);
		topicVersion.setEditType(TopicVersion.EDIT_UPLOAD);
		// hard code image details - Image:Test Image.jpg will be created for both the "en"
		// and "test" virtual wikis, while Image:Test Image2.jpg will be created only for
		// the "test" virtual wiki.
		WikiFileVersion wikiFileVersion = new WikiFileVersion();
		if (topic.getName().equals("Image:Test Image.jpg") && virtualWiki.getName().equals("en")) {
			WikiBase.getDataHandler().writeTopic(topic, topicVersion, null, null);
			ImageUtil.writeWikiFile(topic, wikiFileVersion, null, "127.0.0.1", "test_image.jpg", "/test_image.jpg", "image/jpeg", 61136);
		} else if (topic.getName().equals("Image:Test Image.jpg") && virtualWiki.getName().equals("test")) {
			WikiBase.getDataHandler().writeTopic(topic, topicVersion, null, null);
			ImageUtil.writeWikiFile(topic, wikiFileVersion, null, "127.0.0.1", "test_image_shared.jpg", "/test_image_shared.jpg", "image/jpeg", 61136);
		} else if (topic.getName().equals("Image:Test Image2.jpg") && virtualWiki.getName().equals("test")) {
			WikiBase.getDataHandler().writeTopic(topic, topicVersion, null, null);
			ImageUtil.writeWikiFile(topic, wikiFileVersion, null, "127.0.0.1", "test_image2_shared.jpg", "/test_image2_shared.jpg", "image/jpeg", 61136);
		}
	}
}
