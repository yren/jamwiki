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
package org.jamwiki.utils;

import java.io.FileNotFoundException;
import java.util.Locale;
import org.jamwiki.JAMWikiUnitTest;
import org.jamwiki.WikiException;
import org.jamwiki.WikiMessage;
import org.jamwiki.model.Topic;
import org.jamwiki.model.TopicType;
import org.junit.Test;
import static org.junit.Assert.*;

public class WikiUtilTest extends JAMWikiUnitTest {

	/**
	 *
	 */
	@Test
	public void testEncodeForFilename() throws Throwable {
		//TODO
		String result = WikiUtil.encodeForFilename("testUtilitiesName");
		assertEquals("result", "testUtilitiesName", result);
	}

	/**
	 *
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testEncodeForFilename2() throws Throwable {
		WikiUtil.encodeForFilename(null);
	}

	/**
	 *
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testEncodeForFilename3() throws Throwable {
		WikiUtil.encodeForFilename(" ");
	}

	/**
	 *
	 */
	@Test
	public void testExtractTopicLink() throws Throwable {
		//TODO
		String result = WikiUtil.extractTopicLink("en", "testWikiUtilName");
		assertSame("result", "testWikiUtilName", result);
	}

	/**
	 *
	 */
	@Test
	public void testFindRedirectedTopic1() throws Throwable {
		Topic parent = new Topic("en", "Test");
		parent.setTopicType(TopicType.REDIRECT);
		Topic result = WikiUtil.findRedirectedTopic(parent, 100);
		assertSame("result", parent, result);
	}

	/**
	 *
	 */
	@Test
	public void testValidateDirectory1() throws Throwable {
		WikiMessage result = WikiUtil.validateDirectory("testUtilitiesName");
		assertEquals("result.getKey()", "error.directoryinvalid", result.getKey());
	}

	/**
	 *
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testExtractCommentsLinkThrowsException() throws Throwable {
		WikiUtil.extractCommentsLink("en", "");
	}

	/**
	 *
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testExtractTopicLinkThrowsException() throws Throwable {
		WikiUtil.extractTopicLink("en", "");
	}

	/**
	 *
	 */
	@Test(expected=NullPointerException.class)
	public void testValidateDirectoryThrowsNullPointerException() throws Throwable {
		WikiUtil.validateDirectory(null);
	}

	/**
	 *
	 */
	@Test
	public void testValidateNamespaceName1() throws Throwable {
		WikiUtil.validateNamespaceName("New Namespace");
	}

	/**
	 *
	 */
	@Test
	public void testValidateNamespaceName2() throws Throwable {
		WikiUtil.validateNamespaceName("New-Namespace");
	}

	/**
	 *
	 */
	@Test
	public void testValidateNamespaceName3() throws Throwable {
		WikiUtil.validateNamespaceName("什么意思");
	}

	/**
	 *
	 */
	@Test
	public void testValidateNamespaceName4() throws Throwable {
		try {
			WikiUtil.validateNamespaceName(" ");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("Expected error message key", "admin.vwiki.error.namespace.whitespace", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test
	public void testValidateNamespaceName5() throws Throwable {
		try {
			WikiUtil.validateNamespaceName("");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("Expected error message key", "admin.vwiki.error.namespace.unique", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test
	public void testValidateNamespaceName6() throws Throwable {
		try {
			WikiUtil.validateNamespaceName("Special");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("Expected error message key", "admin.vwiki.error.namespace.unique", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test
	public void testValidateNamespaceName7() throws Throwable {
		try {
			WikiUtil.validateNamespaceName("Name/Space");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("Expected error message key", "admin.vwiki.error.namespace.characters", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test
	public void testValidateNamespaceName8() throws Throwable {
		try {
			WikiUtil.validateNamespaceName(" Name space");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("Expected error message key", "admin.vwiki.error.namespace.whitespace", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test
	public void testValidateNamespaceName9() throws Throwable {
		try {
			WikiUtil.validateNamespaceName("My: Namespace");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("Expected error message key", "admin.vwiki.error.namespace.characters", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test
	public void testValidateNamespaceName10() throws Throwable {
		try {
			WikiUtil.validateNamespaceName("User");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("Expected error message key", "admin.vwiki.error.namespace.unique", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test(expected=WikiException.class)
	public void testValidateTopicNameThrowsNullPointerException() throws Throwable {
		WikiUtil.validateTopicName(null, null);
	}

	/**
	 *
	 */
	@Test
	public void testValidateTopicNameThrowsWikiException1() throws Throwable {
		try {
			WikiUtil.validateTopicName("en", "");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("ex.getWikiMessage().getKey()", "common.exception.notopic", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test
	public void testValidateTopicNameThrowsWikiException2() throws Throwable {
		try {
			WikiUtil.validateTopicName("en", "/Test");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("ex.getWikiMessage().getKey()", "common.exception.name", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test
	public void testValidateTopicNameThrowsWikiException3() throws Throwable {
		try {
			WikiUtil.validateTopicName("en", "Comments:/Test");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("ex.getWikiMessage().getKey()", "common.exception.name", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test
	public void testValidateTopicNameThrowsWikiException4() throws Throwable {
		try {
			WikiUtil.validateTopicName("en", "Comments: /Test");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("ex.getWikiMessage().getKey()", "common.exception.name", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test
	public void testValidateTopicNameThrowsWikiException5() throws Throwable {
		try {
			WikiUtil.validateTopicName("en", "Comments: /Test");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("ex.getWikiMessage().getKey()", "common.exception.name", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test
	public void testValidateTopicNameThrowsWikiException6() throws Throwable {
		try {
			WikiUtil.validateTopicName("en", "Test?");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("ex.getWikiMessage().getKey()", "common.exception.name", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test
	public void testValidateUserNameThrowsWikiException() throws Throwable {
		try {
			WikiUtil.validateUserName("testWikiUtil\rName");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("ex.getWikiMessage().getKey()", "common.exception.name", ex.getWikiMessage().getKey());
		}
	}

	/**
	 *
	 */
	@Test
	public void testValidateUserNameThrowsWikiException1() throws Throwable {
		try {
			WikiUtil.validateUserName("");
			fail("Expected WikiException to be thrown");
		} catch (WikiException ex) {
			assertEquals("ex.getWikiMessage().getKey()", "error.loginempty", ex.getWikiMessage().getKey());
		}
	}
}