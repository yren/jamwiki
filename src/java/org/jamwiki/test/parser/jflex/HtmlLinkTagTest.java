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
package org.jamwiki.test.parser.jflex;

import java.util.Locale;

/**
 *
 */
public class HtmlLinkTagTest extends JFlexParserTest {

	/**
	 *
	 */
	public HtmlLinkTagTest(String name) {
		super(name);
	}

	/**
	 *
	 */
	public void testHttpLinks() {
		String input = "";
		String output = "";
		input = "http://www.google.com";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"http://www.google.com\" href=\"http://www.google.com\">http://www.google.com</a>\n</p>";
		assertEquals(output, this.parse(input));
		input = "[http://www.google.com]";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"http://www.google.com\" href=\"http://www.google.com\">http://www.google.com</a>\n</p>";
		assertEquals(output, this.parse(input));
		input = "[http://www.google.com Google]";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"Google\" href=\"http://www.google.com\">Google</a>\n</p>";
		assertEquals(output, this.parse(input));
		input = "[http://www.google.com Google dot com]";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"Google dot com\" href=\"http://www.google.com\">Google dot com</a>\n</p>";
		assertEquals(output, this.parse(input));
	}

	/**
	 *
	 */
	public void testHttpsLinks() {
		String input = "";
		String output = "";
		input = "https://www.google.com";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"https://www.google.com\" href=\"https://www.google.com\">https://www.google.com</a>\n</p>";
		assertEquals(output, this.parse(input));
		input = "[https://www.google.com]";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"https://www.google.com\" href=\"https://www.google.com\">https://www.google.com</a>\n</p>";
		assertEquals(output, this.parse(input));
		input = "[https://www.google.com Google]";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"Google\" href=\"https://www.google.com\">Google</a>\n</p>";
		assertEquals(output, this.parse(input));
		input = "[https://www.google.com Google dot com]";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"Google dot com\" href=\"https://www.google.com\">Google dot com</a>\n</p>";
		assertEquals(output, this.parse(input));
	}

	/**
	 *
	 */
	public void testMailtoLinks() {
		String input = "";
		String output = "";
		input = "mailto:email@email.com";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"mailto:email@email.com\" href=\"mailto:email@email.com\">mailto:email@email.com</a>\n</p>";
		assertEquals(output, this.parse(input));
		input = "mailto://email@email.com";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"mailto:email@email.com\" href=\"mailto:email@email.com\">mailto:email@email.com</a>\n</p>";
		assertEquals(output, this.parse(input));
		input = "[mailto:email@email.com]";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"mailto:email@email.com\" href=\"mailto:email@email.com\">mailto:email@email.com</a>\n</p>";
		assertEquals(output, this.parse(input));
		input = "[mailto://email@email.com]";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"mailto:email@email.com\" href=\"mailto:email@email.com\">mailto:email@email.com</a>\n</p>";
		assertEquals(output, this.parse(input));
		input = "[mailto:email@email.com email@email.com]";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"email@email.com\" href=\"mailto:email@email.com\">email@email.com</a>\n</p>";
		assertEquals(output, this.parse(input));
		input = "[mailto:email@email.com email at email dot com]";
		output = "<p><a class=\"externallink\" rel=\"nofollow\" title=\"email at email dot com\" href=\"mailto:email@email.com\">email at email dot com</a>\n</p>";
		assertEquals(output, this.parse(input));
	}
}