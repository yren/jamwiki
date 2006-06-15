package org.vqwiki.persistency.file;

import java.io.File;
import java.io.FilenameFilter;

/*
Very Quick Wiki - WikiWikiWeb clone
Copyright (C) 2001-2002 Gareth Cronin

This program is free software; you can redistribute it and/or modify
it under the terms of the latest version of the GNU Lesser General
Public License as published by the Free Software Foundation;

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program (gpl.txt); if not, write to the Free Software
Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

public class FileStartFilter implements FilenameFilter {

	String prefix;

	/**
	 *
	 */
	public FileStartFilter(String prefix) {
		this.prefix = prefix;
	}

	/**
	 *
	 */
	public boolean accept(File file, String name) {
		return (name.startsWith(prefix));
	}
}