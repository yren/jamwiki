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

/**
 * Wrapper for exceptions thrown during transactions since TransactionCallback
 * does not support exception handling directly.
 */
public class TransactionRuntimeException extends RuntimeException {

	private final Exception wrappedException;

	/**
	 * Standard constructor including the wrapped exception.
	 *
	 * @param wrappedException The wrapped exception.
	 */
	public TransactionRuntimeException(Exception wrappedException) {
		this.wrappedException = wrappedException;
	}

	/**
	 * Return the wrapped exception.
	 *
	 * @return wrappedException The wrapped exception.
	 */
	public Exception getWrappedException() {
		return this.wrappedException;
	}
}
