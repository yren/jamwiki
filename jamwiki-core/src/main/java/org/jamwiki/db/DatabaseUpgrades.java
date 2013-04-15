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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.jamwiki.DataAccessException;
import org.jamwiki.WikiBase;
import org.jamwiki.WikiException;
import org.jamwiki.WikiMessage;
import org.jamwiki.utils.WikiLogger;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;

/**
 * This class simply contains utility methods for upgrading database schemas
 * (if needed) between JAMWiki versions.  These methods are typically called automatically
 * by the UpgradeServlet when an upgrade is detected and will automatically upgrade the
 * database schema without the need for manual intervention from the user.
 *
 * In general upgrade methods will only be maintained for two major releases and then
 * deleted - for example, JAMWiki version 0.9.0 will not support upgrading from versions
 * prior to 0.7.0.
 */
public class DatabaseUpgrades {

	private static final WikiLogger logger = WikiLogger.getLogger(DatabaseUpgrades.class.getName());

	/**
	 *
	 */
	private DatabaseUpgrades() {
	}

	/**
	 * Perform the required database upgrade steps when upgrading from versions
	 * older than JAMWiki 1.3.
	 */
	public static void upgrade130(final List<WikiMessage> messages) throws WikiException {
		try {
			DatabaseConnection.getTransactionTemplate().execute(
				new TransactionCallbackWithoutResult() {
					protected void doInTransactionWithoutResult(TransactionStatus status) {
						try {
							// New tables as of JAMWiki 1.3
							WikiBase.getDataHandler().queryHandler().executeUpgradeUpdate("STATEMENT_CREATE_USER_PREFERENCES_DEFAULTS_TABLE");
							messages.add(new WikiMessage("upgrade.message.db.table.added", "jam_user_preferences_defaults"));
							WikiBase.getDataHandler().queryHandler().executeUpgradeUpdate("STATEMENT_CREATE_USER_PREFERENCES_TABLE");
							messages.add(new WikiMessage("upgrade.message.db.table.added", "jam_user_preferences"));
							WikiDatabase.setupUserPreferencesDefaults();
							// Create default values for user preferences.
							messages.add(new WikiMessage("upgrade.message.db.data.updated", "jam_user_preferences_defaults"));
							// Migrate existing user preferences to new tables
							WikiBase.getDataHandler().queryHandler().executeUpgradeUpdate("UPGRADE_130_MIGRATE_USER_PREFERENCES_DEFAULT_LOCALE");
							WikiBase.getDataHandler().queryHandler().executeUpgradeUpdate("UPGRADE_130_MIGRATE_USER_PREFERENCES_EDITOR");
							WikiBase.getDataHandler().queryHandler().executeUpgradeUpdate("UPGRADE_130_MIGRATE_USER_PREFERENCES_SIGNATURE");
							messages.add(new WikiMessage("upgrade.message.db.data.updated", "jam_user_preferences"));
							// Drop old user preference columns from jam_wiki_user
							WikiBase.getDataHandler().queryHandler().executeUpgradeUpdate("UPGRADE_130_REMOVE_WIKI_USER_TABLE_COLUMN_DEFAULT_LOCALE");
							messages.add(new WikiMessage("upgrade.message.db.data.updated", "jam_wiki_user"));
							WikiBase.getDataHandler().queryHandler().executeUpgradeUpdate("UPGRADE_130_REMOVE_WIKI_USER_TABLE_COLUMN_EDITOR");
							messages.add(new WikiMessage("upgrade.message.db.data.updated", "jam_wiki_user"));
							WikiBase.getDataHandler().queryHandler().executeUpgradeUpdate("UPGRADE_130_REMOVE_WIKI_USER_TABLE_COLUMN_SIGNATURE");
							messages.add(new WikiMessage("upgrade.message.db.data.updated", "jam_wiki_user"));
							WikiBase.getDataHandler().queryHandler().executeUpgradeUpdate("UPGRADE_130_ADD_USER_TABLE_COLUMN_CHALLENGE_VALUE");
							messages.add(new WikiMessage("upgrade.message.db.data.updated", "jam_users"));
							WikiBase.getDataHandler().queryHandler().executeUpgradeUpdate("UPGRADE_130_ADD_USER_TABLE_COLUMN_CHALLENGE_DATE");
							messages.add(new WikiMessage("upgrade.message.db.data.updated", "jam_users"));
							WikiBase.getDataHandler().queryHandler().executeUpgradeUpdate("UPGRADE_130_ADD_USER_TABLE_COLUMN_CHALLENGE_IP");
							messages.add(new WikiMessage("upgrade.message.db.data.updated", "jam_users"));
							WikiBase.getDataHandler().queryHandler().executeUpgradeUpdate("UPGRADE_130_ADD_USER_TABLE_COLUMN_CHALLENGE_TRIES");
							messages.add(new WikiMessage("upgrade.message.db.data.updated", "jam_users"));
						} catch (SQLException e) {
							status.setRollbackOnly();
							throw new TransactionRuntimeException(e);
						} catch (DataAccessException e) {
							status.setRollbackOnly();
							throw new TransactionRuntimeException(e);
						} catch (WikiException e) {
							status.setRollbackOnly();
							throw new TransactionRuntimeException(e);
						}
					}
				}
			);
		} catch (TransactionRuntimeException e) {
			throw new WikiException(new WikiMessage("upgrade.error.fatal", e.getMessage()));
		}
	}
}
