using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Threading;
using DC.Data.SqlDependencies.Properties;

namespace DC.Data.SqlDependencies
{
	/// <summary>
	/// Helper class
	/// </summary>
	internal static class Helper
	{
		private static readonly object _syncLock = new object();
		private static readonly Dictionary<string, int> _refCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

		#region External Access
		/// <summary>
		/// Ensures that the required db objects exist
		/// </summary>
		/// <param name="dependencyParams">Dependency parameters</param>
		public static void CreateSqlObjects(SqlDependencyParams dependencyParams)
		{
			// Create connection
			using (new MonitorLock(_syncLock))
			using (var conn = new SqlConnection(dependencyParams.ConnectionString))
			{
				// Open
				EnsureOpenConnection(conn);

				// Take a backup of ref counts
				var backupRefCounts = new Dictionary<string, int>(_refCounts);

				// Transaction
				using (var trans = conn.BeginTransaction(IsolationLevel.Serializable))
				{
					// Enclose to rollback uncommitted transaction
					var transCommitted = false;
					try
					{
						// Changes table
						EnsureChangeHolderTable(conn, trans, dependencyParams);

						// Ensure Table Triggers for all monitored tables
						EnsureTableTriggers(conn, trans, dependencyParams);

						// Commit
						trans.Commit();
						transCommitted = true;
					}
					finally
					{
						if (!transCommitted)
						{
							// Rollback db
							trans.Rollback();

							// Rollback Ref Counts
							_refCounts.Clear();
							backupRefCounts.ToList().ForEach(kv => _refCounts[kv.Key] = kv.Value);
						}
					}
				}
			}
		}

		/// <summary>
		/// Ensures that the required db objects are deleted
		/// </summary>
		/// <param name="dependencyParams">Dependency parameters</param>
		public static void DeleteSqlObjects(SqlDependencyParams dependencyParams)
		{
			// Create connection
			using (new MonitorLock(_syncLock))
			using (var conn = new SqlConnection(dependencyParams.ConnectionString))
			{
				// Open
				EnsureOpenConnection(conn);

				// Take a backup of ref counts
				var backupRefCounts = new Dictionary<string, int>(_refCounts);

				// Transaction
				using (var trans = conn.BeginTransaction(IsolationLevel.Serializable))
				{
					// Enclose to rollback uncommitted transaction
					var transCommitted = false;
					try
					{
						// Delete Table Triggers for all monitored tables
						DeleteTableTriggers(conn, trans, dependencyParams);

						// Delete Changes table
						DeleteChangeHolderTable(conn, trans, dependencyParams);

						// Commit
						trans.Commit();
						transCommitted = true;
					}
					finally
					{
						if (!transCommitted)
						{
							// Rollback db
							trans.Rollback();

							// Rollback Ref Counts
							_refCounts.Clear();
							backupRefCounts.ToList().ForEach(kv => _refCounts[kv.Key] = kv.Value);
						}
					}
				}
			}
		}
		#endregion

		#region Helpers
		/// <summary>
		/// Checks if the connection is open. If not, tries to open the connection
		/// </summary>
		/// <param name="connection">Connection object</param>
		/// <param name="maxTries">Number of times a connection will be tried</param>
		[SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
		public static void EnsureOpenConnection(DbConnection connection, int maxTries = 5)
		{
			// Validate
			if (connection == null)
			{
				throw new ArgumentNullException(nameof(connection));
			}
			if (maxTries < 0)
			{
				maxTries = 5;
			}

			var numTries = 0;
			Exception previousException = null;
			while (true)
			{
				// Check State
				if ((connection.State == ConnectionState.Open)
					|| (connection.State == ConnectionState.Executing)
					|| (connection.State == ConnectionState.Fetching))
				{
					return;
				}
				else if (connection.State == ConnectionState.Connecting)
				{
					Thread.Sleep(250);
					continue;
				}

				// Check if retries exceeded
				if (numTries >= maxTries)
				{
#if USETRANSLATIONS
					throw new InvalidOperationException(Translations.Default["SText_DatabaseConnectionCouldNotBeOpened"], previousException);
#else
					throw new InvalidOperationException(Resources.SText_DatabaseConnectionCouldNotBeOpened, previousException);
#endif
				}

				// Delay
				if (numTries > 0)
				{
					Thread.Sleep(1000);
				}

				try
				{
					// Open
					connection.Open();
				}
				catch (InvalidOperationException)
				{
					// Data source/server not specified error. Cannot retry
					throw;
				}
				catch (SqlException ex)
				{
					// Check password error
					if ((ex.Number == 18487)
						|| (ex.Number == 18488))
					{
						// Password expired. Cannot retry
						throw;
					}

					// Save exception
					previousException = ex;
				}
				catch (Exception ex)
				{
					// Continue on all other errors
					// Save exception
					previousException = ex;
				}
				finally
				{
					numTries += 1;
				}
			}
		}

		/// <summary>
		/// Check if a table exists
		/// </summary>
		[SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
		internal static bool TableExists(SqlConnection connection, SqlTransaction trans, SqlDependencyQualifiedObjectName table)
		{
			var commandText = string.Format(CultureInfo.InvariantCulture,
				"SELECT COUNT(*) FROM [sys].[tables] WHERE [object_id] = OBJECT_ID('{0}')",
				table);
			using (var cmd = new SqlCommand(commandText, connection))
			{
				cmd.CommandType = CommandType.Text;
				cmd.Transaction = trans;
				return Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture) > 0;
			}
		}

		/// <summary>
		/// Check if a trigger exists
		/// </summary>
		[SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
		internal static bool TriggerExists(SqlConnection connection, SqlTransaction trans, SqlDependencyQualifiedObjectName trigger)
		{
			var commandText = string.Format(CultureInfo.InvariantCulture,
				"SELECT COUNT(*) FROM [sys].[triggers] WHERE [object_id] = OBJECT_ID('{0}')",
				trigger);
			using (var cmd = new SqlCommand(commandText, connection))
			{
				cmd.CommandType = CommandType.Text;
				cmd.Transaction = trans;
				return Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture) > 0;
			}
		}

		/// <summary>
		/// Gets a nullable field value from the data reader
		/// </summary>
		[SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
		public static TValue? GetNullableFieldValue<TValue>(DbDataReader dataReader, string fieldName)
			where TValue: struct
		{
			object fieldValue = dataReader[fieldName];
			if (Convert.IsDBNull(fieldValue) || (fieldValue == null))
			{
				return null;
			}
			try
			{
				return (TValue) fieldValue;
			}
			catch
			{
				return (TValue) Convert.ChangeType(fieldValue, typeof (TValue), CultureInfo.CurrentCulture);
			}
		}

		/// <summary>
		/// Increments the object ref count
		/// </summary>
		private static int IncrementRefCount(string objectName)
		{
			int refCount;
			if (!_refCounts.TryGetValue(objectName, out refCount))
			{
				_refCounts[objectName] = refCount = 1;
			}
			else
			{
				_refCounts[objectName] = refCount += 1;
			}
			return refCount;
		}

		/// <summary>
		/// Decrements the object ref count
		/// </summary>
		private static int DecrementRefCount(string objectName)
		{
			int refCount;
			if (_refCounts.TryGetValue(objectName, out refCount)
				&& (refCount > 0))
			{
				_refCounts[objectName] = refCount -= 1;
			}
			return refCount;
		}
		#endregion

		#region Sql Objects
		/// <summary>
		/// Ensures changes holder table exists
		/// </summary>
		[SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
		private static void EnsureChangeHolderTable(SqlConnection conn, SqlTransaction trans,
			SqlDependencyParams dependencyParams)
		{
			var cmdText = string.Format(CultureInfo.InvariantCulture,
				@"IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [object_id] = OBJECT_ID('{0}'))
					BEGIN
						CREATE TABLE {0} (
								[ObjectId] [int] NOT NULL,
								[ObjectName] [nvarchar](128) NOT NULL,
								[LastInsertDate] [datetimeoffset](7) NULL,
								[LastUpdateDate] [datetimeoffset](7) NULL,
								[LastDeleteDate] [datetimeoffset](7) NULL,
							CONSTRAINT [PK_{1}] PRIMARY KEY CLUSTERED
								([ObjectId] ASC)
							WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
						) ON [PRIMARY];
						CREATE UNIQUE NONCLUSTERED INDEX [IX_{1}] ON {0}
								([ObjectName] ASC)
							WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
					END",
				dependencyParams.Options.ChangeHolderTable,
				dependencyParams.Options.ChangeHolderTable.Name);

			// Execute
			using (var cmd = new SqlCommand(cmdText, conn))
			{
				// Process
				EnsureOpenConnection(conn);

				// Properties
				cmd.CommandType = CommandType.Text;
				cmd.Transaction = trans;

				// Execute
				cmd.ExecuteNonQuery();
			}

			// Increment ref count
			IncrementRefCount(dependencyParams.Options.ChangeHolderTable);
		}

		/// <summary>
		/// Deletes changes holder table
		/// </summary>
		[SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
		private static void DeleteChangeHolderTable(SqlConnection conn, SqlTransaction trans,
			SqlDependencyParams dependencyParams)
		{
			var refCount = DecrementRefCount(dependencyParams.Options.ChangeHolderTable);
			if (refCount <= 0)
			{
				var cmdText = string.Format(CultureInfo.InvariantCulture,
					@"IF EXISTS (SELECT 1 FROM [sys].[tables] WHERE [object_id] = OBJECT_ID('{0}'))
						DROP TABLE {0};",
					dependencyParams.Options.ChangeHolderTable);

				// Execute
				using (var cmd = new SqlCommand(cmdText, conn))
				{
					// Process
					EnsureOpenConnection(conn);

					// Properties
					cmd.CommandType = CommandType.Text;
					cmd.Transaction = trans;

					// Execute
					cmd.ExecuteNonQuery();
				}
			}
		}

		/// <summary>
		/// Ensures table triggers
		/// </summary>
		[SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
		private static void EnsureTableTriggers(SqlConnection conn, SqlTransaction trans,
			SqlDependencyParams dependencyParams)
		{
			// Format for each table
			var cmdTextFormat =
				@"CREATE TRIGGER {0}
					   ON {1}
					   AFTER INSERT, UPDATE, DELETE
					AS
					BEGIN
						SET NOCOUNT ON;

						-- Insert a record in changes table for this table
						IF NOT EXISTS (SELECT 1 FROM {2} WHERE [ObjectId] = OBJECT_ID('{1}'))
							INSERT INTO {2} VALUES (OBJECT_ID('{1}'), '{1}', NULL, NULL, NULL);

						-- Update the record appropriately
						IF EXISTS (SELECT 1 FROM INSERTED)
							IF EXISTS (SELECT 1 FROM DELETED)
								UPDATE {2} SET [LastUpdateDate] = SYSDATETIMEOFFSET() WHERE ([ObjectId] = OBJECT_ID('{1}'));
							ELSE
								UPDATE {2} SET [LastInsertDate] = SYSDATETIMEOFFSET() WHERE ([ObjectId] = OBJECT_ID('{1}'));
						ELSE
							IF EXISTS (SELECT 1 FROM DELETED)
								UPDATE {2} SET [LastDeleteDate] = SYSDATETIMEOFFSET() WHERE ([ObjectId] = OBJECT_ID('{1}'));
					END";

			// Create
			dependencyParams.Tables.ForEach(t =>
			{
				var trigger = dependencyParams.Options.GetTableTrigger(t);
				if (!TriggerExists(conn, trans, trigger))
				{
					var cmdText = string.Format(CultureInfo.InvariantCulture, cmdTextFormat,
						trigger, t, dependencyParams.Options.ChangeHolderTable);

					// Execute
					using (var cmd = new SqlCommand(cmdText, conn))
					{
						// Process
						EnsureOpenConnection(conn);

						// Properties
						cmd.CommandType = CommandType.Text;
						cmd.Transaction = trans;

						// Execute
						cmd.ExecuteNonQuery();
					}
				}
			});

			// Increment ref counts
			dependencyParams.Tables.ForEach(t => IncrementRefCount(dependencyParams.Options.GetTableTrigger(t)));
		}

		/// <summary>
		/// Deletes table triggers
		/// </summary>
		[SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
		private static void DeleteTableTriggers(SqlConnection conn, SqlTransaction trans,
			SqlDependencyParams dependencyParams)
		{
			// Delete for all
			dependencyParams.Tables.ForEach(t =>
			{
				// Trigger
				var tableTrigger = dependencyParams.Options.GetTableTrigger(t);

				// Decrement ref count
				var refCount = DecrementRefCount(tableTrigger);

				// Delete if ref count is 0
				if (refCount <= 0)
				{
					// Delete
					var cmdText = string.Format(CultureInfo.InvariantCulture,
						@"IF EXISTS (SELECT 1 FROM [sys].[triggers] WHERE [object_id] = OBJECT_ID('{0}'))
							DROP TRIGGER {0};",
						tableTrigger);

					// Execute
					using (var cmd = new SqlCommand(cmdText, conn))
					{
						// Process
						EnsureOpenConnection(conn);

						// Properties
						cmd.CommandType = CommandType.Text;
						cmd.Transaction = trans;

						// Execute
						cmd.ExecuteNonQuery();
					}
				}
			});
		}
		#endregion
	}
}
