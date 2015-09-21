using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Threading;
using DC.Data.SqlDependencies;

namespace DC.Data.Notifications
{
	/// <summary>
	/// Internal provider based on service broker/.NET Sql Dependency
	/// </summary>
	internal sealed class BrokerSqlDependencyProvider : SqlDependencyProvider
	{
		public const string DefaultQueueName = null;
		private long _exiting;

		#region Construction & Disposal
		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="dependencyParams">Notification parameters to use</param>
		public BrokerSqlDependencyProvider(SqlDependencyParams dependencyParams)
			: base(dependencyParams)
		{
		}
		#endregion

		#region Implementation
		private readonly Dictionary<SqlDependencyQualifiedObjectName, SqlDependency> _dependencies = new Dictionary<SqlDependencyQualifiedObjectName, SqlDependency>();

		/// <summary>
		/// Starts monitoring for changes
		/// </summary>
		protected override void StartInternal()
		{
			// Flag exiting
			Interlocked.Exchange(ref _exiting, 0);

			// Process
			using (var conn = new SqlConnection(Parameters.ConnectionString))
			{
				Helper.EnsureOpenConnection(conn);
				Parameters.Tables.ForEach(t => StartDependency(t, conn));
			}
		}

		/// <summary>
		/// Stops monitoring for changes
		/// </summary>
		protected override void StopInternal()
		{
			// No specific functionality required.
			// Cleanup will be called from base.Stop
		}

		/// <summary>
		/// Cleans the used resources
		/// </summary>
		protected override void Cleanup()
		{
			// Flag exiting
			Interlocked.Exchange(ref _exiting, 1);

			// Kill all dependencies
			using (new MonitorLock(_dependencies))
			{
				_dependencies.ToList().ForEach(kv => StopDependency(kv.Value, kv.Key));
				_dependencies.Clear();
			}

			// Base
			base.Cleanup();
		}
		#endregion

		#region Sql Dependency
		/// <summary>
		/// Starts dependency on a table
		/// </summary>
		[SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
		[SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
		private void StartDependency(SqlDependencyQualifiedObjectName table, SqlConnection conn = null)
		{
			// Execute
			var newConnection = conn == null;
			conn = conn ?? new SqlConnection(Parameters.ConnectionString);
			try
			{
				var cmdText = string.Format(CultureInfo.InvariantCulture,
					@"SELECT [ObjectId], [ObjectName], [LastInsertDate], [LastUpdateDate], [LastDeleteDate]
						FROM {0}
						WHERE [ObjectName] = '{1}'",
					Parameters.Options.ChangeHolderTable.AlternateFullName, table);

				using (var cmd = new SqlCommand(cmdText, conn))
				{
					// Ensure connection
					Helper.EnsureOpenConnection(conn);

					// Properties
					cmd.CommandType = CommandType.Text;

					// Set cmd notification to null
					cmd.Notification = null;

					// Dependency
					var sqlDependency = new SqlDependency(cmd);
					sqlDependency.OnChange += OnSqlDependencyChanged;

					// Execute reader
					using (var dataTable = new DataTable())
					{
						dataTable.Locale = CultureInfo.InvariantCulture;
						using (var dataAdapter = new SqlDataAdapter(cmd))
						{
							dataAdapter.Fill(dataTable);
						}
					}

					// Add
					using (new MonitorLock(_dependencies))
					{
						_dependencies[table] = sqlDependency;
					}
				}
			}
			finally
			{
				if (newConnection)
				{
					conn.Dispose();
				}
			}
		}

		/// <summary>
		/// Stops dependency on a table
		/// </summary>
		private SqlDependencyQualifiedObjectName StopDependency(SqlDependency dependency, SqlDependencyQualifiedObjectName table = null)
		{
			dependency.OnChange -= OnSqlDependencyChanged;
			using (new MonitorLock(_dependencies))
			{
				if (table == null)
				{
					table = _dependencies.Where(kv => kv.Value == dependency).Select(kv => kv.Key).SingleOrDefault();
				}
				if (table != null)
				{
					_dependencies.Remove(table);
				}
			}
			return table;
		}

		/// <summary>
		/// Invoked by SqlDependency
		/// </summary>
		/// <param name="sender">SqlDependency</param>
		/// <param name="e">Event arguments</param>
		private void OnSqlDependencyChanged(object sender, SqlNotificationEventArgs e)
		{
			// Stop this dependency
			var table = StopDependency(sender as SqlDependency);

			// If invalid notification during subscribr phase, return
			if ((e.Info == SqlNotificationInfo.Invalid) && (e.Type == SqlNotificationType.Subscribe))
			{
				// Dont proceed further
				return;
			}

			// Send the changes
			if ((e.Info != SqlNotificationInfo.Invalid) && (e.Type == SqlNotificationType.Change)
				 && (table != null))
			{
				if (((Parameters.MonitoredChanges & SqlDependencyMonitoredChanges.Insert) == SqlDependencyMonitoredChanges.Insert)
					&& (e.Info == SqlNotificationInfo.Insert))
				{
					FireTableChangedEvent(table, SqlDependencyMonitoredChanges.Insert);
				}
				if (((Parameters.MonitoredChanges & SqlDependencyMonitoredChanges.Update) == SqlDependencyMonitoredChanges.Update)
					&& (e.Info == SqlNotificationInfo.Update))
				{
					FireTableChangedEvent(table, SqlDependencyMonitoredChanges.Update);
				}
				if (((Parameters.MonitoredChanges & SqlDependencyMonitoredChanges.Delete) == SqlDependencyMonitoredChanges.Delete)
					&& (e.Info == SqlNotificationInfo.Delete))
				{
					FireTableChangedEvent(table, SqlDependencyMonitoredChanges.Delete);
				}
			}

			// Check if exiting
			if (Interlocked.Read(ref _exiting) == 1)
			{
				return;
			}

			// Start a new dependency
			if (table != null)
			{
				StartDependency(table);
			}
		}
		#endregion
	}
}
