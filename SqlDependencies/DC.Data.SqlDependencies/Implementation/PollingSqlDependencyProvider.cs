using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Threading;

namespace DC.Data.SqlDependencies
{
	/// <summary>
	/// Internal provider based on periodic polling
	/// </summary>
	internal sealed class PollingSqlDependencyProvider : SqlDependencyProvider
	{
		public readonly TimeSpan DefaultPollingPeriod = TimeSpan.FromSeconds(5.0D);
		private readonly TimeSpan _pollingPeriod;
		private Thread _thread;
		private ManualResetEvent _exitThread = new ManualResetEvent(false);

		#region Construction & Disposal
		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="dependencyParams">Notification parameters to use</param>
		/// <param name="pollingPeriod">Polling period</param>
		public PollingSqlDependencyProvider(SqlDependencyParams dependencyParams, TimeSpan? pollingPeriod)
			: base(dependencyParams)
		{
			if (pollingPeriod.HasValue
				&& (pollingPeriod.Value.TotalMilliseconds < 0))
			{
				throw new ArgumentNullException(nameof(pollingPeriod));
			}
			_pollingPeriod = pollingPeriod ?? DefaultPollingPeriod;
		}

		/// <summary>
		/// Disposes this instance
		/// </summary>
		/// <param name="disposing">True if being called from IDisposable.Dispose. False if being called from the finalizer</param>
		protected override void Dispose(bool disposing)
		{
			// Sync
			using (new MonitorLock(SyncLock))
			{
				// Base
				base.Dispose(disposing);

				// Stop
				StopInternal();

				// Dispose the event
				_exitThread.Dispose();
				_exitThread = null;
			}
		}
		#endregion

		#region Implementation
		/// <summary>
		/// Starts monitoring for changes
		/// </summary>
		protected override void StartInternal()
		{
			// Create thread
			_thread = new Thread(StartPolling)
			{
				IsBackground = true,
				Priority = ThreadPriority.BelowNormal
			};

			// Start
			_exitThread.Reset();
			_thread.Start();
		}

		/// <summary>
		/// Stops monitoring for changes
		/// </summary>
		protected override void StopInternal()
		{
			// Exit
			_exitThread.Set();

			// Stop the thread
			if ((_thread != null) && _thread.IsAlive)
			{
				// Wait for 2 seconds
				_thread.Join(2000);

				// Kill if still running
				if (_thread.IsAlive)
				{
					_thread.Abort();
				}
			}
		}

		/// <summary>
		/// Cleans the used resources
		/// </summary>
		protected override void Cleanup()
		{
			// Set Null
			_thread = null;

			// Base
			base.Cleanup();
		}
		#endregion

		#region Polling Thread Method
		/// <summary>
		/// Polls for changes
		/// </summary>
		[SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
		private void StartPolling()
		{
			try
			{
				while (true)
				{
					try
					{
						// Poll
						Poll();

						// Sleep
						var waitResult = _exitThread.WaitOne(_pollingPeriod);
						if (waitResult)
						{
							// Exit signalled
							return;
						}
					}
					catch (ThreadAbortException)
					{
						Thread.ResetAbort();
						return;
					}
					catch (Exception ex)
					{
						if (!FireExceptionEvent(ex))
						{
							return;
						}
					}
				}
			}
			finally
			{
				// Cleanup when the thread exits
				Cleanup();
			}
		}
		#endregion

		#region Poll
		private readonly Dictionary<SqlDependencyQualifiedObjectName, Tuple<DateTimeOffset?, DateTimeOffset?, DateTimeOffset?>> _previousChanges = new Dictionary<SqlDependencyQualifiedObjectName, Tuple<DateTimeOffset?, DateTimeOffset?, DateTimeOffset?>>();

		/// <summary>
		/// Checks the db for changes
		/// </summary>
		[SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity")]
		[SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
		private void Poll()
		{
			// Current values
			var currentChanges = new Dictionary<SqlDependencyQualifiedObjectName, SqlDependencyMonitoredChanges>();

			// Open connection
			using (var conn = new SqlConnection(Parameters.ConnectionString))
			{
				// Ensure open
				Helper.EnsureOpenConnection(conn);

				// Command Text
				var cmdText = string.Format(CultureInfo.InvariantCulture,
					@"SELECT * FROM {0} WHERE [ObjectId] IN ({1})",
					Parameters.Options.ChangeHolderTable,
					string.Join(", ", Parameters.Tables.Select(t => string.Format(CultureInfo.InvariantCulture,
						@"OBJECT_ID('{0}')", t))));

				// Execute
				using (var cmd = new SqlCommand(cmdText, conn))
				{
					cmd.CommandType = CommandType.Text;
					using (var dataReader = cmd.ExecuteReader())
					{
						while (dataReader.Read())
						{
							// Get object details
							var objectName = dataReader["ObjectName"] as string;
							var table = Parameters.Tables.SingleOrDefault(t => t.FullName.Equals(objectName, StringComparison.OrdinalIgnoreCase));
							if (table == null)
							{
								continue;
							}

							// Get current dates
							var lastInsertDate = Helper.GetNullableFieldValue<DateTimeOffset>(dataReader, "LastInsertDate");
							var lastUpdateDate = Helper.GetNullableFieldValue<DateTimeOffset>(dataReader, "LastUpdateDate");
							var lastDeleteDate = Helper.GetNullableFieldValue<DateTimeOffset>(dataReader, "LastDeleteDate");

							// Previous dates
							Tuple<DateTimeOffset?, DateTimeOffset?, DateTimeOffset?> prevDates;
							if (!_previousChanges.TryGetValue(table, out prevDates))
							{
								// ReSharper disable once RedundantAssignment
								prevDates = null;
							}

							// Set Changes to none
							currentChanges[table] = SqlDependencyMonitoredChanges.None;

							// Check what has changed
							if (((Parameters.MonitoredChanges & SqlDependencyMonitoredChanges.Insert) == SqlDependencyMonitoredChanges.Insert)
								&& lastInsertDate.HasValue)
							{
								if ((prevDates?.Item1 == null)
									|| (lastInsertDate.Value > prevDates.Item1.Value))
								{
									currentChanges[table] |= SqlDependencyMonitoredChanges.Insert;
								}
							}
							if (((Parameters.MonitoredChanges & SqlDependencyMonitoredChanges.Update) == SqlDependencyMonitoredChanges.Update)
								&& lastUpdateDate.HasValue)
							{
								if ((prevDates?.Item2 == null)
									|| (lastUpdateDate.Value > prevDates.Item2.Value))
								{
									currentChanges[table] |= SqlDependencyMonitoredChanges.Update;
								}
							}
							if (((Parameters.MonitoredChanges & SqlDependencyMonitoredChanges.Delete) == SqlDependencyMonitoredChanges.Delete)
								&& lastDeleteDate.HasValue)
							{
								if ((prevDates?.Item3 == null)
									|| (lastDeleteDate.Value > prevDates.Item3.Value))
								{
									currentChanges[table] |= SqlDependencyMonitoredChanges.Delete;
								}
							}

							// Update previous changes data
							_previousChanges[table] = new Tuple<DateTimeOffset?, DateTimeOffset?, DateTimeOffset?>(lastInsertDate, lastUpdateDate, lastDeleteDate);
						}
					}
				}
			}

			// Fire changes
			currentChanges
				.Where(kv => kv.Value != SqlDependencyMonitoredChanges.None)
				.ToList().ForEach(kv => FireTableChangedEvent(kv.Key, kv.Value));
		}
		#endregion
	}
}
