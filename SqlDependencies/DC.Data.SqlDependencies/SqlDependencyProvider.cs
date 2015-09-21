using System;
using System.Collections.Concurrent;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using DC.Data.Notifications;
using DC.Data.SqlDependencies.Properties;

namespace DC.Data.SqlDependencies
{
	/// <summary>
	/// Sql dependency provider base implementation
	/// </summary>
	public abstract class SqlDependencyProvider : ISqlDependencyProvider
	{
		#region Dependency Start/Stop
		private static readonly ConcurrentDictionary<string, bool> _sqlDependencyStartStatus = new ConcurrentDictionary<string, bool>(StringComparer.OrdinalIgnoreCase);

		/// <summary>Starts the listener for receiving dependency change notifications from the instance of SQL Server specified by the connection string using the specified SQL Server Service Broker queue.</summary>
		/// <returns>true if the listener initialized successfully; false if a compatible listener already exists.</returns>
		/// <param name="connectionString">The connection string for the instance of SQL Server from which to obtain change notifications.</param>
		/// <exception cref="T:System.ArgumentNullException">The <paramref name="connectionString" /> parameter is NULL.</exception>
		/// <exception cref="T:System.InvalidOperationException">The <paramref name="connectionString" /> parameter is the same as a previous call to this method, but the parameters are different.The method was called from within the CLR.</exception>
		/// <exception cref="T:System.Security.SecurityException">The caller does not have the required <see cref="T:System.Data.SqlClient.SqlClientPermission" /> code access security (CAS) permission.</exception>
		/// <exception cref="T:System.Data.SqlClient.SqlException">A subsequent call to the method has been made with an equivalent <paramref name="connectionString" /> parameter but a different user, or a user that does not default to the same schema.Also, any underlying SqlClient exceptions.</exception>

		public static bool StartServiceBroker(string connectionString)
		{
			return StartServiceBroker(connectionString, null);
		}

		/// <summary>Starts the listener for receiving dependency change notifications from the instance of SQL Server specified by the connection string using the specified SQL Server Service Broker queue.</summary>
		/// <returns>true if the listener initialized successfully; false if a compatible listener already exists.</returns>
		/// <param name="connectionString">The connection string for the instance of SQL Server from which to obtain change notifications.</param>
		/// <param name="queue">An existing SQL Server Service Broker queue to be used. If null, the default queue is used.</param>
		/// <exception cref="T:System.ArgumentNullException">The <paramref name="connectionString" /> parameter is NULL.</exception>
		/// <exception cref="T:System.InvalidOperationException">The <paramref name="connectionString" /> parameter is the same as a previous call to this method, but the parameters are different.The method was called from within the CLR.</exception>
		/// <exception cref="T:System.Security.SecurityException">The caller does not have the required <see cref="T:System.Data.SqlClient.SqlClientPermission" /> code access security (CAS) permission.</exception>
		/// <exception cref="T:System.Data.SqlClient.SqlException">A subsequent call to the method has been made with an equivalent <paramref name="connectionString" /> parameter but a different user, or a user that does not default to the same schema.Also, any underlying SqlClient exceptions.</exception>

		public static bool StartServiceBroker(string connectionString, string queue)
		{
			// Validate
			if (string.IsNullOrWhiteSpace(connectionString))
			{
				throw new ArgumentNullException(nameof(connectionString));
			}
			connectionString = connectionString.Trim();

			// Start if doesnt exist
			var queueName = (queue ?? string.Empty).Trim();
			var statusKey = (connectionString + "#" + queueName).ToUpperInvariant();
			return _sqlDependencyStartStatus.GetOrAdd(statusKey, k => SqlDependency.Start(connectionString, queue));
		}

		/// <summary>Stops a listener for a connection specified in a previous Overload:System.Data.SqlClient.SqlDependency.Start call.</summary>
		/// <returns>true if the listener was completely stopped; false if the <see cref="T:System.AppDomain" /> was unbound from the listener, but there is at least one other <see cref="T:System.AppDomain" /> using the same listener.</returns>
		/// <param name="connectionString">Connection string for the instance of SQL Server that was used in a previous <see cref="M:System.Data.SqlClient.SqlDependency.Start(System.String,System.String)" /> call.</param>
		/// <exception cref="T:System.ArgumentNullException">The <paramref name="connectionString" /> parameter is NULL. </exception>
		/// <exception cref="T:System.InvalidOperationException">The method was called from within SQLCLR.</exception>
		/// <exception cref="T:System.Security.SecurityException">The caller does not have the required <see cref="T:System.Data.SqlClient.SqlClientPermission" /> code access security (CAS) permission.</exception>
		/// <exception cref="T:System.Data.SqlClient.SqlException">And underlying SqlClient exception occurred.</exception>
		public static bool StopServiceBroker(string connectionString)
		{
			return StopServiceBroker(connectionString, null);
		}

		/// <summary>Stops a listener for a connection specified in a previous Overload:System.Data.SqlClient.SqlDependency.Start call.</summary>
		/// <returns>true if the listener was completely stopped; false if the <see cref="T:System.AppDomain" /> was unbound from the listener, but there is at least one other <see cref="T:System.AppDomain" /> using the same listener.</returns>
		/// <param name="connectionString">Connection string for the instance of SQL Server that was used in a previous <see cref="M:System.Data.SqlClient.SqlDependency.Start(System.String,System.String)" /> call.</param>
		/// <param name="queue">The SQL Server Service Broker queue that was used in a previous <see cref="M:System.Data.SqlClient.SqlDependency.Start(System.String,System.String)" /> call.</param>
		/// <exception cref="T:System.ArgumentNullException">The <paramref name="connectionString" /> parameter is NULL. </exception>
		/// <exception cref="T:System.InvalidOperationException">The method was called from within SQLCLR.</exception>
		/// <exception cref="T:System.Security.SecurityException">The caller does not have the required <see cref="T:System.Data.SqlClient.SqlClientPermission" /> code access security (CAS) permission.</exception>
		/// <exception cref="T:System.Data.SqlClient.SqlException">And underlying SqlClient exception occurred.</exception>
		public static bool StopServiceBroker(string connectionString, string queue)
		{
			// Validate
			if (string.IsNullOrWhiteSpace(connectionString))
			{
				throw new ArgumentNullException(nameof(connectionString));
			}
			connectionString = connectionString.Trim();

			// Stop if exists
			var queueName = (queue ?? string.Empty).Trim();
			var statusKey = (connectionString + "#" + queueName).ToUpperInvariant();
			bool exists;
			if (_sqlDependencyStartStatus.TryRemove(statusKey, out exists))
			{
				return SqlDependency.Stop(connectionString, queue);
			}
			return false;
		}

		/// <summary>
		/// Whether the broker is enabled
		/// </summary>
		/// <param name="connectionString">Connection string for the instance of SQL Server that was used in a previous <see cref="M:System.Data.SqlClient.SqlDependency.Start(System.String,System.String)" /> call.</param>
		/// <returns>True if broker is enabled. False otherwise</returns>
		public static bool IsServiceBrokerEnabled(string connectionString)
		{
			return IsServiceBrokerEnabled(connectionString, null);
		}

		/// <summary>
		/// Whether the broker is enabled
		/// </summary>
		/// <param name="connectionString">Connection string for the instance of SQL Server that was used in a previous <see cref="M:System.Data.SqlClient.SqlDependency.Start(System.String,System.String)" /> call.</param>
		/// <param name="queue">The SQL Server Service Broker queue that was used in a previous <see cref="M:System.Data.SqlClient.SqlDependency.Start(System.String,System.String)" /> call.</param>
		/// <returns>True if broker is enabled. False otherwise</returns>
		public static bool IsServiceBrokerEnabled(string connectionString, string queue)
		{
			// Validate
			if (string.IsNullOrWhiteSpace(connectionString))
			{
				throw new ArgumentNullException(nameof(connectionString));
			}
			connectionString = connectionString.Trim();

			// Check
			var queueName = (queue ?? string.Empty).Trim();
			var statusKey = (connectionString + "#" + queueName).ToUpperInvariant();
			return _sqlDependencyStartStatus.ContainsKey(statusKey);
		}
		#endregion

		#region Factory
		/// <summary>
		/// Creates a provider implementation
		/// </summary>
		/// <param name="dependencyParams">Dependency monitoring parameters</param>
		/// <returns>Sql Dependency Provider instance</returns>
		public static SqlDependencyProvider Create(SqlDependencyParams dependencyParams)
		{
			return Create(dependencyParams, SqlDependencyProviderType.Auto, null, null);
		}

		/// <summary>
		/// Creates a provider implementation
		/// </summary>
		/// <param name="dependencyParams">Dependency monitoring parameters</param>
		/// <param name="providerType">Type of the provider to create</param>
		/// <returns>Sql Dependency Provider instance</returns>
		public static SqlDependencyProvider Create(SqlDependencyParams dependencyParams,
			SqlDependencyProviderType providerType)
		{
			return Create(dependencyParams, providerType, null, null);
		}

		/// <summary>
		/// Creates a provider implementation
		/// </summary>
		/// <param name="dependencyParams">Dependency monitoring parameters</param>
		/// <param name="providerType">Type of the provider to create</param>
		/// <param name="serviceBrokerQueue">Queue name for the service broker implementation</param>
		/// <returns>Sql Dependency Provider instance</returns>
		public static SqlDependencyProvider Create(SqlDependencyParams dependencyParams,
			SqlDependencyProviderType providerType, string serviceBrokerQueue)
		{
			return Create(dependencyParams, providerType, serviceBrokerQueue, null);
		}

		/// <summary>
		/// Creates a provider implementation
		/// </summary>
		/// <param name="dependencyParams">Dependency monitoring parameters</param>
		/// <param name="providerType">Type of the provider to create</param>
		/// <param name="serviceBrokerQueue">Queue name for the service broker implementation</param>
		/// <param name="periodForPollingProvider">Period for polling implementation</param>
		/// <returns>Sql Dependency Provider instance</returns>
		public static SqlDependencyProvider Create(SqlDependencyParams dependencyParams,
			SqlDependencyProviderType providerType,
			string serviceBrokerQueue, TimeSpan? periodForPollingProvider)
		{
			// Validate
			if (dependencyParams == null)
			{
				throw new ArgumentNullException(nameof(dependencyParams));
			}
			if (periodForPollingProvider.HasValue
				&& (periodForPollingProvider.Value.TotalMilliseconds < 0))
			{
				throw new ArgumentNullException(nameof(periodForPollingProvider));
			}
			serviceBrokerQueue = string.IsNullOrWhiteSpace(serviceBrokerQueue) ? null : serviceBrokerQueue.Trim();

			// Make a copy & validate
			dependencyParams = dependencyParams.Clone() as SqlDependencyParams;
			if (dependencyParams == null)
			{
				throw new ArgumentNullException(nameof(dependencyParams));
			}
			dependencyParams.ValidateAndAdjust();

			// Check service broker availability
			if ((providerType == SqlDependencyProviderType.Polling)
				|| ((providerType == SqlDependencyProviderType.Auto)
					&& !IsServiceBrokerEnabled(dependencyParams.ConnectionString, serviceBrokerQueue)))
			{
				return new PollingSqlDependencyProvider(dependencyParams, periodForPollingProvider);
			}
			return new BrokerSqlDependencyProvider(dependencyParams);
		}
		#endregion

		#region Construction & Disposal
		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="dependencyParams">Notification parameters to use</param>
		protected SqlDependencyProvider(SqlDependencyParams dependencyParams)
		{
			if (dependencyParams == null)
			{
				throw new ArgumentNullException(nameof(dependencyParams));
			}

			Parameters = dependencyParams;
			SyncLock = new object();
		}

		/// <summary>
		/// Finalizer
		/// </summary>
		~SqlDependencyProvider()
		{
			Dispose(false);
		}

		/// <summary>
		/// Disposes this instance
		/// </summary>
		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		/// <summary>
		/// Disposes this instance
		/// </summary>
		/// <param name="disposing">True if being called from IDisposable.Dispose. False if being called from the finalizer</param>
		protected virtual void Dispose(bool disposing)
		{
			using (new MonitorLock(SyncLock))
			{
				// Only if not already disposed
				if (!IsDisposed)
				{
					// Mark as disposed
					IsDisposed = true;

					// Stop
					StopService();
				}
			}
		}
		#endregion

		#region Properties
		/// <summary>
		/// Notification parameters to use
		/// </summary>
		protected SqlDependencyParams Parameters
		{
			get;
		}

		/// <summary>
		/// Synchronization lock
		/// </summary>
		protected object SyncLock
		{
			get;
		}

		private bool _isDisposed;
		/// <summary>
		/// Whether this object is disposed
		/// </summary>
		public bool IsDisposed
		{
			get
			{
				using (new MonitorLock(SyncLock))
				{
					return _isDisposed;
				}
			}
			private set
			{
				using (new MonitorLock(SyncLock))
				{
					_isDisposed = value;
				}
			}
		}
		#endregion

		#region Control (Start/Stop)
		private bool _isStarted;
		/// <summary>
		/// Indicates whether the monitoring is started
		/// </summary>
		public bool IsStarted
		{
			get
			{
				using (new MonitorLock(SyncLock))
				{
					return _isStarted;
				}
			}
			private set
			{
				using (new MonitorLock(SyncLock))
				{
					_isStarted = value;
				}
			}
		}

		/// <summary>
		/// Starts monitoring for changes
		/// </summary>
		public void StartService()
		{
			using (new MonitorLock(SyncLock))
			{
				// Check state
				if (IsStarted)
				{
#if USETRANSLATIONS
					throw new InvalidOperationException(Translations.Default["SText_SqlDependencyProviderAlreadyStarted"]);
#else
					throw new InvalidOperationException(Resources.SText_SqlDependencyProviderAlreadyStarted);
#endif
				}

				try
				{
					// Create Sql Objects
					Helper.CreateSqlObjects(Parameters);

					// Start
					StartInternal();

					// Set Flag
					IsStarted = true;
				}
				catch (Exception ex)
				{
					// Unhandled exception
					FireFatalExceptionEvent(ex);

					// Call Stop for cleanup in case of exception
					Cleanup();

					// Rethrow
					throw;
				}
			}
		}

		/// <summary>
		/// Stops monitoring for changes
		/// </summary>
		public void StopService()
		{
			using (new MonitorLock(SyncLock))
			{
				// Check state
				if (IsStarted)
				{
					try
					{
						// Stop
						StopInternal();
					}
					catch (Exception ex)
					{
						// Unhandled exception
						FireFatalExceptionEvent(ex);

						// Rethrow
						throw;
					}
					finally
					{
						// In case we could not stop (due to an exception), we do not know about the internal state.
						// So mark as stopped anyway
						IsStarted = false;

						// Cleanup
						Cleanup();
					}
				}
			}
		}

		/// <summary>
		/// Cleans the used resources
		/// </summary>
		[SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
		protected virtual void Cleanup()
		{
			// Delete Sql Objects
			try { Helper.DeleteSqlObjects(Parameters); }
			catch (Exception ex) { FireExceptionEvent(ex); }
		}
		#endregion

		#region Abstracts - Implementation
		/// <summary>
		/// Starts monitoring for changes
		/// </summary>
		protected abstract void StartInternal();

		/// <summary>
		/// Stops monitoring for changes
		/// </summary>
		protected abstract void StopInternal();
		#endregion

		#region Events
		/// <summary>
		/// Raised when a table is changed
		/// </summary>
		public event EventHandler<SqlDependencyTableChangedEventArgs> OnTableChanged;

		/// <summary>
		/// Fires the changed event to the subscribers
		/// </summary>
		/// <param name="table">Changed table</param>
		/// <param name="changes">Changes happened to the table</param>
		[SuppressMessage("Microsoft.Design", "CA1030:UseEventsWhereAppropriate")]
		protected void FireTableChangedEvent(SqlDependencyQualifiedObjectName table, SqlDependencyMonitoredChanges changes)
		{
			OnTableChanged?.Invoke(this, new SqlDependencyTableChangedEventArgs(table, changes));
		}

		/// <summary>
		/// Raised when an unhandled fatal exception is raised in the provider
		/// </summary>
		public event EventHandler<SqlDependencyFatalExceptionEventArgs> OnFatalException;

		/// <summary>
		/// Fires the fatal exception event to the subscribers
		/// </summary>
		/// <param name="exception">Exception</param>
		[SuppressMessage("Microsoft.Design", "CA1030:UseEventsWhereAppropriate")]
		protected void FireFatalExceptionEvent(Exception exception)
		{
			OnFatalException?.Invoke(this, new SqlDependencyFatalExceptionEventArgs(exception));
		}

		/// <summary>
		/// Raised when an exception is raised in the provider
		/// The subscriber can choose to ignore the exception and continue
		/// The default behavior is to continue
		/// </summary>
		public event EventHandler<SqlDependencyExceptionEventArgs> OnException;

		/// <summary>
		/// Fires the exception event to the subscribers
		/// The subscriber can choose to ignore the exception and continue
		/// The default behavior is to continue
		/// </summary>
		/// <param name="exception">Exception</param>
		/// <returns>True to ignore this exception and continue. False to exit.</returns>
		[SuppressMessage("Microsoft.Design", "CA1030:UseEventsWhereAppropriate")]
		protected bool FireExceptionEvent(Exception exception)
		{
			return FireExceptionEvent(exception, true);
		}

		/// <summary>
		/// Fires the exception event to the subscribers
		/// The subscriber can choose to ignore the exception and continue
		/// The default behavior is to continue
		/// </summary>
		/// <param name="exception">Exception</param>
		/// <param name="ignore">Whether to ignore this exception and continue</param>
		/// <returns>True to ignore this exception and continue. False to exit.</returns>
		[SuppressMessage("Microsoft.Design", "CA1030:UseEventsWhereAppropriate")]
		protected bool FireExceptionEvent(Exception exception, bool ignore)
		{
			// Invoke
			var eventArgs = new SqlDependencyExceptionEventArgs(exception, ignore);
			OnException?.Invoke(this, eventArgs);

			// If not ignored, fire fatal exception
			if (!eventArgs.Ignore)
			{
				FireFatalExceptionEvent(exception);
			}

			// Return
			return eventArgs.Ignore;
		}
		#endregion
	}
}
