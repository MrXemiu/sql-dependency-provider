using System;

namespace DC.Data.SqlDependencies
{
	/// <summary>
	/// Sql Notification Provider interface
	/// </summary>
	public interface ISqlDependencyProvider : IDisposable
	{
		/// <summary>
		/// Indicates whether the monitoring is started
		/// </summary>
		bool IsStarted { get; }

		/// <summary>
		/// Starts monitoring for changes
		/// </summary>
		void StartService();

		/// <summary>
		/// Stops monitoring for changes
		/// </summary>
		void StopService();

		/// <summary>
		/// Raised when a table is changed
		/// </summary>
		event EventHandler<SqlDependencyTableChangedEventArgs> OnTableChanged;

		/// <summary>
		/// Raised when an unhandled fatal exception is raised in the provider
		/// </summary>
		event EventHandler<SqlDependencyFatalExceptionEventArgs> OnFatalException;

		/// <summary>
		/// Raised when an exception is raised in the provider
		/// The subscriber can choose to ignore the exception and continue
		/// The default behavior is to continue
		/// </summary>
		event EventHandler<SqlDependencyExceptionEventArgs> OnException;
	}
}
