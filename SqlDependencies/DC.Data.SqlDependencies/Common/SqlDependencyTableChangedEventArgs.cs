using System;

namespace DC.Data.SqlDependencies
{
	/// <summary>
	/// Table changed event arguments
	/// </summary>
	public sealed class SqlDependencyTableChangedEventArgs : EventArgs
	{
		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="table">Changed table</param>
		/// <param name="changes">Changes</param>
		public SqlDependencyTableChangedEventArgs(SqlDependencyQualifiedObjectName table, SqlDependencyMonitoredChanges changes)
		{
			if (table?.Name == null)
			{
				throw new ArgumentNullException(nameof(table));
			}
			if (changes == SqlDependencyMonitoredChanges.None)
			{
				throw new ArgumentNullException(nameof(changes));
			}

			Table = table;
			Changes = changes;
		}

		/// <summary>
		/// Table that is changed
		/// </summary>
		public SqlDependencyQualifiedObjectName Table
		{
			get;
		}

		/// <summary>
		/// Changes
		/// </summary>
		public SqlDependencyMonitoredChanges Changes
		{
			get;
		}
	}
}
