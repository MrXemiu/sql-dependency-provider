using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using DC.Data.SqlDependencies.Properties;

namespace DC.Data.SqlDependencies
{
	/// <summary>
	/// Parameters required for Sql Notifications
	/// </summary>
	[Serializable]
	public sealed class SqlDependencyParams : ICloneable
	{
		#region Properties
		private string _connectionString;
		/// <summary>
		/// Connection string
		/// </summary>
		public string ConnectionString
		{
			get { return _connectionString; }
			set { _connectionString = string.IsNullOrWhiteSpace(value) ? null : value.Trim(); }
		}

		/// <summary>
		/// Tables
		/// </summary>
		[SuppressMessage("Microsoft.Design", "CA1002:DoNotExposeGenericLists")]
		public List<SqlDependencyQualifiedObjectName> Tables
		{
			get;
			private set;
		} = new List<SqlDependencyQualifiedObjectName>();

		/// <summary>
		/// Changes to be monitored
		/// </summary>
		public SqlDependencyMonitoredChanges MonitoredChanges
		{
			get;
			set;
		} = SqlDependencyMonitoredChanges.All;

		/// <summary>
		/// Options
		/// </summary>
		public SqlDependencyOptions Options
		{
			get;
			private set;
		} = new SqlDependencyOptions();
		#endregion

		#region ICloneable
		/// <summary>
		/// Creates a new object that is a copy of the current instance.
		/// </summary>
		/// <returns>
		/// A new object that is a copy of this instance.
		/// </returns>
		public object Clone()
		{
			return new SqlDependencyParams
			{
				ConnectionString = ConnectionString,
				MonitoredChanges = MonitoredChanges,
				Options = Options.Clone() as SqlDependencyOptions,
				Tables = new List<SqlDependencyQualifiedObjectName>(Tables.Select(t => t.Clone() as SqlDependencyQualifiedObjectName))
			};
		}
		#endregion

		#region Helper Methods
		/// <summary>
		/// Validates the params
		/// </summary>
		[SuppressMessage("Microsoft.Usage", "CA2208:InstantiateArgumentExceptionsCorrectly")]
		internal void ValidateAndAdjust()
		{
			// Connection string
			if (ConnectionString == null)
			{
				// ReSharper disable once NotResolvedInText
				throw new ArgumentNullException("SqlDependencyProvider.Parameters.ConnectionString");
			}

			// Tables
			var tableInfos = new List<SqlDependencyQualifiedObjectName>(Tables);
			tableInfos = tableInfos
				.Where(p => p?.Name != null)
				.Distinct()
				.ToList();
			if (tableInfos.Count == 0)
			{
				// ReSharper disable once NotResolvedInText
				throw new ArgumentNullException("SqlDependencyProvider.Parameters.Tables");
			}
			Tables.Clear();
			Tables.AddRange(tableInfos.OrderBy(p => p.Schema, StringComparer.OrdinalIgnoreCase)
										.ThenBy(p => p.Name, StringComparer.OrdinalIgnoreCase));

			// Monitored changes
			if (MonitoredChanges == SqlDependencyMonitoredChanges.None)
			{
				// ReSharper disable once NotResolvedInText
				throw new ArgumentNullException("SqlDependencyProvider.Parameters.MonitoredChanges");
			}

			// Check all monitored tables exist
			using (var conn = new SqlConnection(ConnectionString))
			{
				// Ensure open
				Helper.EnsureOpenConnection(conn);

				// Check
				var nonExistingTableNames = new List<string>();
				if (!Tables.Any(t =>
				{
					if (!Helper.TableExists(conn, null, t))
					{
						nonExistingTableNames.Add(t.ToString());
						return false;
					}
					return true;
				}))
				{
#if USETRANSLATIONS
					throw new InvalidOperationException(
						Translations.Default["SText_SqlDependencyOneOrMoreMonitoredTablesDoNotExist_Names",
						string.Join(", ", nonExistingTableNames)]);
#else
					throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
						Resources.SText_SqlDependencyOneOrMoreMonitoredTablesDoNotExist_Names,
						string.Join(", ", nonExistingTableNames)));
#endif
				}
			}
		}
#endregion
	}


}
