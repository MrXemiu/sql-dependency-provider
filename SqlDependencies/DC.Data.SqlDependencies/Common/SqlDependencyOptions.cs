using System;
using System.Diagnostics.CodeAnalysis;

namespace DC.Data.SqlDependencies
{
	/// <summary>
	/// Options
	/// </summary>
	[Serializable]
	public sealed class SqlDependencyOptions : ICloneable
	{
		#region Properties
		private const string _defaultIntermediateObjectSchema = "dbo";
		private string _intermediateObjectSchema = _defaultIntermediateObjectSchema;
		/// <summary>
		/// Schema for intermediate objects
		/// </summary>
		public string IntermediateObjectSchema
		{
			get { return _intermediateObjectSchema; }
			set
			{
				_intermediateObjectSchema = string.IsNullOrWhiteSpace(value) ? _defaultIntermediateObjectSchema : value.Trim();
				_changeHolderTable = null;
			}
		}

		private string _intermediateObjectPrefix = string.Empty;
		/// <summary>
		/// Prefix for intermediate option names
		/// </summary>
		public string IntermediateObjectPrefix
		{
			get { return _intermediateObjectPrefix; }
			set
			{
				_intermediateObjectPrefix = string.IsNullOrWhiteSpace(value) ? string.Empty : value.Trim();
				_changeHolderTable = null;
			}
		}

		private string _defaultChangeHolderTableBaseName = "SqlDependencyChanges";
		private string _changeHolderTableBaseName;
		/// <summary>
		/// Base name of the table holding changes
		/// </summary>
		public string ChangeHolderTableBaseName
		{
			get { return _changeHolderTableBaseName; }
			set
			{
				_changeHolderTableBaseName = string.IsNullOrWhiteSpace(value) ? _defaultChangeHolderTableBaseName : value.Trim();
				_changeHolderTable = null;
			}
		}

		private string _defaultTableTriggerBaseName = "SqlDependencyChangeTrigger";
		private string _tableTriggerBaseName;
		/// <summary>
		/// Base name of the table trigger
		/// </summary>
		public string TableTriggerBaseName
		{
			get { return _tableTriggerBaseName; }
			set { _tableTriggerBaseName = string.IsNullOrWhiteSpace(value) ? _defaultTableTriggerBaseName : value.Trim(); }
		}

		private SqlDependencyQualifiedObjectName _changeHolderTable;
		/// <summary>
		/// Table definition of changes table
		/// </summary>
		public SqlDependencyQualifiedObjectName ChangeHolderTable => _changeHolderTable ??
														(_changeHolderTable = new SqlDependencyQualifiedObjectName(IntermediateObjectPrefix + ChangeHolderTableBaseName, IntermediateObjectSchema));
		#endregion

		#region Helpers
		/// <summary>
		/// Creates a qualified name for a table trigger
		/// </summary>
		[SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
		public SqlDependencyQualifiedObjectName GetTableTrigger(SqlDependencyQualifiedObjectName table)
		{
			if (table?.Name == null)
			{
				throw new ArgumentNullException(nameof(table));
			}
			return new SqlDependencyQualifiedObjectName(IntermediateObjectPrefix + TableTriggerBaseName + "_" + table.Name.Trim(), table.Schema);
		}
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
			return new SqlDependencyOptions
			{
				IntermediateObjectSchema = IntermediateObjectSchema,
				IntermediateObjectPrefix = IntermediateObjectPrefix,
				ChangeHolderTableBaseName = ChangeHolderTableBaseName,
				TableTriggerBaseName = TableTriggerBaseName
			};
		}
		#endregion
	}
}
