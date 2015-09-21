using System;
using System.Globalization;

namespace DC.Data.SqlDependencies
{
	/// <summary>
	/// Table info
	/// </summary>
	[Serializable]
	public sealed class SqlDependencyQualifiedObjectName : ICloneable
	{
		#region Construction
		/// <summary>
		/// Constructor
		/// </summary>
		public SqlDependencyQualifiedObjectName()
		{
		}

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="name">Table name</param>
		public SqlDependencyQualifiedObjectName(string name)
		{
			Name = name;
		}

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="name">Table name</param>
		/// <param name="schema">Schema name</param>
		public SqlDependencyQualifiedObjectName(string name, string schema)
		{
			Name = name;
			Schema = schema;
		}
		#endregion

		#region Properties
		private const string _defaultSchema = "dbo";
		private string _schema = _defaultSchema;
		/// <summary>
		/// Table schema
		/// </summary>
		public string Schema
		{
			get { return _schema; }
			set { _schema = string.IsNullOrWhiteSpace(value) ? _defaultSchema : value.Trim(); }
		}

		private string _name;
		/// <summary>
		/// Table name
		/// </summary>
		public string Name
		{
			get { return _name; }
			set { _name = string.IsNullOrWhiteSpace(value) ? null : value.Trim(); }
		}

		/// <summary>
		/// Full table name
		/// </summary>
		public string FullName => string.Format(CultureInfo.InvariantCulture, "[{0}].[{1}]", Schema, (Name ?? string.Empty));

		/// <summary>
		/// Full table name without the square brackets
		/// </summary>
		public string AlternateFullName => string.Format(CultureInfo.InvariantCulture, "{0}.{1}", Schema, (Name ?? string.Empty));
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
			return new SqlDependencyQualifiedObjectName(Name, Schema);
		}
		#endregion

		#region Operators
		/// <summary>
		/// Converts this object into a string
		/// </summary>
		/// <param name="obj">Object to convert</param>
		public static implicit operator string(SqlDependencyQualifiedObjectName obj)
		{
			return obj?.ToString();
		}
		#endregion

		#region Overrides of Object
		/// <summary>
		/// Returns a string that represents the current object.
		/// </summary>
		/// <returns>
		/// A string that represents the current object.
		/// </returns>
		public override string ToString()
		{
			return FullName;
		}

		/// <summary>
		/// Determines whether the specified object is equal to the current object.
		/// </summary>
		/// <returns>
		/// true if the specified object  is equal to the current object; otherwise, false.
		/// </returns>
		/// <param name="obj">The object to compare with the current object. </param>
		public override bool Equals(object obj)
		{
			var other = obj as SqlDependencyQualifiedObjectName;
			if (other == null)
			{
				return false;
			}
			return string.Equals(FullName, other.FullName, StringComparison.OrdinalIgnoreCase);
		}

		/// <summary>
		/// Serves as the default hash function.
		/// </summary>
		/// <returns>
		/// A hash code for the current object.
		/// </returns>
		public override int GetHashCode()
		{
			return ToString().GetHashCode();
		}
		#endregion
	}
}
