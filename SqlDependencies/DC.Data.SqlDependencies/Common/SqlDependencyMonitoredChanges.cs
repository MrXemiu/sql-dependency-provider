using System;

namespace DC.Data.SqlDependencies
{
	/// <summary>
	/// Monitored change type
	/// </summary>
	public enum SqlDependencyMonitoredChange
	{
		/// <summary>
		/// None
		/// </summary>
		None,

		/// <summary>
		/// Record inserted
		/// </summary>
		Insert,

		/// <summary>
		/// Record updated
		/// </summary>
		Update,

		/// <summary>
		/// Record deleted
		/// </summary>
		Delete
	}

	/// <summary>
	/// Monitored change types
	/// </summary>
	[Flags]
	public enum SqlDependencyMonitoredChanges
	{
		/// <summary>
		/// None
		/// </summary>
		None = 0x0,

		/// <summary>
		/// Record inserted
		/// </summary>
		Insert = 0x1,

		/// <summary>
		/// Record updated
		/// </summary>
		Update = 0x2,

		/// <summary>
		/// Record deleted
		/// </summary>
		Delete = 0x4,

		/// <summary>
		/// Record inserted/updated/deleted
		/// </summary>
		All = Insert | Update | Delete
	}
}
