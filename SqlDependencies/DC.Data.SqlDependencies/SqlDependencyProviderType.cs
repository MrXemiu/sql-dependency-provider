namespace DC.Data.SqlDependencies
{
	/// <summary>
	/// Provider implementation type
	/// </summary>
	public enum SqlDependencyProviderType
	{
		/// <summary>
		/// Implementation is chosen based on service broker availability
		/// </summary>
		Auto,

		/// <summary>
		/// Polling implementation
		/// </summary>
		Polling,

		/// <summary>
		/// Service broker implementation
		/// </summary>
		ServiceBroker
	}
}
