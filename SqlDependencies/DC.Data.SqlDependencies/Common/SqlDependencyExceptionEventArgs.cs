using System;

namespace DC.Data.SqlDependencies
{
	/// <summary>
	/// Exception event arguments
	/// </summary>
	public sealed class SqlDependencyExceptionEventArgs : EventArgs
	{
		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="exception">Exception</param>
		/// <param name="ignore">Whether to ignore this exception and continue</param>
		public SqlDependencyExceptionEventArgs(Exception exception, bool ignore)
		{
			if (exception == null)
			{
				throw new ArgumentNullException(nameof(exception));
			}

			Exception = exception;
			Ignore = ignore;
		}

		/// <summary>
		/// Exception raised
		/// </summary>
		public Exception Exception
		{
			get;
		}

		/// <summary>
		/// Whether to ignore this exception and continue
		/// </summary>
		public bool Ignore
		{
			get;
			set;
		}
	}
}
