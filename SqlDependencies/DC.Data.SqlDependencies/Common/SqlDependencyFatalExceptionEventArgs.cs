using System;

namespace DC.Data.SqlDependencies
{
	/// <summary>
	/// Fatal exception event arguments
	/// </summary>
	public sealed class SqlDependencyFatalExceptionEventArgs : EventArgs
	{
		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="exception">Exception</param>
		public SqlDependencyFatalExceptionEventArgs(Exception exception)
		{
			if (exception == null)
			{
				throw new ArgumentNullException(nameof(exception));
			}

			Exception = exception;
		}

		/// <summary>
		/// Exception raised
		/// </summary>
		public Exception Exception
		{
			get;
		}
	}
}
