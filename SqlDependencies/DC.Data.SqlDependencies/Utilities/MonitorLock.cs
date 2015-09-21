using System;
using System.Threading;

namespace DC.Data.SqlDependencies
{
	/// <summary>
	/// Wrapper for Monitor locking
	/// </summary>
	internal sealed class MonitorLock : IDisposable
	{
		private object _lockObj;
		private bool _lockAcquired;

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="lockObj">Locking object</param>
		public MonitorLock(object lockObj)
		{
			if (lockObj == null)
			{
				throw new ArgumentNullException(nameof(lockObj));
			}

			_lockObj = lockObj;
			_lockAcquired = false;
			Monitor.Enter(_lockObj, ref _lockAcquired);
		}

		/// <summary>
		/// Disposes this instance
		/// </summary>
		public void Dispose()
		{
			if (_lockObj != null)
			{
				if (_lockAcquired)
				{
					Monitor.Exit(_lockObj);
				}
				_lockObj = null;
				_lockAcquired = false;
			}
		}
	}
}
