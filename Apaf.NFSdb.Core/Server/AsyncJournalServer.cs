using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

namespace Apaf.NFSdb.Core.Server
{
    public class AsyncJournalServer : IJournalServer, IDisposable
    {
        private readonly ThreadPriority _priority;
        private readonly string _name;
        private readonly BlockingCollection<Action> _serverTasks = new BlockingCollection<Action>();
        private volatile bool _isStopped;
        private readonly Lazy<bool> _isStarted;

        public AsyncJournalServer(ThreadPriority priority = ThreadPriority.Normal, string name = null)
        {
            _priority = priority;
            _name = name;
            _isStarted = new Lazy<bool>(StartJobThread, LazyThreadSafetyMode.ExecutionAndPublication);
        }

        private bool StartJobThread()
        {
            var jobThread = new Thread(JobThreadMain)
            {
                IsBackground = true,
                Name = "NFSdb server" + _name,
                Priority = _priority
            };
            jobThread.Start();
            return true;
        }

        private void JobThreadMain()
        {
            foreach (var task in _serverTasks.GetConsumingEnumerable())
            {
                try
                {
                    task.Invoke();
                    if (_isStopped)
                    {
                        _serverTasks.Dispose();
                        return;
                    }
                }
                catch (Exception ex)
                {
                    Trace.TraceWarning(Thread.CurrentThread.Name + "Error executing journal task: " + ex);
                }
            }
        }

        public void SchedulePartitionAppendComplete(Action cleanAction)
        {
            if (_isStarted.Value)
            {
                _serverTasks.Add(cleanAction);
            }
        }

        public void SoftStop()
        {
            Trace.TraceInformation(Thread.CurrentThread.Name + " requested to stop.");
            _isStopped = true;
            _serverTasks.Add(() => { });
        }

        public void Dispose()
        {
            Trace.TraceInformation(Thread.CurrentThread.Name + " is disposed and will be stopped now.");
            _serverTasks.Dispose();
        }
    }
}