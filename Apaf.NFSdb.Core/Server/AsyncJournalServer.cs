using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Apaf.NFSdb.Core.Storage;

namespace Apaf.NFSdb.Core.Server
{
    public class AsyncJournalServer : IJournalServer, IDisposable
    {
        private readonly TimeSpan _latency;
        private readonly ConcurrentQueue<ScheduleAction> _serverTasks = new ConcurrentQueue<ScheduleAction>();
        private readonly List<ScheduleAction> _tasks = new List<ScheduleAction>();
        private Timer _timer;
        public static readonly TimeSpan INFINITE_TIME_SPAN = new TimeSpan(0, 0, 0, 0, -1);
        private bool _isStopped;
        private int _tasksInQueue;

        public AsyncJournalServer(TimeSpan latency)
        {
            _latency = latency;
        }

        private void StartJobThread()
        {
            if (_timer == null)
            {
                _timer = new Timer(DoWork, null, _latency, INFINITE_TIME_SPAN);
            }
            else
            {
                _timer.Change(_latency, INFINITE_TIME_SPAN);
            }
        }

        private void DoWork(object state)
        {
            try
            {
                if (_isStopped)
                {
                    return;
                }

                ScheduleAction act;
                while (_serverTasks.TryDequeue(out act))
                {
                    // Min PQ would work better.
                    _tasks.Add(act);
                }
                _tasks.Sort();

                int i = _tasks.Count - 1;

                for (; i >= 0; i--)
                {
                    if (_tasks[i].DueTime <= DateTime.UtcNow)
                    {
                        try
                        {
                            _tasks[i].Action();
                        }
                        catch (Exception ex)
                        {
                            Trace.TraceError("Error executing server task '{0}'. {1}", _tasks[i].Name, ex);
                        }
                    }
                    else
                    {
                        // 1 will be added anyway.
                        i--;
                        break;
                    }
                }

                // Remove all completed.
                i++;
                int processed = _tasks.Count;
                _tasks.RemoveRange(i, processed);

                // Set tasks tracker.
                var tasksLeft = Interlocked.Add(ref _tasksInQueue, -processed);

                if (tasksLeft > 0)
                {
                    _timer.Change(_latency, INFINITE_TIME_SPAN);
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError("Error in server tasks thread" + ex);
            }
        }
        
        public void SignalUnusedPartition(IPartitionCore partitionCore, int offloadTimeoutTtl)
        {
            if (offloadTimeoutTtl >= 0)
            {
                _serverTasks.Enqueue(new ScheduleAction(DateTime.UtcNow.AddMilliseconds(offloadTimeoutTtl),
                    () => ExecuteOffloadPartition(partitionCore), string.Format("Offload partition '{0}'", partitionCore.StartDate)));

                if (Interlocked.Increment(ref _tasksInQueue) == 1)
                {
                    StartJobThread();
                }
            }
        }

        private void ExecuteOffloadPartition(IPartitionCore partitionCore)
        {
            partitionCore.TryCloseFiles();
        }

        public void SoftStop()
        {
            Trace.TraceInformation(Thread.CurrentThread.Name + " requested to stop.");
            _isStopped = true;
        }

        public void Dispose()
        {
            Trace.TraceInformation(Thread.CurrentThread.Name + " is disposed and will be stopped now.");
            _timer.Dispose();
        }

        private class ScheduleAction : IComparable<ScheduleAction>
        {
            public readonly DateTime DueTime;
            public readonly Action Action;
            public readonly string Name;

            public ScheduleAction(DateTime dueTime, Action action, string name)
            {
                DueTime = dueTime;
                Action = action;
                Name = name;
            }

            public int CompareTo(ScheduleAction other)
            {
                // By due time descending.
                return other.DueTime.CompareTo(DueTime);
            }
        }
    }
}