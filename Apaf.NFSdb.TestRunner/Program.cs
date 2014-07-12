using System;
using System.Collections.Generic;
using System.Linq;

namespace Apaf.NFSdb.TestRunner
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var taskList = ScanAllTasks();
            foreach (var task in taskList)
            {
                if (task.Name.Equals(args[0], StringComparison.OrdinalIgnoreCase))
                {
                    task.Run();
                }
            }
        }

        private static IEnumerable<ITask> ScanAllTasks()
        {
            return (
                from tp in typeof (ITask).Assembly.GetTypes() 
                where tp.GetInterfaces().Contains(typeof (ITask)) 
                select (ITask) Activator.CreateInstance(tp)
                );
        }
    }
}