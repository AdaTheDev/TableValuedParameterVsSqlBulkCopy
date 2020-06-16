namespace SqlBulkCopyVsTVP.Scenarios
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DataLoaders;

    public class SqlBulkCopyLoadScenario : IDataLoadScenario
    {
        private readonly int _numberOfTasks;
        private readonly int _numberOfRows;
        
        public SqlBulkCopyLoadScenario(int numberOfRows, int numberOfTasks)
        {
            _numberOfTasks = numberOfTasks;
            _numberOfRows = numberOfRows;
        }

        public string Description => $"SqlBulkCopy: {_numberOfRows} rows over {_numberOfTasks} task(s)";

        public TimeSpan Run(string connectionString, string destinationTableName)
        {
            var rowsPerInstance = _numberOfRows / _numberOfTasks;

            Console.WriteLine("Generating data...");
            List<DbDataReader> dataTables = new List<DbDataReader>();
            for (var instanceNumber = 0; instanceNumber < _numberOfTasks; instanceNumber++)
            {
                dataTables.Add(DataGenerator.GenerateData(rowsPerInstance, instanceNumber * rowsPerInstance).CreateDataReader());
            }
            Console.WriteLine("Done.");
            Console.WriteLine("Loading data...");
            var stopWatch = Stopwatch.StartNew();
            Parallel.ForEach(dataTables,
                t =>
                {
                    var loader = new SqlBulkCopyDataLoader(connectionString, destinationTableName);
                    loader.LoadData(t, rowsPerInstance);
                });
            stopWatch.Stop();
            Console.WriteLine("Done.");
            return stopWatch.Elapsed;
        }
    }
}