namespace SqlBulkCopyVsTVP.Scenarios
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DataLoaders;

    public class TvpSprocLoadScenario : IDataLoadScenario
    {
        private readonly int _numberOfRows;
        private readonly int _numberOfTasks;
        
        public TvpSprocLoadScenario(int numberOfRows, int numberOfTasks)
        {
            _numberOfRows = numberOfRows;
            _numberOfTasks = numberOfTasks;
        }

        public string Description => $"TVP load via stored procedure: {_numberOfRows} rows over {_numberOfTasks} task(s)";

        public TimeSpan Run(string connectionString, string destinationTableName)
        {
            var rowsPerInstance = _numberOfRows / _numberOfTasks;

            Console.WriteLine("Generating data...");
            List<DataTable> dataTables = new List<DataTable>();
            for (var instanceNumber = 0; instanceNumber < _numberOfTasks; instanceNumber++)
            {
                dataTables.Add(DataGenerator.GenerateData(rowsPerInstance, instanceNumber * rowsPerInstance));
            }
            Console.WriteLine("Done.");
            Console.WriteLine("Loading data...");
            
            
            var stopWatch = Stopwatch.StartNew();
            Parallel.ForEach(dataTables,
                t =>
                {
                    var loader = new TvpDataLoader(connectionString, destinationTableName);
                    loader.LoadData(t);
                });
            stopWatch.Stop();
            Console.WriteLine("Done.");
            return stopWatch.Elapsed;
        }
    }
}