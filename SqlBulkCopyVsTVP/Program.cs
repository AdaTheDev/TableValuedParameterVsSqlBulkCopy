using System;

namespace SqlBulkCopyVsTVP
{
    using Scenarios;

    class Program
    {
        static void Main(string[] args)
        {
            /*************************************************
             * SQL Server configuration for the test runs
             * The database will be dropped & recreated for each test run.
             *************************************************/
            var destinationTableName = "dbo.LoadTest";
            var connectionString = $@"server=localhost\sql2019_ci;initial catalog=LoadTestDb_TVPvsSqlBulkCopy;integrated security=true";
            var sqlDataFolder = @"C:\Program Files\Microsoft SQL Server\MSSQL15.SQL2019_CI\MSSQL\DATA\";
            /*************************************************/

            Console.WriteLine($"The tests will be run against the following database:");
            Console.WriteLine($"    {connectionString}");
            Console.WriteLine($"The path to where the database data and log files will be created:");
            Console.WriteLine($"    {sqlDataFolder}");
            Console.WriteLine($"The target table is:");
            Console.WriteLine($"   {destinationTableName}");
            Console.WriteLine();
            Console.WriteLine("*** The database will be dropped if it already exists and recreated. ***");
            Console.WriteLine("Confirm these details are correct by pressing 'Y'. Press any other key to cancel.");
            var key = Console.ReadKey();
            Console.WriteLine();
            
            if (key.KeyChar == 'y')
            {
                Console.WriteLine();
                var runner = new TestScenarioRunner(connectionString, destinationTableName, sqlDataFolder);
                runner
                    .AddScenario(new SqlBulkCopyLoadScenario(100, 1))
                    .AddScenario(new SqlBulkCopyLoadScenario(100, 4))
                    .AddScenario(new SqlBulkCopyLoadScenario(1000, 1))
                    .AddScenario(new SqlBulkCopyLoadScenario(1000, 4))
                    .AddScenario(new SqlBulkCopyLoadScenario(10000, 1))
                    .AddScenario(new SqlBulkCopyLoadScenario(10000, 4))
                    .AddScenario(new SqlBulkCopyLoadScenario(100000, 1))
                    .AddScenario(new SqlBulkCopyLoadScenario(100000, 4))
                    .AddScenario(new SqlBulkCopyLoadScenario(1000000, 1))
                    .AddScenario(new SqlBulkCopyLoadScenario(1000000, 4))
                    .AddScenario(new SqlBulkCopyLoadScenario(10000000, 1))
                    .AddScenario(new SqlBulkCopyLoadScenario(10000000, 4))
                    .AddScenario(new TvpSprocLoadScenario(100, 1))
                    .AddScenario(new TvpSprocLoadScenario(100, 4))
                    .AddScenario(new TvpSprocLoadScenario(1000, 1))
                    .AddScenario(new TvpSprocLoadScenario(1000, 4))
                    .AddScenario(new TvpSprocLoadScenario(10000, 1))
                    .AddScenario(new TvpSprocLoadScenario(10000, 4))
                    .AddScenario(new TvpSprocLoadScenario(100000, 1))
                    .AddScenario(new TvpSprocLoadScenario(100000, 4))
                    .AddScenario(new TvpSprocLoadScenario(1000000, 1))
                    .AddScenario(new TvpSprocLoadScenario(1000000, 4))
                    .AddScenario(new TvpSprocLoadScenario(10000000, 1))
                    .AddScenario(new TvpSprocLoadScenario(10000000, 4));

                var results = runner.RunAllScenarios();

                Console.WriteLine("================================");
                Console.WriteLine("== RESULTS =====================");
                Console.WriteLine("================================");

                foreach (var result in results)
                {
                    Console.WriteLine($"{result.Key} : {result.Value.TotalMilliseconds:F3}ms");
                }

                Console.WriteLine("Press any key to exit");
                Console.ReadKey();
            }
        }
    }
}
