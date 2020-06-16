using System;
using System.Collections.Generic;
using System.Text;

namespace SqlBulkCopyVsTVP
{
    using System.Data.SqlClient;
    using SqlBulkCopyVsTVP.Scenarios;

    public class TestScenarioRunner
    {
        private readonly string _connectionString;
        private readonly string _destinationTableName;
        private readonly string _sqlDataFolder;
        private readonly List<IDataLoadScenario> _scenarios = new List<IDataLoadScenario>();

        public TestScenarioRunner(string connectionString, string destinationTableName, string sqlDataFolder)
        {
            _connectionString = connectionString;
            _destinationTableName = destinationTableName;
            _sqlDataFolder = sqlDataFolder;
        }


        public TestScenarioRunner AddScenario(IDataLoadScenario scenario)
        {
            _scenarios.Add(scenario);
            return this;
        }

        private void InitializeDatabase()
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(_connectionString);
            var databaseName = connectionStringBuilder.InitialCatalog;

            connectionStringBuilder.InitialCatalog = "master";
            using (var connection = new SqlConnection(connectionStringBuilder.ConnectionString))
            using (var command = new SqlCommand("", connection))
            {
                connection.Open();

                command.CommandText = $@"
                IF EXISTS(SELECT * FROM sys.databases WHERE name = '{databaseName}')
                    BEGIN
                        EXECUTE('ALTER DATABASE [{databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{databaseName}]')
                    END";
                command.ExecuteNonQuery();

                command.CommandText = $@"
                    CREATE DATABASE [{databaseName}]
                        CONTAINMENT = NONE
                    ON  PRIMARY
                    (NAME = N'{databaseName}', FILENAME = N'{_sqlDataFolder}{databaseName}.mdf', SIZE = 524288KB, FILEGROWTH = 65536KB)
                    LOG ON
                    (NAME = N'{databaseName}_log', FILENAME = N'{_sqlDataFolder}{databaseName}_log.ldf', SIZE = 262144KB, FILEGROWTH = 65536KB)";

                command.ExecuteNonQuery();

                connection.ChangeDatabase(databaseName);
                command.CommandText = @"
                CREATE TYPE dbo.TestData AS TABLE 
                (
                    [Index] INTEGER,
                    [ImportantDate] DATETIME,
                    [SomeText] VARCHAR(30)
                )";
                command.ExecuteNonQuery();

                command.CommandText = @"CREATE TABLE dbo.LoadTest
                (
                    [Index] INTEGER,
                    [ImportantDate] DATETIME,
                    [SomeText] VARCHAR(30)
                )";
                command.ExecuteNonQuery();

                command.CommandText = @"
                CREATE PROCEDURE dbo.LoadTvp
                    @Data dbo.TestData READONLY
                AS
                BEGIN
                    INSERT dbo.LoadTest([Index], [ImportantDate], [SomeText])
                    SELECT [Index], [ImportantDate], [SomeText]
                    FROM @Data
                END";
                command.ExecuteNonQuery();
                connection.Close();
            }
        }

        public Dictionary<string, TimeSpan> RunAllScenarios()
        {
            Console.WriteLine($"Number of scenarios to run: {_scenarios.Count}");
            
            var scenarioCounter = 0;
            var results = new Dictionary<string, TimeSpan>();

            foreach (var scenario in _scenarios)
            {
                scenarioCounter++;
                Console.WriteLine($"Starting scenario #{scenarioCounter} of {_scenarios.Count}: {scenario.Description}...");

                Console.WriteLine($"Initializing database...");
                InitializeDatabase();
                Console.WriteLine($"Database initialized");
                Console.WriteLine($"Running scenario...");

                var timeTaken = scenario.Run(_connectionString, _destinationTableName);
                results.Add(scenario.Description, timeTaken);
            }

            return results;
        }
    }
}
