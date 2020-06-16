namespace SqlBulkCopyVsTVP.Scenarios
{
    using System;

    public interface IDataLoadScenario
    {
        string Description { get; }

        TimeSpan Run(string connectionString, string destinationTableName);
    }
}
