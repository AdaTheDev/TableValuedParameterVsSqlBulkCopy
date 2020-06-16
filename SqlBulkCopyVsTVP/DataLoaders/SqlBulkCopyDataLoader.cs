namespace SqlBulkCopyVsTVP.DataLoaders
{
    using System.Data;
    using System.Data.Common;
    using System.Data.SqlClient;

    public class SqlBulkCopyDataLoader
    {
        private readonly string _connectionString;
        private readonly string _destinationTableName;

        public SqlBulkCopyDataLoader(string connectionString, string destinationTableName)
        {
            _connectionString = connectionString;
            _destinationTableName = destinationTableName;
        }

        public void LoadData(DataTable table)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null))
                {
                    bulkCopy.BatchSize = table.Rows.Count;
                    bulkCopy.DestinationTableName = _destinationTableName;

                    bulkCopy.WriteToServer(table);
                }
            }
        }

        public void LoadData(DbDataReader dataReader, int batchSize)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null))
                {
                    bulkCopy.BatchSize = batchSize;
                    bulkCopy.DestinationTableName = _destinationTableName;

                    bulkCopy.WriteToServer(dataReader);
                }
            }
        }
    }
}