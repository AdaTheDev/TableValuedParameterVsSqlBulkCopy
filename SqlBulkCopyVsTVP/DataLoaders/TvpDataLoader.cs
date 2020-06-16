namespace SqlBulkCopyVsTVP.DataLoaders
{
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using Microsoft.SqlServer.Server;

    public class TvpDataLoader
    {
        private readonly string _connectionString;
        private readonly string _destinationTableName;

        public TvpDataLoader(string connectionString, string destinationTableName)
        {
            _connectionString = connectionString;
            _destinationTableName = destinationTableName;
        }

        public void LoadData(DataTable table)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand("dbo.LoadTvp", connection))
                {
                    command.CommandTimeout = 3600;
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.Add(new SqlParameter("@Data", SqlDbType.Structured) {Value = table});
                    command.ExecuteNonQuery();
                }
            }
        }

        public void LoadData(IEnumerable<SqlDataRecord> dataReader)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand("dbo.LoadTvp", connection))
                {
                    command.CommandTimeout = 3600;
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.Add(new SqlParameter("@Data", SqlDbType.Structured) { Value = dataReader });
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}