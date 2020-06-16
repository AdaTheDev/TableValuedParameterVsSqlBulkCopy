namespace SqlBulkCopyVsTVP
{
    using System;
    using System.Data;

    public static class DataGenerator
    {
        public static DataTable GenerateData(int numberOfItems, int startIndex)
        {
            var dataTable = new DataTable();
            dataTable.Columns.Add("Index", typeof(int));
            dataTable.Columns.Add("ImportantDate", typeof(DateTime));
            dataTable.Columns.Add("SomeText", typeof(string));

            for (var counter = 0; counter < numberOfItems; counter++)
            {
                dataTable.Rows.Add(counter, DateTime.UtcNow, "This is record #" + (startIndex + counter));
            }

            return dataTable;
        }
    }
}