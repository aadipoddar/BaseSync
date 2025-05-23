using System.Data;

using Microsoft.Data.SqlClient;

namespace BaseSync;

public static class Sync
{
	/// <summary>
	/// Synchronizes data between local and remote databases
	/// First pulls changes from remote to local, then pushes local changes to remote
	/// </summary>
	/// <param name="localDbConnectionString">Connection string for the local database</param>
	/// <param name="remoteDbConnectionString">Connection string for the remote database</param>
	/// <param name="tableNames">List of tables to synchronize</param>
	/// <returns>A summary of synchronization actions taken</returns>
	public static async Task<SyncResult> SyncDataAsync(string localDbConnectionString, string remoteDbConnectionString, List<string> tableNames)
	{
		var result = new SyncResult();

		foreach (var tableName in tableNames)
		{
			try
			{
				// First pull from remote to local
				var pullResult = await PullFromRemoteToLocalAsync(localDbConnectionString, remoteDbConnectionString, tableName);
				result.TableResults.Add(tableName, new TableSyncResult
				{
					PullInserts = pullResult.inserted,
					PullUpdates = pullResult.updated,
					IsSuccess = true
				});

				// Then push from local to remote
				var pushResult = await PushFromLocalToRemoteAsync(localDbConnectionString, remoteDbConnectionString, tableName);
				result.TableResults[tableName].PushInserts = pushResult.inserted;
				result.TableResults[tableName].PushUpdates = pushResult.updated;
			}
			catch (Exception ex)
			{
				result.TableResults.Add(tableName, new TableSyncResult
				{
					IsSuccess = false,
					ErrorMessage = ex.Message
				});
				result.HasErrors = true;
			}
		}

		return result;
	}

	/// <summary>
	/// Pulls data from remote database to local database
	/// </summary>
	private static async Task<(int inserted, int updated)> PullFromRemoteToLocalAsync(string localDbConnectionString, string remoteDbConnectionString, string tableName)
	{
		int inserted = 0;
		int updated = 0;

		// Get primary key columns for the table
		var primaryKeyColumns = await GetPrimaryKeyColumnsAsync(localDbConnectionString, tableName);
		if (primaryKeyColumns.Count == 0)
		{
			throw new Exception($"No primary key found for table {tableName}. Synchronization requires a primary key.");
		}

		// Get all columns for the table
		var columns = await GetTableColumnsAsync(localDbConnectionString, tableName);

		using var localConnection = new SqlConnection(localDbConnectionString);
		using var remoteConnection = new SqlConnection(remoteDbConnectionString);

		await localConnection.OpenAsync();
		await remoteConnection.OpenAsync();

		// Get all remote data
		var remoteData = await GetTableDataAsync(remoteConnection, tableName, columns);

		// For each row in remote data
		foreach (DataRow remoteRow in remoteData.Rows)
		{
			// Build a condition for the primary key
			var whereClause = BuildPrimaryKeyWhereClause(remoteRow, primaryKeyColumns);

			// Check if record exists in local
			var localRow = await GetRowByPrimaryKeyAsync(localConnection, tableName, whereClause, columns, remoteRow, primaryKeyColumns);

			if (localRow == null)
			{
				// Row doesn't exist locally, insert it
				await InsertRowAsync(localConnection, tableName, remoteRow, columns);
				inserted++;
			}
			else
			{
				// Row exists, check if different
				if (RowsAreDifferent(remoteRow, localRow, columns))
				{
					// Update local row with remote data
					await UpdateRowAsync(localConnection, tableName, remoteRow, whereClause, columns, primaryKeyColumns);
					updated++;
				}
			}
		}

		return (inserted, updated);
	}

	/// <summary>
	/// Pushes data from local database to remote database
	/// </summary>
	private static async Task<(int inserted, int updated)> PushFromLocalToRemoteAsync(string localDbConnectionString, string remoteDbConnectionString, string tableName)
	{
		int inserted = 0;
		int updated = 0;

		// Get primary key columns for the table
		var primaryKeyColumns = await GetPrimaryKeyColumnsAsync(localDbConnectionString, tableName);
		if (primaryKeyColumns.Count == 0)
		{
			throw new Exception($"No primary key found for table {tableName}. Synchronization requires a primary key.");
		}

		// Get all columns for the table
		var columns = await GetTableColumnsAsync(localDbConnectionString, tableName);

		using var localConnection = new SqlConnection(localDbConnectionString);
		using var remoteConnection = new SqlConnection(remoteDbConnectionString);

		await localConnection.OpenAsync();
		await remoteConnection.OpenAsync();

		// Get all local data
		var localData = await GetTableDataAsync(localConnection, tableName, columns);

		// For each row in local data
		foreach (DataRow localRow in localData.Rows)
		{
			// Build a condition for the primary key
			var whereClause = BuildPrimaryKeyWhereClause(localRow, primaryKeyColumns);

			// Check if record exists in remote
			var remoteRow = await GetRowByPrimaryKeyAsync(remoteConnection, tableName, whereClause, columns, localRow, primaryKeyColumns);

			if (remoteRow == null)
			{
				// Row doesn't exist remotely, insert it
				await InsertRowAsync(remoteConnection, tableName, localRow, columns);
				inserted++;
			}
			else
			{
				// Row exists, check if different
				if (RowsAreDifferent(localRow, remoteRow, columns))
				{
					// Update remote row with local data
					await UpdateRowAsync(remoteConnection, tableName, localRow, whereClause, columns, primaryKeyColumns);
					updated++;
				}
			}
		}

		return (inserted, updated);
	}

	private static async Task<List<string>> GetPrimaryKeyColumnsAsync(string connectionString, string tableName)
	{
		var primaryKeyColumns = new List<string>();

		using var connection = new SqlConnection(connectionString);
		await connection.OpenAsync();

		// Get schema information about primary key
		var query = @"
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
            AND TABLE_NAME = @TableName
            ORDER BY ORDINAL_POSITION";

		using var command = new SqlCommand(query, connection);
		command.Parameters.AddWithValue("@TableName", tableName);

		using var reader = await command.ExecuteReaderAsync();
		while (await reader.ReadAsync())
		{
			primaryKeyColumns.Add(reader.GetString(0));
		}

		return primaryKeyColumns;
	}

	private static async Task<List<string>> GetTableColumnsAsync(string connectionString, string tableName)
	{
		var columns = new List<string>();

		using var connection = new SqlConnection(connectionString);
		await connection.OpenAsync();

		// Get column information
		var query = @"
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = @TableName
            ORDER BY ORDINAL_POSITION";

		using var command = new SqlCommand(query, connection);
		command.Parameters.AddWithValue("@TableName", tableName);

		using var reader = await command.ExecuteReaderAsync();
		while (await reader.ReadAsync())
		{
			columns.Add(reader.GetString(0));
		}

		return columns;
	}

	private static async Task<DataTable> GetTableDataAsync(SqlConnection connection, string tableName, List<string> columns)
	{
		var dataTable = new DataTable();

		var columnList = string.Join(", ", columns.Select(c => $"[{c}]"));
		var query = $"SELECT {columnList} FROM [{tableName}]";

		using var adapter = new SqlDataAdapter(query, connection);
		await Task.Run(() => adapter.Fill(dataTable));

		return dataTable;
	}

	private static string BuildPrimaryKeyWhereClause(DataRow row, List<string> primaryKeyColumns)
	{
		var conditions = new List<string>();

		foreach (var column in primaryKeyColumns)
		{
			if (row[column] == DBNull.Value)
			{
				conditions.Add($"[{column}] IS NULL");
			}
			else
			{
				conditions.Add($"[{column}] = @{column}");
			}
		}

		return string.Join(" AND ", conditions);
	}

	private static async Task<DataRow?> GetRowByPrimaryKeyAsync(SqlConnection connection, string tableName, string whereClause, List<string> columns, DataRow sourceRow, List<string> primaryKeyColumns)
	{
		var columnList = string.Join(", ", columns.Select(c => $"[{c}]"));
		var query = $"SELECT {columnList} FROM [{tableName}] WHERE {whereClause}";

		using var command = new SqlCommand(query, connection);

		// Add parameters for primary key values
		foreach (var column in primaryKeyColumns)
		{
			if (whereClause.Contains($"[{column}] = @{column}"))
			{
				command.Parameters.AddWithValue($"@{column}", sourceRow[column]);
			}
		}

		var dataTable = new DataTable();
		using var adapter = new SqlDataAdapter(command);
		await Task.Run(() => adapter.Fill(dataTable));

		return dataTable.Rows.Count > 0 ? dataTable.Rows[0] : null;
	}

	private static bool RowsAreDifferent(DataRow row1, DataRow row2, List<string> columns)
	{
		foreach (var column in columns)
		{
			var value1 = row1[column];
			var value2 = row2[column];

			// Handle DBNull comparisons
			if (value1 is DBNull && value2 is DBNull)
			{
				continue;
			}
			if (value1 is DBNull || value2 is DBNull)
			{
				return true;
			}

			// Compare the string representations to handle different types
			if (!value1.ToString().Equals(value2.ToString()))
			{
				return true;
			}
		}

		return false;
	}

	private static async Task InsertRowAsync(SqlConnection connection, string tableName, DataRow row, List<string> columns)
	{
		var columnList = string.Join(", ", columns.Select(c => $"[{c}]"));
		var paramList = string.Join(", ", columns.Select(c => $"@{c}"));

		var query = $"INSERT INTO [{tableName}] ({columnList}) VALUES ({paramList})";

		using var command = new SqlCommand(query, connection);

		// Add parameters for all columns
		foreach (var column in columns)
		{
			command.Parameters.AddWithValue($"@{column}", row[column] ?? DBNull.Value);
		}

		await command.ExecuteNonQueryAsync();
	}

	private static async Task UpdateRowAsync(SqlConnection connection, string tableName, DataRow row, string whereClause, List<string> columns, List<string> primaryKeyColumns)
	{
		var setColumns = columns.Except(primaryKeyColumns).ToList();
		var setClause = string.Join(", ", setColumns.Select(c => $"[{c}] = @{c}"));

		var query = $"UPDATE [{tableName}] SET {setClause} WHERE {whereClause}";

		using var command = new SqlCommand(query, connection);

		// Add parameters for all columns
		foreach (var column in columns)
		{
			command.Parameters.AddWithValue($"@{column}", row[column] ?? DBNull.Value);
		}

		await command.ExecuteNonQueryAsync();
	}

	/// <summary>
	/// Legacy synchronous method for backward compatibility
	/// </summary>
	public static void SyncData(string localDbConnectionString, string remoteDbConnectionString, List<string> tableNames)
	{
		SyncDataAsync(localDbConnectionString, remoteDbConnectionString, tableNames).GetAwaiter().GetResult();
	}
}

/// <summary>
/// Contains the results of a database synchronization operation
/// </summary>
public class SyncResult
{
	public Dictionary<string, TableSyncResult> TableResults { get; } = new();
	public bool HasErrors { get; set; }
}

/// <summary>
/// Contains synchronization results for a single table
/// </summary>
public class TableSyncResult
{
	public int PullInserts { get; set; }
	public int PullUpdates { get; set; }
	public int PushInserts { get; set; }
	public int PushUpdates { get; set; }
	public bool IsSuccess { get; set; }
	public string? ErrorMessage { get; set; }
}