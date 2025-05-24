using System.Collections.Concurrent;
using System.Data;
using System.Text;

using Microsoft.Data.SqlClient;

namespace BaseSync;

public static class BaseSync
{
	// Caches for schema information
	private static readonly ConcurrentDictionary<string, Task<List<string>>> _primaryKeyCache = new();
	private static readonly ConcurrentDictionary<string, Task<List<string>>> _tableColumnCache = new();

	/// <summary>
	/// Synchronizes data between local and remote databases.
	/// First pulls changes from remote to local, then pushes local changes to remote.
	/// </summary>
	public static async Task<SyncResult> SyncDataAsync(string localDbConnectionString, string remoteDbConnectionString, List<string> tableNames)
	{
		var result = new SyncResult();

		foreach (var tableName in tableNames)
		{
			try
			{
				// Pull from remote to local
				var pullStats = await SynchronizeTableAsync(remoteDbConnectionString, localDbConnectionString, tableName);
				result.TableResults.Add(tableName, new TableSyncResult
				{
					PullInserts = pullStats.inserted,
					PullUpdates = pullStats.updated,
					IsSuccess = true
				});

				// Push from local to remote
				var (inserted, updated) = await SynchronizeTableAsync(localDbConnectionString, remoteDbConnectionString, tableName);
				result.TableResults[tableName].PushInserts = inserted;
				result.TableResults[tableName].PushUpdates = updated;
			}
			catch (Exception ex)
			{
				if (result.TableResults.TryGetValue(tableName, out var tableSyncResult))
				{
					tableSyncResult.IsSuccess = false;
					tableSyncResult.ErrorMessage = (tableSyncResult.ErrorMessage ?? "") + $"Error during {(tableSyncResult.PullInserts > 0 || tableSyncResult.PullUpdates > 0 ? "Push" : "Pull")}: {ex.Message}";
				}
				else
				{
					result.TableResults.Add(tableName, new TableSyncResult
					{
						IsSuccess = false,
						ErrorMessage = ex.Message
					});
				}
				result.HasErrors = true;
			}
		}
		return result;
	}

	private static async Task<(int inserted, int updated)> SynchronizeTableAsync(
		string sourceDbConnectionString, string destinationDbConnectionString,
		string tableName)
	{
		int insertedCount = 0;
		int updatedCount = 0;

		var primaryKeyColumns = await GetPrimaryKeyColumnsAsync(sourceDbConnectionString, tableName);
		if (primaryKeyColumns.Count == 0)
		{
			throw new Exception($"No primary key found for table {tableName}. Synchronization requires a primary key.");
		}
		var allColumns = await GetTableColumnsAsync(sourceDbConnectionString, tableName);

		using var sourceConnection = new SqlConnection(sourceDbConnectionString);
		using var destinationConnection = new SqlConnection(destinationDbConnectionString);
		await sourceConnection.OpenAsync();
		await destinationConnection.OpenAsync();

		DataTable sourceData = await GetTableDataAsync(sourceConnection, tableName, allColumns);
		DataTable destinationData = await GetTableDataAsync(destinationConnection, tableName, allColumns);

		var destinationRowsMap = new Dictionary<string, DataRow>();
		foreach (DataRow row in destinationData.Rows)
		{
			destinationRowsMap[GetCompositeKeyValue(row, primaryKeyColumns)] = row;
		}

		var rowsToInsert = sourceData.Clone();
		var rowsToUpdate = sourceData.Clone();

		foreach (DataRow sourceRow in sourceData.Rows)
		{
			var key = GetCompositeKeyValue(sourceRow, primaryKeyColumns);
			if (destinationRowsMap.TryGetValue(key, out DataRow? destinationRow))
			{
				if (RowsAreDifferent(sourceRow, destinationRow, allColumns))
				{
					rowsToUpdate.ImportRow(sourceRow);
				}
			}
			else
			{
				rowsToInsert.ImportRow(sourceRow);
			}
		}

		// Bulk Insert
		if (rowsToInsert.Rows.Count > 0)
		{
			await BulkInsertAsync(destinationConnection, tableName, rowsToInsert);
			insertedCount = rowsToInsert.Rows.Count;
		}

		// Bulk Update
		if (rowsToUpdate.Rows.Count > 0)
		{
			await BulkUpdateAsync(destinationConnection, tableName, rowsToUpdate, allColumns, primaryKeyColumns);
			updatedCount = rowsToUpdate.Rows.Count;
		}

		return (insertedCount, updatedCount);
	}

	private static string GetCompositeKeyValue(DataRow row, List<string> primaryKeyColumns)
	{
		// Simple composite key by joining PK values with a separator.
		// Ensure separator doesn't appear in key values or use a more robust method.
		return string.Join("||", primaryKeyColumns.Select(pkCol => row[pkCol]?.ToString() ?? "NULL"));
	}

	private static async Task BulkInsertAsync(SqlConnection connection, string tableName, DataTable dataTable)
	{
		using var bulkCopy = new SqlBulkCopy(connection);
		bulkCopy.DestinationTableName = $"[{tableName}]";
		foreach (DataColumn col in dataTable.Columns)
		{
			bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
		}
		await bulkCopy.WriteToServerAsync(dataTable);
	}

	private static async Task BulkUpdateAsync(SqlConnection connection, string tableName, DataTable dataTable, List<string> allColumns, List<string> primaryKeyColumns)
	{
		string tempTableName = $"#TempUpdate_{tableName}_{Guid.NewGuid():N}";

		// Create temp table
		var columnsToSet = allColumns.Except(primaryKeyColumns).ToList();
		var createTempTableCommandText = new StringBuilder($"SELECT TOP 0 ");
		createTempTableCommandText.Append(string.Join(", ", allColumns.Select(c => $"[{c}]")));
		createTempTableCommandText.Append($" INTO [{tempTableName}] FROM [{tableName}]");

		using (var cmdCreate = new SqlCommand(createTempTableCommandText.ToString(), connection))
		{
			await cmdCreate.ExecuteNonQueryAsync();
		}

		// Bulk copy data to temp table
		using (var bulkCopy = new SqlBulkCopy(connection))
		{
			bulkCopy.DestinationTableName = tempTableName;
			foreach (DataColumn col in dataTable.Columns)
			{
				bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
			}
			await bulkCopy.WriteToServerAsync(dataTable);
		}

		// Update actual table from temp table
		var updateSql = new StringBuilder($"UPDATE dest SET ");
		updateSql.Append(string.Join(", ", columnsToSet.Select(c => $"dest.[{c}] = tmp.[{c}]")));
		updateSql.Append($" FROM [{tableName}] dest INNER JOIN [{tempTableName}] tmp ON ");
		updateSql.Append(string.Join(" AND ", primaryKeyColumns.Select(pk => $"dest.[{pk}] = tmp.[{pk}]")));

		using (var cmdUpdate = new SqlCommand(updateSql.ToString(), connection))
		{
			await cmdUpdate.ExecuteNonQueryAsync();
		}

		// Drop temp table
		using (var cmdDrop = new SqlCommand($"DROP TABLE [{tempTableName}]", connection))
		{
			await cmdDrop.ExecuteNonQueryAsync();
		}
	}

	private static Task<List<string>> GetPrimaryKeyColumnsAsync(string connectionString, string tableName)
	{
		return _primaryKeyCache.GetOrAdd($"{connectionString}_{tableName}", async (_) =>
		{
			var pks = new List<string>();
			using var connection = new SqlConnection(connectionString);
			await connection.OpenAsync();
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
				pks.Add(reader.GetString(0));
			}
			return pks;
		});
	}

	private static Task<List<string>> GetTableColumnsAsync(string connectionString, string tableName)
	{
		return _tableColumnCache.GetOrAdd($"{connectionString}_{tableName}", async (_) =>
		{
			var cols = new List<string>();
			using var connection = new SqlConnection(connectionString);
			await connection.OpenAsync();
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
				cols.Add(reader.GetString(0));
			}
			return cols;
		});
	}

	private static async Task<DataTable> GetTableDataAsync(SqlConnection connection, string tableName, List<string> columns)
	{
		var dataTable = new DataTable(tableName); // Give DataTable a name for clarity
		var columnList = string.Join(", ", columns.Select(c => $"[{c}]"));
		var query = $"SELECT {columnList} FROM [{tableName}]";

		using var command = new SqlCommand(query, connection);
		using var adapter = new SqlDataAdapter(command);
		// Fill is blocking, Task.Run offloads it.
		// For very large tables, consider SqlDataReader for streaming if memory becomes an issue.
		await Task.Run(() => adapter.Fill(dataTable));
		return dataTable;
	}

	private static bool RowsAreDifferent(DataRow row1, DataRow row2, List<string> columns)
	{
		foreach (var column in columns)
		{
			var val1 = row1[column];
			var val2 = row2[column];

			// Using object.Equals for a more robust comparison than ToString()
			if (!object.Equals(val1, val2))
			{
				// Special handling for byte arrays, as object.Equals might compare references.
				if (val1 is byte[] b1 && val2 is byte[] b2)
				{
					if (!b1.SequenceEqual(b2)) return true;
				}
				// If one is DBNull and the other isn't, they are different.
				// object.Equals handles one DBNull correctly, but not if both are DBNull (returns true).
				// However, if both are DBNull, object.Equals(DBNull.Value, DBNull.Value) is true, which is fine.
				// The main case is one is DBNull and the other is not.
				else if ((val1 == DBNull.Value && val2 != DBNull.Value) || (val1 != DBNull.Value && val2 == DBNull.Value))
				{
					return true;
				}
				else if (!(val1 == DBNull.Value && val2 == DBNull.Value)) // If not both DBNull, and not equal
				{
					return true;
				}
			}
		}
		return false;
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