using BaseSync;

namespace BaseSyncTest;

class Program
{
	// Replace these with your actual connection strings
	private const string LocalDbConnectionString = "Server=localhost;Database=LocalDB;Trusted_Connection=True;TrustServerCertificate=True;";
	private const string RemoteDbConnectionString = "Server=remote-server;Database=RemoteDB;User Id=user;Password=password;TrustServerCertificate=True;";

	static async Task Main(string[] args)
	{
		Console.WriteLine("Database Synchronization Tool");
		Console.WriteLine("=============================");

		try
		{
			// List of tables to synchronize
			var tablesToSync = new List<string>
			{
				"Customers",
				"Orders",
				"Products"
                // Add more tables as needed
			};

			Console.WriteLine($"Starting synchronization of {tablesToSync.Count} tables...");
			Console.WriteLine("First pulling from remote to local, then pushing from local to remote\n");

			// Perform the synchronization
			var result = await BaseSync.BaseSync.SyncDataAsync(LocalDbConnectionString, RemoteDbConnectionString, tablesToSync);

			// Display results
			DisplaySyncResults(result);
		}
		catch (Exception ex)
		{
			Console.WriteLine($"\nError: {ex.Message}");
			if (ex.InnerException != null)
			{
				Console.WriteLine($"Inner Error: {ex.InnerException.Message}");
			}
			Console.WriteLine("\nStack Trace:");
			Console.WriteLine(ex.StackTrace);
		}

		Console.WriteLine("\nPress any key to exit...");
		Console.ReadKey();
	}

	private static void DisplaySyncResults(SyncResult result)
	{
		Console.WriteLine("\nSynchronization Results:");
		Console.WriteLine("=======================");

		foreach (var tableResult in result.TableResults)
		{
			Console.WriteLine($"\nTable: {tableResult.Key}");
			if (tableResult.Value.IsSuccess)
			{
				Console.WriteLine("  Status: Success");
				Console.WriteLine("  Pull from Remote to Local:");
				Console.WriteLine($"    - Inserted: {tableResult.Value.PullInserts} rows");
				Console.WriteLine($"    - Updated: {tableResult.Value.PullUpdates} rows");
				Console.WriteLine("  Push from Local to Remote:");
				Console.WriteLine($"    - Inserted: {tableResult.Value.PushInserts} rows");
				Console.WriteLine($"    - Updated: {tableResult.Value.PushUpdates} rows");
				Console.WriteLine($"  Total Changes: {tableResult.Value.PullInserts + tableResult.Value.PullUpdates + tableResult.Value.PushInserts + tableResult.Value.PushUpdates} rows");
			}
			else
			{
				Console.WriteLine("  Status: Failed");
				Console.WriteLine($"  Error: {tableResult.Value.ErrorMessage}");
			}
		}

		Console.WriteLine($"\nOverall Status: {(result.HasErrors ? "Completed with errors" : "Successful")}");
	}
}
