# BaseSync

![NuGet Version](https://img.shields.io/nuget/v/BaseSync.svg?style=flat&label=NuGet)
![Downloads](https://img.shields.io/nuget/dt/BaseSync.svg?style=flat&label=Downloads)
![License](https://img.shields.io/github/license/aadipoddar/BaseSync)
![.NET](https://img.shields.io/badge/.NET-9.0-512BD4)

A high-performance .NET library for bidirectional synchronization between online and offline SQL Server databases. BaseSync efficiently handles large datasets with 100,000+ rows through optimized bulk operations.

## ✨ Features

- **Bidirectional Synchronization**: Pull from remote to local, then push from local to remote
- **High-Performance Design**: Optimized for large tables (100,000+ rows) using:
  - Bulk insert operations with `SqlBulkCopy`
  - Set-based updates via temporary tables
  - Schema information caching
- **Intelligent Comparison**: Only updates records that have actually changed
- **Zero Configuration**: Automatically detects primary keys and table schemas
- **Detailed Reports**: Comprehensive statistics for sync operations
- **Error Handling**: Robust error handling with detailed feedback
- **Async First**: Built with modern async patterns

## 📦 Installation

Install via NuGet Package Manager:

```
Install-Package BaseSync
```

Or via .NET CLI:

```
dotnet add package BaseSync
```

## 🚀 Quick Start


```cs
using BaseSync;

// Define connection strings
string localDb = "Server=localhost;Database=LocalDB;Trusted_Connection=True;TrustServerCertificate=True;";
string remoteDb = "Server=remote-server;Database=RemoteDB;User Id=user;Password=password;TrustServerCertificate=True;";

// Specify tables to synchronize
var tables = new List<string> { "Customers", "Orders", "Products" };

// Perform synchronization
SyncResult result = await BaseSync.SyncDataAsync(localDb, remoteDb, tables);

// Check results
foreach (var tableResult in result.TableResults) {
	Console.WriteLine($"Table: {tableResult.Key}");
	if (tableResult.Value.IsSuccess) {
		Console.WriteLine($"  Pull: {tableResult.Value.PullInserts} inserts, {tableResult.Value.PullUpdates} updates");
		Console.WriteLine($"  Push: {tableResult.Value.PushInserts} inserts, {tableResult.Value.PushUpdates} updates");
	} else {
		Console.WriteLine($"  Error: {tableResult.Value.ErrorMessage}");
	}
}
```


## 🔄 How It Works

BaseSync uses a sophisticated process to efficiently synchronize data:

1. **Schema Discovery**: Automatically detects primary keys and columns for each table
2. **Schema Caching**: Caches schema information to minimize database roundtrips
3. **Data Retrieval**: Loads data from both source and destination databases
4. **Memory-Efficient Comparison**: Creates dictionaries of destination rows indexed by primary key for O(1) lookups
5. **Change Detection**: Identifies which rows need to be inserted or updated
6. **Bulk Operations**:
   - Uses `SqlBulkCopy` for fast insertion of new rows
   - Creates temporary tables and performs set-based updates for changed rows
7. **Bidirectional Sync**: First pulls from remote to local, then pushes from local to remote

## 📊 Performance Optimizations

BaseSync is engineered to handle large datasets efficiently:

- **Reduced Database Roundtrips**: Performs bulk operations instead of row-by-row processing
- **Memory Management**: Efficiently manages large datasets in memory
- **Set-Based Operations**: Uses SQL's native strengths for updates via temp tables
- **Caching**: Minimizes redundant schema queries

## 🛠️ Requirements

- .NET 9.0 or higher
- SQL Server databases (source and destination)
- Tables must have primary keys defined

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👨‍💻 About the Author

BaseSync is developed and maintained by [AadiSoft](https://aadi.vercel.app/). 

## 📫 Contact

For any inquiries, please reach out to [contact@aadi.vercel.app](mailto:aadipoddarmail@gmail.com).

---

<div align="center">
  <sub>Built with ❤️ by AadiSoft</sub>
</div>