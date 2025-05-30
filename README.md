# <img src="https://github.com/user-attachments/assets/5b0a4a37-ecab-4ca6-b955-1a2bbccad0b4" />

# <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=25 /> GigAPI Metadata Engine

Gigapi Metadata provides a high-performance indexing system for managing metadata about data files (typically Parquet files) organized in time-partitioned structures. It supports efficient querying, merging operations, and provides both local JSON file storage and distributed Redis storage backends.

## Features

- **Dual Storage Backends**: JSON file-based storage for local deployments and Redis for distributed systems
- **Time-Partitioned Data**: Optimized for date/hour partitioned data structures [1](#0-0) 
- **Merge Planning**: Intelligent merge planning for data consolidation across different layers [2](#0-1) 
- **Async Operations**: Promise-based asynchronous operations for better performance [3](#0-2) 
- **Efficient Querying**: Time-range and folder-based querying capabilities [4](#0-3) 

## Installation

```bash
go get github.com/gigapi/metadata
```

## Core Concepts

### IndexEntry

The fundamental data structure representing metadata about a single data file: [5](#0-4) 

### Storage Backends

#### JSON Index
For local file-based storage, suitable for single-node deployments: [6](#0-5) 

#### Redis Index  
For distributed deployments with Redis backend: [7](#0-6) 

## Configuration

### Merge Configurations

Before using the library, initialize merge configurations which define merge behavior across different iterations: [8](#0-7) 

Example configuration:
```go
import "github.com/gigapi/metadata"

// Configure merge settings: [timeout_sec, max_size_bytes, iteration_id]
metadata.MergeConfigurations = [][3]int64{
    {10, 10 * 1024 * 1024, 1}, // 10s timeout, 10MB max size, iteration 1
    {30, 50 * 1024 * 1024, 2}, // 30s timeout, 50MB max size, iteration 2
}
```

## Usage Examples

### Basic JSON Index Usage

```go
// Create a JSON-based table index
tableIndex := metadata.NewJSONIndex("/data/root", "my_database", "my_table")

// Add metadata entries
entries := []*metadata.IndexEntry{
    {
        Database:  "my_database",
        Table:     "my_table", 
        Path:      "date=2024-01-15/hour=14/file1.parquet",
        SizeBytes: 1000000,
        MinTime:   1705327200000000000, // nanoseconds
        MaxTime:   1705327800000000000,
    },
}

// Batch operation (async)
promise := tableIndex.Batch(entries, nil)
result, err := promise.Get()
```

### Redis Index Usage [9](#0-8) 

### Querying Data

```go
// Query with time range
options := metadata.QueryOptions{
    After:  time.Now().Add(-24 * time.Hour),
    Before: time.Now(),
}

entries, err := tableIndex.GetQuerier().Query(options)
```

### Merge Operations

```go
// Get merge plan
planner := tableIndex.GetMergePlanner()
plan, err := planner.GetMergePlan("layer1", 1)

if plan != nil {
    // Execute merge (external process)
    // ...
    
    // Mark merge as complete
    err = planner.EndMerge(plan)
}
```

## Interfaces

### TableIndex Interface

The main interface for table-level operations: [10](#0-9) 

### DBIndex Interface  

For database-level operations: [11](#0-10) 

## Data Organization

The system expects data organized in the following structure:
```
/root/
  ├── database1/
  │   ├── table1/
  │   │   ├── date=2024-01-15/
  │   │   │   ├── hour=00/
  │   │   │   ├── hour=01/
  │   │   │   └── ...
  │   │   └── date=2024-01-16/
  │   └── table2/
  └── database2/
```

## Redis Configuration

For Redis backend, use standard Redis connection URLs:
- `redis://localhost:6379/0` - Standard Redis
- `rediss://user:pass@host:6380/1` - Redis with TLS [12](#0-11) 

## Error Handling

All operations return errors through the Promise interface or standard Go error handling. The library uses async operations for better performance in high-throughput scenarios.

## Thread Safety

Both JSON and Redis implementations are thread-safe and can be used concurrently across multiple goroutines.

## Testing

Run tests with a local Redis instance:
```bash
# Start Redis
docker run -d -p 6379:6379 redis:alpine

# Run tests  
go test ./...
```

## License

This project is licensed under the Apache License 2.0. [13](#0-12) 

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Notes

- The library is optimized for time-series data workloads with frequent writes and time-range queries
- Redis backend is recommended for distributed deployments and high-throughput scenarios
- JSON backend is suitable for single-node deployments and development environments
- Merge operations are designed to be executed by external processes, with the library managing the planning and coordination
- All time values are stored as Unix nanoseconds for high precision temporal operations
