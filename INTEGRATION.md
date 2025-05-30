# INTEGRATION

## Redis Integration Guide for Python

This guide demonstrates how to implement the Gigapi Metadata Redis operations in Python, based on the Go implementation patterns found in the test suite.

### Prerequisites

```bash
pip install redis python-dateutil uuid
```

### Core Data Structures

First, let's define the Python equivalents of the Go data structures:

```python
import json
import time
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import redis

class IndexEntry:
    def __init__(self, database: str, table: str, path: str, 
                 size_bytes: int, min_time: int, max_time: int,
                 layer: str = "", row_count: int = 0, chunk_time: int = 0,
                 min_data: Dict[str, Any] = None, max_data: Dict[str, Any] = None):
        self.layer = layer
        self.database = database
        self.table = table
        self.path = path
        self.size_bytes = size_bytes
        self.row_count = row_count
        self.chunk_time = chunk_time
        self.min = min_data or {}
        self.max = max_data or {}
        self.min_time = min_time
        self.max_time = max_time

class RedisIndexEntry:
    def __init__(self, index_entry: IndexEntry, cmd: str):
        self.index_entry = index_entry
        self.str_min_time = str(index_entry.min_time)
        self.str_max_time = str(index_entry.max_time)
        self.str_chunk_time = str(index_entry.chunk_time)
        self.cmd = cmd
    
    def to_dict(self):
        return {
            "layer": self.index_entry.layer,
            "database": self.index_entry.database,
            "table": self.index_entry.table,
            "path": self.index_entry.path,
            "size_bytes": self.index_entry.size_bytes,
            "row_count": self.index_entry.row_count,
            "chunk_time": 0,  # Set to 0 as in Go implementation
            "min": self.index_entry.min,
            "max": self.index_entry.max,
            "min_time": 0,    # Set to 0 as in Go implementation
            "max_time": 0,    # Set to 0 as in Go implementation
            "str_min_time": self.str_min_time,
            "str_max_time": self.str_max_time,
            "str_chunk_time": self.str_chunk_time,
            "cmd": self.cmd
        }
```

### Redis Connection Setup

Based on the connection pattern in [2](#1-1) :

```python
from urllib.parse import urlparse
import ssl

class RedisConnection:
    def __init__(self, url: str):
        self.url = urlparse(url)
        self.client = self._create_client()
        self._load_scripts()
    
    def _create_client(self) -> redis.Redis:
        # Extract database number from path
        db_str = self.url.path.strip('/')
        db_num = int(db_str) if db_str else 0
        
        # Basic connection options
        kwargs = {
            'host': self.url.hostname,
            'port': self.url.port or 6379,
            'db': db_num,
            'decode_responses': False  # Keep as bytes for consistency with Go
        }
        
        # Add authentication if present
        if self.url.username:
            kwargs['username'] = self.url.username
        if self.url.password:
            kwargs['password'] = self.url.password
            
        # Handle TLS for rediss://
        if self.url.scheme == 'rediss':
            kwargs['ssl'] = True
            kwargs['ssl_cert_reqs'] = ssl.CERT_NONE  # InsecureSkipVerify equivalent
            
        return redis.Redis(**kwargs)
    
    def _load_scripts(self):
        """Load Lua scripts equivalent to Go's initFuncs()"""
        # These would contain the Lua scripts from redis_scripts/
        self.patch_sha = self.client.script_load(PATCH_INDEX_SCRIPT)
        self.get_merge_plan_sha = self.client.script_load(GET_MERGE_PLAN_SCRIPT)
        self.end_merge_sha = self.client.script_load(END_MERGE_SCRIPT)
```

### Core Operations Implementation

Following the pattern from [3](#1-2) :

```python
class RedisIndex:
    def __init__(self, url: str, database: str, table: str):
        self.connection = RedisConnection(url)
        self.database = database
        self.table = table
        
        # Merge configurations equivalent to Go's MergeConfigurations
        self.merge_configurations = [
            [10, 10 * 1024 * 1024, 1],  # [timeout_sec, max_size, iteration]
        ]
    
    def batch(self, add_entries: List[IndexEntry], rm_entries: List[IndexEntry] = None) -> int:
        """Equivalent to Go's Batch method"""
        if rm_entries is None:
            rm_entries = []
            
        commands = []
        
        # Process additions
        for entry in add_entries:
            redis_entry = RedisIndexEntry(entry, "ADD")
            commands.append(json.dumps(redis_entry.to_dict()))
        
        # Process removals
        for entry in rm_entries:
            redis_entry = RedisIndexEntry(entry, "DELETE")
            commands.append(json.dumps(redis_entry.to_dict()))
        
        # Prepare keys (size limits from merge configurations)
        keys = [str(config[1]) for config in self.merge_configurations]
        
        # Execute the patch script
        result = self.connection.client.evalsha(
            self.connection.patch_sha,
            len(keys),
            *keys,
            *commands
        )
        
        return 0  # Success indicator
    
    def get(self, path: str) -> Optional[IndexEntry]:
        """Equivalent to Go's Get method"""
        first_folder = path.split('/')[0]
        main_key = f"files:{self.database}:{self.table}:{first_folder}"
        
        try:
            result = self.connection.client.hget(main_key, path)
            if result:
                data = json.loads(result)
                # Convert back to IndexEntry
                return IndexEntry(
                    database=data['database'],
                    table=data['table'],
                    path=data['path'],
                    size_bytes=data['size_bytes'],
                    min_time=int(data.get('str_min_time', 0)),
                    max_time=int(data.get('str_max_time', 0)),
                    layer=data.get('layer', ''),
                    row_count=data.get('row_count', 0),
                    chunk_time=int(data.get('str_chunk_time', 0)),
                    min_data=data.get('min', {}),
                    max_data=data.get('max', {})
                )
        except (redis.RedisError, json.JSONDecodeError, ValueError):
            return None
        
        return None
```

### Test Implementation

Based on [1](#1-0) :

```python
def test_redis_integration():
    """Python equivalent of Go's TestSave function"""
    
    # Initialize Redis index
    redis_index = RedisIndex("redis://localhost:6379/0", "default", "test")
    
    # Generate test data (equivalent to Go test)
    entries = []
    now = datetime.now()
    three_days_ago = now - timedelta(days=3)
    
    current_time = three_days_ago
    while current_time < now:
        # Create path in same format as Go test
        path = f"date={current_time.strftime('%Y-%m-%d')}/hour={current_time.hour:02d}/{uuid.uuid4()}.1.parquet"
        
        entry = IndexEntry(
            database="default",
            table="test",
            path=path,
            size_bytes=1000000,
            min_time=int(current_time.timestamp() * 1_000_000_000),  # Convert to nanoseconds
            max_time=int((current_time + timedelta(seconds=15)).timestamp() * 1_000_000_000)
        )
        entries.append(entry)
        
        current_time += timedelta(seconds=15)
    
    # Execute batch operation
    result = redis_index.batch(entries)
    print(f"Items saved: {len(entries)}")
    
    # Test retrieval
    if entries:
        retrieved = redis_index.get(entries[0].path)
        if retrieved:
            print(f"Successfully retrieved: {retrieved.path}")
        else:
            print("Failed to retrieve entry")

if __name__ == "__main__":
    test_redis_integration()
```

### Query Operations

Following the pattern from [4](#1-3) :

```python
class QueryOptions:
    def __init__(self, folder: str = "", after: datetime = None, 
                 before: datetime = None, iteration: int = 0):
        self.folder = folder
        self.after = after or datetime.min
        self.before = before or datetime.max
        self.iteration = iteration

def query(self, options: QueryOptions) -> List[IndexEntry]:
    """Equivalent to Go's Query method"""
    main_keys = self._get_main_keys(options)
    results = []
    
    for main_key in main_keys:
        # Extract date from key for filtering
        key_parts = main_key.split(':')
        if len(key_parts) >= 4:
            date_str = key_parts[3][5:]  # Remove 'date=' prefix
            try:
                day = datetime.strptime(date_str, '%Y-%m-%d')
                
                # Scan hash fields
                cursor = 0
                while True:
                    cursor, fields = self.connection.client.hscan(
                        main_key, cursor, count=10000
                    )
                    
                    # Filter and process fields
                    filtered_fields = self._filter_keys(fields, day, options)
                    entries = self._filter_values(filtered_fields, options)
                    results.extend(entries)
                    
                    if cursor == 0:
                        break
                        
            except ValueError:
                continue
    
    return results
```

### Lua Scripts

The system relies on Lua scripts for atomic operations. You'll need to implement the equivalent of [5](#1-4) :


### Usage Example

```python
# Initialize the Redis index
redis_index = RedisIndex("redis://localhost:6379/0", "analytics", "events")

# Create some sample data
entries = [
    IndexEntry(
        database="analytics",
        table="events",
        path="date=2024-01-15/hour=14/event_batch_001.1.parquet",
        size_bytes=5000000,
        min_time=int(datetime(2024, 1, 15, 14, 0).timestamp() * 1_000_000_000),
        max_time=int(datetime(2024, 1, 15, 14, 59).timestamp() * 1_000_000_000)
    )
]

# Save to Redis
redis_index.batch(entries)

# Query data
options = QueryOptions(
    after=datetime(2024, 1, 15),
    before=datetime(2024, 1, 16)
)
results = redis_index.query(options)
```

## Notes

- The Python implementation maintains the same Redis key structure as the Go version for compatibility
- Lua scripts ensure atomic operations just like in the Go implementation
- Time values are handled as nanoseconds to match the Go precision
- The async Promise pattern from Go can be implemented using Python's `asyncio` if needed
- Error handling should follow the same patterns as the Go implementation for consistency

Wiki pages you might want to explore:
- [Architecture Overview (gigapi/metadata)](/wiki/gigapi/metadata#2)
- [Advanced Features (gigapi/metadata)](/wiki/gigapi/metadata#6)
