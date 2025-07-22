# FUSE Directory Listing Infinite Loop Fix

## Problem Description

The original FUSE implementation had a critical infinite loop issue in directory listing operations (`ls`, `ReadDirAll`). Here's what was happening:

1. User runs `ls` command → FUSE `ReadDirAll` called
2. `ReadDirAll` calls `cacheManager.List()`  
3. `List()` calls `listFromPeers()` to query all peers
4. Each peer receives HTTP request to `/api/cache` endpoint
5. Peer's API handler calls `cacheManager.List()` again → **INFINITE LOOP**

## Solution: Snapshot-Based Approach

Instead of querying peers on-demand during FUSE operations, we now maintain directory snapshots ahead of time with background synchronization.

### Key Changes

#### 1. New SnapshotManager (`internal/cache/snapshot.go`)
- Maintains local directory snapshot with background peer synchronization
- Periodic sync with other peers (default: 30-second intervals)
- Uses special `/api/cache/local` endpoint to prevent recursive calls
- Thread-safe with proper mutex locking

#### 2. Updated CacheManager (`internal/cache/cache.go`)
- Integrated `SnapshotManager` for directory listings
- `List()` now returns local snapshot immediately (no peer queries)
- Added `ListAll()` for full snapshot including peer entries (use carefully)
- Removed old recursive peer querying methods
- Added proper shutdown handling

#### 3. New API Endpoint (`internal/api/handler.go`)
- `/api/cache/local` - Returns only local entries (prevents recursion)
- `/api/cache/snapshot-status` - Shows sync status and statistics
- Updated `/api/cache` to use new snapshot approach

#### 4. Enhanced Shutdown Process (`cmd/client/main.go`)
- Graceful shutdown of background synchronization
- Added `Shutdown()` method to `CacheManager` interface

## Architecture Overview

```
┌─────────────────┐    Background Sync     ┌─────────────────┐
│   Peer A        │◄──────────────────────►│   Peer B        │
│                 │    (30s intervals)     │                 │ 
│ SnapshotManager │                        │ SnapshotManager │
│ ├─ Local Cache  │                        │ ├─ Local Cache  │
│ └─ Peer Cache   │                        │ └─ Peer Cache   │
└─────────────────┘                        └─────────────────┘
         ▲                                           ▲
         │                                           │
         │              ┌─────────────────┐         │
         └──────────────►│  Coordinator    │◄────────┘
                         │ (Peer Registry) │
                         └─────────────────┘

FUSE Operations (ls, readdir):
┌──────────┐    ┌─────────────┐    ┌──────────────┐
│ User 'ls'│───►│ FUSE ReadDir│───►│ Local        │
└──────────┘    └─────────────┘    │ Snapshot     │
                                   │ (Immediate)  │
                                   └──────────────┘
```

## Benefits

1. **No More Infinite Loops**: Directory operations are now instant and safe
2. **Better Performance**: Local snapshots provide immediate responses
3. **Background Sync**: Peer data synchronized in background without blocking users
4. **Graceful Degradation**: Works even if peers are temporarily unavailable
5. **Reduced Network Traffic**: Periodic sync vs per-operation queries

## Configuration

- **Sync Interval**: 30 seconds (configurable in `NewSnapshotManager`)
- **Timeout**: 30 seconds for peer sync operations
- **API Endpoints**:
  - `/api/cache` - Standard cache listing (local entries)
  - `/api/cache/local` - Explicitly local entries only
  - `/api/cache/snapshot-status` - Sync status information

## Testing

The fix has been implemented and both client and coordinator compile successfully:

```bash
go build -o bin/client cmd/client/main.go      ✓ 
go build -o bin/coordinator cmd/coordinator/main.go  ✓
```

## Migration Notes

- **Breaking Change**: Old recursive peer querying removed
- **New Methods**: `ListAll()` method available for full peer data
- **Shutdown**: Proper cleanup required (`cacheManager.Shutdown()`)
- **API**: New `/api/cache/local` endpoint required for peer sync

## Future Improvements

1. **Configurable Sync Intervals**: Make sync frequency adjustable per deployment
2. **Delta Sync**: Only synchronize changed entries to reduce bandwidth
3. **Conflict Resolution**: Handle cases where multiple peers have different versions
4. **Metrics**: Add detailed sync performance and error metrics
5. **Health Checks**: Monitor peer sync health and alert on failures 