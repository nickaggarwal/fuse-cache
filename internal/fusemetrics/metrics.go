package fusemetrics

import "sync/atomic"

type GoFuseMetricsSnapshot struct {
	PassthroughEnabled     int64
	WritebackCacheEnabled  int64
	MaxWriteBytes          int64
	WriteAppendBufferBytes int64
	WriteSummaries         int64
	WriteBytes             int64
	WriteCalls             int64
	StageWriteCalls        int64
	StageWriteBytes        int64
	BufferFlushCalls       int64
	WritePhaseNanos        int64
	BufferFlushNanos       int64
	SyncNanos              int64
	PutNanos               int64
	FlushTotalNanos        int64
}

type goFuseMetrics struct {
	passthroughEnabled     atomic.Int64
	writebackCacheEnabled  atomic.Int64
	maxWriteBytes          atomic.Int64
	writeAppendBufferBytes atomic.Int64
	writeSummaries         atomic.Int64
	writeBytes             atomic.Int64
	writeCalls             atomic.Int64
	stageWriteCalls        atomic.Int64
	stageWriteBytes        atomic.Int64
	bufferFlushCalls       atomic.Int64
	writePhaseNanos        atomic.Int64
	bufferFlushNanos       atomic.Int64
	syncNanos              atomic.Int64
	putNanos               atomic.Int64
	flushTotalNanos        atomic.Int64
}

var globalGoFuseMetrics goFuseMetrics

func RecordGoFuseMountConfig(passthrough bool, writeback bool, maxWriteBytes int, writeAppendBufferBytes int) {
	globalGoFuseMetrics.maxWriteBytes.Store(int64(maxWriteBytes))
	globalGoFuseMetrics.writeAppendBufferBytes.Store(int64(writeAppendBufferBytes))
	if passthrough {
		globalGoFuseMetrics.passthroughEnabled.Store(1)
	} else {
		globalGoFuseMetrics.passthroughEnabled.Store(0)
	}
	if writeback {
		globalGoFuseMetrics.writebackCacheEnabled.Store(1)
	} else {
		globalGoFuseMetrics.writebackCacheEnabled.Store(0)
	}
}

func RecordGoFuseWriteSummary(writeBytes int64, writeCalls int64, stageWriteCalls int64, stageWriteBytes int64, bufferFlushCalls int64, writePhaseNanos int64, bufferFlushNanos int64, syncNanos int64, putNanos int64, flushTotalNanos int64) {
	globalGoFuseMetrics.writeSummaries.Add(1)
	globalGoFuseMetrics.writeBytes.Add(writeBytes)
	globalGoFuseMetrics.writeCalls.Add(writeCalls)
	globalGoFuseMetrics.stageWriteCalls.Add(stageWriteCalls)
	globalGoFuseMetrics.stageWriteBytes.Add(stageWriteBytes)
	globalGoFuseMetrics.bufferFlushCalls.Add(bufferFlushCalls)
	globalGoFuseMetrics.writePhaseNanos.Add(writePhaseNanos)
	globalGoFuseMetrics.bufferFlushNanos.Add(bufferFlushNanos)
	globalGoFuseMetrics.syncNanos.Add(syncNanos)
	globalGoFuseMetrics.putNanos.Add(putNanos)
	globalGoFuseMetrics.flushTotalNanos.Add(flushTotalNanos)
}

func Snapshot() GoFuseMetricsSnapshot {
	return GoFuseMetricsSnapshot{
		PassthroughEnabled:     globalGoFuseMetrics.passthroughEnabled.Load(),
		WritebackCacheEnabled:  globalGoFuseMetrics.writebackCacheEnabled.Load(),
		MaxWriteBytes:          globalGoFuseMetrics.maxWriteBytes.Load(),
		WriteAppendBufferBytes: globalGoFuseMetrics.writeAppendBufferBytes.Load(),
		WriteSummaries:         globalGoFuseMetrics.writeSummaries.Load(),
		WriteBytes:             globalGoFuseMetrics.writeBytes.Load(),
		WriteCalls:             globalGoFuseMetrics.writeCalls.Load(),
		StageWriteCalls:        globalGoFuseMetrics.stageWriteCalls.Load(),
		StageWriteBytes:        globalGoFuseMetrics.stageWriteBytes.Load(),
		BufferFlushCalls:       globalGoFuseMetrics.bufferFlushCalls.Load(),
		WritePhaseNanos:        globalGoFuseMetrics.writePhaseNanos.Load(),
		BufferFlushNanos:       globalGoFuseMetrics.bufferFlushNanos.Load(),
		SyncNanos:              globalGoFuseMetrics.syncNanos.Load(),
		PutNanos:               globalGoFuseMetrics.putNanos.Load(),
		FlushTotalNanos:        globalGoFuseMetrics.flushTotalNanos.Load(),
	}
}
