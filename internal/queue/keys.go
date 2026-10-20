package queue

import "encoding/binary"

// Bucket layout:
//
//	jobs/<id>                          -> JSON-encoded Job
//	queues/<name>/<visibleAt><id>      -> one byte: the job's State
//	dead/<name>/<deadAt><id>           -> empty: the queue's dead-letter queue
//	counters/<name>/acked              -> uint64
//	meta/next_id                       -> uint64: last assigned job ID
//	meta/applied                       -> uint64: last applied Raft log index
//	meta/clock                         -> uint64: latest command time applied
//
// Each queue's index is sorted by (visibleAt, id), so the first key is always
// the job that became visible earliest. A claim moves the key forward by the
// visibility timeout; an expired lease therefore sorts back to the front on
// its own, with no background reaper.
var (
	bucketJobs     = []byte("jobs")
	bucketQueues   = []byte("queues")
	bucketDead     = []byte("dead")
	bucketCounters = []byte("counters")
	bucketMeta     = []byte("meta")

	keyNextID  = []byte("next_id")
	keyApplied = []byte("applied")
	keyClock   = []byte("clock")
	keyAcked   = []byte("acked")
)

func idKey(id uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, id)
}

func indexKey(visibleAt int64, id uint64) []byte {
	k := make([]byte, 0, 16)
	k = binary.BigEndian.AppendUint64(k, uint64(visibleAt))
	return binary.BigEndian.AppendUint64(k, id)
}

func parseIndexKey(k []byte) (visibleAt int64, id uint64) {
	return int64(binary.BigEndian.Uint64(k[:8])), binary.BigEndian.Uint64(k[8:16])
}

func getUint64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
