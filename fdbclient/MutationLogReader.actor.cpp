/*
 * MutationLogReader.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/MutationLogReader.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/UnitTest.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

Key versionToKey(Version version, Key prefix) {
	uint64_t versionBigEndian = bigEndian64(version);
	return KeyRef((uint8_t*)&versionBigEndian, sizeof(uint64_t)).withPrefix(prefix);
}

Version keyRefToVersion(Key key, Key prefix) {
	Key keyWithoutPrefix = key.removePrefix(prefix);
	return (Version)bigEndian64(*((uint64_t*)keyWithoutPrefix.begin()));
}

Standalone<RangeResultRef> RangeResultBlock::consume(Key prefix) {
	Version stopVersion =
		std::min(lastVersion,
					(firstVersion + CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE - 1) / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE *
						CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE) + 1; // firstVersion rounded up to the nearest 1M versions, then + 1
	int startIndex = indexToRead;
	while (indexToRead < result.size() && keyRefToVersion(result[indexToRead].key, prefix) < stopVersion) {
		++indexToRead;
	}
	if (indexToRead < result.size()) {
		firstVersion = keyRefToVersion(result[indexToRead].key, prefix); // the version of result[indexToRead]
	}
	return Standalone<RangeResultRef>(
		RangeResultRef(result.slice(startIndex, indexToRead), result.more, result.readThrough), result.arena());
}

void PipelinedReader::startReading(Database cx) {
	reader = getNext(cx);
}

Future<Void> PipelinedReader::getNext(Database cx) {
	return getNext_impl(this, cx);
}

ACTOR Future<Void> PipelinedReader::getNext_impl(PipelinedReader* self, Database cx) {
	state Transaction tr(cx);

	state GetRangeLimits limits(GetRangeLimits::ROW_LIMIT_UNLIMITED,
	                            (g_network->isSimulated() && !g_simulator.speedUpSimulation)
	                                ? CLIENT_KNOBS->BACKUP_SIMULATED_LIMIT_BYTES
	                                : CLIENT_KNOBS->BACKUP_GET_RANGE_LIMIT_BYTES);

	state RangeResultBlock previousResult = RangeResultBlock{ .result = RangeResult(),
		                                                              .firstVersion = self->currentBeginVersion,
		                                                              .lastVersion = self->currentBeginVersion - 1,
		                                                              .hash = self->hash,
		                                                              .indexToRead = 0 };
	self->reads.send(previousResult);
	wait(self->readerLimit.take());
	state Key end = versionToKey(self->endVersion, self->prefix);

	loop {
		// Get the lock
		wait(self->readerLimit.take());

		// Init read boundaries
		state Key begin = versionToKey(previousResult.lastVersion, self->prefix);

		// Read begin to end forever until successful
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

				RangeResult rangevalue = wait(tr.getRange(KeyRangeRef(keyAfter(begin), end), limits));

				// No more results, send end of stream
				if (rangevalue.empty()) {
					self->reads.sendError(end_of_stream());
					return Void();
				}
				else {
					// Got results, update previousResult
					previousResult = RangeResultBlock{ .result = rangevalue,
													.firstVersion = keyRefToVersion(rangevalue.front().key, self->prefix),
													.lastVersion = keyRefToVersion(rangevalue.back().key, self->prefix),
													.hash = self->hash,
													.indexToRead = 0 };

					// Send results to the reads stream
					self->reads.send(previousResult);
				}
				break;
			} catch (Error& e) {
				if (e.code() == error_code_transaction_too_old) {
					// We are using this transaction until it's too old and then resetting to a fresh one,
					// so we don't need to delay.
					tr.fullReset();
				} else {
					wait(tr.onError(e));
				}
			}
		}
	}
}

ACTOR Future<Void> MutationLogReader::initializePQ(MutationLogReader* self) {
	state int h;
	for (h = 0; h < 256; ++h) {
		RangeResultBlock front = waitNext(self->pipelinedReaders[h]->reads.getFuture());
		self->priorityQueue.push(front);
	}
	return Void();
}

Future<Standalone<RangeResultRef>> MutationLogReader::getNext() {
	return getNext_impl(this);
}

ACTOR Future<Standalone<RangeResultRef>> MutationLogReader::getNext_impl(MutationLogReader* self) {
	loop {
		if (self->finished == 256) {
			state int i;
			for(i = 0; i < self->pipelinedReaders.size(); ++i) {
				wait(self->pipelinedReaders[i]->done());
			}
			throw end_of_stream();
		}
		RangeResultBlock top = self->priorityQueue.top();
		self->priorityQueue.pop();
		state uint8_t hash = top.hash;
		Key prefix = self->pipelinedReaders[(int)hash]->prefix;
		state Standalone<RangeResultRef> ret = top.consume(prefix);
		if (top.empty()) {
			self->pipelinedReaders[(int)hash]->release();
			try {
				RangeResultBlock next = waitNext(self->pipelinedReaders[(int)hash]->reads.getFuture());
				self->priorityQueue.push(next);
			} catch(Error &e) {
				if(e.code() == error_code_end_of_stream) {
					++self->finished;
				}
				else {
					throw e;
				}
			}
		} else {
			self->priorityQueue.push(top);
		}
		if (ret.size() != 0) {
			return ret;
		}
	}
}

// UNIT TESTS
TEST_CASE("/fdbclient/mutationlogreader/VersionKeyRefConversion") {
	Key prefix = LiteralStringRef("foos");

	ASSERT(keyRefToVersion(versionToKey(0, prefix), prefix) == 0);
	ASSERT(keyRefToVersion(versionToKey(1, prefix), prefix) == 1);
	ASSERT(keyRefToVersion(versionToKey(-1, prefix), prefix) == -1);
	ASSERT(keyRefToVersion(versionToKey(std::numeric_limits<int64_t>::min(), prefix), prefix) ==
	       std::numeric_limits<int64_t>::min());
	ASSERT(keyRefToVersion(versionToKey(std::numeric_limits<int64_t>::max(), prefix), prefix) ==
	       std::numeric_limits<int64_t>::max());

	return Void();
}

void forceLinkMutationLogReaderTests() {}
