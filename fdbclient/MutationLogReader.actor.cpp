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

KeyRef versionToKeyRef(Version version, const Key& prefix) {
	uint64_t versionBigEndian = bigEndian64(version);
	return KeyRef((uint8_t*)&versionBigEndian, sizeof(uint64_t)).withPrefix(prefix);
}

Version keyRefToVersion(const KeyRef& key, const Key& prefix) {
	KeyRef keyWithoutPrefix = key.removePrefix(prefix);
	return (Version)bigEndian64(*((uint64_t*)keyWithoutPrefix.begin()));
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

	state Future<RangeResultBlock> previousResult = RangeResultBlock{ .result = RangeResult(),
		                                                              .firstVersion = self->currentBeginVersion,
		                                                              .lastVersion = self->currentBeginVersion - 1,
		                                                              .hash = self->hash,
		                                                              .indexToRead = 0 };

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			if (self->reads.size() > self->pipelineDepth) {
				wait(self->t.onTrigger());
			}
			RangeResultBlock p = wait(previousResult);

			KeySelector begin = firstGreaterOrEqual(versionToKeyRef(p.lastVersion + 1, self->prefix)),
			            end = firstGreaterOrEqual(versionToKeyRef(self->endVersion, self->prefix));
			previousResult = map(tr.getRange(begin, end, limits), [&](const RangeResult& rangevalue) {
				if (rangevalue.more) {
					Version lastVersion = keyRefToVersion(rangevalue.readThrough.get(), self->prefix);
					return RangeResultBlock{ .result = rangevalue,
						                     .firstVersion = p.lastVersion + 1,
						                     .lastVersion = lastVersion,
						                     .hash = self->hash,
						                     .indexToRead = 0 };
				} else {
					self->finished = true;
					return RangeResultBlock{
						.result = RangeResult(), .firstVersion = -1, .lastVersion = -1, .hash = self->hash
					};
				}
			});
			if (self->finished) {
				return Void();
			}
			self->reads.push_back(previousResult);
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

ACTOR Future<Void> MutationLogReader::initializePQ(MutationLogReader* self) {
	state uint8_t h;
	for (h = 0; h < 256; ++h) {
		RangeResultBlock front = wait(self->pipelinedReaders[h].reads.front());
		self->priorityQueue.push(front);
	}
	return Void();
}

Future<Standalone<RangeResultRef>> MutationLogReader::getNext() {
	return getNext_impl(this);
}

ACTOR Future<Standalone<RangeResultRef>> MutationLogReader::getNext_impl(MutationLogReader* self) {
	RangeResultBlock top = self->priorityQueue.top();
	self->priorityQueue.pop();
	uint8_t hash = top.hash;
	Key prefix = self->pipelinedReaders[hash].prefix;
	state Standalone<RangeResultRef> ret = top.consume(prefix);
	if (top.empty()) {
		self->pipelinedReaders[hash].reads.pop_front();
		self->pipelinedReaders[hash].trigger();
		if (!self->pipelinedReaders[hash].reads.empty()) {
			RangeResultBlock next = wait(self->pipelinedReaders[hash].reads.front());
			self->priorityQueue.push(next);
		}
	} else {
		self->priorityQueue.push(top);
	}
	return ret;
}

// UNIT TESTS
TEST_CASE("/fdbclient/mutationlogreader/VersionKeyRefConversion") {
	Key prefix = LiteralStringRef("foos");

	ASSERT(keyRefToVersion(versionToKeyRef(0, prefix), prefix) == 0);
	ASSERT(keyRefToVersion(versionToKeyRef(1, prefix), prefix) == 1);
	ASSERT(keyRefToVersion(versionToKeyRef(-1, prefix), prefix) == -1);
	ASSERT(keyRefToVersion(versionToKeyRef(std::numeric_limits<int64_t>::min(), prefix), prefix) ==
	       std::numeric_limits<int64_t>::min());
	ASSERT(keyRefToVersion(versionToKeyRef(std::numeric_limits<int64_t>::max(), prefix), prefix) ==
	       std::numeric_limits<int64_t>::max());

	return Void();
}

void forceLinkMutationLogReaderTests() {}
