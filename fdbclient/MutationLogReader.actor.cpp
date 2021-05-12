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

Future<Void> PipelinedReader::getNext(Database cx) {
	return getNext_impl(this, cx);
}

ACTOR Future<Void> PipelinedReader::getNext_impl(PipelinedReader* self, Database cx) {
	state Transaction tr(cx);

	state GetRangeLimits limits(GetRangeLimits::ROW_LIMIT_UNLIMITED,
	                            (g_network->isSimulated() && !g_simulator.speedUpSimulation)
	                                ? CLIENT_KNOBS->BACKUP_SIMULATED_LIMIT_BYTES
	                                : CLIENT_KNOBS->BACKUP_GET_RANGE_LIMIT_BYTES);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			if (self->reads.size() > self->pipelineDepth) {
				wait(self->t.onTrigger());
				self->reads.pop_front();
			}

			KeySelector begin = firstGreaterOrEqual(versionToKeyRef(self->currentBeginVersion, self->prefix)),
			            end = firstGreaterOrEqual(versionToKeyRef(self->endVersion, self->prefix));

			self->reads.push_back(RangeResultBlock{ .beginVersion = self->currentBeginVersion,
			                                        .endVersion = std::numeric_limits<int64_t>::min(),
			                                        .result = tr.getRange(begin, end, limits),
			                                        .hash = self->hash });

			state RangeResult rangevalue = wait(self->reads.back().result);

			if (rangevalue.more) {
				Version lastReadVersion = keyRefToVersion(rangevalue.readThrough.get(), self->prefix);
				self->reads.back().endVersion = lastReadVersion;
				self->currentBeginVersion = lastReadVersion + 1; // keyRefToVersion(rangevalue.end()[-1].key) + 1;
			} else {
				self->finished = true;
				return Void();
			}
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

KeyRef versionToKeyRef(Version version, const Key& prefix) {
	uint64_t versionBigEndian = bigEndian64(version);
	return KeyRef((uint8_t*)&versionBigEndian, sizeof(uint64_t)).withPrefix(prefix);
}

Version keyRefToVersion(const KeyRef& key, const Key& prefix) {
	KeyRef keyWithoutPrefix = key.removePrefix(prefix);
	return (Version)bigEndian64(*((uint64_t*)keyWithoutPrefix.begin()));
}

// Future<RangeResult> MutationLogReader::getNext(Database cx) {
//     RangeResultBlock* top = priorityQueue.top();
//     priorityQueue.pop();
//     uint8_t hash = top->hash;

//     if (!pipelinedReaders[hash].finished) {
//         pipelinedReaders[hash].getNext(cx);
//     }

//     Future<RangeResult> rangeResult = top.rangeResult;
//     delete(top);
//     return rangeResult;
// }

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
