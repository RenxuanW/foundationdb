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

Key versionToKey(Version version, const KeyRef& prefix) {
	uint64_t versionBigEndian = bigEndian64(version);
	return KeyRef((uint8_t*)&versionBigEndian, sizeof(uint64_t)).withPrefix(prefix);
}

Version keyRefToVersion(const KeyRef& key, const KeyRef& prefix) {
	KeyRef keyWithoutPrefix = key.removePrefix(prefix);
	return (Version)bigEndian64(*((uint64_t*)keyWithoutPrefix.begin()));
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

	state Future<RangeResultBlock> previousResult = RangeResultBlock{ .result = RangeResult(),
		                                                              .firstVersion = self->currentBeginVersion,
		                                                              .lastVersion = self->currentBeginVersion - 1,
		                                                              .hash = self->hash,
		                                                              // .prefix = self->prefix,
		                                                              .indexToRead = 0 };

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			if (self->reads.size() > self->pipelineDepth) {
				wait(self->t.onTrigger());
			}

			std::cout << "litian 1 " << (int)self->hash << std::endl;
			RangeResultBlock p = wait(previousResult);

			uint8_t hash = self->hash;
			Key prefix = self->prefix;
			std::cout << "litian 7 " << (int)hash << " " << p.firstVersion << std::endl;
			if (p.firstVersion == -1) {
				return Void();
			}

			KeySelector begin = firstGreaterOrEqual(versionToKey(p.lastVersion + 1, prefix)),
			            end = firstGreaterOrEqual(versionToKey(self->endVersion, prefix));

			// std::cout << "litian 3 " << (int)self->hash << " " << p.lastVersion + 1 << " " << self->endVersion << " " << begin.toString() << " " << end.toString() << std::endl;
			previousResult = map(tr.getRange(begin, end, limits), [=](const RangeResult& rangevalue) {
				if (rangevalue.size() != 0) {
					std::cout << "litian 4 " << (int)hash << " " << prefix.printable() << " " << rangevalue.toString() << std::endl;
					return RangeResultBlock{ .result = rangevalue,
						                     .firstVersion = keyRefToVersion(rangevalue.front().key, prefix),
						                     .lastVersion = keyRefToVersion(rangevalue.back().key, prefix),
						                     .hash = hash,
						                     // .prefix = p.prefix,
						                     .indexToRead = 0 };

				} else {
					std::cout << "litian 5 " << (int)hash << " " << prefix.printable() << " "  << std::endl;
					return RangeResultBlock{ .result = RangeResult(),
						                     .firstVersion = -1,
						                     .lastVersion = -1,
						                     .hash = hash,
						                     // .prefix = p.prefix,
						                     .indexToRead = 0 };
				}
			});
			self->reads.push_back(previousResult);
		} catch (Error& e) {
			// The whole loop is only executed once for every getNext() call. Also, e.code() = 1101 below, which is "Asynchronous operation cancelled". 
			// Does this provide some clue?
			std::cout << "litian 6 " << (int)self->hash << " " << e.code() << std::endl;

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
	state uint32_t h;
	for (h = 0; h < 256; ++h) {
		// std::cout << "litian 2 " << h << " " << self->pipelinedReaders[h].reads.size() << std::endl;
		RangeResultBlock front = wait(self->pipelinedReaders[h].reads.front());
		// std::cout << "litian 2.5 " << (int)front.hash << " " << front.firstVersion << " " << front.lastVersion << " "
		// << front.indexToRead << std::endl;
		self->priorityQueue.push(front);
	}
	return Void();
}

Future<Standalone<RangeResultRef>> MutationLogReader::getNext() {
	return getNext_impl(this);
}

ACTOR Future<Standalone<RangeResultRef>> MutationLogReader::getNext_impl(MutationLogReader* self) {
	if (self->priorityQueue.empty()) {
		return Standalone<RangeResultRef>();
	}
	RangeResultBlock top = self->priorityQueue.top();
	self->priorityQueue.pop();
	uint8_t hash = top.hash;
	Key prefix = self->pipelinedReaders[(uint32_t)hash].prefix;

	state Standalone<RangeResultRef> ret = top.consume(prefix);

	// std::cout << "litian aaa " << ret.size() << std::endl;
	if (top.empty()) {
		self->pipelinedReaders[(uint32_t)hash].reads.pop_front();
		self->pipelinedReaders[(uint32_t)hash].trigger();
		if (!self->pipelinedReaders[(uint32_t)hash].reads.empty()) {
			RangeResultBlock next = wait(self->pipelinedReaders[(uint32_t)hash].reads.front());
			self->priorityQueue.push(next);
		} else {
			++self->finished;
		}
	} else {
		self->priorityQueue.push(top);
	}
	return ret;
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
