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

	state uint8_t hash = self->hash;
	state Key prefix = self->prefix;

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
			if (p.firstVersion == -1) {
				std::cout << "litian 7 " << (int)hash << std::endl;
				return Void();
			}

			KeySelector begin = firstGreaterOrEqual(versionToKeyRef(p.lastVersion + 1, self->prefix)),
			            end = firstGreaterOrEqual(versionToKeyRef(self->endVersion, self->prefix));

			std::cout << "litian 3 " << (int)hash << " " << begin.toString() << " " << end.toString() << std::endl;

			previousResult = map(tr.getRange(begin, end, limits), [=](const RangeResult& rangevalue) {
				std::cout << "litian 4 " << (int)p.hash << std::endl;

				if (rangevalue.more) {
					std::cout << "litian 4.a " << (int)p.hash << std::endl;
					Version lastVersion = keyRefToVersion(rangevalue.readThrough.get(), prefix);
					std::cout << "litian 4.aa " << (int)p.hash << std::endl;
					return RangeResultBlock{ .result = rangevalue,
						                     .firstVersion = p.lastVersion + 1,
						                     .lastVersion = lastVersion,
						                     .hash = p.hash,
						                     .indexToRead = 0 };
				} else {
					std::cout << "litian 4.b " << (int)p.hash << std::endl;
					return RangeResultBlock{
						.result = RangeResult(), .firstVersion = -1, .lastVersion = -1, .hash = p.hash,
						                     .indexToRead = 0
					};
				}
			});
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

// Future<Void> MutationLogReader::initializePQ() {
// 	return initializePQ_impl(this);
// }

ACTOR Future<Void> MutationLogReader::initializePQ(MutationLogReader* self) {
	state u_int32_t h;
	std::cout << "litian 1" << std::endl;
	for (h = 0; h < 256; ++h) {
		std::cout << "litian 2 " << h << " " << self->pipelinedReaders[h].reads.size() << std::endl;
		// state Future<RangeResultBlock> fff = self->pipelinedReaders[h].reads.front();
		RangeResultBlock front = wait(self->pipelinedReaders[h].reads.front());
		// RangeResultBlock front = wait(fff);
		std::cout << "litian 2.5 " << (int)front.hash << " " << front.firstVersion << " " << front.lastVersion << " " << front.indexToRead << std::endl;
		self->priorityQueue.push(front);
	}
	std::cout << "litian 6" << std::endl;
	return Void();
}

Future<Standalone<RangeResultRef>> MutationLogReader::getNext() {
	return getNext_impl(this);
}

ACTOR Future<Standalone<RangeResultRef>> MutationLogReader::getNext_impl(MutationLogReader* self) {
	if(self->priorityQueue.empty()) {
		return Standalone<RangeResultRef>();
	}
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
