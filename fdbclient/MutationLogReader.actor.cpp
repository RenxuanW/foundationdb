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
	// std::cout << "litian 9 " << (int)hash << " " << firstVersion << " " << lastVersion << " " << stopVersion << std::endl;
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

ACTOR Future<RangeResultBlock> getRange(Transaction* tr, PipelinedReader* self, uint8_t hash, Key prefix, KeySelector begin, KeySelector end, GetRangeLimits limits) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	// std::cout << "litian 4 " << (int)hash << " " << (int)self->hash << " " << begin.toString() << " " << end.toString()  << " " << self->prefix.printable()<< std::endl;
	RangeResult rangevalue = wait(tr->getRange(begin, end, limits));
	// However, in line 46 and 50, `begin` and `end` are the same, which means tr->getRange() doesn't pollute them.
	// Then how is that possiblt that `self->hash` is suddenly changed at line 50, and self->prefix.printable() even causes a seg fault???
	// std::cout << "litian 5 " << (int)hash << " " << (int)self->hash << " " << begin.toString() << " " << end.toString()  << " " << self->prefix.printable()<< std::endl;
	if (rangevalue.size() != 0) {
		// std::cout << "litian 4 " << (int)hash << " " << (int)self->hash << " " << self->prefix.printable() << " " << rangevalue.size() << std::endl;
		return RangeResultBlock{ .result = rangevalue,
									.firstVersion = keyRefToVersion(rangevalue.front().key, prefix),
									.lastVersion = keyRefToVersion(rangevalue.back().key, prefix),
									.hash = hash,
									.indexToRead = 0 };

	} else {
		// std::cout << "litian 5 " << (int)hash << " " << (int)self->hash << " " << self->prefix.printable() << std::endl;
		return RangeResultBlock{ .result = RangeResult(),
									.firstVersion = -1,
									.lastVersion = -1,
									.hash = hash,
									.indexToRead = 0 };
	}
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
	state uint8_t hash = self->hash;
	state Key prefix = self->prefix;
	state Version endVersion = self->endVersion;

	loop {
		try {
			if (self->reads.size() > self->pipelineDepth) {
				std::cout << "litian 1 " << (int)hash << " " << std::endl;
				wait(self->t.onTrigger());
			}

			// std::cout << "litian 7 " << (int)hash << " " << (int)self->hash << " " << self->endVersion << " " << self->prefix.printable() << std::endl;
			RangeResultBlock p = wait(previousResult);

			if (p.firstVersion == -1) {
				return Void();
			}

			Key beginKey = versionToKey(p.lastVersion + 1, prefix), endKey = versionToKey(endVersion, prefix);
			KeySelector begin = firstGreaterOrEqual(beginKey), end = firstGreaterOrEqual(endKey);

			previousResult = getRange(&tr, self, hash, prefix, begin, end, limits);

			// previousResult = map(tr.getRange(begin, end, limits), [=](RangeResult rangevalue) {
			// 	if (rangevalue.size() != 0) {
			// 		std::cout << "litian 4 " << (int)hash << " " << rangevalue.size() << std::endl;
			// 		return RangeResultBlock{ .result = rangevalue,
			// 			                     .firstVersion = keyRefToVersion(rangevalue.front().key, prefix),
			// 			                     .lastVersion = keyRefToVersion(rangevalue.back().key, prefix),
			// 			                     .hash = hash,
			// 			                     .indexToRead = 0 };

			// 	} else {
			// 		std::cout << "litian 5 " << (int)hash << " " << prefix.printable() << " "  << std::endl;
			// 		return RangeResultBlock{ .result = RangeResult(),
			// 			                     .firstVersion = -1,
			// 			                     .lastVersion = -1,
			// 			                     .hash = hash,
			// 			                     .indexToRead = 0 };
			// 	}
			// });
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
	state int h;
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
	state uint8_t hash = top.hash;
	Key prefix = self->pipelinedReaders[(int)hash].prefix;

	state Standalone<RangeResultRef> ret = top.consume(prefix);

	// std::cout << "litian aaa " << (int)hash << " " << ret.size() << std::endl;
	if (top.empty()) {
		self->pipelinedReaders[(int)hash].reads.pop_front();
		self->pipelinedReaders[(int)hash].trigger();
		if (!self->pipelinedReaders[(int)hash].reads.empty()) {
			RangeResultBlock next = wait(self->pipelinedReaders[(int)hash].reads.front());
			self->priorityQueue.push(next);
		} else {
			++self->finished;
		}
	} else {
		self->priorityQueue.push(top);
	}

	if(self->finished == 256) {
		state int i;
		for(i = 0; i < self->pipelinedReaders.size(); ++i) {
			wait(self->pipelinedReaders[i].done());
		}

		throw end_of_stream();
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
