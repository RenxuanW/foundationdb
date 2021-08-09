/*
 * MutationLogReader.h
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

#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_MUTATION_LOG_READER_ACTOR_G_H)
#define FDBCLIENT_MUTATION_LOG_READER_ACTOR_G_H
#include "fdbclient/MutationLogReader.actor.g.h"
#elif !defined(FDBCLIENT_MUTATION_LOG_READER_ACTOR_H)
#define FDBCLIENT_MUTATION_LOG_READER_ACTOR_H

#include <deque>
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/flow.h"
#include "flow/ActorCollection.h"
#include "flow/actorcompiler.h" // has to be last include

namespace mutation_log_reader {

struct RangeResultBlock {
	RangeResult result;
	Version firstVersion; // version of first record
	Version lastVersion; // version of last record
	uint8_t hash; // points back to the PipelinedReader
	int prefixLen; // size of keyspace, uid, and hash prefix
	int indexToRead; // index of first unconsumed record

	Standalone<RangeResultRef> consume();

	bool empty() { return indexToRead == result.size(); }

	bool operator<(const RangeResultBlock& r) const {
		// We want a min heap. The standard C++ priority queue is a max heap.
		return firstVersion > r.firstVersion;
	}
};

class PipelinedReader {
public:
	PipelinedReader(uint8_t h, Version bv, Version ev, unsigned pd, Key p)
	  : readerLimit(pd), hash(h), prefix(StringRef(&hash, sizeof(uint8_t)).withPrefix(p)), beginVersion(bv),
	    endVersion(ev), currentBeginVersion(bv), pipelineDepth(pd) {}

	void startReading(Database cx);
	Future<Void> getNext(Database cx);
	ACTOR static Future<Void> getNext_impl(PipelinedReader* self, Database cx);

	void release() { readerLimit.release(); }

	PromiseStream<RangeResultBlock> reads;
	FlowLock readerLimit;
	uint8_t hash;
	Key prefix; // "\xff\x02/alog/UID/hash/" for restore, or "\xff\x02/blog/UID/hash/" for backup

	Future<Void> done() { return reader; }

private:
	Version beginVersion, endVersion, currentBeginVersion;
	unsigned pipelineDepth;
	Future<Void> reader;
};

} // namespace mutation_log_reader

class MutationLogReader : public ReferenceCounted<MutationLogReader> {
public:
	MutationLogReader() : finished(256) {}
	MutationLogReader(Database cx, Version bv, Version ev, Key uid, Key beginKey, unsigned pd)
	  : beginVersion(bv), endVersion(ev), prefix(uid.withPrefix(beginKey)), pipelineDepth(pd), finished(0) {
		pipelinedReaders.reserve(256);
		if (pipelineDepth > 0) {
			for (int h = 0; h < 256; ++h) {
				pipelinedReaders.emplace_back(new mutation_log_reader::PipelinedReader(
				    (uint8_t)h, beginVersion, endVersion, pipelineDepth, prefix));
				pipelinedReaders[h]->startReading(cx);
			}
		}
	}

	ACTOR static Future<Reference<MutationLogReader>> Create(Database cx,
	                                                         Version bv,
	                                                         Version ev,
	                                                         Key uid,
	                                                         Key beginKey,
	                                                         unsigned pd) {
		state Reference<MutationLogReader> self(new MutationLogReader(cx, bv, ev, uid, beginKey, pd));
		wait(self->initializePQ(self.getPtr()));
		return self;
	}

	Future<Standalone<RangeResultRef>> getNext();

private:
	ACTOR static Future<Void> initializePQ(MutationLogReader* self);
	ACTOR static Future<Standalone<RangeResultRef>> getNext_impl(MutationLogReader* self);

	std::vector<std::unique_ptr<mutation_log_reader::PipelinedReader>> pipelinedReaders;
	std::priority_queue<mutation_log_reader::RangeResultBlock> priorityQueue;
	Version beginVersion, endVersion;
	Key prefix; // "\xff\x02/alog/UID/" for restore, or "\xff\x02/blog/UID/" for backup
	unsigned pipelineDepth;
	unsigned finished;
};

#include "flow/unactorcompiler.h"
#endif
