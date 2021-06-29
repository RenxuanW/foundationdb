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

Key versionToKey(Version version, const KeyRef& prefix);
Version keyRefToVersion(const KeyRef& key, const KeyRef& prefix);

struct RangeResultBlock {
	RangeResult result;
	Version firstVersion;
	Version lastVersion; // not filled in until result.isReady()
	uint8_t hash; // points back to the PipelinedReader
	// Key prefix;
	int indexToRead;

	Standalone<RangeResultRef> consume(const KeyRef& prefix) {
		Version stopVersion =
		    std::min(lastVersion + 1,
		             (firstVersion + CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE - 1) / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE *
		                 CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE); // firstVersion rounded up to the nearest 1M versions
		// std::cout << "litian 9 " << firstVersion << " " << lastVersion << " " << stopVersion << std::endl;
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

	bool empty() {
		// std::cout << "litian bbb " << indexToRead << " " << result.size() << std::endl;
		return indexToRead == result.size();
	}

	bool operator<(const RangeResultBlock& r) const {
		// We want a min heap. The standard C++ priority queue is a max heap.
		return firstVersion > r.firstVersion;
	}
};

class PipelinedReader {
public:
	PipelinedReader(uint8_t h, Version bv, Version ev, int pd, Key p)
	  : hash(h), beginVersion(bv), endVersion(ev), currentBeginVersion(bv), pipelineDepth(pd) {
		prefix = StringRef(&hash, sizeof(uint8_t)).withPrefix(p);
	}

	Future<Void> getNext(Database cx);
	ACTOR static Future<Void> getNext_impl(PipelinedReader* self, Database cx);

	void trigger() { t.trigger(); }

	// bool isFinished() { return finished; }

	std::deque<Future<RangeResultBlock>> reads;
	Key prefix; // "\xff\x02/alog/UID/hash/" for restore, or "\xff\x02/blog/UID/hash/" for backup

private:
	uint8_t hash;
	Version beginVersion, endVersion, currentBeginVersion;
	int pipelineDepth;
	// bool finished = false;
	AsyncTrigger t;
};

class MutationLogReader : public ReferenceCounted<MutationLogReader> {
public:
	MutationLogReader() : finished(256) {}

	// MutationLogReader(Database cx = Database(), Version bv = -1, Version ev = -1, Key uid = Key(), KeyRef beginKey =
	// KeyRef(), int pd = 0)
	MutationLogReader(Database cx, Version bv, Version ev, Key uid, KeyRef beginKey, int pd)
	  : beginVersion(bv), endVersion(ev), prefix(uid.withPrefix(beginKey)), pipelineDepth(pd) {
		if (pipelineDepth > 0) {
			for (uint32_t h = 0; h < 256; ++h) {
				pipelinedReaders.emplace_back((uint8_t)h, beginVersion, endVersion, pipelineDepth, prefix);
				pipelinedReaders[h].getNext(cx);
			}
		}
	}

	ACTOR static Future<Reference<MutationLogReader>> Create(Database cx,
	                                                         Version bv,
	                                                         Version ev,
	                                                         Key uid,
	                                                         KeyRef beginKey,
	                                                         int pd) {
		state Reference<MutationLogReader> self(new MutationLogReader(cx, bv, ev, uid, beginKey, pd));
		wait(self->initializePQ(self.getPtr()));
		return self;
	}

	// Future<Void> initializePQ();
	ACTOR static Future<Void> initializePQ(MutationLogReader* self);

	// Should always call isFinished() before calling getNext.
	Future<Standalone<RangeResultRef>> getNext();
	ACTOR Future<Standalone<RangeResultRef>> getNext_impl(MutationLogReader* self);

	bool isFinished() { return finished == 256; }

	int pqSize() { return priorityQueue.size(); }

private:
	std::vector<PipelinedReader> pipelinedReaders;
	std::priority_queue<RangeResultBlock> priorityQueue;
	Version beginVersion, endVersion;
	Key prefix; // "\xff\x02/alog/UID/" for restore, or "\xff\x02/blog/UID/" for backup
	int pipelineDepth;
	int finished = 0;
};

#include "flow/unactorcompiler.h"
#endif
