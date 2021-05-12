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

struct RangeResultBlock {
	Version beginVersion;
	Version endVersion; // not filled in until result.isReady()
	Future<RangeResult> result;
	uint8_t hash; // points back to the PipelinedReader

	bool operator<(const RangeResultBlock& r) const {
		// We want a min heap. The standard C++ priority queue is a max heap.
		return beginVersion > r.beginVersion;
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

	std::deque<RangeResultBlock> reads;

private:
	uint8_t hash;
	Key prefix; // "\xff\x02/alog/UID/hash/" for restore, or "\xff\x02/blog/UID/hash/" for backup
	Version beginVersion, endVersion, currentBeginVersion;
	int pipelineDepth;
	bool finished = false;
	AsyncTrigger t;
};

KeyRef versionToKeyRef(Version version, const Key& prefix);
Version keyRefToVersion(const KeyRef& key, const Key& prefix);

class MutationLogReader {
public:
	MutationLogReader(Database cx, Version bv, Version ev, Key uid, KeyRef beginKey, int pd)
	  : beginVersion(bv), endVersion(ev), prefix(uid.withPrefix(beginKey)), pipelineDepth(pd) {
		for (uint8_t h = 0; h < 256; ++h) {
			pipelinedReaders.emplace_back(h, beginVersion, endVersion, pipelineDepth, prefix);
			pipelinedReaders[h].getNext(cx);
			priorityQueue.push(pipelinedReaders[h].reads.front());
		}
	}
	// Future<RangeResult> getNext(Database cx);

private:
	std::vector<PipelinedReader> pipelinedReaders;
	std::priority_queue<RangeResultBlock> priorityQueue;
	Version beginVersion, endVersion;
	Key prefix; // "\xff\x02/alog/UID/" for restore, or "\xff\x02/blog/UID/" for backup
	int pipelineDepth;
	bool finished = false;
};

#include "flow/unactorcompiler.h"
#endif
