/*
 * MutationLogReaderCorrectness.actor.cpp
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

#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/MutationLogReader.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct MutationLogReaderCorrectnessWorkload : TestWorkload {
    bool enabled;
	int records = 100000;
	int k;
	Key uid;
	Key baLogRangePrefix;
	Version beginVersion, endVersion;

	MutationLogReaderCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
        enabled = !clientId; // only do this on the "first" client
		k = deterministicRandom()->randomInt(1, 100);
		uid = BinaryWriter::toValue(deterministicRandom()->randomUniqueID(), Unversioned()); // StringRef(std::string("uid"));
		beginVersion = k;
		endVersion = k * records + 1;
		baLogRangePrefix = uid.withPrefix(backupLogKeys.begin);
	}

	std::string description() const override { return "MutationLogReaderCorrectness"; }

	Future<Void> start(Database const& cx) override {
        if (enabled) {
            std::cout << "litian mmm " << baLogRangePrefix.printable() << std::endl;
            return _start(cx, this); 
        }
		return Void();    
    }

	ACTOR Future<Void> _start(Database cx, MutationLogReaderCorrectnessWorkload* self) {
		state Transaction tr(cx);
		loop {
			try {
                tr.reset();
			    tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
                state int i;
				for (i = 1; i <= self->records; ++i) {
					Version v = self->k * i;
					Key key = getLogKey(v, self->uid);

					tr.set(key, StringRef(format("%d", v)));

                    if (i % 10000 == 0) {
                        std::cout << "litian nnn " << key.printable() << " " << v << std::endl;
				        wait(tr.commit());
                        tr.reset();
			            tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
                    }
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

        state Reference<MutationLogReader> reader = wait(MutationLogReader::Create(
		    cx, self->beginVersion, self->endVersion, self->uid, backupLogKeys.begin, /*pipelineDepth=*/3));

		state int globalSize = 0;

		loop {
			// std::cout << "litian ooo" << std::endl;
			if (reader->isFinished()) {
				break;
			}

			state Standalone<RangeResultRef> nextResultSet = wait(reader->getNext());

			if (nextResultSet.empty()) {
				continue;
			}

			for (i = 0; i < nextResultSet.size(); ++i) {
				++globalSize;
				// Remove the backupLogPrefix + UID bytes from the key
                Value v = nextResultSet[i].value;
				Key actualKey = nextResultSet[i].key, expectedKey = getLogKey(std::stoul(v.printable()), self->uid);
				if (actualKey.compare(expectedKey)) {
                    std::cout << "Value is wrong, Expected: " << expectedKey.printable() << " Actual: " << actualKey.printable() << std::endl;
					TraceEvent(SevError, "TestFailure").detail("Reason", "Value is wrong").detail("Expected", expectedKey.printable()).detail("Actual", actualKey.printable());
                    return Void();
				}
			}
		}

        if (globalSize != self->records) {
            std::cout << "Size is wrong, Expected: " << self->records << " Actual: " << globalSize << std::endl;
            TraceEvent(SevError, "TestFailure").detail("Reason", "Size is wrong").detail("Expected", self->records).detail("Actual", globalSize);
        }
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<MutationLogReaderCorrectnessWorkload> MutationLogReaderCorrectnessWorkloadFactory(
    "MutationLogReaderCorrectness");
