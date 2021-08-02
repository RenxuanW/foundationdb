/*
 * ConfigDatabaseUnitTests.actor.cpp
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

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/IConfigTransaction.h"
#include "fdbclient/TestKnobCollection.h"
#include "fdbserver/ConfigBroadcaster.h"
#include "fdbserver/IConfigDatabaseNode.h"
#include "fdbserver/LocalConfiguration.h"
#include "fdbclient/Tuple.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

Key encodeConfigKey(Optional<KeyRef> configClass, KeyRef knobName) {
	Tuple tuple;
	if (configClass.present()) {
		tuple.append(configClass.get());
	} else {
		tuple.appendNull();
	}
	tuple << knobName;
	return tuple.pack();
}

void appendVersionedMutation(Standalone<VectorRef<VersionedConfigMutationRef>>& versionedMutations,
                             Version version,
                             Optional<KeyRef> configClass,
                             KeyRef knobName,
                             Optional<KnobValueRef> knobValue) {
	auto configKey = ConfigKeyRef(configClass, knobName);
	auto mutation = ConfigMutationRef(configKey, knobValue);
	versionedMutations.emplace_back_deep(versionedMutations.arena(), version, mutation);
}

class WriteToTransactionEnvironment {
	std::string dataDir;
	ConfigTransactionInterface cti;
	ConfigFollowerInterface cfi;
	Reference<IConfigDatabaseNode> node;
	Future<Void> ctiServer;
	Future<Void> cfiServer;
	Version lastWrittenVersion{ 0 };

	static Value longToValue(int64_t v) {
		auto s = format("%ld", v);
		return StringRef(reinterpret_cast<uint8_t const*>(s.c_str()), s.size());
	}

	ACTOR template <class T>
	static Future<Void> set(WriteToTransactionEnvironment* self,
	                        Optional<KeyRef> configClass,
	                        T value,
	                        KeyRef knobName) {
		state Reference<IConfigTransaction> tr = IConfigTransaction::createTestSimple(self->cti);
		auto configKey = encodeConfigKey(configClass, knobName);
		tr->set(configKey, longToValue(value));
		wait(tr->commit());
		self->lastWrittenVersion = tr->getCommittedVersion();
		return Void();
	}

	ACTOR static Future<Void> clear(WriteToTransactionEnvironment* self, Optional<KeyRef> configClass) {
		state Reference<IConfigTransaction> tr = IConfigTransaction::createTestSimple(self->cti);
		auto configKey = encodeConfigKey(configClass, "test_long"_sr);
		tr->clear(configKey);
		wait(tr->commit());
		self->lastWrittenVersion = tr->getCommittedVersion();
		return Void();
	}

	void setup() {
		ctiServer = node->serve(cti);
		cfiServer = node->serve(cfi);
	}

public:
	WriteToTransactionEnvironment(std::string const& dataDir)
	  : dataDir(dataDir), node(IConfigDatabaseNode::createSimple(dataDir)) {
		platform::eraseDirectoryRecursive(dataDir);
		setup();
	}

	template <class T>
	Future<Void> set(Optional<KeyRef> configClass, T value, KeyRef knobName = "test_long"_sr) {
		return set(this, configClass, value, knobName);
	}

	Future<Void> clear(Optional<KeyRef> configClass) { return clear(this, configClass); }

	Future<Void> compact() { return cfi.compact.getReply(ConfigFollowerCompactRequest{ lastWrittenVersion }); }

	void restartNode() {
		cfiServer.cancel();
		ctiServer.cancel();
		node = IConfigDatabaseNode::createSimple(dataDir);
		setup();
	}

	ConfigTransactionInterface getTransactionInterface() const { return cti; }

	ConfigFollowerInterface getFollowerInterface() const { return cfi; }

	Future<Void> getError() const { return cfiServer || ctiServer; }
};

class ReadFromLocalConfigEnvironment {
	UID id;
	std::string dataDir;
	LocalConfiguration localConfiguration;
	Reference<IAsyncListener<ConfigBroadcastFollowerInterface> const> cbfi;
	Future<Void> consumer;

	ACTOR static Future<Void> checkEventually(LocalConfiguration const* localConfiguration,
	                                          Optional<int64_t> expected) {
		state double lastMismatchTime = now();
		loop {
			if (localConfiguration->getTestKnobs().TEST_LONG == expected.orDefault(0)) {
				return Void();
			}
			if (now() > lastMismatchTime + 1.0) {
				TraceEvent(SevWarn, "CheckEventuallyStillChecking")
				    .detail("Expected", expected.present() ? expected.get() : 0)
				    .detail("TestLong", localConfiguration->getTestKnobs().TEST_LONG);
				lastMismatchTime = now();
			}
			wait(delayJittered(0.1));
		}
	}

	ACTOR static Future<Void> setup(ReadFromLocalConfigEnvironment* self) {
		wait(self->localConfiguration.initialize());
		if (self->cbfi) {
			self->consumer = self->localConfiguration.consume(self->cbfi);
		}
		return Void();
	}

public:
	ReadFromLocalConfigEnvironment(std::string const& dataDir,
	                               std::string const& configPath,
	                               std::map<std::string, std::string> const& manualKnobOverrides)
	  : dataDir(dataDir), localConfiguration(dataDir, configPath, manualKnobOverrides, IsTest::True),
	    consumer(Never()) {}

	Future<Void> setup() { return setup(this); }

	Future<Void> restartLocalConfig(std::string const& newConfigPath) {
		localConfiguration = LocalConfiguration(dataDir, newConfigPath, {}, IsTest::True);
		return setup();
	}

	void connectToBroadcaster(Reference<IAsyncListener<ConfigBroadcastFollowerInterface> const> const& cbfi) {
		ASSERT(!this->cbfi);
		this->cbfi = cbfi;
		consumer = localConfiguration.consume(cbfi);
	}

	void checkImmediate(Optional<int64_t> expected) const {
		if (expected.present()) {
			ASSERT_EQ(localConfiguration.getTestKnobs().TEST_LONG, expected.get());
		} else {
			ASSERT_EQ(localConfiguration.getTestKnobs().TEST_LONG, 0);
		}
	}

	Future<Void> checkEventually(Optional<int64_t> expected) const {
		return checkEventually(&localConfiguration, expected);
	}

	LocalConfiguration& getMutableLocalConfiguration() { return localConfiguration; }

	Future<Void> getError() const { return consumer; }
};

class LocalConfigEnvironment {
	ReadFromLocalConfigEnvironment readFrom;
	Version lastWrittenVersion{ 0 };

	Future<Void> addMutation(Optional<KeyRef> configClass, Optional<KnobValueRef> value) {
		Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
		appendVersionedMutation(versionedMutations, ++lastWrittenVersion, configClass, "test_long"_sr, value);
		return readFrom.getMutableLocalConfiguration().addChanges(versionedMutations, lastWrittenVersion);
	}

public:
	LocalConfigEnvironment(std::string const& dataDir,
	                       std::string const& configPath,
	                       std::map<std::string, std::string> const& manualKnobOverrides = {})
	  : readFrom(dataDir, configPath, manualKnobOverrides) {}
	Future<Void> setup() { return readFrom.setup(); }
	Future<Void> restartLocalConfig(std::string const& newConfigPath) {
		return readFrom.restartLocalConfig(newConfigPath);
	}
	Future<Void> getError() const { return Never(); }
	Future<Void> clear(Optional<KeyRef> configClass) { return addMutation(configClass, {}); }
	Future<Void> set(Optional<KeyRef> configClass, int64_t value) {
		auto knobValue = KnobValueRef::create(value);
		return addMutation(configClass, knobValue.contents());
	}
	void check(Optional<int64_t> value) const { return readFrom.checkImmediate(value); }
};

class BroadcasterToLocalConfigEnvironment {
	ReadFromLocalConfigEnvironment readFrom;
	Reference<AsyncVar<ConfigBroadcastFollowerInterface>> cbfi;
	ConfigBroadcaster broadcaster;
	Version lastWrittenVersion{ 0 };
	Future<Void> broadcastServer;

	ACTOR static Future<Void> setup(BroadcasterToLocalConfigEnvironment* self) {
		wait(self->readFrom.setup());
		self->readFrom.connectToBroadcaster(IAsyncListener<ConfigBroadcastFollowerInterface>::create(self->cbfi));
		self->broadcastServer = self->broadcaster.serve(self->cbfi->get());
		return Void();
	}

	void addMutation(Optional<KeyRef> configClass, KnobValueRef value) {
		Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
		appendVersionedMutation(versionedMutations, ++lastWrittenVersion, configClass, "test_long"_sr, value);
		broadcaster.applyChanges(versionedMutations, lastWrittenVersion, {});
	}

public:
	BroadcasterToLocalConfigEnvironment(std::string const& dataDir, std::string const& configPath)
	  : readFrom(dataDir, configPath, {}), cbfi(makeReference<AsyncVar<ConfigBroadcastFollowerInterface>>()),
	    broadcaster(ConfigFollowerInterface{}) {}

	Future<Void> setup() { return setup(this); }

	void set(Optional<KeyRef> configClass, int64_t value) {
		auto knobValue = KnobValueRef::create(value);
		addMutation(configClass, knobValue.contents());
	}

	void clear(Optional<KeyRef> configClass) { addMutation(configClass, {}); }

	Future<Void> check(Optional<int64_t> value) const { return readFrom.checkEventually(value); }

	void changeBroadcaster() {
		broadcastServer.cancel();
		cbfi->set(ConfigBroadcastFollowerInterface{});
		broadcastServer = broadcaster.serve(cbfi->get());
	}

	Future<Void> restartLocalConfig(std::string const& newConfigPath) {
		return readFrom.restartLocalConfig(newConfigPath);
	}

	void compact() { broadcaster.compact(lastWrittenVersion); }

	Future<Void> getError() const { return readFrom.getError() || broadcastServer; }
};

class TransactionEnvironment {
	WriteToTransactionEnvironment writeTo;

	ACTOR static Future<Void> check(TransactionEnvironment* self,
	                                Optional<KeyRef> configClass,
	                                Optional<int64_t> expected) {
		state Reference<IConfigTransaction> tr =
		    IConfigTransaction::createTestSimple(self->writeTo.getTransactionInterface());
		state Key configKey = encodeConfigKey(configClass, "test_long"_sr);
		state Optional<Value> value = wait(tr->get(configKey));
		if (expected.present()) {
			ASSERT_EQ(BinaryReader::fromStringRef<int64_t>(value.get(), Unversioned()), expected.get());
		} else {
			ASSERT(!value.present());
		}
		return Void();
	}

	ACTOR static Future<Standalone<VectorRef<KeyRef>>> getConfigClasses(TransactionEnvironment* self) {
		state Reference<IConfigTransaction> tr =
		    IConfigTransaction::createTestSimple(self->writeTo.getTransactionInterface());
		state KeySelector begin = firstGreaterOrEqual(configClassKeys.begin);
		state KeySelector end = firstGreaterOrEqual(configClassKeys.end);
		Standalone<RangeResultRef> range = wait(tr->getRange(begin, end, 1000));
		Standalone<VectorRef<KeyRef>> result;
		for (const auto& kv : range) {
			result.push_back_deep(result.arena(), kv.key);
			ASSERT(kv.value == ""_sr);
		}
		return result;
	}

	ACTOR static Future<Standalone<VectorRef<KeyRef>>> getKnobNames(TransactionEnvironment* self,
	                                                                Optional<KeyRef> configClass) {
		state Reference<IConfigTransaction> tr =
		    IConfigTransaction::createTestSimple(self->writeTo.getTransactionInterface());
		state KeyRange keys = globalConfigKnobKeys;
		if (configClass.present()) {
			keys = singleKeyRange(configClass.get().withPrefix(configKnobKeys.begin));
		}
		KeySelector begin = firstGreaterOrEqual(keys.begin);
		KeySelector end = firstGreaterOrEqual(keys.end);
		Standalone<RangeResultRef> range = wait(tr->getRange(begin, end, 1000));
		Standalone<VectorRef<KeyRef>> result;
		for (const auto& kv : range) {
			result.push_back_deep(result.arena(), kv.key);
			ASSERT(kv.value == ""_sr);
		}
		return result;
	}

	ACTOR static Future<Void> badRangeRead(TransactionEnvironment* self) {
		state Reference<IConfigTransaction> tr =
		    IConfigTransaction::createTestSimple(self->writeTo.getTransactionInterface());
		KeySelector begin = firstGreaterOrEqual(normalKeys.begin);
		KeySelector end = firstGreaterOrEqual(normalKeys.end);
		wait(success(tr->getRange(begin, end, 1000)));
		return Void();
	}

public:
	TransactionEnvironment(std::string const& dataDir) : writeTo(dataDir) {}

	Future<Void> setup() { return Void(); }

	void restartNode() { writeTo.restartNode(); }
	template <class T>
	Future<Void> set(Optional<KeyRef> configClass, T value, KeyRef knobName = "test_long"_sr) {
		return writeTo.set(configClass, value, knobName);
	}
	Future<Void> clear(Optional<KeyRef> configClass) { return writeTo.clear(configClass); }
	Future<Void> check(Optional<KeyRef> configClass, Optional<int64_t> expected) {
		return check(this, configClass, expected);
	}
	Future<Void> badRangeRead() { return badRangeRead(this); }

	Future<Standalone<VectorRef<KeyRef>>> getConfigClasses() { return getConfigClasses(this); }
	Future<Standalone<VectorRef<KeyRef>>> getKnobNames(Optional<KeyRef> configClass) {
		return getKnobNames(this, configClass);
	}

	Future<Void> compact() { return writeTo.compact(); }
	Future<Void> getError() const { return writeTo.getError(); }
};

class TransactionToLocalConfigEnvironment {
	WriteToTransactionEnvironment writeTo;
	ReadFromLocalConfigEnvironment readFrom;
	Reference<AsyncVar<ConfigBroadcastFollowerInterface>> cbfi;
	ConfigBroadcaster broadcaster;
	Future<Void> broadcastServer;

	ACTOR static Future<Void> setup(TransactionToLocalConfigEnvironment* self) {
		wait(self->readFrom.setup());
		self->readFrom.connectToBroadcaster(IAsyncListener<ConfigBroadcastFollowerInterface>::create(self->cbfi));
		self->broadcastServer = self->broadcaster.serve(self->cbfi->get());
		return Void();
	}

public:
	TransactionToLocalConfigEnvironment(std::string const& dataDir, std::string const& configPath)
	  : writeTo(dataDir), readFrom(dataDir, configPath, {}),
	    cbfi(makeReference<AsyncVar<ConfigBroadcastFollowerInterface>>()), broadcaster(writeTo.getFollowerInterface()) {
	}

	Future<Void> setup() { return setup(this); }

	void restartNode() { writeTo.restartNode(); }

	void changeBroadcaster() {
		broadcastServer.cancel();
		cbfi->set(ConfigBroadcastFollowerInterface{});
		broadcastServer = broadcaster.serve(cbfi->get());
	}

	Future<Void> restartLocalConfig(std::string const& newConfigPath) {
		return readFrom.restartLocalConfig(newConfigPath);
	}

	Future<Void> compact() { return writeTo.compact(); }

	template <class T>
	Future<Void> set(Optional<KeyRef> configClass, T const& value) {
		return writeTo.set(configClass, value);
	}
	Future<Void> clear(Optional<KeyRef> configClass) { return writeTo.clear(configClass); }
	Future<Void> check(Optional<int64_t> value) const { return readFrom.checkEventually(value); }
	Future<Void> getError() const { return writeTo.getError() || readFrom.getError() || broadcastServer; }
};

// These functions give a common interface to all environments, to improve code reuse
template <class Env, class... Args>
Future<Void> set(Env& env, Args&&... args) {
	return waitOrError(env.set(std::forward<Args>(args)...), env.getError());
}
template <class... Args>
Future<Void> set(BroadcasterToLocalConfigEnvironment& env, Args&&... args) {
	env.set(std::forward<Args>(args)...);
	return Void();
}
template <class Env, class... Args>
Future<Void> clear(Env& env, Args&&... args) {
	return waitOrError(env.clear(std::forward<Args>(args)...), env.getError());
}
template <class... Args>
Future<Void> clear(BroadcasterToLocalConfigEnvironment& env, Args&&... args) {
	env.clear(std::forward<Args>(args)...);
	return Void();
}
template <class Env, class... Args>
Future<Void> check(Env& env, Args&&... args) {
	return waitOrError(env.check(std::forward<Args>(args)...), env.getError());
}
template <class... Args>
Future<Void> check(LocalConfigEnvironment& env, Args&&... args) {
	env.check(std::forward<Args>(args)...);
	return Void();
}
template <class Env>
Future<Void> compact(Env& env) {
	return waitOrError(env.compact(), env.getError());
}
Future<Void> compact(BroadcasterToLocalConfigEnvironment& env) {
	env.compact();
	return Void();
}

ACTOR template <class Env>
Future<Void> testRestartLocalConfig(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	wait(check(env, int64_t{ 1 }));
	wait(env.restartLocalConfig("class-A"));
	wait(check(env, int64_t{ 1 }));
	wait(set(env, "class-A"_sr, 2));
	wait(check(env, int64_t{ 2 }));
	return Void();
}

ACTOR template <class Env>
Future<Void> testRestartLocalConfigAndChangeClass(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	wait(check(env, int64_t{ 1 }));
	wait(env.restartLocalConfig("class-B"));
	wait(check(env, int64_t{ 0 }));
	wait(set(env, "class-B"_sr, int64_t{ 2 }));
	wait(check(env, int64_t{ 2 }));
	return Void();
}

ACTOR template <class Env>
Future<Void> testSet(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	wait(check(env, int64_t{ 1 }));
	return Void();
}

ACTOR template <class Env>
Future<Void> testClear(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	wait(clear(env, "class-A"_sr));
	wait(check(env, Optional<int64_t>{}));
	return Void();
}

ACTOR template <class Env>
Future<Void> testGlobalSet(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup());
	wait(set(env, Optional<KeyRef>{}, int64_t{ 1 }));
	wait(check(env, int64_t{ 1 }));
	wait(set(env, "class-A"_sr, int64_t{ 10 }));
	wait(check(env, int64_t{ 10 }));
	return Void();
}

ACTOR template <class Env>
Future<Void> testIgnore(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup());
	wait(set(env, "class-B"_sr, int64_t{ 1 }));
	choose {
		when(wait(delay(5))) {}
		when(wait(check(env, int64_t{ 1 }))) { ASSERT(false); }
	}
	return Void();
}

ACTOR template <class Env>
Future<Void> testCompact(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	wait(compact(env));
	wait(check(env, 1));
	wait(set(env, "class-A"_sr, int64_t{ 2 }));
	wait(check(env, 2));
	return Void();
}

ACTOR template <class Env>
Future<Void> testChangeBroadcaster(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	wait(check(env, int64_t{ 1 }));
	env.changeBroadcaster();
	wait(set(env, "class-A"_sr, int64_t{ 2 }));
	wait(check(env, int64_t{ 2 }));
	return Void();
}

bool matches(Standalone<VectorRef<KeyRef>> const& vec, std::set<Key> const& compareTo) {
	std::set<Key> s;
	for (const auto& value : vec) {
		s.insert(value);
	}
	return (s == compareTo);
}

ACTOR Future<Void> testGetConfigClasses(UnitTestParameters params, bool doCompact) {
	state TransactionEnvironment env(params.getDataDir());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	wait(set(env, "class-B"_sr, int64_t{ 1 }));
	if (doCompact) {
		wait(compact(env));
	}
	Standalone<VectorRef<KeyRef>> configClasses = wait(env.getConfigClasses());
	ASSERT(matches(configClasses, { "class-A"_sr, "class-B"_sr }));
	return Void();
}

ACTOR Future<Void> testGetKnobs(UnitTestParameters params, bool global, bool doCompact) {
	state TransactionEnvironment env(params.getDataDir());
	state Optional<Key> configClass;
	if (!global) {
		configClass = "class-A"_sr;
	}
	wait(set(env, configClass.castTo<KeyRef>(), int64_t{ 1 }, "test_long"_sr));
	wait(set(env, configClass.castTo<KeyRef>(), int{ 2 }, "test_int"_sr));
	wait(set(env, "class-B"_sr, double{ 3.0 }, "test_double"_sr)); // ignored
	if (doCompact) {
		wait(compact(env));
	}
	Standalone<VectorRef<KeyRef>> knobNames =
	    wait(waitOrError(env.getKnobNames(configClass.castTo<KeyRef>()), env.getError()));
	ASSERT(matches(knobNames, { "test_long"_sr, "test_int"_sr }));
	return Void();
}

} // namespace

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Set") {
	wait(testSet<LocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Restart") {
	wait(testRestartLocalConfig<LocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/RestartFresh") {
	wait(testRestartLocalConfigAndChangeClass<LocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Clear") {
	wait(testClear<LocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/GlobalSet") {
	wait(testGlobalSet<LocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/ConflictingOverrides") {
	state LocalConfigEnvironment env(params.getDataDir(), "class-A/class-B", {});
	wait(env.setup());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	wait(set(env, "class-B"_sr, int64_t{ 10 }));
	env.check(10);
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Manual") {
	state LocalConfigEnvironment env(params.getDataDir(), "class-A", { { "test_long", "1000" } });
	wait(env.setup());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	env.check(1000);
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Set") {
	wait(testSet<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Clear") {
	wait(testClear<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Ignore") {
	wait(testIgnore<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/GlobalSet") {
	wait(testGlobalSet<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/ChangeBroadcaster") {
	wait(testChangeBroadcaster<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/RestartLocalConfig") {
	wait(testRestartLocalConfig<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/RestartLocalConfigAndChangeClass") {
	wait(testRestartLocalConfigAndChangeClass<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Compact") {
	wait(testCompact<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/Set") {
	wait(testSet<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/Clear") {
	wait(testClear<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/GlobalSet") {
	wait(testGlobalSet<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/RestartNode") {
	state TransactionToLocalConfigEnvironment env(params.getDataDir(), "class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	env.restartNode();
	wait(check(env, int64_t{ 1 }));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/ChangeBroadcaster") {
	wait(testChangeBroadcaster<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/RestartLocalConfigAndChangeClass") {
	wait(testRestartLocalConfigAndChangeClass<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/CompactNode") {
	wait(testCompact<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Set") {
	state TransactionEnvironment env(params.getDataDir());
	wait(env.setup());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	wait(check(env, "class-A"_sr, int64_t{ 1 }));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Clear") {
	state TransactionEnvironment env(params.getDataDir());
	wait(env.setup());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	wait(clear(env, "class-A"_sr));
	wait(check(env, "class-A"_sr, Optional<int64_t>{}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Restart") {
	state TransactionEnvironment env(params.getDataDir());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	env.restartNode();
	wait(check(env, "class-A"_sr, int64_t{ 1 }));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/CompactNode") {
	state TransactionEnvironment env(params.getDataDir());
	wait(set(env, "class-A"_sr, int64_t{ 1 }));
	wait(compact(env));
	wait(check(env, "class-A"_sr, int64_t{ 1 }));
	wait(set(env, "class-A"_sr, int64_t{ 2 }));
	wait(check(env, "class-A"_sr, int64_t{ 2 }));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/GetConfigClasses") {
	wait(testGetConfigClasses(params, false));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/CompactThenGetConfigClasses") {
	wait(testGetConfigClasses(params, true));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/GetKnobs") {
	wait(testGetKnobs(params, false, false));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/CompactThenGetKnobs") {
	wait(testGetKnobs(params, false, true));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/GetGlobalKnobs") {
	wait(testGetKnobs(params, true, false));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/CompactThenGetGlobalKnobs") {
	wait(testGetKnobs(params, true, true));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/BadRangeRead") {
	state TransactionEnvironment env(params.getDataDir());
	try {
		wait(env.badRangeRead() || env.getError());
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_invalid_config_db_range_read);
	}
	return Void();
}
