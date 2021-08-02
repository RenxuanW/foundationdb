/*
 * LocalConfiguration.actor.cpp
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

#include "fdbclient/IKnobCollection.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/ConfigBroadcastFollowerInterface.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/LocalConfiguration.h"
#include "fdbserver/OnDemandStore.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // This must be the last #include.

FDB_DEFINE_BOOLEAN_PARAM(IsTest);

namespace {

const KeyRef configPathKey = "configPath"_sr;
const KeyRef lastSeenVersionKey = "lastSeenVersion"_sr;
const KeyRangeRef knobOverrideKeys = KeyRangeRef("knobOverride/"_sr, "knobOverride0"_sr);

KeyRef stringToKeyRef(std::string const& s) {
	return StringRef(reinterpret_cast<uint8_t const*>(s.c_str()), s.size());
}

} // namespace

class LocalConfigurationImpl {
	UID id;
	OnDemandStore kvStore;
	Future<Void> initFuture;
	Version lastSeenVersion{ 0 };
	std::unique_ptr<IKnobCollection> testKnobCollection;

	class ConfigKnobOverrides {
		Standalone<VectorRef<KeyRef>> configPath;
		std::map<Optional<Key>, std::map<Key, KnobValue>> configClassToKnobToValue;

	public:
		ConfigKnobOverrides() = default;
		explicit ConfigKnobOverrides(std::string const& paramString) {
			configClassToKnobToValue[{}] = {};
			if (std::all_of(paramString.begin(), paramString.end(), [](char c) {
				    return isalpha(c) || isdigit(c) || c == '/' || c == '-';
			    })) {
				StringRef s = stringToKeyRef(paramString);
				while (s.size()) {
					configPath.push_back_deep(configPath.arena(), s.eat("/"_sr));
					configClassToKnobToValue[configPath.back()] = {};
				}
			} else {
				TEST(true); // Invalid configuration path
				if (!g_network->isSimulated()) {
					fprintf(stderr, "WARNING: Invalid configuration path: `%s'\n", paramString.c_str());
				}
				throw invalid_config_path();
			}
		}
		ConfigClassSet getConfigClassSet() const { return ConfigClassSet(configPath); }
		void set(Optional<KeyRef> configClass, KeyRef knobName, KnobValueRef value) {
			configClassToKnobToValue[configClass.castTo<Key>()][knobName] = value;
		}
		void remove(Optional<KeyRef> configClass, KeyRef knobName) {
			configClassToKnobToValue[configClass.castTo<Key>()].erase(knobName);
		}

		void update(IKnobCollection& knobCollection) const {
			// Apply global overrides
			const auto& knobToValue = configClassToKnobToValue.at({});
			for (const auto& [knobName, knobValue] : knobToValue) {
				try {
					knobCollection.setKnob(knobName.toString(), knobValue);
				} catch (Error& e) {
					if (e.code() == error_code_invalid_option_value) {
						TEST(true); // invalid knob in configuration database
						TraceEvent(SevWarnAlways, "InvalidKnobOptionValue")
						    .detail("KnobName", knobName)
						    .detail("KnobValue", knobValue.toString());
					} else {
						throw e;
					}
				}
			}
			// Apply specific overrides
			for (const auto& configClass : configPath) {
				const auto& knobToValue = configClassToKnobToValue.at(configClass);
				for (const auto& [knobName, knobValue] : knobToValue) {
					knobCollection.setKnob(knobName.toString(), knobValue);
				}
			}
		}

		bool hasSameConfigPath(ConfigKnobOverrides const& other) const { return configPath == other.configPath; }

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, configPath);
		}
	} configKnobOverrides;

	class ManualKnobOverrides {
		std::map<Key, KnobValue> overrides;

	public:
		explicit ManualKnobOverrides(std::map<std::string, std::string> const& overrides) {
			for (const auto& [knobName, knobValueString] : overrides) {
				try {
					auto knobValue =
					    IKnobCollection::parseKnobValue(knobName, knobValueString, IKnobCollection::Type::TEST);
					this->overrides[stringToKeyRef(knobName)] = knobValue;
				} catch (Error& e) {
					if (e.code() == error_code_invalid_option) {
						TEST(true); // Attempted to manually set invalid knob option
						if (!g_network->isSimulated()) {
							fprintf(stderr, "WARNING: Unrecognized knob option '%s'\n", knobName.c_str());
						}
						TraceEvent(SevWarnAlways, "UnrecognizedKnobOption").detail("Knob", printable(knobName));
					} else if (e.code() == error_code_invalid_option_value) {
						TEST(true); // Invalid manually set knob value
						if (!g_network->isSimulated()) {
							fprintf(stderr,
							        "WARNING: Invalid value '%s' for knob option '%s'\n",
							        knobValueString.c_str(),
							        knobName.c_str());
						}
						TraceEvent(SevWarnAlways, "InvalidKnobValue")
						    .detail("Knob", printable(knobName))
						    .detail("Value", printable(knobValueString));
					} else {
						throw e;
					}
				}
			}
		}

		void update(IKnobCollection& knobCollection) {
			for (const auto& [knobName, knobValue] : overrides) {
				knobCollection.setKnob(knobName.toString(), knobValue);
			}
		}
	} manualKnobOverrides;

	IKnobCollection& getKnobs() {
		return testKnobCollection ? *testKnobCollection : IKnobCollection::getMutableGlobalKnobCollection();
	}

	IKnobCollection const& getKnobs() const {
		return testKnobCollection ? *testKnobCollection : IKnobCollection::getGlobalKnobCollection();
	}

	CounterCollection cc;
	Counter broadcasterChanges;
	Counter snapshots;
	Counter changeRequestsFetched;
	Counter mutations;
	Future<Void> logger;

	ACTOR static Future<Void> saveConfigPath(LocalConfigurationImpl* self) {
		self->kvStore->set(
		    KeyValueRef(configPathKey, BinaryWriter::toValue(self->configKnobOverrides, IncludeVersion())));
		wait(self->kvStore->commit());
		return Void();
	}

	ACTOR static Future<Void> clearKVStore(LocalConfigurationImpl* self) {
		self->kvStore->clear(singleKeyRange(configPathKey));
		self->kvStore->clear(knobOverrideKeys);
		wait(self->kvStore->commit());
		return Void();
	}

	ACTOR static Future<Version> getLastSeenVersion(LocalConfigurationImpl* self) {
		state Version result = 0;
		state Optional<Value> lastSeenVersionValue = wait(self->kvStore->readValue(lastSeenVersionKey));
		if (!lastSeenVersionValue.present()) {
			self->kvStore->set(KeyValueRef(lastSeenVersionKey, BinaryWriter::toValue(result, IncludeVersion())));
			wait(self->kvStore->commit());
		} else {
			result = BinaryReader::fromStringRef<Version>(lastSeenVersionValue.get(), IncludeVersion());
		}
		return result;
	}

	ACTOR static Future<Void> initialize(LocalConfigurationImpl* self) {
		state Version lastSeenVersion = wait(getLastSeenVersion(self));
		state Optional<Value> storedConfigPathValue = wait(self->kvStore->readValue(configPathKey));
		if (!storedConfigPathValue.present()) {
			wait(saveConfigPath(self));
			self->updateInMemoryState(lastSeenVersion);
			return Void();
		}
		state ConfigKnobOverrides storedConfigPath =
		    BinaryReader::fromStringRef<ConfigKnobOverrides>(storedConfigPathValue.get(), IncludeVersion());
		if (!storedConfigPath.hasSameConfigPath(self->configKnobOverrides)) {
			TEST(true); // All local information is outdated
			wait(clearKVStore(self));
			wait(saveConfigPath(self));
			self->updateInMemoryState(lastSeenVersion);
			return Void();
		}
		Standalone<RangeResultRef> range = wait(self->kvStore->readRange(knobOverrideKeys));
		for (const auto& kv : range) {
			auto configKey =
			    BinaryReader::fromStringRef<ConfigKey>(kv.key.removePrefix(knobOverrideKeys.begin), IncludeVersion());
			self->configKnobOverrides.set(configKey.configClass,
			                              configKey.knobName,
			                              ObjectReader::fromStringRef<KnobValue>(kv.value, IncludeVersion()));
		}
		self->updateInMemoryState(lastSeenVersion);
		return Void();
	}

	void updateInMemoryState(Version lastSeenVersion) {
		this->lastSeenVersion = lastSeenVersion;
		// TODO: Support randomization?
		getKnobs().reset(Randomize::False, g_network->isSimulated() ? IsSimulated::True : IsSimulated::False);
		configKnobOverrides.update(getKnobs());
		manualKnobOverrides.update(getKnobs());
		// Must reinitialize in order to update dependent knobs
		getKnobs().initialize(Randomize::False, g_network->isSimulated() ? IsSimulated::True : IsSimulated::False);
	}

	ACTOR static Future<Void> setSnapshot(LocalConfigurationImpl* self,
	                                      std::map<ConfigKey, KnobValue> snapshot,
	                                      Version snapshotVersion) {
		// TODO: Concurrency control?
		ASSERT(self->initFuture.isValid() && self->initFuture.isReady());
		++self->snapshots;
		self->kvStore->clear(knobOverrideKeys);
		for (const auto& [configKey, knobValue] : snapshot) {
			self->configKnobOverrides.set(configKey.configClass, configKey.knobName, knobValue);
			self->kvStore->set(
			    KeyValueRef(BinaryWriter::toValue(configKey, IncludeVersion()).withPrefix(knobOverrideKeys.begin),
			                ObjectWriter::toValue(knobValue, IncludeVersion())));
		}
		ASSERT_GE(snapshotVersion, self->lastSeenVersion);
		self->kvStore->set(KeyValueRef(lastSeenVersionKey, BinaryWriter::toValue(snapshotVersion, IncludeVersion())));
		wait(self->kvStore->commit());
		self->updateInMemoryState(snapshotVersion);
		return Void();
	}

	ACTOR static Future<Void> addChanges(LocalConfigurationImpl* self,
	                                     Standalone<VectorRef<VersionedConfigMutationRef>> changes,
	                                     Version mostRecentVersion) {
		// TODO: Concurrency control?
		ASSERT(self->initFuture.isValid() && self->initFuture.isReady());
		++self->changeRequestsFetched;
		for (const auto& versionedMutation : changes) {
			ASSERT_GT(versionedMutation.version, self->lastSeenVersion);
			++self->mutations;
			const auto& mutation = versionedMutation.mutation;
			auto serializedKey = BinaryWriter::toValue(mutation.getKey(), IncludeVersion());
			if (mutation.isSet()) {
				self->kvStore->set(KeyValueRef(serializedKey.withPrefix(knobOverrideKeys.begin),
				                               ObjectWriter::toValue(mutation.getValue(), IncludeVersion())));
				self->configKnobOverrides.set(mutation.getConfigClass(), mutation.getKnobName(), mutation.getValue());
			} else {
				self->kvStore->clear(singleKeyRange(serializedKey.withPrefix(knobOverrideKeys.begin)));
				self->configKnobOverrides.remove(mutation.getConfigClass(), mutation.getKnobName());
			}
		}
		self->kvStore->set(KeyValueRef(lastSeenVersionKey, BinaryWriter::toValue(mostRecentVersion, IncludeVersion())));
		wait(self->kvStore->commit());
		self->updateInMemoryState(mostRecentVersion);
		return Void();
	}

	ACTOR static Future<Void> consumeInternal(LocalConfigurationImpl* self,
	                                          ConfigBroadcastFollowerInterface broadcaster) {
		loop {
			try {
				state ConfigBroadcastFollowerGetChangesReply changesReply =
				    wait(broadcaster.getChanges.getReply(ConfigBroadcastFollowerGetChangesRequest{
				        self->lastSeenVersion, self->configKnobOverrides.getConfigClassSet() }));
				TraceEvent(SevDebug, "LocalConfigGotChanges", self->id)
				    .detail("Size", changesReply.changes.size())
				    .detail("Version", changesReply.mostRecentVersion);
				wait(self->addChanges(changesReply.changes, changesReply.mostRecentVersion));
			} catch (Error& e) {
				if (e.code() == error_code_version_already_compacted) {
					state ConfigBroadcastFollowerGetSnapshotReply snapshotReply = wait(broadcaster.getSnapshot.getReply(
					    ConfigBroadcastFollowerGetSnapshotRequest{ self->configKnobOverrides.getConfigClassSet() }));
					ASSERT_GT(snapshotReply.version, self->lastSeenVersion);
					++self->snapshots;
					wait(setSnapshot(self, std::move(snapshotReply.snapshot), snapshotReply.version));
				} else {
					throw e;
				}
			}
			wait(yield()); // Necessary to not immediately trigger retry?
		}
	}

	ACTOR static Future<Void> consume(LocalConfigurationImpl* self,
	                                  Reference<IAsyncListener<ConfigBroadcastFollowerInterface> const> broadcaster) {
		ASSERT(self->initFuture.isValid() && self->initFuture.isReady());
		loop {
			choose {
				when(wait(brokenPromiseToNever(consumeInternal(self, broadcaster->get())))) { ASSERT(false); }
				when(wait(broadcaster->onChange())) { ++self->broadcasterChanges; }
				when(wait(self->kvStore->getError())) { ASSERT(false); }
			}
		}
	}

public:
	LocalConfigurationImpl(std::string const& dataFolder,
	                       std::string const& configPath,
	                       std::map<std::string, std::string> const& manualKnobOverrides,
	                       IsTest isTest)
	  : id(deterministicRandom()->randomUniqueID()), kvStore(dataFolder, id, "localconf-"),
	    configKnobOverrides(configPath), manualKnobOverrides(manualKnobOverrides), cc("LocalConfiguration"),
	    broadcasterChanges("BroadcasterChanges", cc), snapshots("Snapshots", cc),
	    changeRequestsFetched("ChangeRequestsFetched", cc), mutations("Mutations", cc) {
		if (isTest) {
			testKnobCollection =
			    IKnobCollection::create(IKnobCollection::Type::TEST,
			                            Randomize::False,
			                            g_network->isSimulated() ? IsSimulated::True : IsSimulated::False);
		}
		logger = traceCounters(
		    "LocalConfigurationMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "LocalConfigurationMetrics");
	}

	Future<Void> initialize() {
		ASSERT(!initFuture.isValid());
		initFuture = initialize(this);
		return initFuture;
	}

	Future<Void> addChanges(Standalone<VectorRef<VersionedConfigMutationRef>> changes, Version mostRecentVersion) {
		return addChanges(this, changes, mostRecentVersion);
	}

	FlowKnobs const& getFlowKnobs() const {
		ASSERT(initFuture.isValid() && initFuture.isReady());
		return getKnobs().getFlowKnobs();
	}

	ClientKnobs const& getClientKnobs() const {
		ASSERT(initFuture.isValid() && initFuture.isReady());
		return getKnobs().getClientKnobs();
	}

	ServerKnobs const& getServerKnobs() const {
		ASSERT(initFuture.isValid() && initFuture.isReady());
		return getKnobs().getServerKnobs();
	}

	TestKnobs const& getTestKnobs() const {
		ASSERT(initFuture.isValid() && initFuture.isReady());
		return getKnobs().getTestKnobs();
	}

	Future<Void> consume(Reference<IAsyncListener<ConfigBroadcastFollowerInterface> const> const& broadcaster) {
		return consume(this, broadcaster);
	}

	UID getID() const { return id; }

	static void testManualKnobOverridesInvalidName() {
		std::map<std::string, std::string> invalidOverrides;
		invalidOverrides["knob_name_that_does_not_exist"] = "";
		// Should only trace and not throw an error:
		ManualKnobOverrides manualKnobOverrides(invalidOverrides);
	}

	static void testManualKnobOverridesInvalidValue() {
		std::map<std::string, std::string> invalidOverrides;
		invalidOverrides["test_int"] = "not_an_int";
		// Should only trace and not throw an error:
		ManualKnobOverrides manualKnobOverrides(invalidOverrides);
	}

	static void testConfigKnobOverridesInvalidConfigPath() {
		try {
			ConfigKnobOverrides configKnobOverrides("#invalid_config_path");
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_invalid_config_path);
		}
	}

	static void testConfigKnobOverridesInvalidName() {
		ConfigKnobOverrides configKnobOverrides;
		configKnobOverrides.set(
		    {}, "knob_name_that_does_not_exist"_sr, KnobValueRef::create(ParsedKnobValue(int{ 1 })));
		auto testKnobCollection =
		    IKnobCollection::create(IKnobCollection::Type::TEST, Randomize::False, IsSimulated::False);
		// Should only trace and not throw an error:
		configKnobOverrides.update(*testKnobCollection);
	}

	static void testConfigKnobOverridesInvalidValue() {
		ConfigKnobOverrides configKnobOverrides;
		configKnobOverrides.set({}, "test_int"_sr, KnobValueRef::create(ParsedKnobValue("not_an_int")));
		auto testKnobCollection =
		    IKnobCollection::create(IKnobCollection::Type::TEST, Randomize::False, IsSimulated::False);
		// Should only trace and not throw an error:
		configKnobOverrides.update(*testKnobCollection);
	}
};

LocalConfiguration::LocalConfiguration(std::string const& dataFolder,
                                       std::string const& configPath,
                                       std::map<std::string, std::string> const& manualKnobOverrides,
                                       IsTest isTest)
  : _impl(std::make_unique<LocalConfigurationImpl>(dataFolder, configPath, manualKnobOverrides, isTest)) {}

LocalConfiguration::LocalConfiguration(LocalConfiguration&&) = default;

LocalConfiguration& LocalConfiguration::operator=(LocalConfiguration&&) = default;

LocalConfiguration::~LocalConfiguration() = default;

Future<Void> LocalConfiguration::initialize() {
	return impl().initialize();
}

FlowKnobs const& LocalConfiguration::getFlowKnobs() const {
	return impl().getFlowKnobs();
}

ClientKnobs const& LocalConfiguration::getClientKnobs() const {
	return impl().getClientKnobs();
}

ServerKnobs const& LocalConfiguration::getServerKnobs() const {
	return impl().getServerKnobs();
}

TestKnobs const& LocalConfiguration::getTestKnobs() const {
	return impl().getTestKnobs();
}

Future<Void> LocalConfiguration::consume(
    Reference<IAsyncListener<ConfigBroadcastFollowerInterface> const> const& broadcaster) {
	return impl().consume(broadcaster);
}

Future<Void> LocalConfiguration::addChanges(Standalone<VectorRef<VersionedConfigMutationRef>> changes,
                                            Version mostRecentVersion) {
	return impl().addChanges(changes, mostRecentVersion);
}

UID LocalConfiguration::getID() const {
	return impl().getID();
}

TEST_CASE("/fdbserver/ConfigDB/ManualKnobOverrides/InvalidName") {
	LocalConfigurationImpl::testManualKnobOverridesInvalidName();
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/ManualKnobOverrides/InvalidValue") {
	LocalConfigurationImpl::testManualKnobOverridesInvalidValue();
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/ConfigKnobOverrides/InvalidConfigPath") {
	LocalConfigurationImpl::testConfigKnobOverridesInvalidConfigPath();
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/ConfigKnobOverrides/InvalidName") {
	LocalConfigurationImpl::testConfigKnobOverridesInvalidName();
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/ConfigKnobOverrides/InvalidValue") {
	LocalConfigurationImpl::testConfigKnobOverridesInvalidValue();
	return Void();
}
