/*
 * SimpleConfigTransaction.actor.cpp
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

#include <algorithm>

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/SimpleConfigTransaction.h"
#include "fdbserver/Knobs.h"
#include "flow/Arena.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class SimpleConfigTransactionImpl {
	ConfigTransactionCommitRequest toCommit;
	Future<Version> getVersionFuture;
	ConfigTransactionInterface cti;
	int numRetries{ 0 };
	bool committed{ false };
	Optional<UID> dID;
	Database cx;

	ACTOR static Future<Version> getReadVersion(SimpleConfigTransactionImpl* self) {
		if (self->dID.present()) {
			TraceEvent("SimpleConfigTransactionGettingReadVersion", self->dID.get());
		}
		ConfigTransactionGetVersionRequest req;
		ConfigTransactionGetVersionReply reply =
		    wait(self->cti.getVersion.getReply(ConfigTransactionGetVersionRequest{}));
		if (self->dID.present()) {
			TraceEvent("SimpleConfigTransactionGotReadVersion", self->dID.get()).detail("Version", reply.version);
		}
		return reply.version;
	}

	ACTOR static Future<Optional<Value>> get(SimpleConfigTransactionImpl* self, KeyRef key) {
		if (!self->getVersionFuture.isValid()) {
			self->getVersionFuture = getReadVersion(self);
		}
		state ConfigKey configKey = ConfigKey::decodeKey(key);
		Version version = wait(self->getVersionFuture);
		if (self->dID.present()) {
			TraceEvent("SimpleConfigTransactionGettingValue", self->dID.get())
			    .detail("ConfigClass", configKey.configClass)
			    .detail("KnobName", configKey.knobName);
		}
		ConfigTransactionGetReply reply =
		    wait(self->cti.get.getReply(ConfigTransactionGetRequest{ version, configKey }));
		if (self->dID.present()) {
			TraceEvent("SimpleConfigTransactionGotValue", self->dID.get())
			    .detail("Value", reply.value.get().toString());
		}
		if (reply.value.present()) {
			return reply.value.get().toValue();
		} else {
			return {};
		}
	}

	ACTOR static Future<Standalone<RangeResultRef>> getConfigClasses(SimpleConfigTransactionImpl* self) {
		if (!self->getVersionFuture.isValid()) {
			self->getVersionFuture = getReadVersion(self);
		}
		Version version = wait(self->getVersionFuture);
		ConfigTransactionGetConfigClassesReply reply =
		    wait(self->cti.getClasses.getReply(ConfigTransactionGetConfigClassesRequest{ version }));
		Standalone<RangeResultRef> result;
		for (const auto& configClass : reply.configClasses) {
			result.push_back_deep(result.arena(), KeyValueRef(configClass, ""_sr));
		}
		return result;
	}

	ACTOR static Future<Standalone<RangeResultRef>> getKnobs(SimpleConfigTransactionImpl* self,
	                                                         Optional<Key> configClass) {
		if (!self->getVersionFuture.isValid()) {
			self->getVersionFuture = getReadVersion(self);
		}
		Version version = wait(self->getVersionFuture);
		ConfigTransactionGetKnobsReply reply =
		    wait(self->cti.getKnobs.getReply(ConfigTransactionGetKnobsRequest{ version, configClass }));
		Standalone<RangeResultRef> result;
		for (const auto& knobName : reply.knobNames) {
			result.push_back_deep(result.arena(), KeyValueRef(knobName, ""_sr));
		}
		return result;
	}

	ACTOR static Future<Void> commit(SimpleConfigTransactionImpl* self) {
		if (!self->getVersionFuture.isValid()) {
			self->getVersionFuture = getReadVersion(self);
		}
		wait(store(self->toCommit.version, self->getVersionFuture));
		self->toCommit.annotation.timestamp = now();
		wait(self->cti.commit.getReply(self->toCommit));
		self->committed = true;
		return Void();
	}

public:
	SimpleConfigTransactionImpl(Database const& cx) : cx(cx) {
		auto coordinators = cx->getConnectionFile()->getConnectionString().coordinators();
		std::sort(coordinators.begin(), coordinators.end());
		cti = ConfigTransactionInterface(coordinators[0]);
	}

	SimpleConfigTransactionImpl(ConfigTransactionInterface const& cti) : cti(cti) {}

	void set(KeyRef key, ValueRef value) {
		if (key == configTransactionDescriptionKey) {
			toCommit.annotation.description = KeyRef(toCommit.arena, value);
		} else {
			ConfigKey configKey = ConfigKeyRef::decodeKey(key);
			auto knobValue = IKnobCollection::parseKnobValue(
			    configKey.knobName.toString(), value.toString(), IKnobCollection::Type::TEST);
			toCommit.mutations.emplace_back_deep(toCommit.arena, configKey, knobValue.contents());
		}
	}

	void clear(KeyRef key) {
		if (key == configTransactionDescriptionKey) {
			toCommit.annotation.description = ""_sr;
		} else {
			toCommit.mutations.emplace_back_deep(
			    toCommit.arena, ConfigKeyRef::decodeKey(key), Optional<KnobValueRef>{});
		}
	}

	Future<Optional<Value>> get(KeyRef key) { return get(this, key); }

	Future<Standalone<RangeResultRef>> getRange(KeyRangeRef keys) {
		if (keys == configClassKeys) {
			return getConfigClasses(this);
		} else if (keys == globalConfigKnobKeys) {
			return getKnobs(this, {});
		} else if (configKnobKeys.contains(keys) && keys.singleKeyRange()) {
			const auto configClass = keys.begin.removePrefix(configKnobKeys.begin);
			return getKnobs(this, configClass);
		} else {
			throw invalid_config_db_range_read();
		}
	}

	Future<Void> commit() { return commit(this); }

	Future<Void> onError(Error const& e) {
		// TODO: Improve this:
		if (e.code() == error_code_transaction_too_old) {
			reset();
			return delay((1 << numRetries++) * 0.01 * deterministicRandom()->random01());
		}
		throw e;
	}

	Future<Version> getReadVersion() {
		if (!getVersionFuture.isValid())
			getVersionFuture = getReadVersion(this);
		return getVersionFuture;
	}

	Optional<Version> getCachedReadVersion() const {
		if (getVersionFuture.isValid() && getVersionFuture.isReady() && !getVersionFuture.isError()) {
			return getVersionFuture.get();
		} else {
			return {};
		}
	}

	Version getCommittedVersion() const { return committed ? getVersionFuture.get() : ::invalidVersion; }

	void reset() {
		getVersionFuture = Future<Version>{};
		toCommit = {};
		committed = false;
	}

	void fullReset() {
		numRetries = 0;
		dID = {};
		reset();
	}

	size_t getApproximateSize() const { return toCommit.expectedSize(); }

	void debugTransaction(UID dID) {
		this->dID = dID;
	}

	void checkDeferredError(Error const& deferredError) const {
		if (deferredError.code() != invalid_error_code) {
			throw deferredError;
		}
		if (cx.getPtr()) {
			cx->checkDeferredError();
		}
	}
}; // SimpleConfigTransactionImpl

Future<Version> SimpleConfigTransaction::getReadVersion() {
	return impl().getReadVersion();
}

Optional<Version> SimpleConfigTransaction::getCachedReadVersion() const {
	return impl().getCachedReadVersion();
}

Future<Optional<Value>> SimpleConfigTransaction::get(Key const& key, Snapshot snapshot) {
	return impl().get(key);
}

Future<Standalone<RangeResultRef>> SimpleConfigTransaction::getRange(KeySelector const& begin,
                                                                     KeySelector const& end,
                                                                     int limit,
                                                                     Snapshot snapshot,
                                                                     Reverse reverse) {
	return impl().getRange(KeyRangeRef(begin.getKey(), end.getKey()));
}

Future<Standalone<RangeResultRef>> SimpleConfigTransaction::getRange(KeySelector begin,
                                                                     KeySelector end,
                                                                     GetRangeLimits limits,
                                                                     Snapshot snapshot,
                                                                     Reverse reverse) {
	return impl().getRange(KeyRangeRef(begin.getKey(), end.getKey()));
}

void SimpleConfigTransaction::set(KeyRef const& key, ValueRef const& value) {
	impl().set(key, value);
}

void SimpleConfigTransaction::clear(KeyRef const& key) {
	impl().clear(key);
}

Future<Void> SimpleConfigTransaction::commit() {
	return impl().commit();
}

Version SimpleConfigTransaction::getCommittedVersion() const {
	return impl().getCommittedVersion();
}

int64_t SimpleConfigTransaction::getApproximateSize() const {
	return impl().getApproximateSize();
}

void SimpleConfigTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	// TODO: Support using this option to determine atomicity
}

Future<Void> SimpleConfigTransaction::onError(Error const& e) {
	return impl().onError(e);
}

void SimpleConfigTransaction::cancel() {
	// TODO: Implement someday
	throw client_invalid_operation();
}

void SimpleConfigTransaction::reset() {
	return impl().reset();
}

void SimpleConfigTransaction::fullReset() {
	return impl().fullReset();
}

void SimpleConfigTransaction::debugTransaction(UID dID) {
	impl().debugTransaction(dID);
}

void SimpleConfigTransaction::checkDeferredError() const {
	impl().checkDeferredError(deferredError);
}

void SimpleConfigTransaction::setDatabase(Database const& cx) {
	_impl = std::make_unique<SimpleConfigTransactionImpl>(cx);
}

SimpleConfigTransaction::SimpleConfigTransaction(ConfigTransactionInterface const& cti)
  : _impl(std::make_unique<SimpleConfigTransactionImpl>(cti)) {}

SimpleConfigTransaction::SimpleConfigTransaction() = default;

SimpleConfigTransaction::~SimpleConfigTransaction() = default;
