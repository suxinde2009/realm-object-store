////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "catch.hpp"

#include "util/event_loop.hpp"
#include "util/index_helpers.hpp"
#include "util/test_file.hpp"

#include "impl/realm_coordinator.hpp"
#include "global_notifier.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "property.hpp"
#include "schema.hpp"
#include "sync_config.hpp"
#include "sync_manager.hpp"
#include "sync_session.hpp"

#include <realm/group_shared.hpp>

using namespace realm;

// {"identity":"test", "access": ["download", "upload"]}
static const std::string s_test_token = "eyJpZGVudGl0eSI6InRlc3QiLCAiYWNjZXNzIjogWyJkb3dubG9hZCIsICJ1cGxvYWQiXX0=";

static void login(const std::string& path, const SyncConfig& config)
{
    auto thread = std::thread([path, config]{
        auto session = SyncManager::shared().get_existing_active_session(path);
        session->refresh_access_token(s_test_token, config.realm_url);
    });
    thread.detach();
}

static void wait_for_upload(Realm& realm)
{
    std::condition_variable cv;
    std::mutex mutex;
    bool done = false;

    auto session = SyncManager::shared().get_existing_active_session(realm.config().path);
    session->wait_for_upload_completion([&] {
        done = true;
        cv.notify_one();
    });

    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] { return done; });
}

TEST_CASE("GlobalNotifier: callback invocation") {
    SyncServer server;

    SyncManager::shared().set_login_function(&login);

    auto root = util::make_temp_dir();
    _impl::AdminRealmManager admin_manager(root, server.base_url(), s_test_token);

    SyncTestFile config(admin_manager, "id", "/name");
    config.schema = Schema{
        {"object", {
            {"value", PropertyType::Int, "", "", false, false, false}
        }},
    };

    struct Target : GlobalNotifier::Callback {
        std::map<std::string, size_t> filter_calls;
        std::map<std::string, size_t> change_calls;
        GlobalNotifier* notifier;

        bool filter_callback(std::string const& name) override
        {
            ++filter_calls[name];
            return name == "/name";
        }

        void realm_changed(GlobalNotifier::ChangeNotification change) override
        {
            ++change_calls[change.get_new_realm()->config().sync_config->realm_url];
        }
    };
    auto unique_target = std::make_unique<Target>();
    auto target = unique_target.get();
    GlobalNotifier notifier(std::move(unique_target), root, server.base_url(), s_test_token);
    target->notifier = &notifier;
    notifier.start();

    SECTION("filter_callback()") {
        SECTION("is called when a new realm is added") {
            REQUIRE(target->filter_calls.empty());
            admin_manager.create_realm("id", "/name");
            REQUIRE(target->filter_calls.empty());
            util::run_event_loop_until([&]{ return !target->filter_calls.empty(); });
            REQUIRE(target->filter_calls.size() == 1);
            REQUIRE(target->filter_calls["/name"] == 1);
        }

        SECTION("is not called multiple times for the same name, regardless of result") {
            admin_manager.create_realm("id", "/name");
            util::run_event_loop_until([&]{ return target->filter_calls.size() > 0; });
            REQUIRE(target->filter_calls["/name"] == 1);

            admin_manager.create_realm("id2", "/name2");
            util::run_event_loop_until([&]{ return target->filter_calls.size() > 1; });
            REQUIRE(target->filter_calls["/name"] == 1);
            REQUIRE(target->filter_calls["/name2"] == 1);

            admin_manager.create_realm("id3", "/name3");
            util::run_event_loop_until([&]{ return target->filter_calls.size() > 2; });
            REQUIRE(target->filter_calls["/name"] == 1);
            REQUIRE(target->filter_calls["/name2"] == 1);
            REQUIRE(target->filter_calls["/name3"] == 1);
        }
    }

    SECTION("realm_changed()") {
        SECTION("is not called until the schema has been initialized") {
            admin_manager.create_realm("id", "/name");
            util::run_event_loop_until([&]{ return !target->filter_calls.empty(); });
            // Run the event loop one more time after the filter call to ensure
            // that it would have delivered if there was anything to deliver
            int calls = 0;
            util::run_event_loop_until([&]{ return ++calls == 2; });
            REQUIRE(target->change_calls.empty());

            wait_for_upload(*Realm::get_shared_realm(config));
            util::run_event_loop_until([&]{ return target->change_calls.size() > 0; });
            REQUIRE(target->change_calls[config.sync_config->realm_url] == 1);
        }

        SECTION("is not called for realms which were filtered out") {
            admin_manager.create_realm("id", "/name");
            admin_manager.create_realm("id2", "/name2");

            SyncTestFile config2(admin_manager, "id2", "/name2");
            config2.schema = config.schema;
            wait_for_upload(*Realm::get_shared_realm(config2));
            wait_for_upload(*Realm::get_shared_realm(config));

            util::run_event_loop_until([&]{ return target->change_calls.size() > 0; });
            REQUIRE(target->change_calls[config.sync_config->realm_url] == 1);
            REQUIRE(target->change_calls[config2.sync_config->realm_url] == 0);
        }

        SECTION("is called after each change") {
            admin_manager.create_realm("id", "/name");
            auto realm = Realm::get_shared_realm(config);

            util::run_event_loop_until([&]{ return target->change_calls.size() > 0; });
            REQUIRE(target->change_calls[config.sync_config->realm_url] == 1);

            realm->begin_transaction();
            realm->read_group().get_table("class_object")->add_empty_row();
            realm->commit_transaction();
            util::run_event_loop_until([&] { return target->change_calls[config.sync_config->realm_url] > 1; });
            REQUIRE(target->change_calls[config.sync_config->realm_url] == 2);

            realm->begin_transaction();
            realm->read_group().get_table("class_object")->add_empty_row();
            realm->commit_transaction();
            util::run_event_loop_until([&] { return target->change_calls[config.sync_config->realm_url] > 2; });
            REQUIRE(target->change_calls[config.sync_config->realm_url] == 3);
        }

        SECTION("is not called while paused") {
            admin_manager.create_realm("id", "/name");
            auto realm = Realm::get_shared_realm(config);

            util::run_event_loop_until([&]{ return target->change_calls.size() > 0; });
            REQUIRE(target->change_calls[config.sync_config->realm_url] == 1);

            realm->begin_transaction();
            realm->read_group().get_table("class_object")->add_empty_row();
            realm->commit_transaction();
            notifier.pause();
            util::run_event_loop_until([&] { return notifier.has_pending(); });
            REQUIRE(target->change_calls[config.sync_config->realm_url] == 1);

            notifier.resume();
            REQUIRE(target->change_calls[config.sync_config->realm_url] == 2);
        }
    }
}

TEST_CASE("GlobalNotifier: fine-grained") {
    SyncServer server;

    SyncManager::shared().set_login_function(&login);

    auto root = util::make_temp_dir();
    _impl::AdminRealmManager admin_manager(root, server.base_url(), s_test_token);

    SyncTestFile config(admin_manager, "id", "/name");
    config.schema = Schema{
        {"object", {
            {"value", PropertyType::Int, "", "", false, false, false}
        }},
    };

    struct Target : GlobalNotifier::Callback {
        util::Optional<GlobalNotifier::ChangeNotification> notification;

        bool filter_callback(std::string const&) override { return true; }

        void realm_changed(GlobalNotifier::ChangeNotification change) override
        {
            notification = std::move(change);
        }

        GlobalNotifier::ChangeNotification wait_for_change()
        {
            util::run_event_loop_until([=] { return bool(notification); });
            auto ret = std::move(*notification);
            notification = util::none;
            return ret;
        }
    };
    auto unique_target = std::make_unique<Target>();
    auto target = unique_target.get();
    GlobalNotifier notifier(std::move(unique_target), root, server.base_url(), s_test_token);

    SECTION("initial changes are empty if the file already exists") {
        admin_manager.create_realm("id", "/name");
        auto admin_config = admin_manager.get_config("id", "/name");
        admin_config.schema = config.schema;
        auto realm = Realm::get_shared_realm(admin_config);
        realm->begin_transaction();
        realm->read_group().get_table("class_object")->add_empty_row();
        realm->commit_transaction();

        notifier.start();
        auto change = target->wait_for_change();
        REQUIRE(change.get_changes().empty());
        REQUIRE(change.get_old_realm() == nullptr);
        REQUIRE(change.get_new_realm() != nullptr);
    }

    SECTION("basic modifications") {
        admin_manager.create_realm("id", "/name");
        notifier.start();

        auto realm = Realm::get_shared_realm(config);
        auto& table = *realm->read_group().get_table("class_object");

        realm->begin_transaction();
        table.add_empty_row(5);
        realm->commit_transaction();

        auto change = target->wait_for_change();
        REQUIRE_INDICES(change.get_changes().find("object")->second.insertions, 0, 1, 2, 3, 4);

        realm->begin_transaction();
        table.set_int(0, 3, 5);
        realm->commit_transaction();
        change = target->wait_for_change();
        REQUIRE_INDICES(change.get_changes().find("object")->second.modifications, 3);

        realm->begin_transaction();
        table.move_last_over(4);
        realm->commit_transaction();
        change = target->wait_for_change();
        REQUIRE_INDICES(change.get_changes().find("object")->second.deletions, 4);
    }

    SECTION("changes are reported for new tables") {
        admin_manager.create_realm("id", "/name");
        notifier.start();
        auto realm = Realm::get_shared_realm(config);
        target->wait_for_change();

        realm->begin_transaction();
        auto table = realm->read_group().add_table("class_newobject");
        table->add_column(type_Int, "col");
        table->add_empty_row();
        realm->commit_transaction();

        auto change = target->wait_for_change();
        REQUIRE_INDICES(change.get_changes().find("newobject")->second.insertions, 0);
    }

    SECTION("changes are reported for multiple tables") {
        config.schema = Schema{
            {"object", {
                {"value", PropertyType::Int, "", "", false, false, false}
            }},
            {"object2", {
                {"value", PropertyType::Int, "", "", false, false, false}
            }},
        };

        admin_manager.create_realm("id", "/name");
        notifier.start();
        auto realm = Realm::get_shared_realm(config);
        target->wait_for_change();

        realm->begin_transaction();
        auto& table = *realm->read_group().get_table("class_object");
        auto& table2 = *realm->read_group().get_table("class_object2");
        table.add_empty_row(5);
        table2.add_empty_row(2);
        realm->commit_transaction();

        auto change = target->wait_for_change();
        REQUIRE_INDICES(change.get_changes().find("object")->second.insertions, 0, 1, 2, 3, 4);
        REQUIRE_INDICES(change.get_changes().find("object2")->second.insertions, 0, 1);

        realm->begin_transaction();
        table.move_last_over(4);
        table.set_int(0, 3, 3);

        table2.add_empty_row(1);
        table2.set_int(0, 1, 1);
        realm->commit_transaction();

        change = target->wait_for_change();
        REQUIRE_INDICES(change.get_changes().find("object")->second.deletions, 4);
        REQUIRE_INDICES(change.get_changes().find("object")->second.modifications, 3);
        REQUIRE_INDICES(change.get_changes().find("object")->second.modifications_new, 3);
        REQUIRE_INDICES(change.get_changes().find("object2")->second.insertions, 2);
        REQUIRE_INDICES(change.get_changes().find("object2")->second.modifications, 1);
    }
}
