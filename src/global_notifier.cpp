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

#include "global_notifier.hpp"

#include "impl/realm_coordinator.hpp"
#include "impl/sync.hpp"
#include "object_store.hpp"

#include <realm/util/file.hpp>
#include <realm/util/uri.hpp>
#include <realm/sync/history.hpp>
#include <realm/group_shared.hpp>

#include <utility>
#include <stdexcept>

using namespace realm;


namespace {

// FIXME: This operation ought to be supported by realm::util::Uri and it ought
// to do it in a way that is consistent with RFC 3986 (or one of its
// alternatives).
std::string util_Uri_resolve(const std::string& path, const std::string& base_uri)
{
    util::Uri uri(base_uri); // Throws
    uri.set_path(path); // Throws
    uri.set_query(std::string());
    uri.set_frag(std::string());
    return uri.recompose(); // Throws
}

} // unnamed namespace


GlobalNotifierBase::GlobalNotifierBase(std::shared_ptr<AsyncTarget> async_target,
                                       std::string local_root_dir, std::string server_base_url,
                                       std::string access_token):
    m_async_target(std::move(async_target)),
    m_local_admin_realm_path(util::File::resolve("admin.realm", local_root_dir)), // Throws
    m_regular_realms_dir(util::File::resolve("realms", local_root_dir)), // Throws
    m_server_base_url(std::move(server_base_url)),
    m_access_token(std::move(access_token))
{
    util::try_make_dir(m_regular_realms_dir); // Throws
}


void GlobalNotifierBase::start()
{
    std::string local_path = m_local_admin_realm_path; // Throws (copy)
    std::string virtual_path = "/admin"; // Throws (copy)
    std::string server_url = util_Uri_resolve(virtual_path, m_server_base_url); // Throws
    std::string access_token = m_access_token; // Throws (copy)
    using version_type = sync::Session::version_type;
    auto sync_transact_callback = [async_target=m_async_target](version_type) {
        async_target->on_admin_change(); // Throws
    };
    m_admin_realm_sync_session =
        _impl::RealmCoordinator::get_sync_session(std::move(local_path),
                                                  std::move(server_url),
                                                  std::move(access_token),
                                                  std::move(sync_transact_callback)); // Throws
}


void GlobalNotifierBase::on_admin_change()
{
    std::unique_ptr<Replication> history =
        sync::make_sync_history(m_local_admin_realm_path); // Throws
    SharedGroup sg(*history); // Throws
    ReadTransaction rt(sg); // Throws
    const Group& group = rt.get_group();
    if (group.is_empty())
        return; // No schema yet
    ConstTableRef realms = ObjectStore::table_for_object_type(group, "RealmFile"); // Throws
    if (!realms)
        throw std::runtime_error("Unexpected schema in admin Realm (1)");
    size_t id_col_ndx   = realms->get_column_index("id");
    size_t name_col_ndx = realms->get_column_index("path");
    if (id_col_ndx == realm::not_found || name_col_ndx == realm::not_found)
        throw std::runtime_error("Unexpected schema in admin Realm (2)");
    size_t n = realms->size();
    for (size_t i = 0; i < n; ++i) {
        StringData realm_id = realms->get_string(id_col_ndx, i);
        std::string realm_id_2 = realm_id; // Throws (copy)
        if (m_known_realms.count(realm_id_2) > 0)
            continue;
        m_known_realms.insert(realm_id_2); // Throws
        listen_ident_type listen_ident = m_next_listen_ident;
        try {
            StringData realm_name = realms->get_string(name_col_ndx, i);
            std::string realm_name_2 = realm_name; // Throws (copy)
            bool yes = filter_callback(realm_name_2); // Throws
            if (!yes)
                continue;
            std::string local_path = get_local_path(realm_id_2); // Throws
            std::string server_url = get_server_url(realm_name_2); // Throws
            std::string access_token = m_access_token; // Throws (copy)
            using version_type = sync::Session::version_type;
            auto sync_transact_callback = [async_target=m_async_target,
                                           listen_ident](version_type) {
                async_target->on_regular_change(listen_ident); // Throws
            };
            std::shared_ptr<_impl::SyncSession> session =
                _impl::RealmCoordinator::get_sync_session(std::move(local_path),
                                                          std::move(server_url),
                                                          std::move(access_token),
                                                          std::move(sync_transact_callback)); // Throws
            ListenEntry entry;
            entry.realm_id = realm_id_2; // Throws (copy)
            entry.realm_name = std::move(realm_name_2);
            entry.sync_session = std::move(session);
            m_listen_entries.emplace(listen_ident, std::move(entry)); // Throws
            ++m_next_listen_ident;
        }
        catch (...) {
            m_known_realms.erase(realm_id_2);
            throw;
        }
        m_async_target->on_regular_change(listen_ident); // Throws
    }
}


std::string GlobalNotifierBase::get_realm_name(listen_ident_type listen_ident) const
{
    const ListenEntry& entry = m_listen_entries.at(listen_ident); // Throws
    std::string realm_name = entry.realm_name; // Throws (copy)
    return realm_name;
}


SharedRealm GlobalNotifierBase::get_realm(listen_ident_type listen_ident) const
{
    const ListenEntry& entry = m_listen_entries.at(listen_ident); // Throws
    Realm::Config config;
    config.path = get_local_path(entry.realm_id); // Throws
    config.automatic_change_notifications = false;
    config.cache = false; // No need for caching.
    config.sync_server_url = get_server_url(entry.realm_name); // Throws
    config.sync_user_token = m_access_token; // Throws (copy)
    SharedRealm realm = Realm::get_shared_realm(std::move(config)); // Throws
    bool empty_schema = realm->read_group().is_empty();
    if (empty_schema)
        realm.reset();
    return realm;
}


std::string GlobalNotifierBase::get_local_path(const std::string& realm_id) const
{
    std::string file_name = realm_id + ".realm"; // Throws
    std::string local_path = util::File::resolve(file_name, m_regular_realms_dir); // Throws
    return local_path;
}


std::string GlobalNotifierBase::get_server_url(const std::string& realm_name) const
{
    std::string server_url = util_Uri_resolve(realm_name, m_server_base_url); // Throws
    return server_url;
}
