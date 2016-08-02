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

#ifndef REALM_OBJECT_STORE_GLOBAL_NOTIFIER_HPP
#define REALM_OBJECT_STORE_GLOBAL_NOTIFIER_HPP

#include "impl/sync_fwd.hpp"
#include "shared_realm.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <unordered_map>

namespace realm {

/// Used to listen for changes across all, or a subset of all Realms on a
/// particular sync server.
class GlobalNotifierBase {
protected:
    using listen_ident_type = std::int_fast64_t;

    class AsyncTarget;

    GlobalNotifierBase(std::shared_ptr<AsyncTarget>, std::string local_root_dir,
                       std::string server_base_url, std::string access_token);

    virtual ~GlobalNotifierBase() noexcept {}

    /// Start listening. No callbacks will be called until this function is
    /// called.
    void start();

    /// Called synchronously from on_admin_change() to determine whether the
    /// application wants to listen for changes to a particular Realm.
    ///
    /// The Realm name that is passed to the callback is hierarchical and takes
    /// the form of an abosulte path (separated by forward slashes). This is a
    /// *virtual path*, i.e, it is not necesarily the file system path of the
    /// Realm on the server.
    ///
    /// \param name The name (virtual path) by which the server knows that
    /// Realm.
    virtual bool filter_callback(const std::string& name) = 0;

    /// This function checks whether new Realms have appeared. For each new
    /// Realm, a new sync session is started if filter_callback() returns
    /// true. It calls filter_callback() in a synchronous fashion.
    ///
    /// The application (subclass) must arrange for this function to be called
    /// shortly after it has been notified once (or multiple times) about a
    /// change in the admin Realm via AsyncTarget::on_admin_change().
    ///
    /// The application (subclass) must ensure that this function is never
    /// executed concurrently with itself, or with get_realm_name() or
    /// get_realm() for the same notifier object.
    void on_admin_change();

    /// The application (subclass) may call this function to get the name
    /// (virtual path) of the Realm associated with a specific listener slot.
    ///
    /// The application (subclass) must ensure that this function is never
    /// executed concurrently with itself, or with on_admin_change() or
    /// get_realm() for the same notifier object.
    std::string get_realm_name(listen_ident_type listen_ident) const;

    /// The application (subclass) may call this function to get the accessor
    /// for the Realm associated with a specific listener slot. This function
    /// returns null until that Realm has a nonempty schema.
    ///
    /// The application (subclass) must ensure that this function is never
    /// executed concurrently with itself, or with on_admin_change() or
    /// get_realm_name() for the same notifier object.
    SharedRealm get_realm(listen_ident_type listen_ident) const;

    AsyncTarget& get_async_target() noexcept;

private:
    const std::shared_ptr<AsyncTarget> m_async_target;
    const std::string m_local_admin_realm_path;
    const std::string m_regular_realms_dir;
    const std::string m_server_base_url;
    const std::string m_access_token;

    std::shared_ptr<_impl::SyncSession> m_admin_realm_sync_session;

    // Set of Realm identifiers as stored in the admin Realm
    // (RealmFile.schema.id).
    std::unordered_set<std::string> m_known_realms;

    struct ListenEntry {
        std::string realm_id;
        std::string realm_name; // Server side virtual path
        std::shared_ptr<_impl::SyncSession> sync_session;
    };
    std::unordered_map<listen_ident_type, ListenEntry> m_listen_entries;

    listen_ident_type m_next_listen_ident = 0;

    std::string get_local_path(const std::string& realm_id) const; // Thread-safe
    std::string get_server_url(const std::string& realm_name) const; // Thread-safe
};


class GlobalNotifierBase::AsyncTarget {
public:
    /// Called asynchronously when the admin Realm has changed.
    ///
    /// The application (subclass) must assume that this function is executed by
    /// an arbitrary thread, and that multiple executions may overlap in time.
    ///
    /// \sa GlobalNotifierBase::on_admin_change().
    virtual void on_admin_change() = 0;

    /// Called asynchronously when a regular Realm (not the admin Realm) has
    /// changed due to a transaction performed on behalf of the synchronization
    /// mechanism. This function is not called as a result of changing the Realm
    /// locally.
    ///
    /// The application (subclass) must assume that this function is executed by
    /// an arbitrary thread, and that multiple executions may overlap in time.
    ///
    /// \param listen_ident An integer used to identify the listener slot
    /// associated with the Realm that has changed. The application (subclass)
    /// can pass that integer back to get_realm().
    virtual void on_regular_change(listen_ident_type listen_ident) = 0;
};




// Implementation

inline GlobalNotifierBase::AsyncTarget& GlobalNotifierBase::get_async_target() noexcept
{
    return *m_async_target;
}

} // namespace realm

#endif // REALM_OBJECT_STORE_GLOBAL_NOTIFIER_HPP
