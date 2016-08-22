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

#ifndef REALM_OBJECT_STORE_IMPL_SYNC_HPP
#define REALM_OBJECT_STORE_IMPL_SYNC_HPP

#include "impl/sync_fwd.hpp"

#include <realm/util/logger.hpp>
#include <realm/sync/client.hpp>

#include <functional>
#include <utility>
#include <string>
#include <thread>

namespace realm {
namespace _impl {


class SyncClient {
public:
    sync::Client client;

    SyncClient(std::unique_ptr<util::Logger> logger):
        client(make_client(*logger)), // Throws
        m_logger(std::move(logger))
    {
        auto run = [this] {
            client.run(); // Throws
        };
        m_thread = std::thread(std::move(run)); // Throws
    }

    ~SyncClient()
    {
        client.stop();
        m_thread.join();
    }

private:
    const std::unique_ptr<util::Logger> m_logger;
    std::thread m_thread;

    static sync::Client make_client(util::Logger& logger)
    {
        sync::Client::Config client_config;
        client_config.logger = &logger;
        return sync::Client(client_config); // Throws
    }
};


class SyncSession {
public:
    const std::shared_ptr<SyncClient> client;
    const std::string path;
    const std::string server_url;
    const std::string access_token;
    sync::Session session;

    SyncSession(std::shared_ptr<SyncClient> client, std::string path, std::string server_url,
                std::string access_token,
                std::function<void (VersionID, VersionID)> sync_transact_callback):
        client(std::move(client)),
        path(std::move(path)),
        server_url(std::move(server_url)),
        access_token(std::move(access_token)),
        session(this->client->client, this->path) // Throws
    {
        session.set_sync_transact_callback(std::move(sync_transact_callback)); // Throws
        session.bind(this->server_url, this->access_token); // Throws
    }
};


} // namespace _impl
} // namespace realm

#endif // REALM_OBJECT_STORE_IMPL_SYNC_HPP
