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

#ifndef REALM_OS_SYNC_CLIENT_HPP
#define REALM_OS_SYNC_CLIENT_HPP

#include "binding_callback_thread_observer.hpp"

#include <realm/sync/client.hpp>
#include <realm/util/scope_exit.hpp>

#include <thread>

#include "sync/sync_manager.hpp"
#include "sync/impl/network_reachability.hpp"

#if NETWORK_REACHABILITY_AVAILABLE
#include "sync/impl/apple/network_reachability_observer.hpp"
#endif

namespace realm {
namespace _impl {

using ReconnectMode = sync::Client::ReconnectMode;

struct SyncClient {
    SyncClient(std::unique_ptr<util::Logger> logger, SyncClientConfig const& config)
        : m_client(make_client(*logger, config)) // Throws
        , m_logger(std::move(logger))
        , m_thread([this] {
            if (g_binding_callback_thread_observer) {
                g_binding_callback_thread_observer->did_create_thread();
                auto will_destroy_thread = util::make_scope_exit([&]() noexcept {
                    g_binding_callback_thread_observer->will_destroy_thread();
                });
                try {
                    m_client.run(); // Throws
                }
                catch (std::exception const& e) {
                    g_binding_callback_thread_observer->handle_error(e);
                }
            }
            else {
                m_client.run(); // Throws
            }
        }) // Throws
#if NETWORK_REACHABILITY_AVAILABLE
    , m_reachability_observer(none, [=](const NetworkReachabilityStatus status) {
        if (status != NotReachable)
            SyncManager::shared().reconnect();
    })
    {
        if (!m_reachability_observer.start_observing())
            m_logger->error("Failed to set up network reachability observer");
    }
#else
    {
    }
#endif

    void cancel_reconnect_delay() {
        m_client.cancel_reconnect_delay();
    }

    void stop()
    {
        m_client.stop();
        if (m_thread.joinable())
            m_thread.join();
    }

    std::unique_ptr<sync::Session> make_session(std::string path, sync::Session::Config config)
    {
        return std::make_unique<sync::Session>(m_client, std::move(path), std::move(config));
    }


    ~SyncClient()
    {
        stop();
    }

private:
    static sync::Client make_client(util::Logger& logger, SyncClientConfig const& c)
    {
        sync::Client::Config config;
        config.logger = &logger;
        config.reconnect_mode = c.reconnect_mode;
        config.one_connection_per_session = !c.multiplex_sessions;
        config.user_agent_application_info = util::format("%1 %1", c.user_agent_binding_info, c.user_agent_application_info);

        if (c.connect_timeout)
            config.connect_timeout = c.connect_timeout;
        if (c.connection_linger_time)
            config.connection_linger_time = c.connection_linger_time;
        if (c.ping_keepalive_period)
            config.ping_keepalive_period = c.ping_keepalive_period;
        if (c.pong_keepalive_timeout)
            config.pong_keepalive_timeout = c.pong_keepalive_timeout;
        if (c.fast_reconnect_limit)
            config.fast_reconnect_limit = c.fast_reconnect_limit;

        return sync::Client(std::move(config)); // Throws
    }

    sync::Client m_client;
    const std::unique_ptr<util::Logger> m_logger;
    std::thread m_thread;
#if NETWORK_REACHABILITY_AVAILABLE
    NetworkReachabilityObserver m_reachability_observer;
#endif
};

}
}

#endif // REALM_OS_SYNC_CLIENT_HPP
