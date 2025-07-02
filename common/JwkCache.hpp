#pragma once

#include <string>
#include <map>
#include <time.h>
#include <iostream>
#include <functional>
#include <shared_mutex>
#include <optional>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>

#include "exception/Exception.hpp"
#include "log/LogContext.hpp"

namespace cta {
CTA_GENERATE_EXCEPTION_CLASS(JSONParseException);
CTA_GENERATE_EXCEPTION_CLASS(CurlException);

struct JwkCacheEntry {
    time_t last_refresh_time;
    std::string pubkey; // public key in PEM format
};

class JwkCache {
public:
    JwkCache(const std::string& jwkUri, int cacheRefreshInterval, int pubkeyTimeout, const cta::log::LogContext& lc);
    ~JwkCache();

    void purgeCache(); // remove all entries
    void updateCache(time_t now);
    void insert(const std::string& key, const JwkCacheEntry& e); // only used for tests
    std::optional<JwkCacheEntry> find(const std::string& key);
    void startRefreshThread();
    void stopRefreshThread();
private:
    void refreshLoop();
    virtual std::string fetchJWKS(const std::string& jwksUrl);

    std::string m_jwksUri;
    int m_cacheRefreshInterval; // value in seconds
    // This gives the option to keep public keys around for longer than the refresh interval.
    int m_pubkeyTimeout;
    std::shared_mutex m_mutex; // mutex to handle parallel requests
    std::map<std::string, JwkCacheEntry> m_keymap;
    std::thread m_refreshThread;
    std::mutex m_cv_mutex;
    std::condition_variable m_cv; // condition: stop the refresh thread, will be performed in destructor
                                  // this is needed because the refresh thread might be sleeping, if not actively performing cache update
    std::atomic<bool> m_stopThread;
    // The logging context
    cta::log::LogContext m_lc; // always make a copy for thread safety
};
} // namespace cta