#include <string>
#include <map>
#include <time.h>
#include <iostream>
#include <json-c/json.h>
#include <functional>
#include <shared_mutex>
#include <optional>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>

#include "exception/Exception.hpp"
#include "log/LogContext.hpp"

CTA_GENERATE_EXCEPTION_CLASS(JSONParseException);
CTA_GENERATE_EXCEPTION_CLASS(CurlException);

struct JwkCacheEntry {
    time_t last_refresh_time;
    std::string pubkey; // public key in PEM format
};

// forward declaration;
json_object* FetchJWKS(const std::string& jwksUrl);
class JwkCache {
public:
    // Constructor with optional fetch function for testing
    using FetchFunction = std::function<json_object* (const std::string& jwkUri)>;
    JwkCache(const std::string& jwkUri, int cacheRefreshInterval, int pubkeyTimeout, FetchFunction fetchFunc, const cta::log::LogContext& lc)
                : m_jwksUri(jwkUri),
                    m_cacheRefreshInterval(cacheRefreshInterval),
                    m_pubkeyTimeout(pubkeyTimeout),
                    m_fetchFunc(fetchFunc),
                    m_lc(lc) {
                        if (pubkeyTimeout < cacheRefreshInterval) {
                            cta::log::LogContext lc(m_lc);
                            lc.log(cta::log::ERR, "Cannot use a value for grpc.jwks.cache.timeout_secs that is less than grpc.jwks.cache.refresh_interval_secs."
                            "Setting timeout_secs equal to cache_refresh_interval_secs.");
                            m_pubkeyTimeout = m_cacheRefreshInterval;
                        }
                    };
    ~JwkCache();

    void PurgeCache(); // remove all entries
    void UpdateCache(time_t now);
    void Insert(const std::string& key, const JwkCacheEntry& e); // only used for tests
    std::optional<JwkCacheEntry> find(const std::string& key);
    void StartRefreshThread();
    void StopRefreshThread();
private:
    void RefreshLoop();

    std::string m_jwksUri;
    int m_cacheRefreshInterval; // value in seconds
    // This gives the option to keep public keys around for longer than the refresh interval.
    int m_pubkeyTimeout;
    FetchFunction m_fetchFunc;
    // TODO: add mutex to handle parallel requests
    std::shared_mutex m_mutex;
    std::map<std::string, JwkCacheEntry> m_keymap;
    std::thread m_refreshThread;
    std::mutex m_cv_mutex;
    std::condition_variable m_cv; // condition: stop the refresh thread, will be performed in destructor
                                  // this is needed because the refresh thread might be sleeping, if not actively performing cache update
    std::atomic<bool> m_stopThread;
    // The logging context
    cta::log::LogContext m_lc; // always make a copy for thread safety
};