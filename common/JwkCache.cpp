#include "JwkCache.hpp"
#include <curl/curl.h>
#include "jwt-cpp/jwt.h"
#include <mutex>

namespace cta {
JwkCache::JwkCache(const std::string& jwkUri, int cacheRefreshInterval, int pubkeyTimeout, const log::LogContext& lc)
            : m_jwksUri(jwkUri),
              m_cacheRefreshInterval(cacheRefreshInterval),
              m_pubkeyTimeout(pubkeyTimeout),
              m_lc(lc) {};

// Function to handle curl responses
size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* output) {
    size_t totalSize = size * nmemb;
    output->append((char*)contents, totalSize);
    return totalSize;
};

std::string JwkCache::FetchJWKS(const std::string& jwksUrl) {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;

    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, jwksUrl.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
        res = curl_easy_perform(curl);
        if(res != CURLE_OK) {
            throw CurlException(std::string("CURL failed in FetchJWKS: ") + curl_easy_strerror(res));
        }
        curl_easy_cleanup(curl);
    }
    curl_global_cleanup();

    std::string jwks = readBuffer;
    return jwks;
}

JwkCache::~JwkCache() {
    StopRefreshThread();
}

std::optional<JwkCacheEntry> JwkCache::find(const std::string& key) {
    log::LogContext lc(m_lc);
    lc.log(log::INFO, "Waiting to acquire shared_lock in JwkCache::find");
    std::shared_lock<std::shared_mutex> lock(m_mutex);
    lc.log(log::INFO, "Just acquired the shared_lock in JwkCache::find");
    auto it = m_keymap.find(key);
    if (it == m_keymap.end()) {
        lc.log(log::INFO, std::string("Entry not found for kid ") + key);
        return std::nullopt;
    } else {
        lc.log(log::INFO, std::string("Entry found in cache for kid ") + key);
        return std::optional<JwkCacheEntry>(it->second);
    }
}

void JwkCache::Insert(const std::string &key, const JwkCacheEntry& e) {
    log::LogContext lc(m_lc);
    lc.log(log::INFO, "Waiting to acquire unique_lock in JwkCache::Insert");
    std::unique_lock<std::shared_mutex> lock(m_mutex);
    lc.log(log::INFO, "Just acquired the unique_lock in JwkCache::Insert");
    m_keymap[key] = e;
}

void JwkCache::StartRefreshThread() {
    if (m_refreshThread.joinable()) {
        // Already running
        return;
    }
    log::LogContext lc(m_lc);
    lc.log(log::INFO, "Starting cache refresh thread");
    m_stopThread = false;
    m_refreshThread = std::thread(&JwkCache::RefreshLoop, this);
    lc.log(log::INFO, "Cache refresh thread started");
}

void JwkCache::StopRefreshThread() {
    log::LogContext lc(m_lc);
    lc.log(log::INFO, "In StopRefershThread, stopping the thread and notifying the cv");
    m_stopThread = true;
    m_cv.notify_all();  // Wake the thread if sleeping
    lc.log(log::INFO, "Notified condition variable");
    if (m_refreshThread.joinable()) {
        m_refreshThread.join();
    }
}

void JwkCache::RefreshLoop() {
    log::LogContext lc(m_lc);
    lc.log(log::INFO, "Entering function Refresh loop");
    while (!m_stopThread.load()) {
        try {
            time_t now = time(NULL);
            UpdateCache(now);
        }
        catch (const std::exception& ex) {
            log::ScopedParamContainer params(lc);
            lc.log(log::ERR, "Some exception thrown in the RefreshLoop");
            params.add("exceptionMessage", ex.what());
        }
        std::unique_lock<std::mutex> lk(m_cv_mutex);
        m_cv.wait_for(lk, std::chrono::seconds(m_cacheRefreshInterval), [this]() {
            log::LogContext lc(m_lc);
            lc.log(log::INFO, "Waiting on condition variable or explicit wakeup...");
            return m_stopThread.load();
        });
    }
    lc.log(log::INFO, "Received notification in RefreshLoop");
}

void JwkCache::UpdateCache(time_t now) {
    log::LogContext lc(m_lc);
    log::ScopedParamContainer spc(lc);
    lc.log(log::INFO, "In function UpdateCache");
    std::string raw_jwks;
    // jwks<traits::nlohmann_json> jwks;
    try {
        raw_jwks = FetchJWKS(m_jwksUri);
    } catch (CurlException& ex) {
        lc.log(log::ERR, ex.getMessageValue());
        return;
    } catch (JSONParseException& ex) {
        lc.log(log::ERR, ex.getMessageValue());
        return;
    }
    // purge any keys that have expired
    lc.log(log::INFO, "In function UpdateCache, waiting to acquire unique lock");
    std::unique_lock<std::shared_mutex> lock(m_mutex);
    lc.log(log::INFO, "In UpdateCache, just acquired the unique lock");
    for (auto it = m_keymap.begin(); it != m_keymap.end() ;) {
        int lastRefresh = it->second.last_refresh_time;
        if (lastRefresh + m_pubkeyTimeout <= now) {
            lc.log(log::DEBUG, std::string("Removing entry for key with kid ") + it->first);
            it = m_keymap.erase(it); // erase returns next valid iterator
        } else {
            ++it;
        }
    }
    // add they new keys
    auto jwks = jwt::parse_jwks(raw_jwks);
    std::string kid;
    std::string x5c;
    // now iterate over the keys, add the key if it's used for signing
    for (auto it = jwks.begin(); it != jwks.end(); ++it) {
        auto jwk = *it;
        try {
            std::string use = jwk.get_use();
            if (use != "sig")
                continue;
            kid = jwk.get_key_id();
            x5c = jwk.get_x5c_key_value();
            if (x5c.empty()) {
                lc.log(log::ERR, "Field \"x5c\" missing from JWKS entry");
                return;
            }
            if (kid.empty()) {
                lc.log(log::ERR, "Field \"kid\" missing from JWKS entry");
                return;
            }
        } catch (std::runtime_error &ex) {
            spc.add("exceptionMessage", ex.what());
            lc.log(log::ERR, "Runtime error thrown when parsing JWKS");
        }

        std::string pubkeyPem = jwt::helper::convert_base64_der_to_pem(x5c);
        JwkCacheEntry entry = {now, pubkeyPem};
        m_keymap[kid] = entry;
        lc.log(log::INFO, "Adding new key entry in cache");
        spc.add("kid", kid);
        spc.add("cachedTime", std::to_string(now));
    }

    // json_object_put(jwks); // Clean up ref count
}

// Remove all entries from the cache
void JwkCache::PurgeCache() {
    std::unique_lock<std::shared_mutex> lock(m_mutex);
    m_keymap.clear();
}
}