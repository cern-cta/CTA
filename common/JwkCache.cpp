#include "JwkCache.hpp"
#include <json-c/json.h>
#include <curl/curl.h>
#include "jwt-cpp/jwt.h"
#include <mutex>


// Function to handle curl responses
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* output) {
    size_t totalSize = size * nmemb;
    output->append((char*)contents, totalSize);
    return totalSize;
}

json_object* FetchJWKS(const std::string& jwksUrl) {
    std::cout << "In FetchJWKS function" << std::endl;
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
            std::cout << "Curl failed: " << curl_easy_strerror(res) << std::endl;
            throw std::runtime_error("CURL failed in FetchJWKS");
        }
        curl_easy_cleanup(curl);
    }
    curl_global_cleanup();

    // Parse JSON using json-c
    json_object* jwks = json_tokener_parse(readBuffer.c_str());
    if (!jwks) {
        std::cerr << "Failed to parse JSON response from JWKS" << std::endl;
        throw std::runtime_error("Failed to parse JSON in FetchJWKS");
    }

    return jwks;
}

JwkCache::~JwkCache() {
    StopRefreshThread();
}

std::optional<JwkCacheEntry> JwkCache::find(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(m_mutex);
    auto it = m_keymap.find(key);
    if (it == m_keymap.end())
        return std::nullopt;
    else
        return std::optional<JwkCacheEntry>(it->second);
}

void JwkCache::Insert(const std::string &key, const JwkCacheEntry& e) {
    std::unique_lock<std::shared_mutex> lock(m_mutex);
    m_keymap[key] = e;
}

void JwkCache::StartRefreshThread() {
    if (m_refreshThread.joinable()) {
        // Already running
        return;
    }
    std::cout << "Starting cache refresh thread" << std::endl;
    m_stopThread = false;
    m_refreshThread = std::thread(&JwkCache::RefreshLoop, this);
}

void JwkCache::StopRefreshThread() {
    // no need for locking because it's atomic
    m_stopThread = true;
    m_cv.notify_all();  // Wake the thread if sleeping
    if (m_refreshThread.joinable()) {
        m_refreshThread.join();
    }
}

void JwkCache::RefreshLoop() {
    // do your update and wait for the next one
    while (!m_stopThread.load()) {
        try {
            time_t now = time(NULL);
            UpdateCache(now);
        }
        catch (const std::exception& ex) {
            std::cout << "Some exception thrown in the RefreshLoop " << ex.what() << std::endl;
        }
        std::unique_lock<std::mutex> lk(m_cv_mutex);
        m_cv.wait_for(lk, std::chrono::seconds(m_cacheRefreshInterval), [this]() {
            return m_stopThread.load();
        }); // wait to be woken up
    }
}

void JwkCache::UpdateCache(time_t now) {
    std::cout << "In updateCache() function" << std::endl;
    json_object* jwks = m_fetchFunc(m_jwksUri);
    // purge any keys that have expired
    std::unique_lock<std::shared_mutex> lock(m_mutex);
    for (auto it = m_keymap.begin(); it != m_keymap.end() ;) {
        int lastRefresh = it->second.last_refresh_time;
        if (lastRefresh + m_pubkeyTimeout <= now) {
            std::cout << "Removing entry for key with kid " << it->first << std::endl;
            it = m_keymap.erase(it); // erase returns next valid iterator
        } else {
            ++it;
        }
    }
    // add they new keys
    // Parse the "keys" array from JWKS
    json_object* keys = nullptr;
    if (!json_object_object_get_ex(jwks, "keys", &keys) || !json_object_is_type(keys, json_type_array)) {
        std::cerr << "\"keys\" field missing or not an array in JWKS" << std::endl;
        json_object_put(jwks);  // Cleanup
        return;
    }
    // Iterate over the keys array
    int len = json_object_array_length(keys);
    for (int i = 0; i < len; ++i) {
        json_object* key = json_object_array_get_idx(keys, i);
        if (!key || !json_object_is_type(key, json_type_object))
            continue;

        json_object *use_obj = nullptr;
        if (!json_object_object_get_ex(key, "use", &use_obj))
            continue;
        // we only care about keys used for signing
        const char* use_str = json_object_get_string(use_obj);
        if (!use_str || std::string(use_str) != "sig")
            continue;

        json_object *kid_obj = nullptr, *x5c_arr = nullptr;
        if (!json_object_object_get_ex(key, "kid", &kid_obj) ||
            !json_object_object_get_ex(key, "x5c", &x5c_arr) ||
            !json_object_is_type(x5c_arr, json_type_array) ||
            json_object_array_length(x5c_arr) == 0)
            continue;

        const char* kid = json_object_get_string(kid_obj);
        const char* x5c = json_object_get_string(json_object_array_get_idx(x5c_arr, 0));

        std::string pubkeyPem = jwt::helper::convert_base64_der_to_pem(x5c);
        JwkCacheEntry entry = {now, pubkeyPem};
        m_keymap[kid] = entry;

        std::cout << "Adding entry for key with kid " << kid << " and cachedTime " << now << std::endl;
    }

    json_object_put(jwks);  // Clean up ref count
}

// Remove all entries from the cache
void JwkCache::PurgeCache() {
    std::unique_lock<std::shared_mutex> lock(m_mutex);
    m_keymap.clear();
}