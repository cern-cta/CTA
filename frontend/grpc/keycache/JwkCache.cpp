#include "JwkCache.hpp"
#include <json/json.h>
#include <curl/curl.h>
#include <jwt-cpp/jwt.h>


// Function to handle curl responses
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* output) {
    size_t totalSize = size * nmemb;
    output->append((char*)contents, totalSize);
    return totalSize;
}

Json::Value FetchJWKS(const std::string& jwksUrl) {
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

    // Parse the response JSON
    Json::CharReaderBuilder readerBuilder;
    Json::Value jwks;
    std::istringstream sstream(readBuffer);
    std::string errs;
    if (!Json::parseFromStream(readerBuilder, sstream, &jwks, &errs)) {
        std::cout << "Failed to parse JSON: " << errs << std::endl;
        // throw some exception here
        throw std::runtime_error("Failed to parse JSON in FetchJWKS");
    }

    return jwks;
}

std::map<std::string, JwkCacheEntry>::iterator JwkCache::find(std::string key) {
    return m_keymap.find(key);
}

void JwkCache::UpdateCache(time_t now) {
    std::cout << "In updateCache() function" << std::endl;
    Json::Value jwks = m_fetchFunc(m_jwksUri);
    // purge any keys that have expired
    for (const auto& entry: m_keymap) {
        int lastRefresh = entry.second.last_refresh_time;
        if (lastRefresh + m_pubkeyRefreshInterval <= now) {
            m_keymap.erase(entry.first);
            std::cout << "Removing entry for key with kid " << entry.first << std::endl;
        }
    }
    // add they new keys
    // we only care about keys used for signing
    for (const auto& key : jwks["keys"]) {
        if (key["use"] != "sig")
            continue;
        std::string kid = key["kid"].asString();
        std::string x5c = key["x5c"][0].asString();
        std::string pubkeyPem = jwt::helper::convert_base64_der_to_pem(x5c);
        JwkCacheEntry entry = {now, pubkeyPem};
        m_keymap[kid] = entry; // store the certificate in PEM format
        std::cout << "Adding entry for key with kid " << kid << " and cachedTime " << now << std::endl;
    }
}

// Remove all entries from the cache
void JwkCache::PurgeCache() {
    m_keymap.clear();
}