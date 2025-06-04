#include <string>
#include <map>
#include <time.h>
#include <iostream>
#include <json-c/json.h>
#include <functional>
#include <shared_mutex>
#include <optional>

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
    JwkCache(const std::string& jwkUri, int cacheRefreshInterval, int pubkeyTimeout, FetchFunction fetchFunc)
                : m_jwksUri(jwkUri),
                    m_cacheRefreshInterval(cacheRefreshInterval),
                    m_pubkeyTimeout(pubkeyTimeout),
                    m_fetchFunc(fetchFunc)
        {
            std::cout << "In JwkCache constructor, cacheRefreshInterval value is " << m_cacheRefreshInterval << std::endl;
        };

    void PurgeCache(); // remove all entries
    void UpdateCache(time_t now);
    void Insert(const std::string& key, const JwkCacheEntry& e); // only used for tests
    std::optional<JwkCacheEntry> find(std::string key);
private:
    std::string m_jwksUri;
    int m_cacheRefreshInterval; // value in seconds
    // This gives the option to keep public keys around for longer than the refresh interval.
    int m_pubkeyTimeout;
    FetchFunction m_fetchFunc;
    // TODO: add mutex to handle parallel requests
    std::shared_mutex m_mutex;
    std::map<std::string, JwkCacheEntry> m_keymap;
};