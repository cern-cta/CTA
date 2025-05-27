#include <string>
#include <map>
#include <time.h>
#include <iostream>
#include <json/json.h>
#include <functional>

typedef struct JwkCacheEntry {
    time_t last_refresh_time;
    std::string pubkey; // public key in PEM format
} JwkCacheEntry;

// forward declaration;
Json::Value FetchJWKS(const std::string& jwksUrl);
class JwkCache {
public:
    // Constructor with optional fetch function for testing
    using FetchFunction = std::function<Json::Value(const std::string& jwkUri)>;
    std::string m_jwksUri;
    int m_cacheRefreshInterval; // value in seconds
    // at some point will need a mutex added to handle parallel requests
    // This gives the option to keep public keys around for longer than the refresh interval.
    int m_pubkeyRefreshInterval;
    void PurgeCache(); // remove all entries
    void UpdateCache(time_t now); // 
    std::map<std::string, JwkCacheEntry>::iterator find(std::string key);
    std::map<std::string, JwkCacheEntry> m_keymap;
    FetchFunction m_fetchFunc;
    JwkCache(const std::string& jwkUri, int cacheRefreshInterval = 600, int pubkeyRefreshInterval = 600, FetchFunction fetchFunc = FetchJWKS)
                : m_jwksUri(jwkUri),
                  m_cacheRefreshInterval(cacheRefreshInterval),
                  m_pubkeyRefreshInterval(pubkeyRefreshInterval),
                  m_fetchFunc(fetchFunc)
        {
            std::cout << "In JwkCache constructor, cacheRefreshInterval value is " << m_cacheRefreshInterval << std::endl;
        };
};