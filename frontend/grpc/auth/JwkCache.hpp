#include <string>
#include <map>
#include <time.h>

typedef struct JwkCacheEntry {
    time_t last_refresh_time;
    std::string pubkey; // public key in PEM format
} JwkCacheEntry;

class JwkCache {
public:
    std::string m_jwksUri;
    int m_cacheRefreshInterval; // value in seconds
    // at some point will need a mutex added to handle parallel requests
    // This gives the option to keep public keys around for longer than the refresh interval.
    int m_pubkeyRefreshInterval;
    void PurgeCache(); // remove all entries
    void UpdateCache(); // 
    std::map<std::string, JwkCacheEntry>::iterator find(std::string key);
    std::map<std::string, JwkCacheEntry> m_keymap;
    JwkCache(const std::string& jwkUri, int cacheRefreshInterval = 600, int pubkeyRefreshInterval = 600)
                : m_jwksUri(jwkUri),
                  m_cacheRefreshInterval(cacheRefreshInterval),
                  m_pubkeyRefreshInterval(pubkeyRefreshInterval) {};
};