/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/JwkCache.hpp"
#include <gtest/gtest.h>
#include <json-c/json.h>
#include "common/log/StringLogger.hpp"
#include "common/log/LogContext.hpp"

namespace integrationTests {
// Fake FetchJWKS for testing
json_object* FakeFetchJWKS(const std::string& uri) {
    // Create root JSON object
    json_object* root = json_object_new_object();

    // Create keys array
    json_object* keys = json_object_new_array();

    // Create a key object
    json_object* key = json_object_new_object();

    // Add "use": "sig"
    json_object_object_add(key, "use", json_object_new_string("sig"));

    // Add "kid": "test-kid"
    json_object_object_add(key, "kid", json_object_new_string("test-kid"));

    // Add "x5c" array
    json_object* x5c_array = json_object_new_array();

    std::string sample_cert_base64_der = 
        "MIIDHDCCAgSgAwIBAgIIGlbUz5cvweUwDQYJKoZIhvcNAQEFBQAwMTEvMC0GA1UE"
        "AxMmc2VjdXJldG9rZW4uc3lzdGVtLmdzZXJ2aWNlYWNjb3VudC5jb20wHhcNMTkw"
        "NDEwMjEyMDUxWhcNMTkwNDI3MDkzNTUxWjAxMS8wLQYDVQQDEyZzZWN1cmV0b2tl"
        "bi5zeXN0ZW0uZ3NlcnZpY2VhY2NvdW50LmNvbTCCASIwDQYJKoZIhvcNAQEBBQAD"
        "ggEPADCCAQoCggEBALRPFSsUi/bGcMVkD+XjJ6z/71u+7Wn1C1bRnM9sU3q7+Ere"
        "DV6an+z+YsjblskBX73h1GyYvmtkyuL7Uq0N+y+RTOmd2fwDw48gM5FEq6DNpVVW"
        "ZRIzzoMSLZCB+tg1eQZdGKtmctdd5Jjhwihf9Aa759fcj60GDG39G6A/w4Jok+J6"
        "7sRabxxontJ4Kpo6zmwUKbWF8naJeCRTO0VAYLkJqEWO4VJTIHJeu2WpxM0qzvY9"
        "IY5Yd7Njegu64FoHU55dSfee2KwDa0/bajrknJfxWBN4hk/rqgGjxQmzAYMCB7/p"
        "/9Snfg4NmfX5cJJ01SNzY6Q/mJRjB3iX2PBz+GsCAwEAAaM4MDYwDAYDVR0TAQH/"
        "BAIwADAOBgNVHQ8BAf8EBAMCB4AwFgYDVR0lAQH/BAwwCgYIKwYBBQUHAwIwDQYJ"
        "KoZIhvcNAQEFBQADggEBAKCSq0D+NIAsGULZrhXouurxInxDyq03xLNcxvKDQchc"
        "XfGA1r3eltmlyKQb5TmAsuKwS/LAQ5z8SlRTOmDGVEtDwnw3S83C4ufXbP0eMB6H"
        "eKf2XCA00T3odUfXmQZme8hG6z7GKVOdn/0oY+vaX38brlCpRXDTm1WldyddUpMz"
        "ftcs6dibdnbQtbX6o9E+KuvGHoNW5xcSjX8lwXTpopfvufPOLPcnFXi4UoYZ8NZ2"
        "2mRG78LkOA+SkOMutbt6w7TBDvADmFzuzvAULy4gsfcamOYcQ7uiHnnD+PoNiNbw"
        "flE/m/0zymX8I/Xu3+KKLhUnUROGC6zO3OnLHXCnEns=";

    // Append the cert string to the x5c array
    json_object_array_add(x5c_array, json_object_new_string(sample_cert_base64_der.c_str()));

    // Add the x5c array to the key object
    json_object_object_add(key, "x5c", x5c_array);

    // Append the key object to keys array
    json_object_array_add(keys, key);

    // Add keys array to root object
    json_object_object_add(root, "keys", keys);

    return root;  // Caller is responsible for json_object_put()
}

TEST(JwkCacheTest, UpdateCacheAddsKey) {
    cta::log::StringLogger log("dummy","JwkCacheTest_UpdateCacheAddsKey",cta::log::DEBUG);
    cta::log::LogContext lc(log);
    JwkCache cache("http://fake-jwks-uri", 1200, 1200, FakeFetchJWKS, lc);

    time_t fakeNow = 1000;
    cache.UpdateCache(fakeNow);

    auto entry = cache.find("test-kid");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry.value().last_refresh_time, fakeNow);
    EXPECT_FALSE(entry.value().pubkey.empty());
}

TEST(JwkCacheTest, PurgeCacheClearsKeys) {
    cta::log::StringLogger log("dummy","JwkCacheTest_PurgeCacheClearsKeys",cta::log::DEBUG);
    cta::log::LogContext lc(log);
    JwkCache cache("http://fake-jwks-uri", 600, 600, FakeFetchJWKS, lc);

    time_t fakeNow = 1000;
    cache.UpdateCache(fakeNow);

    cache.PurgeCache();
    auto entry = cache.find("test-kid");
    EXPECT_FALSE(entry.has_value());
}

TEST(JwkCacheTest, UpdateCacheRemovesExpiredKeys) {
    cta::log::StringLogger log("dummy","JwkCacheTest_UpdateCacheRemovesExpiredKeys",cta::log::DEBUG);
    cta::log::LogContext lc(log);
    JwkCache cache("http://fake-jwks-uri", 100, 200, FakeFetchJWKS, lc);  // very short pubkeyTimeout

    time_t lastRefreshTime = 1000;
    cache.UpdateCache(lastRefreshTime);

    // Manually insert a stale key
    JwkCacheEntry expiredEntry = {lastRefreshTime, "old-pubkey"};
    cache.Insert("expired-key", expiredEntry);

    time_t now = lastRefreshTime + 2;
    cache.UpdateCache(now);
    // should not be removed yet, it should be removed after lastRefreshTime + 200 = 1200
    EXPECT_TRUE(cache.find("expired-key").has_value());
    now = lastRefreshTime + 120;
    cache.UpdateCache(now);
    // still here
    EXPECT_TRUE(cache.find("expired-key").has_value());
    // now the PK has expired, should be removed
    now = lastRefreshTime + 220;
    cache.UpdateCache(now);
    EXPECT_FALSE(cache.find("expired-key").has_value());
}
}

