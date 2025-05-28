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

#include "frontend/grpc/keycache/JwkCache.hpp"
#include <gtest/gtest.h>
#include <json/json.h>
// #include <jwt-cpp/jwt.h>

namespace integrationTests {
// Fake FetchJWKS for testing
Json::Value FakeFetchJWKS(const std::string& uri) {
    // Create a fake JWKS JSON structure
    Json::Value root;
    Json::Value key;
    key["use"] = "sig";
    key["kid"] = "test-kid-123";
    key["x5c"] = Json::arrayValue;
    key["x5c"].append("MIIBIjANBgkqh...");  // fake base64 cert string
    root["keys"].append(key);
    return root;
}

TEST(JwkCacheTest, UpdateCacheAddsKey) {
    JwkCache cache("http://fake-jwks-uri", 600, 600, FakeFetchJWKS);

    time_t fakeNow = 1000;
    cache.UpdateCache(fakeNow);

    auto it = cache.find("test-kid-123");
    ASSERT_NE(it, cache.m_keymap.end());
    EXPECT_EQ(it->second.last_refresh_time, fakeNow);
    EXPECT_FALSE(it->second.pubkey.empty());
}

TEST(JwkCacheTest, PurgeCacheClearsKeys) {
    JwkCache cache("http://fake-jwks-uri", 600, 600, FakeFetchJWKS);

    time_t fakeNow = 1000;
    cache.UpdateCache(fakeNow);

    cache.PurgeCache();

    EXPECT_TRUE(cache.m_keymap.empty());
}

TEST(JwkCacheTest, UpdateCacheRemovesExpiredKeys) {
    JwkCache cache("http://fake-jwks-uri", 600, 1, FakeFetchJWKS);  // very short pubkeyRefreshInterval

    time_t oldTime = 1000;
    cache.UpdateCache(oldTime);

    // Manually insert a stale key
    cache.m_keymap["expired-key"] = {oldTime - 10, "old-pubkey"};

    time_t now = oldTime + 2;
    cache.UpdateCache(now);

    EXPECT_EQ(cache.m_keymap.count("expired-key"), 0);
}
}

// int main(int argc, char **argv) {
//   ::testing::InitGoogleTest(&argc, argv);
//   return RUN_ALL_TESTS();
// }

