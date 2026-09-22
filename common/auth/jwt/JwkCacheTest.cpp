/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/auth/jwt/JwtAuthManager.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "common/log/StringLogger.hpp"

#include <gtest/gtest.h>
#include <optional>
#include <set>

namespace unitTests {

class MockJwksFetcher : public cta::auth::JwksFetcher {
private:
  std::map<std::string, std::string> m_responses;

public:
  void setResponse(const std::string& url, const std::string& jwks) { m_responses[url] = jwks; }

  std::string fetchJWKS(const std::string& jwksUrl) override {
    if (auto it = m_responses.find(jwksUrl); it != m_responses.end()) {
      return it->second;
    }
    return generateTestJWKS();
  }

private:
  std::string generateTestJWKS() {
    std::string raw_jwks = R"({
    "keys": [{
        "kid": "test-kid",
        "alg": "RS256",
        "kty": "RSA",
        "use": "sig",
        "n": "h0gY8QsodbL06ls2vRuY9ETBefO2llFkpfYpExdT0WVeNq7iV5AXD-pyI1rHt6ua59gnvhSFwpIqcMF1DXW4LuQIFy0hTOYPDBpbt6YBDb5imHHosE-pWUu6fU_dBy0m0cp84z0_UHDuHQSYYsDMDLnSTIk_F8k4idPkZfoYN2a7gNTiMfxM7MvoJkZ43FSU_LVnm2dymn-5LZJHT5-oZLx70tjNBqCSiroYTmHWnu79agWr0Yiv3U9UKCkjmz3hHemcz5mJdoHtaVHe2-FoprnT0pY_nyLFcmlsTsIDYHTZRi9sfE_RnC2ANaWV4T3L_DLPOghy56gGICRAXudUqw",
        "e": "AQAB"
    }]
    })";

    return raw_jwks;
  }
};

TEST(JwkCacheTest, UpdateCacheAddsKey) {
  cta::log::StringLogger log("dummy", "JwkCacheTest_UpdateCacheAddsKey", cta::log::DEBUG);
  cta::auth::JwkCache cache(std::make_unique<MockJwksFetcher>(), "http://fake-jwks-uri", 1200, log);

  time_t fakeNow = 1000;
  cache.update(fakeNow);

  auto entry = cache.find("test-kid");
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry.value().last_refresh_time, fakeNow);
  EXPECT_FALSE(entry.value().pubkey.empty());
}

TEST(JwkCacheTest, UpdateCacheRemovesExpiredKeys) {
  cta::log::StringLogger log("dummy", "JwkCacheTest_UpdateCacheRemovesExpiredKeys", cta::log::DEBUG);
  auto mockFetcher {std::make_unique<MockJwksFetcher>()};

  // Set up JWKS with a key that will initially be added
  std::string jwksWithKey = R"({
    "keys": [{
        "kid": "expired-key",
        "alg": "RS256",
        "kty": "RSA",
        "use": "sig",
        "n": "h0gY8QsodbL06ls2vRuY9ETBefO2llFkpfYpExdT0WVeNq7iV5AXD-pyI1rHt6ua59gnvhSFwpIqcMF1DXW4LuQIFy0hTOYPDBpbt6YBDb5imHHosE-pWUu6fU_dBy0m0cp84z0_UHDuHQSYYsDMDLnSTIk_F8k4idPkZfoYN2a7gNTiMfxM7MvoJkZ43FSU_LVnm2dymn-5LZJHT5-oZLx70tjNBqCSiroYTmHWnu79agWr0Yiv3U9UKCkjmz3hHemcz5mJdoHtaVHe2-FoprnT0pY_nyLFcmlsTsIDYHTZRi9sfE_RnC2ANaWV4T3L_DLPOghy56gGICRAXudUqw",
        "e": "AQAB"
    }]
    })";

  mockFetcher->setResponse("http://fake-jwks-uri", jwksWithKey);
  cta::auth::JwkCache cache(std::move(mockFetcher), "http://fake-jwks-uri", 200,
                            log);  // very short pubkeyTimeout

  time_t lastRefreshTime = 1000;
  cache.update(lastRefreshTime);
  EXPECT_TRUE(cache.find("expired-key").has_value());

  // Change mock fetcher to return empty JWKS so it doesn't re-add the key
  std::string emptyJwks = R"({"keys": []})";
  auto& fetcher = static_cast<MockJwksFetcher&>(cache.getFetcher());
  fetcher.setResponse("http://fake-jwks-uri", emptyJwks);

  time_t now = lastRefreshTime + 2;
  cache.update(now);
  // should not be removed yet, it should be removed after lastRefreshTime + 200 = 1200
  EXPECT_TRUE(cache.find("expired-key").has_value());

  now = lastRefreshTime + 120;
  cache.update(now);
  // still here
  EXPECT_TRUE(cache.find("expired-key").has_value());

  // now the PK has expired, should be removed
  now = lastRefreshTime + 220;
  cache.update(now);
  EXPECT_FALSE(cache.find("expired-key").has_value());
}
}  // namespace unitTests
