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
#include "common/log/StringLogger.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

class MockJwksFetcher : public cta::JwksFetcher {
private:
  std::map<std::string, std::string> m_responses;
  
public:
  void setResponse(const std::string& url, const std::string& jwks) {
    m_responses[url] = jwks;
  }
  
  std::string fetchJWKS(const std::string& jwksUrl) override {
    auto it = m_responses.find(jwksUrl);
    if (it != m_responses.end()) {
      return it->second;
    }
    return generateTestJWKS();
  }
  
private:
  std::string generateTestJWKS() {
    std::string sample_cert_base64_der = "MIIDSTCCAjGgAwIBAgIUQQp5TK9J3SemQXrCF+ffmED4qy4wDQYJKoZIhvcNAQELBQAwTTELMAkG"
                                         "A1UEBhMCQ0gxDzANBgNVBAgMBkdlbmV2YTEPMA0GA1UEBwwGR2VuZXZhMRwwGgYDVQQKDBNEZWZh"
                                         "dWx0IENvbXBhbnkgTHRkMB4XDTI1MDcwOTA4NDYyN1oXDTM1MDcwNzA4NDYyN1owTTELMAkGA1UE"
                                         "BhMCQ0gxDzANBgNVBAgMBkdlbmV2YTEPMA0GA1UEBwwGR2VuZXZhMRwwGgYDVQQKDBNEZWZhdWx0"
                                         "IENvbXBhbnkgTHRkMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAh0gY8QsodbL06ls2"
                                         "vRuY9ETBefO2llFkpfYpExdT0WVeNq7iV5AXD+pyI1rHt6ua59gnvhSFwpIqcMF1DXW4LuQIFy0h"
                                         "TOYPDBpbt6YBDb5imHHosE+pWUu6fU/dBy0m0cp84z0/UHDuHQSYYsDMDLnSTIk/F8k4idPkZfoY"
                                         "N2a7gNTiMfxM7MvoJkZ43FSU/LVnm2dymn+5LZJHT5+oZLx70tjNBqCSiroYTmHWnu79agWr0Yiv"
                                         "3U9UKCkjmz3hHemcz5mJdoHtaVHe2+FoprnT0pY/nyLFcmlsTsIDYHTZRi9sfE/RnC2ANaWV4T3L"
                                         "/DLPOghy56gGICRAXudUqwIDAQABoyEwHzAdBgNVHQ4EFgQUZxkzqXZASKTRanmOKg6r52Wcj1Mw"
                                         "DQYJKoZIhvcNAQELBQADggEBACVb/KiCg1PD+DYSHet5eZ0sskx6AtB4CwCsErTzy4z6Noy3zSuH"
                                         "3RjYFR/1nsG2M8ZMn6LrB3T6VCnGdZAc6DLHaDZWzt+8g1yNP/9+0p3H9FcemIOVEwdvE/ExwFu9"
                                         "W0AKcHVrhUK7OT7RemSfEodzUU+e6Ze/2Joq1vDNW7/ui/pC8XDljqSkwJqPCJeU4KGlTtloXWPw"
                                         "GREcpm5DVoJKJ9li9xIj2VHxmXPcdsmeiBL/5BB/1ldcOueirUPTyGiXxR2R1paHrjHZNBXKZ5Du"
                                         "2N4HyvOmkj/xht5wkZU3OqA31aScrWF5MjMIu4FBVO3fY7El5s0rCp/cJivDq0Y=";

    std::string raw_jwks = R"({
    "keys": [{
        "kid": "test-kid",
        "alg": "RS256",
        "kty": "RSA",
        "use": "sig",
        "x5c": [
        ")" + sample_cert_base64_der +
                           R"("
        ],
        "e": "AQAB"
    }]
    })";

    return raw_jwks;
  }
};

TEST(JwkCacheTest, UpdateCacheAddsKey) {
  cta::log::StringLogger log("dummy", "JwkCacheTest_UpdateCacheAddsKey", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  
  MockJwksFetcher mockFetcher;
  cta::JwkCache cache(mockFetcher, "http://fake-jwks-uri", 1200, lc);

  time_t fakeNow = 1000;
  cache.updateCache(fakeNow);

  auto entry = cache.find("test-kid");
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry.value().last_refresh_time, fakeNow);
  EXPECT_FALSE(entry.value().pubkey.empty());
}

TEST(JwkCacheTest, UpdateCacheRemovesExpiredKeys) {
  cta::log::StringLogger log("dummy", "JwkCacheTest_UpdateCacheRemovesExpiredKeys", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  
  MockJwksFetcher mockFetcher;
  
  // Set up JWKS with a key that will initially be added
  std::string jwksWithKey = R"({
    "keys": [{
        "kid": "expired-key",
        "alg": "RS256",
        "kty": "RSA",
        "use": "sig",
        "x5c": [
        "MIIDSTCCAjGgAwIBAgIUQQp5TK9J3SemQXrCF+ffmED4qy4wDQYJKoZIhvcNAQELBQAwTTELMAkGA1UEBhMCQ0gxDzANBgNVBAgMBkdlbmV2YTEPMA0GA1UEBwwGR2VuZXZhMRwwGgYDVQQKDBNEZWZhdWx0IENvbXBhbnkgTHRkMB4XDTI1MDcwOTA4NDYyN1oXDTM1MDcwNzA4NDYyN1owTTELMAkGA1UEBhMCQ0gxDzANBgNVBAgMBkdlbmV2QTEPMA0GA1UEBwwGR2VuZXZhMRwwGgYDVQQKDBNEZWZhdWx0IENvbXBhbnkgTHRkMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAh0gY8QsodbL06ls2vRuY9ETBefO2llFkpfYpExdT0WVeNq7iV5AXD+pyI1rHt6ua59gnvhSFwpIqcMF1DXW4LuQIFy0hTOYPDBpbt6YBDb5imHHosE+pWUu6fU/dBy0m0cp84z0/UHDuHQSYYsDMDLnSTIk/F8k4idPkZfoYN2a7gNTiMfxM7MvoJkZ43FSU/LVnm2dymn+5LZJHT5+oZLx70tjNBqCSiroYTmHWnu79agWr0Yiv3U9UKCkjmz3hHemcz5mJdoHtaVHe2+FoprnT0pY/nyLFcmlsTsIDYHTZRi9sfE/RnC2ANaWV4T3L/DLPOghy56gGICRAXudUqwIDAQABoyEwHzAdBgNVHQ4EFgQUZxkzqXZASKTRanmOKg6r52Wcj1MwDQYJKoZIhvcNAQELBQADggEBACVb/KiCg1PD+DYSHet5eZ0sskx6AtB4CwCsErTzy4z6Noy3zSuH3RjYFR/1nsG2M8ZMn6LrB3T6VCnGdZAc6DLHaDZWzt+8g1yNP/9+0p3H9FcemIOVEwdvE/ExwFu9W0AKcHVrhUK7OT7RemSfEodzUU+e6Ze/2Joq1vDNW7/ui/pC8XDljqSkwJqPCJeU4KGlTtloXWPwGREcpm5DVoJKJ9li9xIj2VHxmXPcdsmeiBL/5BB/1ldcOueirUPTyGiXxR2R1paHrjHZNBXKZ5Du2N4HyvOmkj/xht5wkZU3OqA31aScrWF5MjMIu4FBVO3fY7El5s0rCp/cJivDq0Y="
        ],
        "e": "AQAB"
    }]
    })";
  
  mockFetcher.setResponse("http://fake-jwks-uri", jwksWithKey);
  cta::JwkCache cache(mockFetcher, "http://fake-jwks-uri", 200, lc);  // very short pubkeyTimeout

  time_t lastRefreshTime = 1000;
  cache.updateCache(lastRefreshTime);
  EXPECT_TRUE(cache.find("expired-key").has_value());

  // Change mock fetcher to return empty JWKS so it doesn't re-add the key
  std::string emptyJwks = R"({"keys": []})";
  mockFetcher.setResponse("http://fake-jwks-uri", emptyJwks);

  time_t now = lastRefreshTime + 2;
  cache.updateCache(now);
  // should not be removed yet, it should be removed after lastRefreshTime + 200 = 1200
  EXPECT_TRUE(cache.find("expired-key").has_value());
  
  now = lastRefreshTime + 120;
  cache.updateCache(now);
  // still here
  EXPECT_TRUE(cache.find("expired-key").has_value());
  
  // now the PK has expired, should be removed
  now = lastRefreshTime + 220;
  cache.updateCache(now);
  EXPECT_FALSE(cache.find("expired-key").has_value());
}
}  // namespace unitTests
