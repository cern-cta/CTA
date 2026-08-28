/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

// required tests: expired token, token with bad field

#include "common/auth/Jwt.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"

#include <chrono>
#include <gtest/gtest.h>
#include <jwt-cpp/jwt.h>
#include <optional>

namespace unitTests {

const std::string rsa_priv_key = R"(-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCHSBjxCyh1svTq
Wza9G5j0RMF587aWUWSl9ikTF1PRZV42ruJXkBcP6nIjWse3q5rn2Ce+FIXCkipw
wXUNdbgu5AgXLSFM5g8MGlu3pgENvmKYceiwT6lZS7p9T90HLSbRynzjPT9QcO4d
BJhiwMwMudJMiT8XyTiJ0+Rl+hg3ZruA1OIx/Ezsy+gmRnjcVJT8tWebZ3Kaf7kt
kkdPn6hkvHvS2M0GoJKKuhhOYdae7v1qBavRiK/dT1QoKSObPeEd6ZzPmYl2ge1p
Ud7b4WimudPSlj+fIsVyaWxOwgNgdNlGL2x8T9GcLYA1pZXhPcv8Ms86CHLnqAYg
JEBe51SrAgMBAAECggEAHafF4+1EwMxqmQdG0BzBImcCHLg6wef0ztbP+UHnW2ND
zGv47SYGkDQeMjbfyhkhu4osaCQ6kEUXbaFTBhDUv964EVCQ2Lkj+ky6517KI1el
aHKsBh5oas1Jg9fihYS12k5voybVfs5KvGy59Qf7kxyXB7Ucchvnu3sKOfqhnV8j
yw6+cb3/6HhPmC13lJqiT3G5Mq9GsWUrgjyN2pxbnKwpPM7PqkB8BgzoHKV8p57I
SGbW9tRmo8nVaIJipYpKFKWo0XiO7o8H3RRoEheabn1tnDoLJGpP8vYjIukhFcHl
EzPVnI7CSvhRSjXrNtPSq7z7V/82zOxRw/XH071GhQKBgQC6t0PMMLc8/3izgeK9
0rhXULBlJMbJA7AJ58TCdEm2JsqwPtYI5yjxq262/tPpenucu0FaunzdgbK0Y6f2
TwYcCsD+P9KnKiDYXByxJ1TUBR5Z/Kq+kj+lffCyuy9catY5Gj9wR8U7v8f7vnOF
PGnBTICjUh2KKsMGky6cpIVrpwKBgQC5eu4H8iIiQ3ojts39srF0y1qldSGYf1Y9
SOJpEJNo5Aqnl8CBelV0grmLbPIJz8ZaykcPERaKX6qJk5gwDVgFPjbtyJrIeeh0
0a/KPQO/DF1mnWUECk3h2+W26WBrexqWkLmBlYijCL8kv8UDPUn6dffFn7jSqEsZ
XZ82DyofXQKBgHp0GLXAuVv63Ek2BOOjYBx7ocQjs28/yOMmKoexRmp81G90NmEO
YW7llK3VQFueZZVrxbfgGGYZWn8t4IkMWKBpeRsF9nyFh5b+Ch8xAVQvqzEvITfs
qGs7xnEhjDUbKDW4/iQAHd1KsLhstkyKS31nU/JIt3DXDKKyQl6fE5V/AoGAMu56
plvq25XD2EK+VcfXysZ8YarESufMeo+k/Ey87bSQ6GxXRDafeJrc8Fg+LkuLoCqj
UJPUqLKUVardw3Qmk2n+E1Vei2ZOWqWpq9MNUEzI6QCXWICr2jVT4uI6w8jOCEI9
bkPtfTdNpX2zT6xowAncu7ucONxVouV+bo3Dd1ECgYBHMrGUrJS0ofnNdXHt8QzY
4jYTbci5EF6LKcoo6pCcWaaIi0R6dlstVCBYuLIpoK2HWmmgMLp2XuXfgnRkY+PF
2YmhJeKlj/ScUGL528Uwr7YB7SHR4A/KZleVgdFkXH3VYXwdTG2COCsvVGWEWmri
az8ZaVQPvmSthMu8suOc8w==
-----END PRIVATE KEY-----)";

const std::string sample_cert_base64_der =
  "MIIDSTCCAjGgAwIBAgIUQQp5TK9J3SemQXrCF+ffmED4qy4wDQYJKoZIhvcNAQELBQAwTTELMAkG"
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

const std::string raw_jwks = R"({
    "keys": [{
        "kid": "test-kid",
        "alg": "RS256",
        "kty": "RSA",
        "use": "sig",
        "x5c": [
        ")" + sample_cert_base64_der
                             +
                             R"("
        ],
        "e": "AQAB"
    }]
    })";

class MockJwksFetcherValidateJwt : public cta::auth::JwksFetcher {
private:
  std::string m_jwks {raw_jwks};

public:
  MockJwksFetcherValidateJwt() {}

  void setJwks(const std::string& jwks) { m_jwks = jwks; }

  std::string fetchJWKS(const std::string& jwksUrl) override { return m_jwks; }
};

const std::string pubkeyPem = jwt::helper::convert_base64_der_to_pem(sample_cert_base64_der);

std::string createTestJwt(bool expired, const std::string& kid) {
  // first get the public key in pem format, then use it to sign stuff

  auto token = jwt::create()
                 .set_issuer("test")
                 .set_payload_claim("jti", jwt::claim(std::string("test-jti")))
                 .set_payload_claim("exp",
                                    jwt::claim(std::chrono::system_clock::now()
                                               + (expired ? -std::chrono::minutes(60) : std::chrono::minutes(60))))
                 .set_header_claim("kid", jwt::claim(kid))
                 .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
                 .set_payload_claim("aud", jwt::claim(std::string("test-audience")))
                 .sign(jwt::algorithm::rs256("", rsa_priv_key, "", ""));
  return token;
}

class ValidateJwtTestFixture : public ::testing::Test {
protected:
  cta::log::StringLogger log {"dummy", "ValidateJwtTests", cta::log::DEBUG};
  cta::log::LogContext lc;

  ValidateJwtTestFixture() : lc(log) {}

  std::shared_ptr<cta::auth::JwtAuthManager>
  createAuthMgrWithMockFetcher(const std::string& expectedAudience = "test-audience") const {
    return std::make_shared<cta::auth::JwtAuthManager>(std::make_unique<MockJwksFetcherValidateJwt>(),
                                                       "http://fake-jwks-uri",
                                                       1200,
                                                       "test",
                                                       expectedAudience,
                                                       std::set<std::string, std::less<>>(),
                                                       lc);
  }

  std::shared_ptr<cta::auth::JwtAuthManager> createAuthMgrWithEmptyMockFetcher() const {
    auto mockFetcher = std::make_unique<MockJwksFetcherValidateJwt>();
    mockFetcher->setJwks("");

    const auto authMgr = std::make_shared<cta::auth::JwtAuthManager>(std::move(mockFetcher),
                                                                     "http://fake-jwks-uri",
                                                                     1200,
                                                                     "test",
                                                                     "test-audience",
                                                                     std::set<std::string, std::less<>>(),
                                                                     lc);
    return authMgr;
  }

  std::shared_ptr<cta::auth::JwtAuthManager> createAuthMgrWithRevokedJti(const std::string& revokedJti) const {
    std::set<std::string, std::less<>> revokedSet;
    revokedSet.insert(revokedJti);
    return std::make_shared<cta::auth::JwtAuthManager>(std::make_unique<MockJwksFetcherValidateJwt>(),
                                                       "http://fake-jwks-uri",
                                                       1200,
                                                       "test",
                                                       "test-audience",
                                                       std::move(revokedSet),
                                                       lc);
  }
};

TEST_F(ValidateJwtTestFixture, ValidTokenWithCachedKey) {
  auto authMgr = createAuthMgrWithMockFetcher();
  std::string token = createTestJwt(false /*expired*/, "test-kid");

  // First populate authMgr by calling updateCache
  authMgr->updateCache(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_TRUE(result.isValid);
}

TEST_F(ValidateJwtTestFixture, ValidTokenWithoutCachedKeyCacheFetchSucceeds) {
  auto authMgr = createAuthMgrWithMockFetcher();
  std::string token = createTestJwt(false /*expired*/, "test-kid");
  auto entry = authMgr->getCache().find("test-kid");
  ASSERT_FALSE(entry.has_value());
  auto result = authMgr->validateJwt(token, lc);
  ASSERT_TRUE(result.isValid);  // validate will succeed even if the key is not already present in the authMgr
  // because it will be fetched
  entry = authMgr->getCache().find("test-kid");
  ASSERT_TRUE(entry.has_value());
}

TEST_F(ValidateJwtTestFixture, ValidTokenWithoutCachedKeyCacheFetchFails) {
  auto authMgr = createAuthMgrWithEmptyMockFetcher();

  std::string token = createTestJwt(false /*expired*/, "test-kid");
  auto entry = authMgr->getCache().find("test-kid");
  ASSERT_FALSE(entry.has_value());
  auto result = authMgr->validateJwt(token, lc);
  EXPECT_FALSE(result.isValid);  // validate will fail if we cannot find the public key
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token validation failed");
  // because it will be fetched
  entry = authMgr->getCache().find("test-kid");
  ASSERT_FALSE(entry.has_value());
}

TEST_F(ValidateJwtTestFixture, ExpiredToken) {
  auto authMgr = createAuthMgrWithMockFetcher();
  std::string token = createTestJwt(true /*expired*/, "test-kid");

  // Populate authMgr by calling updateCache
  authMgr->updateCache(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token validation failed");
}

// Tests for invalid/malformed tokens
TEST_F(ValidateJwtTestFixture, BadTokenMissingKid) {
  auto authMgr = createAuthMgrWithMockFetcher();
  std::string token =
    jwt::create()
      .set_issuer("test")
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
      .sign(jwt::algorithm::rs256("", rsa_priv_key, "", ""));

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token header does not contain a 'kid' field");
}

TEST_F(ValidateJwtTestFixture, BadTokenMissingExp) {
  auto authMgr = createAuthMgrWithMockFetcher();
  std::string token = jwt::create()
                        .set_issuer("test")
                        .set_payload_claim("jti", jwt::claim(std::string("test-jti")))
                        .set_header_claim("kid", jwt::claim(std::string("test-kid")))
                        .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
                        .set_payload_claim("aud", jwt::claim(std::string("test-audience")))
                        .sign(jwt::algorithm::rs256("", rsa_priv_key, "", ""));

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token does not contain an 'exp' claim");
}

TEST_F(ValidateJwtTestFixture, BadTokenInvalidSignature) {
  std::string wrongPrivateKey = R"(-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCHSBjxCyh1svTq
Wza9G5j0RMF587aWUWSl9ikTF1PRZV42ruJXkBcP6nIjWse3q5rn2Ce+FIXCkipw
wXUNdbgu5AgXLSFM5g8MGlu3pgENvmKYceiwT6lZS7p9T90HLSbRynzjPT9QcO4d
BJhiwMwMudJMiT8XyTiJ0+Rl+hg3ZruA1OIx/Ezsy+gmRnjcVJT8tWebZ3Kaf7kt
kkdPn6hkvHvS2M0GoJKKuhhOYdae7v1qBavRiK/dT1QoKSObPeEd6ZzPmYl2ge1p
Ud7b4WimudPSlj+fIsVyaWxOwgNgdNlGL2x8T9GcLYA1pZXhPcv8Ms86CHLnqAYg
JEBe51SrAgMBAAECggEAHafF4+1EwMxqmQdG0BzBImcCHLg6wef0ztbP+UHnW2ND
zGv47SYGkDQeMjbfyhkhu4osaCQ6kEUXbaFTBhDUv964EVCQ2Lkj+ky6517KI1el
aHKsBh5oas1Jg9fihYS12k5voybVfs5KvGy59Qf7kxyXB7Ucg7vnu3sKOfqhnV8j
yw6+cb3/6HhPmC13lJqiT3G5Mq9GsWUrgjyN2pxbnKwpPM7PqkB8BgzoHKV8p57I
SGbW9tRmo8nVaIJipYpKFKWo0XiO7o8H3RRoEheabn1tnDoLJGpP8vYjIukhFcHl
EzPVnI7CSvhRSjXrNtPSq7z7V/82zOxRw/XH071GhQKBgQC6t0PMMLc8/3izgeK9
0rhXULBlJMbJA7AJ58TCdEm2JsqwPtYI5yjxq262/tPpenucu0FaunzdgbK0Y6f2
TwYcCsD+P9KnKiDYXByxJ1TUBR5Z/Kq+kj+lffCyuy9catY5Gj9wR8U7v8f7vnOF
PGnBTICjUh2KKsMGky6cpIVrpwKBgQC5eu4H8iIiQ3ojts39srF0y1qldSGYf1Y9
SOJpEJNo5Aqnl8CBelV0grmLbPIJz8ZaykcPERaKX6qJk5gwDVgFPjbtyJrIeeh0
0a/KPQO/DF1mnWUECk3h2+W26WBrexqWkLmBlYijCL8kv8UDPUn6dffFn7jSqEsZ
XZ82DyofXQKBgHp0GLXAuVv63Ek2BOOjYBx7ocQjs28/yOMmKoexRmp81G90NmEO
YW7llK3VQFueZZVrxbfgGGYZWn8t4IkMWKBpeRsFWRONG5b+Ch8xAVQvqzEvITfs
qGs7xnEhjDUbKDW4/iQAHd1KsLhstkyKS31nU/JIt3DXDKKyQl6fH5V/AoGAMu56
plvq25XD2EK+VcfXysZ8YarESufMeo+k/Ey87bSQ6GxXRDafeJrc8Fg+LkuLoCqj
UJPUqLKUVardw3Qmk2n+E1Vei2ZOWqWpq9MNUEzI6QCXWICr2jVT4uI6w8jOCEI9
bkPtfTdNpX2zT6xowAncu7ucONxVouV+bo3Dd1ECgYBHMrGUrJS0ofnNdXHt8QzY
4jYTbci5EF6LKcoo6pCcWaaIi0R6dlstVCBYuLWRONGHWmmgMLp2XuXfgnRkY+PF
2YmhJeKlj/ScUGL528Uwr7YB7SHR4A/KZleVgdFkXH3VYXwdTG2COCsvVGWWRONG
az8ZaVQPvmSthMu8suOc8w==
-----END PRIVATE KEY-----)";

  std::string token =
    jwt::create()
      .set_issuer("test")
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_header_claim("kid", jwt::claim(std::string("test-kid")))
      .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
      .sign(jwt::algorithm::rs256("", wrongPrivateKey, "", ""));

  auto authMgr = createAuthMgrWithMockFetcher();
  auto result = authMgr->validateJwt(token, lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token validation failed");
}

TEST_F(ValidateJwtTestFixture, BadTokenUnsupportedAlgorithm) {
  auto authMgr = createAuthMgrWithMockFetcher();
  std::string token =
    jwt::create()
      .set_issuer("test")
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_header_claim("kid", jwt::claim(std::string("test-kid")))
      .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
      .sign(jwt::algorithm::hs256(rsa_priv_key));  // we accept RS256 only

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token validation failed");
}

TEST_F(ValidateJwtTestFixture, BadTokenMalformedToken) {
  auto authMgr = createAuthMgrWithMockFetcher();
  auto token = createTestJwt(false, "test-kid");
  // append some garbage to the token string
  token += "GARBAGE";

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token validation failed");
}

TEST_F(ValidateJwtTestFixture, BadTokenEmtpyToken) {
  auto authMgr = createAuthMgrWithMockFetcher();
  auto token = "";
  auto result = authMgr->validateJwt(token, lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token validation failed");
}

TEST_F(ValidateJwtTestFixture, BadTokenMissingSub) {
  auto authMgr = createAuthMgrWithMockFetcher();
  // missing "sub" claim, validation will fail
  std::string token =
    jwt::create()
      .set_issuer("test")
      .set_payload_claim("jti", jwt::claim(std::string("test-jti")))
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_header_claim("kid", jwt::claim(std::string("test-kid")))
      .set_payload_claim("aud", jwt::claim(std::string("test-audience")))
      .sign(jwt::algorithm::rs256("", rsa_priv_key, "", ""));

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token does not contain a 'sub' claim");
}

// Tests for issuer validation
TEST_F(ValidateJwtTestFixture, TokenWithWrongIssuer) {
  // Cache expects issuer "test", token issued by "wrong-issuer"
  auto authMgr = createAuthMgrWithMockFetcher();
  authMgr->updateCache(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

  std::string token =
    jwt::create()
      .set_issuer("wrong-issuer")
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_header_claim("kid", jwt::claim(std::string("test-kid")))
      .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
      .sign(jwt::algorithm::rs256("", rsa_priv_key, "", ""));

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token validation failed");
}

// Tests for JTI revocation
TEST_F(ValidateJwtTestFixture, TokenWithRevokedJti) {
  auto authMgr = createAuthMgrWithRevokedJti("revoked-001");
  authMgr->updateCache(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

  std::string token =
    jwt::create()
      .set_issuer("test")
      .set_payload_claim("jti", jwt::claim(std::string("revoked-001")))
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_header_claim("kid", jwt::claim(std::string("test-kid")))
      .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
      .set_payload_claim("aud", jwt::claim(std::string("test-audience")))
      .sign(jwt::algorithm::rs256("", rsa_priv_key, "", ""));

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_NE(result.errorMessage.value().find("revoked"), std::string::npos);
}

TEST_F(ValidateJwtTestFixture, TokenWithNonRevokedJti) {
  auto authMgr = createAuthMgrWithRevokedJti("revoked-001");
  authMgr->updateCache(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

  std::string token =
    jwt::create()
      .set_issuer("test")
      .set_payload_claim("jti", jwt::claim(std::string("valid-jti-not-revoked")))
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_header_claim("kid", jwt::claim(std::string("test-kid")))
      .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
      .set_payload_claim("aud", jwt::claim(std::string("test-audience")))
      .sign(jwt::algorithm::rs256("", rsa_priv_key, "", ""));

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_TRUE(result.isValid);
}

// Tests for missing JTI with specific error message
TEST_F(ValidateJwtTestFixture, TokenMissingJtiHasSpecificErrorMessage) {
  auto authMgr = createAuthMgrWithMockFetcher();
  std::string token =
    jwt::create()
      .set_issuer("test")
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
      .set_payload_claim("aud", jwt::claim(std::string("test-audience")))
      .set_header_claim("kid", jwt::claim(std::string("test-kid")))
      .sign(jwt::algorithm::rs256("", rsa_priv_key, "", ""));

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token does not contain a 'jti' claim");
}

// Tests for audience (aud) claim validation
TEST_F(ValidateJwtTestFixture, TokenWithMatchingAudienceIsValid) {
  auto authMgr = createAuthMgrWithMockFetcher("cta-frontend");
  authMgr->updateCache(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

  std::string token =
    jwt::create()
      .set_issuer("test")
      .set_payload_claim("jti", jwt::claim(std::string("test-jti")))
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_header_claim("kid", jwt::claim(std::string("test-kid")))
      .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
      .set_payload_claim("aud", jwt::claim(std::string("cta-frontend")))
      .sign(jwt::algorithm::rs256("", rsa_priv_key, "", ""));

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_TRUE(result.isValid);
}

TEST_F(ValidateJwtTestFixture, TokenWithMismatchedAudienceIsRejected) {
  auto authMgr = createAuthMgrWithMockFetcher("cta-frontend");
  authMgr->updateCache(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

  std::string token =
    jwt::create()
      .set_issuer("test")
      .set_payload_claim("jti", jwt::claim(std::string("test-jti")))
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_header_claim("kid", jwt::claim(std::string("test-kid")))
      .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
      .set_payload_claim("aud", jwt::claim(std::string("some-other-audience")))
      .sign(jwt::algorithm::rs256("", rsa_priv_key, "", ""));

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_NE(result.errorMessage.value().find("audience"), std::string::npos);
}

}  // namespace unitTests
