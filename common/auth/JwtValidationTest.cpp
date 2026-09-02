/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/auth/Jwt.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"
#include "common/runtime/RuntimeTestHelpers.hpp"

#include <array>
#include <chrono>
#include <ctime>
#include <gtest/gtest.h>
#include <jwt-cpp/jwt.h>
#include <optional>
#include <string>
#include <vector>

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

// An unrelated, genuine RSA key. Used to sign tokens which must fail signature verification:
// it has to be a *valid* key, otherwise the signing call fails while loading the key and the
// test passes without ever reaching signature verification.
const std::string other_rsa_priv_key = R"(-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDIwFuLInMlxqw0
3/UZjBNGNAIMAo4Xeo18Tox41d5yacnqdwLbxMgc3VQtKAr4G7U9llHsKq4oFg9h
XsOLeiJAl4OMVuHHTDklu5/ErD2Eoft98PSlZTXwshhmxOeXTkP9hWgEO16GZZXR
nPSn6CPU+XBPMrO4AfPGH6iLamJml13UQ90+3zCNp3Ho260g6uQs/eZKpOpQsbIi
9lzkjDEDr6/S7nMZlhOmsJA7mHq4KiKZxabvTC0wSLMVahj61lN+wyvgsGgAwSzf
v8wIGBFOEfzM4zbgGAB/Uqy8vpPwbMUxR+tvoGF+W7pjVC6cyS6zdVUIHWAys/Gx
s3hKgIhdAgMBAAECggEACA2XXoR6RAitPc3CIk05NLa/YkXz90MmS9dG0U4bB1Hx
FKPDjJdfQ7EpQEp77ioGYjsWfDfWFquT4F8RzxWOLDMnxshxdDI9lKLrLNOQADMT
SKES31OpD0fFrNG3TJ2KIrBoVMsg5plKokb+lYG9djyvYa3yAa6qXcdFVppk6iKU
Iw3pD9tFRzpQMcdOr3FA/b/OmCdWa0CfMFmN8HJVcxEgmPD/4kCG9oqE3i6BiCUS
aTfmBE+VRR6uEPuORBMFXBkZmJK2ok1vF+TgrxQCQjR6mAEuFcJ/HDA7ATVwTakV
OcaN6SXdJfrbEwQDdY6/o7QTxy0toasmXoixwdML7QKBgQDm2f2w6FkinnWzCFJf
BRoTgthOev6FgrgvHpitm4MJ7LBAm7vXcbZm8JDZT+cg3xrNxYHAF09JQwFIWjsd
fcL/Z9v7fVF5MAs28fU3yV26pAJokhKq98M0tJzQAN11BC1xyKnhKije+PXEHb1h
PXFAPDj2SW4TSgJ1mY46Z5CFswKBgQDenu5tNnRqllSQnWVUEqsWrWFALVTuoXhZ
V9d5JPryLQu8hmc1MTr96ncFPVq2+QmhJKrN/hnBGGvRM/dCafZEsUu10wfC/qpV
nfJAgaecEnkOY4oipovTavUKQ0kRYMSk1As6gE20+g28DcjOlSv++b8ZMbB81SRi
UhPhbTrRrwKBgQDcMGTHznbmjFobb/6RX15l6dHD2ZDXa72eRALA5KnyV24t0d0z
O+UM/rxKauo78lGwn4iI3jLj4CNjDZ2BHalWz4uFrxx9CRRJTjPlCA58rV8Wuu1P
YY8xwGwqAgk5ScjG1O0kNYBSXQieDPfLvj09VbPHRnbVsyvxW4vLrWit/QKBgBUJ
bbxyReQAmrMjvHyKWxKEhckbv4fhSE8hBuKSxQf3i8Ff7gbPxSRTcLXVC2p49Bj/
LwjSNzrRBPc68uWav8PUATSIYZZinFQE3eyMk/sin7/lVhtaI6Jx3AABRblXrJ1q
0DjSAQXWD1Ay+UPUQtkQXqeR03yoY4zK9sf5m3JPAoGALIEFuuLIoBI9hUpUiIGm
YP9b3W582BScDA8/nsbUAxdhCRv90l53kcujBhb3Z8BWlhs0z6YtiMFI7pDv/46e
sqlyt6igZPE9DH72oIww3jzIhicJeGIw/yptdQm01OAx0RG0cb2BNdYFUatUmM6q
iXXOdCMYe1wqhcfflAMUSDw=
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

/**
 * Builds a single-entry JWKS document advertising the sample certificate under the given 'kid'.
 */
std::string makeJwks(const std::string& kid) {
  return R"({
    "keys": [{
        "kid": ")"
         + kid + R"(",
        "alg": "RS256",
        "kty": "RSA",
        "use": "sig",
        "x5c": [
        ")"
         + sample_cert_base64_der + R"("
        ],
        "e": "AQAB"
    }]
    })";
}

const std::string raw_jwks = makeJwks("test-kid");

class MockJwksFetcherValidateJwt : public cta::auth::JwksFetcher {
private:
  std::string m_jwks {raw_jwks};

public:
  MockJwksFetcherValidateJwt() {}

  void setJwks(const std::string& jwks) { m_jwks = jwks; }

  std::string fetchJWKS(const std::string& jwksUrl) override { return m_jwks; }
};

/**
 * Formats a time point as an unquoted TOML offset date-time, e.g. 2025-01-01T00:00:00Z.
 * The wall-clock part is always the UTC time; pass an explicit offset (e.g. "+02:00") to label
 * that same wall-clock time as belonging to another timezone.
 */
std::string toTomlDateTime(std::chrono::system_clock::time_point timePoint, const std::string& offset = "Z") {
  const std::time_t asTimeT = std::chrono::system_clock::to_time_t(timePoint);
  std::tm tm {};
  ::gmtime_r(&asTimeT, &tm);
  std::array<char, 32> buffer {};
  std::strftime(buffer.data(), buffer.size(), "%Y-%m-%dT%H:%M:%S", &tm);
  return buffer.data() + offset;
}

/**
 * A single '[[revoked_tokens]]' entry, as it appears in a revoke-list TOML file.
 * The revocation date is kept as raw TOML so that tests can also inject invalid values.
 */
struct RevokeListEntry {
  std::string jti;
  std::string revokedAtToml;
};

/**
 * Builds the contents of a revoke-list TOML file, i.e. the file pointed to by the
 * 'revoke_list_path' option of the JWT auth config.
 */
std::string makeRevokeListToml(const std::vector<RevokeListEntry>& entries) {
  std::string contents;
  for (const auto& entry : entries) {
    contents += "[[revoked_tokens]]\n";
    contents += "jti = \"" + entry.jti + "\"\n";
    contents += "reason = \"revoked by unit test\"\n";
    contents += "revoked_at = " + entry.revokedAtToml + "\n\n";
  }
  return contents;
}

/**
 * A revoke-list entry for the given JTI, revoked one hour ago, i.e. a valid entry.
 */
RevokeListEntry revokedAnHourAgo(const std::string& jti) {
  return {jti, toTomlDateTime(std::chrono::system_clock::now() - std::chrono::hours(1))};
}

std::string createTestJwt(bool expired, const std::string& kid, const std::string& jti = "test-jti", int gen = 0) {
  // first get the public key in pem format, then use it to sign stuff

  auto token = jwt::create()
                 .set_issuer("test")
                 .set_payload_claim("jti", jwt::claim(jti))
                 .set_payload_claim("exp",
                                    jwt::claim(std::chrono::system_clock::now()
                                               + (expired ? -std::chrono::minutes(60) : std::chrono::minutes(60))))
                 .set_payload_claim("gen", jwt::claim(picojson::value(static_cast<int64_t>(gen))))
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
  createAuthMgrWithMockFetcher(const std::string& expectedAudience = "test-audience",
                               uint32_t minGeneration = 0) const {
    return std::make_shared<cta::auth::JwtAuthManager>(std::make_unique<MockJwksFetcherValidateJwt>(),
                                                       "http://fake-jwks-uri",
                                                       1200,
                                                       "test",
                                                       expectedAudience,
                                                       minGeneration,
                                                       std::nullopt,
                                                       lc);
  }

  /**
   * Builds an auth manager whose JWKS endpoint serves the given document verbatim, so that tests
   * can serve a JWKS which is unparsable, or which simply does not carry the requested key.
   */
  std::shared_ptr<cta::auth::JwtAuthManager> createAuthMgrServingJwks(const std::string& jwks) const {
    auto mockFetcher = std::make_unique<MockJwksFetcherValidateJwt>();
    mockFetcher->setJwks(jwks);

    return std::make_shared<cta::auth::JwtAuthManager>(std::move(mockFetcher),
                                                       "http://fake-jwks-uri",
                                                       1200,
                                                       "test",
                                                       "test-audience",
                                                       0,
                                                       std::nullopt,
                                                       lc);
  }

  /**
   * Asserts that validation failed, and that it failed for the expected reason.
   *
   * validateJwt() funnels every exception into the same opaque "Token validation failed" message,
   * so asserting on that message alone does not distinguish an expired token from a bad signature,
   * a wrong issuer, or a malformed token: such a test keeps passing when the token is rejected for
   * a completely unrelated reason. The underlying cause is only visible in the log, as the
   * 'exception_message' parameter, so that is what pins the failure down.
   *
   * @param result       The validation result under test.
   * @param expectedCause Exact value expected for the logged 'exception_message' parameter.
   */
  void expectRejectedBecause(const cta::auth::TokenValidationResult& result, const std::string& expectedCause) const {
    EXPECT_FALSE(result.isValid);
    ASSERT_TRUE(result.errorMessage.has_value());
    EXPECT_EQ(result.errorMessage.value(), "Token validation failed");

    const std::string loggedCause = "exception_message=\"" + expectedCause + "\"";
    const std::string logContents = log.getLog();
    EXPECT_NE(logContents.find(loggedCause), std::string::npos)
      << "expected validation to fail because of " << loggedCause << ", but the log says:\n"
      << logContents;
  }

  /**
   * Builds an auth manager whose revoke list is loaded from the given TOML file path.
   * The path is passed through verbatim, so it may also point at a missing or invalid file.
   */
  std::shared_ptr<cta::auth::JwtAuthManager> createAuthMgrWithRevokeListFile(const std::string& revokeListPath) const {
    return std::make_shared<cta::auth::JwtAuthManager>(std::make_unique<MockJwksFetcherValidateJwt>(),
                                                       "http://fake-jwks-uri",
                                                       1200,
                                                       "test",
                                                       "test-audience",
                                                       0,
                                                       revokeListPath,
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

TEST_F(ValidateJwtTestFixture, ValidTokenWithoutCachedKeyNotServedByJwks) {
  // The JWKS parses fine but carries a different key, so the refetch cannot resolve our 'kid'.
  // This is what reaches the "unable to find the public key" branch of validateJwt.
  auto authMgr = createAuthMgrServingJwks(makeJwks("some-other-kid"));

  std::string token = createTestJwt(false /*expired*/, "test-kid");
  ASSERT_FALSE(authMgr->getCache().find("test-kid").has_value());

  auto result = authMgr->validateJwt(token, lc);
  EXPECT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Unable to find the public key for the token, authentication failed");
  // the refetch did happen, it just did not produce the key we asked for
  EXPECT_TRUE(authMgr->getCache().find("some-other-kid").has_value());
  EXPECT_FALSE(authMgr->getCache().find("test-kid").has_value());
}

TEST_F(ValidateJwtTestFixture, ValidTokenWithoutCachedKeyUnparsableJwks) {
  // An unparsable JWKS fails earlier than the above, inside jwt::parse_jwks, so it surfaces as
  // the generic message rather than as the "unable to find the public key" one.
  auto authMgr = createAuthMgrServingJwks("");

  std::string token = createTestJwt(false /*expired*/, "test-kid");
  ASSERT_FALSE(authMgr->getCache().find("test-kid").has_value());

  expectRejectedBecause(authMgr->validateJwt(token, lc), "invalid json");
  EXPECT_FALSE(authMgr->getCache().find("test-kid").has_value());
}

TEST_F(ValidateJwtTestFixture, ExpiredToken) {
  auto authMgr = createAuthMgrWithMockFetcher();
  std::string token = createTestJwt(true /*expired*/, "test-kid");

  // Populate authMgr by calling updateCache
  authMgr->updateCache(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

  expectRejectedBecause(authMgr->validateJwt(token, lc), "token expired");
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
  // Signed with a genuine key which is not the one the JWKS advertises, so the token is
  // well-formed and its signature is a real signature: only verification against the cached
  // public key can reject it.
  std::string token =
    jwt::create()
      .set_issuer("test")
      .set_payload_claim("jti", jwt::claim(std::string("test-jti")))
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_header_claim("kid", jwt::claim(std::string("test-kid")))
      .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
      .set_payload_claim("aud", jwt::claim(std::string("test-audience")))
      .sign(jwt::algorithm::rs256("", other_rsa_priv_key, "", ""));

  auto authMgr = createAuthMgrWithMockFetcher();
  expectRejectedBecause(authMgr->validateJwt(token, lc), "failed to verify signature: VerifyFinal failed");
}

TEST_F(ValidateJwtTestFixture, BadTokenUnsupportedAlgorithm) {
  auto authMgr = createAuthMgrWithMockFetcher();
  std::string token =
    jwt::create()
      .set_issuer("test")
      .set_payload_claim("jti", jwt::claim(std::string("test-jti")))
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_header_claim("kid", jwt::claim(std::string("test-kid")))
      .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
      .set_payload_claim("aud", jwt::claim(std::string("test-audience")))
      .sign(jwt::algorithm::hs256(rsa_priv_key));  // we accept RS256 only

  expectRejectedBecause(authMgr->validateJwt(token, lc), "wrong algorithm");
}

TEST_F(ValidateJwtTestFixture, BadTokenMalformedToken) {
  auto authMgr = createAuthMgrWithMockFetcher();
  auto token = createTestJwt(false, "test-kid");
  // append some garbage to the token string
  token += "GARBAGE";

  expectRejectedBecause(authMgr->validateJwt(token, lc), "Invalid input: too much fill");
}

TEST_F(ValidateJwtTestFixture, BadTokenEmptyToken) {
  auto authMgr = createAuthMgrWithMockFetcher();
  expectRejectedBecause(authMgr->validateJwt("", lc), "invalid token supplied");
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
      .set_payload_claim("jti", jwt::claim(std::string("test-jti")))
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_header_claim("kid", jwt::claim(std::string("test-kid")))
      .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
      .set_payload_claim("aud", jwt::claim(std::string("test-audience")))
      .sign(jwt::algorithm::rs256("", rsa_priv_key, "", ""));

  expectRejectedBecause(authMgr->validateJwt(token, lc), "claim value does not match expected value");
}

TEST_F(ValidateJwtTestFixture, TokenMissingGenClaimIsRejected) {
  auto authMgr = createAuthMgrWithMockFetcher();
  std::string token =
    jwt::create()
      .set_issuer("test")
      .set_payload_claim("jti", jwt::claim(std::string("test-jti")))
      .set_payload_claim("exp", jwt::claim(std::chrono::system_clock::now() + std::chrono::minutes(60)))
      .set_header_claim("kid", jwt::claim(std::string("test-kid")))
      .set_payload_claim("sub", jwt::claim(std::string("subjectClaim")))
      .set_payload_claim("aud", jwt::claim(std::string("test-audience")))
      .sign(jwt::algorithm::rs256("", rsa_priv_key, "", ""));

  auto result = authMgr->validateJwt(token, lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token does not contain a 'gen' claim");
}

TEST_F(ValidateJwtTestFixture, TokenWithGenBelowMinimumIsRejected) {
  auto authMgr = createAuthMgrWithMockFetcher("test-audience", 5 /*minGeneration*/);

  auto result = authMgr->validateJwt(createTestJwt(false /*expired*/, "test-kid", "test-jti", 4 /*gen*/), lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token generation is too old, minimum required is 5");
}

TEST_F(ValidateJwtTestFixture, TokenWithGenEqualToMinimumIsValid) {
  auto authMgr = createAuthMgrWithMockFetcher("test-audience", 5 /*minGeneration*/);

  auto result = authMgr->validateJwt(createTestJwt(false /*expired*/, "test-kid", "test-jti", 5 /*gen*/), lc);
  EXPECT_TRUE(result.isValid);
}

TEST_F(ValidateJwtTestFixture, TokenWithGenAboveMinimumIsValid) {
  auto authMgr = createAuthMgrWithMockFetcher("test-audience", 5 /*minGeneration*/);

  auto result = authMgr->validateJwt(createTestJwt(false /*expired*/, "test-kid", "test-jti", 6 /*gen*/), lc);
  EXPECT_TRUE(result.isValid);
}

TEST_F(ValidateJwtTestFixture, TokenWithRevokedJtiIsRejected) {
  TempFile revokeList(makeRevokeListToml({revokedAnHourAgo("revoked-001")}), ".toml");
  auto authMgr = createAuthMgrWithRevokeListFile(revokeList.path());
  authMgr->updateCache(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

  auto result = authMgr->validateJwt(createTestJwt(false /*expired*/, "test-kid", "revoked-001"), lc);
  ASSERT_FALSE(result.isValid);
  ASSERT_TRUE(result.errorMessage.has_value());
  EXPECT_EQ(result.errorMessage.value(), "Token 'revoked-001' has been revoked");
}

TEST_F(ValidateJwtTestFixture, TokenWithNonRevokedJtiIsValid) {
  // Same revoke list as above, but the token carries a JTI which is not on it.
  TempFile revokeList(makeRevokeListToml({revokedAnHourAgo("revoked-001")}), ".toml");
  auto authMgr = createAuthMgrWithRevokeListFile(revokeList.path());
  authMgr->updateCache(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

  auto result = authMgr->validateJwt(createTestJwt(false /*expired*/, "test-kid", "not-revoked-001"), lc);
  EXPECT_TRUE(result.isValid);
  EXPECT_FALSE(result.errorMessage.has_value());
  ASSERT_TRUE(result.subjectClaim.has_value());
  EXPECT_EQ(result.subjectClaim.value(), "subjectClaim");
}

TEST_F(ValidateJwtTestFixture, RevokeListWithSeveralEntriesRevokesEveryOneOfThem) {
  const std::string toml = makeRevokeListToml(
    {revokedAnHourAgo("revoked-001"), revokedAnHourAgo("revoked-002"), revokedAnHourAgo("revoked-003")});
  TempFile revokeList(toml, ".toml");
  auto authMgr = createAuthMgrWithRevokeListFile(revokeList.path());

  EXPECT_TRUE(authMgr->isRevoked("revoked-001"));
  EXPECT_TRUE(authMgr->isRevoked("revoked-002"));
  EXPECT_TRUE(authMgr->isRevoked("revoked-003"));
  EXPECT_FALSE(authMgr->isRevoked("revoked-004"));
  EXPECT_FALSE(authMgr->isRevoked(""));
}

TEST_F(ValidateJwtTestFixture, RevokeListEntriesAreMatchedExactly) {
  TempFile revokeList(makeRevokeListToml({revokedAnHourAgo("revoked-001")}), ".toml");
  auto authMgr = createAuthMgrWithRevokeListFile(revokeList.path());

  EXPECT_TRUE(authMgr->isRevoked("revoked-001"));
  // no prefix, suffix, substring or case-insensitive matching
  EXPECT_FALSE(authMgr->isRevoked("revoked-0"));
  EXPECT_FALSE(authMgr->isRevoked("revoked-0011"));
  EXPECT_FALSE(authMgr->isRevoked(" revoked-001"));
  EXPECT_FALSE(authMgr->isRevoked("REVOKED-001"));
}

TEST_F(ValidateJwtTestFixture, EmptyRevokeListFileRevokesNothing) {
  // A revoke list with no '[[revoked_tokens]]' entries at all is loaded in non-strict
  // mode, so it is accepted and simply revokes nothing.
  TempFile revokeList("", ".toml");
  auto authMgr = createAuthMgrWithRevokeListFile(revokeList.path());
  authMgr->updateCache(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

  EXPECT_FALSE(authMgr->isRevoked("revoked-001"));
  EXPECT_TRUE(authMgr->validateJwt(createTestJwt(false /*expired*/, "test-kid", "revoked-001"), lc).isValid);
}

TEST_F(ValidateJwtTestFixture, NoRevokeListPathRevokesNothing) {
  // 'revoke_list_path' is optional: with no path configured, no token is ever revoked.
  auto authMgr = createAuthMgrWithMockFetcher();

  EXPECT_FALSE(authMgr->isRevoked("revoked-001"));
  EXPECT_TRUE(authMgr->validateJwt(createTestJwt(false /*expired*/, "test-kid", "revoked-001"), lc).isValid);
}

TEST_F(ValidateJwtTestFixture, MissingRevokeListFileIsRejected) {
  EXPECT_THROW(createAuthMgrWithRevokeListFile("/tmp/this-revoke-list-definitely-does-not-exist.toml"),
               cta::exception::UserError);
}

TEST_F(ValidateJwtTestFixture, MalformedRevokeListFileIsRejected) {
  TempFile revokeList("[[revoked_tokens]]\njti = \"unterminated string\n", ".toml");
  EXPECT_THROW(createAuthMgrWithRevokeListFile(revokeList.path()), cta::exception::UserError);
}

TEST_F(ValidateJwtTestFixture, RevokeListEntryWithEmptyJtiIsRejected) {
  TempFile revokeList(makeRevokeListToml({revokedAnHourAgo("")}), ".toml");
  EXPECT_THROW(createAuthMgrWithRevokeListFile(revokeList.path()), cta::exception::UserError);
}

TEST_F(ValidateJwtTestFixture, RevokeListEntryWithMissingJtiIsRejected) {
  // In non-strict mode a missing 'jti' key leaves the field empty, which must be caught
  // by the same check as an explicitly empty JTI.
  const std::string toml = "[[revoked_tokens]]\nreason = \"no jti given\"\nrevoked_at = "
                           + toTomlDateTime(std::chrono::system_clock::now() - std::chrono::hours(1)) + "\n";
  TempFile revokeList(toml, ".toml");
  EXPECT_THROW(createAuthMgrWithRevokeListFile(revokeList.path()), cta::exception::UserError);
}

TEST_F(ValidateJwtTestFixture, RevokeListEntryWithFutureRevocationDateIsRejected) {
  const auto tomorrow = toTomlDateTime(std::chrono::system_clock::now() + std::chrono::hours(24));
  const std::string toml = makeRevokeListToml({
    RevokeListEntry {"revoked-001", tomorrow}
  });
  TempFile revokeList(toml, ".toml");
  EXPECT_THROW(createAuthMgrWithRevokeListFile(revokeList.path()), cta::exception::UserError);
}

TEST_F(ValidateJwtTestFixture, RevokeListEntryWithRevocationDateBefore1970IsRejected) {
  const std::string toml = makeRevokeListToml({
    RevokeListEntry {"revoked-001", "1969-07-20T20:17:00Z"}
  });
  TempFile revokeList(toml, ".toml");
  EXPECT_THROW(createAuthMgrWithRevokeListFile(revokeList.path()), cta::exception::UserError);
}

TEST_F(ValidateJwtTestFixture, RevokeListEntryWithMissingRevocationDateIsRejected) {
  // A missing 'revoked_at' leaves a zero-initialised date, i.e. a year well before 1970.
  TempFile revokeList("[[revoked_tokens]]\njti = \"revoked-001\"\nreason = \"no date given\"\n", ".toml");
  EXPECT_THROW(createAuthMgrWithRevokeListFile(revokeList.path()), cta::exception::UserError);
}

TEST_F(ValidateJwtTestFixture, RevokeListEntryWithoutReasonIsAccepted) {
  // 'reason' is only kept for auditing, so it is not required in order to load the list.
  const std::string toml = "[[revoked_tokens]]\njti = \"revoked-001\"\nrevoked_at = "
                           + toTomlDateTime(std::chrono::system_clock::now() - std::chrono::hours(1)) + "\n";
  TempFile revokeList(toml, ".toml");
  auto authMgr = createAuthMgrWithRevokeListFile(revokeList.path());
  EXPECT_TRUE(authMgr->isRevoked("revoked-001"));
}

TEST_F(ValidateJwtTestFixture, RevokeListEntryWithTimezoneOffsetIsInterpretedAsUtc) {
  // A revocation date carrying an offset must be converted to UTC before being compared
  // against 'now'. Written as a wall-clock time one hour ahead with a +02:00 offset, this
  // entry is one hour in the *past* in UTC and must therefore be accepted.
  const auto oneHourAheadPlusTwo = toTomlDateTime(std::chrono::system_clock::now() + std::chrono::hours(1), "+02:00");
  const std::string toml = makeRevokeListToml({
    RevokeListEntry {"revoked-001", oneHourAheadPlusTwo}
  });
  TempFile revokeList(toml, ".toml");
  auto authMgr = createAuthMgrWithRevokeListFile(revokeList.path());
  EXPECT_TRUE(authMgr->isRevoked("revoked-001"));
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
      .set_payload_claim("gen", jwt::claim(picojson::value(static_cast<int64_t>(0))))
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
