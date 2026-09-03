/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "JwksFetcher.hpp"

#include <curl/curl.h>

namespace cta::auth {
// Function to handle curl responses
size_t WriteCallback(const char* contents, size_t size, size_t nmemb, std::string* output) {
  size_t totalSize = size * nmemb;
  output->append(contents, totalSize);
  return totalSize;
};

CurlJwksFetcher::CurlJwksFetcher(int totalTimeoutSecs) : m_totalTimeoutSecs(totalTimeoutSecs) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
}

CurlJwksFetcher::~CurlJwksFetcher() {
  curl_global_cleanup();
}

std::string CurlJwksFetcher::fetchJWKS(const std::string& jwksUrl) {
  CURL* curl;
  CURLcode res;
  std::string readBuffer;

  curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, jwksUrl.c_str());
    // use TLS 1.2 or later
    curl_easy_setopt(curl, CURLOPT_SSLVERSION, CURL_SSLVERSION_TLSv1_2);
    // Set timeouts to prevent indefinite hangs
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, m_totalTimeoutSecs);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
    res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      throw CurlException(std::string("CURL failed in fetchJWKS: ") + curl_easy_strerror(res));
    }
    curl_easy_cleanup(curl);
  } else {
    throw CurlException("CURL failed to call curl_easy_init()");
  }

  return readBuffer;
}
}  // namespace cta::auth
