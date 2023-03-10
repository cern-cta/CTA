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

#include "RestClient.hpp"
#include "common/exception/Rest.hpp"
#include "common/exception/UserError.hpp"

#include <curl/curl.h>
#include <string>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <nlohmann/json.hpp>

namespace cta::rest {

size_t writeFunction(void *ptr, size_t size, size_t nmemb, std::string* data) {
    data->append((char*) ptr, size * nmemb);
    return size * nmemb;
}

void RestClient::connectToEndpoint(const std::string& keytabFile) {
  // Open the keytab file for reading
  std::ifstream file(keytabFile);
  if(!file) {
    throw cta::exception::UserError("Failed to open namespace keytab configuration file " + keytabFile);
  }

  // Parse the keytab line by line
  for(std::string line; std::getline(file, line);) {
    // Strip out comments
    if(auto pos = line.find('#'); pos != std::string::npos) {
      line.resize(pos);
    }

    // Parse one line
    std::stringstream ss(line);
    std::string diskInstance;
    std::string endpoint;
    std::string token;
    std::string eol;
    ss >> diskInstance >> endpoint >> token >> eol;

    // Ignore blank lines, all other lines must have exactly 3 elements
    if(diskInstance.empty() && endpoint.empty() && token.empty()) continue;
    if(token.empty() || !eol.empty()) {
      throw cta::exception::UserError("Could not parse namespace keytab configuration file");
    }
    instanceToHostTokenMap.try_emplace(diskInstance, endpoint, token);
  }
}


std::unordered_map<std::string, std::any> RestClient::getFileInfo() const {
  if (auto curl = curl_easy_init()) {

      curl_easy_setopt(curl, CURLOPT_URL, "http://eosctaatlaspps:8444/proc/user/?mgm.cmd=fileinfo&mgm.path=fxid:10074c131&eos.ruid=0&eos.rgid=0&mgm.format=json");
      curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
      curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 50L);
      curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);

      std::string response_string;
      std::string header_string;
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeFunction);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
      curl_easy_setopt(curl, CURLOPT_HEADERDATA, &header_string);

      char* url;
      long response_code;
      double elapsed;
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
      curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &elapsed);
      curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &url);

      curl_easy_perform(curl);
      curl_easy_cleanup(curl);
      curl = nullptr;

      std::cout << response_string << std::endl;
      std::cout << header_string << std::endl;
      std::cout << url << std::endl;

      nlohmann::json j3 = nlohmann::json::parse(response_string);

      for (const auto& [key, val] : j3.items()) {
        std::cout << "key: " << key << ", value: " << val << std::endl;
      }

      return std::unordered_map<std::string, std::any>();
  }
  throw cta::exception::Rest("Unable to initialize curl");
}
} // namespace cta::rest