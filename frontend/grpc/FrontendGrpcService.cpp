/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @copyright      Copyright(C) 2021 DESY
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "FrontendGrpcService.h"

#include "catalogue/Catalogue.hpp"
#include "common/log/LogLevel.hpp"
#include "common/checksum/ChecksumBlobSerDeser.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "frontend/common/FrontendService.hpp"
#include "frontend/common/WorkflowEvent.hpp"

#include <jwt-cpp/jwt.h>
#include <json/json.h>
#include <curl/curl.h>
#include <thread>

/*
 * Validate the storage class and issue the archive ID which should be used for the Archive request
 */

namespace cta::frontend::grpc {

// Function to handle curl responses
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* output) {
    size_t totalSize = size * nmemb;
    output->append((char*)contents, totalSize);
    return totalSize;
}

static Json::Value FetchJWKS(const std::string& jwksUrl) {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;

    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, jwksUrl.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
        res = curl_easy_perform(curl);
        if(res != CURLE_OK) {
            std::cout << "Curl failed: " << curl_easy_strerror(res) << std::endl;
            throw std::runtime_error("CURL failed in FetchJWKS");
        }
        curl_easy_cleanup(curl);
    }
    curl_global_cleanup();

    // Parse the response JSON
    Json::CharReaderBuilder readerBuilder;
    Json::Value jwks;
    std::istringstream sstream(readBuffer);
    std::string errs;
    if (!Json::parseFromStream(readerBuilder, sstream, &jwks, &errs)) {
        std::cout << "Failed to parse JSON: " << errs << std::endl;
        // throw some exception here
        throw std::runtime_error("Failed to parse JSON in FetchJWKS");
    }

    return jwks;
}

// helper function for converting from grpc structure to std::string
static std::string ToString(const ::grpc::string_ref& r) {
  return std::string(r.data(), r.size());
}

bool CtaRpcImpl::ValidateToken(const std::string& encodedJWT) {
  try {
    auto decoded = jwt::decode(encodedJWT);

    // Example validation: check if the token is expired
    auto exp = decoded.get_payload_claim("exp").as_date();
    if (exp < std::chrono::system_clock::now()) {
        std::cout << "Passed-in token has expired!" << std::endl;
        return false;  // Token has expired
    }
    auto header = decoded.get_header_json();
    std::string kid = header["kid"].get<std::string>();
    std::string alg = header["alg"].get<std::string>();
    std::string pubkeyPem;

    std::cout << "KID: " << kid << ",ALG: " << alg << std::endl;
    // Get the JWKS endpoint, find the matching with our token, obtain the public key
    // used to sign the token and validate it
    // first try to use the cached value
    auto it = m_pubkeyCache.m_keymap.find(kid);
    if (it == m_pubkeyCache.m_keymap.end()) {
        std::cout << "No cached key found for kid: " << kid << ", will fetch keys from endpoint" << std::endl;
    } else
      pubkeyPem = it->second.pubkey;
    if (it == m_pubkeyCache.m_keymap.end()) {
      // add the key to the cache, after fetching
      auto m_jwksUri = m_frontendService->getJwksUri().value_or("");
      time_t now = time(NULL);
      m_pubkeyCache.UpdateCache(now);
    }
    it = m_pubkeyCache.m_keymap.find(kid);
    if (it == m_pubkeyCache.m_keymap.end()) {
      // unable to fetch the public key for validation, fail the request
      std::cout << "Unable to find the public key for the token, authentication failed" << std::endl;
      return false;
    }
    pubkeyPem = it->second.pubkey;
    // Validate signature
    auto verifier = jwt::verify()
                        .allow_algorithm(jwt::algorithm::rs256(pubkeyPem, "", "", ""));
                        // .allow_algorithm(jwt::algorithm::rs256(x5c));
                        // .with_issuer("http://auth-keycloak:8080/realms/master");  // Replace with your issuer
    std::cout << "successfully built the verifier" << std::endl;
    verifier.verify(decoded);
    return true;
  } catch (const std::system_error& e) {
    std::cout << "There was a failure in token verification. Code " << e.code() << "meaning: " << e.what() << std::endl;
    return false;
  } catch (const std::runtime_error& e) {
    std::cout << "Failure in token verification, " << e.what() << std::endl;
    return false;
  }
}

Status
CtaRpcImpl::ExtractAuthHeaderAndValidate(::grpc::ServerContext* context) {
  // Retrieve metadata from the incoming request
  auto metadata = context->client_metadata();
  std::string token;

  // Search for the authorization token in the metadata
  auto it = metadata.find("authorization");
  if (it != metadata.end()) {
      std::string auth_header = ToString(it->second);  // "Bearer <token>"
      token = auth_header.substr(7);  // Extract the token part

      std::cout << "Received token: " << token << std::endl;

  } else {
      std::cout << "Authorization header missing" << std::endl;
      return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "Missing Authorization header");
  }
  // Optionally, verify the token here...
  if (ValidateToken(token)) {
    std::cout << "Validate went ok, returning status OK" << std::endl;
    return ::grpc::Status::OK;
  }
  else
  {
    std::cout << "JWT authorization process error. Invalid principal." << std::endl;
    return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "JWT authorization process error. Invalid principal.");
  }
}

Status
CtaRpcImpl::ProcessGrpcRequest(const cta::xrd::Request* request, cta::xrd::Response* response, cta::log::LogContext &lc) const {

  try {
    cta::common::dataStructures::SecurityIdentity clientIdentity(request->notification().wf().instance().name(), cta::utils::getShortHostname());
    cta::frontend::WorkflowEvent wfe(*m_frontendService, clientIdentity, request->notification());
    *response = wfe.process();
  } catch (cta::exception::PbException &ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_PROTOBUF);
    response->set_message_txt(ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, ex.getMessageValue());
  } catch (cta::exception::UserError &ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(ex.getMessageValue());
    /* Logic-wise, this should also be INVALID_ARGUMENT, but we need a way to
     * differentiate between different kinds of errors on the client side,
     * which is why we return ABORTED
     */
    return ::grpc::Status(::grpc::StatusCode::ABORTED, ex.getMessageValue());
  } catch (cta::exception::Exception &ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    response->set_message_txt(ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, ex.getMessageValue());
  } catch (std::runtime_error &ex) {
    lc.log(cta::log::ERR, ex.what());
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    response->set_message_txt(ex.what());
    return ::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, ex.what());
  } catch (...) {
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    return ::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, "Error processing gRPC request");
  }
  return Status::OK;
}

Status
CtaRpcImpl::Create(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  Status status = ExtractAuthHeaderAndValidate(context);
  if (!status.ok())
    return status;

  sp.add("remoteHost", context->peer());
  sp.add("request", "create");
  // check that the workflow is set appropriately for the create event
  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::CREATE) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected CREATE, found " + cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected CREATE, found " + cta::eos::Workflow_EventType_Name(event));
  }
  return ProcessGrpcRequest(request, response, lc);
}

Status
CtaRpcImpl::Archive(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  Status status = ExtractAuthHeaderAndValidate(context);
  if (!status.ok())
    return status;

  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());
  sp.add("request", "archive");
  // check validate request args
  const std::string storageClass = request->notification().file().storage_class();
  if (storageClass.empty()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Storage class is not set.");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set.");
  }

  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::CLOSEW) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected CLOSEW, found " + cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected CLOSEW, found " + cta::eos::Workflow_EventType_Name(event));
  }

  return ProcessGrpcRequest(request, response, lc);
}

Status
CtaRpcImpl::Delete(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  Status status = ExtractAuthHeaderAndValidate(context);
  if (!status.ok())
    return status;

  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());
  sp.add("request", "delete");

  // check validate request args
  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::DELETE) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected DELETE, found " + cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected DELETE, found " + cta::eos::Workflow_EventType_Name(event));
  }

  if (request->notification().file().archive_file_id() == 0) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  // done with validation, now do the workflow processing
  return ProcessGrpcRequest(request, response, lc);
}

Status
CtaRpcImpl::Retrieve(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  Status status = ExtractAuthHeaderAndValidate(context);
  if (!status.ok())
    return status;

  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());
  sp.add("request", "retrieve");

  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::PREPARE) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected PREPARE, found " + cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected PREPARE, found " + cta::eos::Workflow_EventType_Name(event));
  }

  const std::string storageClass = request->notification().file().storage_class();
  if (storageClass.empty()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Storage class is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set.");
  }

  // check validate request args
  if (request->notification().file().archive_file_id() == 0) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  auto instance = request->notification().wf().instance().name();

  sp.add("instance", instance);
  sp.add("username", request->notification().cli().user().username());
  sp.add("groupname", request->notification().cli().user().groupname());
  sp.add("storageClass", storageClass);
  sp.add("archiveID", request->notification().file().archive_file_id());
  sp.add("fileID", request->notification().file().disk_file_id());

  return ProcessGrpcRequest(request, response, lc);
}

Status CtaRpcImpl::CancelRetrieve(::grpc::ServerContext* context,
                                  const cta::xrd::Request* request,
                                  cta::xrd::Response* response) {
  Status status = ExtractAuthHeaderAndValidate(context);
  if (!status.ok())
    return status;

  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());

  lc.log(cta::log::DEBUG, "CancelRetrieve request");
  sp.add("request", "cancel");

  // check validate request args
  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::ABORT_PREPARE) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected ABORT_PREPARE, found " + cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected ABORT_PREPARE, found " + cta::eos::Workflow_EventType_Name(event));
  }

  if (!request->notification().file().archive_file_id()) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  auto instance = request->notification().wf().instance().name();

  sp.add("instance", instance);
  sp.add("username", request->notification().cli().user().username());
  sp.add("groupname", request->notification().cli().user().groupname());
  sp.add("fileID", request->notification().file().archive_file_id());
  sp.add("schedulerJobID", request->notification().file().archive_file_id());

  // field verification done, now try to call the process method
  return ProcessGrpcRequest(request, response, lc);
}

// should be called at server startup
void CtaRpcImpl::StartJwksRefreshThread(int refreshInterval) {
  std::thread([this, refreshInterval]() {
    while (true) {
      time_t now = time(NULL);
      m_pubkeyCache.UpdateCache(now);
      std::this_thread::sleep_for(std::chrono::seconds(refreshInterval));
    }
  }).detach();
}

/* initialize the frontend service
 * this iniitalizes the catalogue, scheduler, logger
 * and makes the rpc calls available through this class
 */
CtaRpcImpl::CtaRpcImpl(const std::string& config)
    : m_frontendService(std::make_unique<cta::frontend::FrontendService>(config)),
      m_pubkeyCache(m_frontendService->getJwksUri().value_or("")) {}

} // namespace cta::frontend::grpc