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

#pragma once

#include "Namespace.hpp"
#include "GrpcClient.hpp"

#include "common/exception/UserError.hpp"

namespace eos {
namespace client {

class Endpoint {
public:
  Endpoint(const Namespace& endpoint) :
  m_grpcClient(::eos::client::GrpcClient::Create(endpoint.endpoint, endpoint.token)) {}

  std::string getPath(const std::string& diskFileId) const;
  ::eos::rpc::InsertReply fileInsert(const ::eos::rpc::FileMdProto& file) const;
  ::eos::rpc::InsertReply containerInsert(const ::eos::rpc::ContainerMdProto& container) const;
  void getCurrentIds(uint64_t& cid, uint64_t& fid) const;
  ::eos::rpc::MDResponse getMD(::eos::rpc::TYPE type, uint64_t id, const std::string& path, bool showJson) const;
  using QueryStatus = int;
  grpc::Status setXAttr(const std::string& path, const std::string& attrKey, const std::string& attrValue) const;

private:
  std::unique_ptr<::eos::client::GrpcClient> m_grpcClient;
};

class EndpointMap {
public:
  EndpointMap(NamespaceMap_t nsMap) {
    for (auto& ns : nsMap) {
      m_endpointMap.insert(std::make_pair(ns.first, Endpoint(ns.second)));
    }
  }

  std::string getPath(const std::string& diskInstance, const std::string& diskFileId) const {
    auto ep_it = m_endpointMap.find(diskInstance);
    if (ep_it == m_endpointMap.end()) {
      throw cta::exception::UserError("Namespace for disk instance \"" + diskInstance +
                                      "\" is not configured in the CTA Frontend");
    }
    else {
      return ep_it->second.getPath(diskFileId);
    }
  }

  ::eos::rpc::InsertReply fileInsert(const std::string& diskInstance, const ::eos::rpc::FileMdProto& file) const {
    auto ep_it = m_endpointMap.find(diskInstance);
    if (ep_it == m_endpointMap.end()) {
      throw cta::exception::UserError("Namespace for disk instance \"" + diskInstance +
                                      "\" is not configured in the CTA Frontend");
    }
    else {
      return ep_it->second.fileInsert(file);
    }
  }

  ::eos::rpc::InsertReply containerInsert(const std::string& diskInstance,
                                          const ::eos::rpc::ContainerMdProto& container) const {
    auto ep_it = m_endpointMap.find(diskInstance);
    if (ep_it == m_endpointMap.end()) {
      throw cta::exception::UserError("Namespace for disk instance \"" + diskInstance +
                                      "\" is not configured in the CTA Frontend");
    }
    else {
      return ep_it->second.containerInsert(container);
    }
  }

  void getCurrentIds(const std::string& diskInstance, uint64_t& cid, uint64_t& fid) const {
    auto ep_it = m_endpointMap.find(diskInstance);
    if (ep_it == m_endpointMap.end()) {
      throw cta::exception::UserError("Namespace for disk instance \"" + diskInstance +
                                      "\" is not configured in the CTA Frontend");
    }
    else {
      return ep_it->second.getCurrentIds(cid, fid);
    }
  }

  ::eos::rpc::MDResponse getMD(const std::string& diskInstance,
                               ::eos::rpc::TYPE type,
                               uint64_t id,
                               const std::string& path,
                               bool showJson) const {
    auto ep_it = m_endpointMap.find(diskInstance);
    if (ep_it == m_endpointMap.end()) {
      throw cta::exception::UserError("Namespace for disk instance \"" + diskInstance +
                                      "\" is not configured in the CTA Frontend");
    }
    else {
      return ep_it->second.getMD(type, id, path, showJson);
    }
  }

  grpc::Status setXAttr(const std::string& diskInstance,
                        const std::string& path,
                        const std::string& attrKey,
                        const std::string& attrValue) const {
    auto ep_it = m_endpointMap.find(diskInstance);
    if (ep_it == m_endpointMap.end()) {
      throw cta::exception::UserError("Namespace for disk instance \"" + diskInstance +
                                      "\" is not configured in the CTA Frontend");
    }
    else {
      return ep_it->second.setXAttr(path, attrKey, attrValue);
    }
  }

private:
  std::map<std::string, Endpoint> m_endpointMap;
};

}  // namespace client
}  // namespace eos