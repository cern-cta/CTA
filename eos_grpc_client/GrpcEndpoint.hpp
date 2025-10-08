/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "Namespace.hpp"
#include "GrpcClient.hpp"

#include "common/exception/UserError.hpp"

namespace eos::client {

class Endpoint
{
public:
  explicit Endpoint(const Namespace& endpoint) :
    m_grpcClient(::eos::client::GrpcClient::Create(endpoint.endpoint, endpoint.token)) { }

  std::string getPath(const std::string &diskFileId) const;
  ::eos::rpc::InsertReply fileInsert(const ::eos::rpc::FileMdProto &file) const;
  ::eos::rpc::InsertReply containerInsert(const ::eos::rpc::ContainerMdProto &container) const;
  void getCurrentIds(uint64_t &cid, uint64_t &fid) const;
  ::eos::rpc::MDResponse getMD(::eos::rpc::TYPE type, uint64_t id, const std::string &path, bool showJson) const;
  using QueryStatus = int;
  grpc::Status setXAttr(const std::string &path, const std::string &attrKey, const std::string &attrValue) const;

private:
  std::unique_ptr<::eos::client::GrpcClient> m_grpcClient;
};


class EndpointMap
{
public:
  explicit EndpointMap(NamespaceMap_t nsMap) {
    for(auto &ns : nsMap) {
      m_endpointMap.insert(std::make_pair(ns.first, Endpoint(ns.second)));
    }
  }

  std::string getPath(const std::string &diskInstance, const std::string &diskFileId) const {
    auto ep_it = m_endpointMap.find(diskInstance);
    if(ep_it == m_endpointMap.end()) {
      throw cta::exception::UserError("Namespace for disk instance \"" + diskInstance + "\" is not configured in the CTA Frontend");
    } else {
      return ep_it->second.getPath(diskFileId);
    }
  }

  ::eos::rpc::InsertReply fileInsert(const std::string &diskInstance, const ::eos::rpc::FileMdProto &file) const{
    auto ep_it = m_endpointMap.find(diskInstance);
    if(ep_it == m_endpointMap.end()) {
      throw cta::exception::UserError("Namespace for disk instance \"" + diskInstance + "\" is not configured in the CTA Frontend");
    } else {
      return ep_it->second.fileInsert(file);
    }
  }

  ::eos::rpc::InsertReply containerInsert(const std::string &diskInstance, const ::eos::rpc::ContainerMdProto &container) const{
    auto ep_it = m_endpointMap.find(diskInstance);
    if(ep_it == m_endpointMap.end()) {
      throw cta::exception::UserError("Namespace for disk instance \"" + diskInstance + "\" is not configured in the CTA Frontend");
    } else {
      return ep_it->second.containerInsert(container);
    }
  }

  void getCurrentIds(const std::string &diskInstance, uint64_t &cid, uint64_t &fid) const {
    auto ep_it = m_endpointMap.find(diskInstance);
    if(ep_it == m_endpointMap.end()) {
      throw cta::exception::UserError("Namespace for disk instance \"" + diskInstance + "\" is not configured in the CTA Frontend");
    } else {
      return ep_it->second.getCurrentIds(cid, fid);
    }
  }

  ::eos::rpc::MDResponse getMD(const std::string &diskInstance, ::eos::rpc::TYPE type,
                              uint64_t id, const std::string &path, bool showJson) const {
    auto ep_it = m_endpointMap.find(diskInstance);
    if(ep_it == m_endpointMap.end()) {
      throw cta::exception::UserError("Namespace for disk instance \"" + diskInstance + "\" is not configured in the CTA Frontend");
    } else {
      return ep_it->second.getMD(type, id, path, showJson);
    }
  }

  grpc::Status setXAttr(const std::string &diskInstance, const std::string &path, const std::string &attrKey, const std::string &attrValue) const {
    auto ep_it = m_endpointMap.find(diskInstance);
    if(ep_it == m_endpointMap.end()) {
      throw cta::exception::UserError("Namespace for disk instance \"" + diskInstance + "\" is not configured in the CTA Frontend");
    } else {
      return ep_it->second.setXAttr(path, attrKey, attrValue);
    }
  }

private:
  std::map<std::string, Endpoint> m_endpointMap;
};

} // namespace eos::client
