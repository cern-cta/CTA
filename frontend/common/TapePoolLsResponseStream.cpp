/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#include "TapePoolLsResponseStream.hpp"

#include "catalogue/TapePoolSearchCriteria.hpp"
#include "frontend/common/AdminCmdOptions.hpp"

namespace cta::frontend {

TapePoolLsResponseStream::TapePoolLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                   cta::Scheduler& scheduler,
                                                   const std::string& instanceName,
                                                   const admin::AdminCmd& adminCmd)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_tapePools(buildTapePoolList(adminCmd)) {}

std::list<cta::catalogue::TapePool> TapePoolLsResponseStream::buildTapePoolList(const admin::AdminCmd& admincmd) {
  cta::frontend::AdminCmdOptions request(admincmd);

  cta::catalogue::TapePoolSearchCriteria searchCriteria;
  searchCriteria.name = request.getOptional(cta::admin::OptionString::TAPE_POOL);
  searchCriteria.vo = request.getOptional(cta::admin::OptionString::VO);
  searchCriteria.encrypted = request.getOptional(cta::admin::OptionBoolean::ENCRYPTED);
  searchCriteria.encryptionKeyName = request.getOptional(cta::admin::OptionString::ENCRYPTION_KEY_NAME);

  if (searchCriteria.encrypted.has_value() && searchCriteria.encryptionKeyName.has_value()) {
    throw cta::exception::UserError("Do not request both '--encrypted' and '--encryptionkeyname' at same time.");
  }

  return m_catalogue.TapePool()->getTapePools(searchCriteria);
}

bool TapePoolLsResponseStream::isDone() {
  return m_tapePools.empty();
}

cta::xrd::Data TapePoolLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto tp = m_tapePools.front();
  m_tapePools.pop_front();

  cta::xrd::Data data;
  auto tp_item = data.mutable_tpls_item();

  tp_item->set_name(tp.name);
  tp_item->set_instance_name(m_instanceName);
  tp_item->set_vo(tp.vo.name);
  tp_item->set_num_tapes(tp.nbTapes);
  tp_item->set_num_partial_tapes(tp.nbPartialTapes);
  tp_item->set_num_physical_files(tp.nbPhysicalFiles);
  tp_item->set_capacity_bytes(tp.capacityBytes);
  tp_item->set_data_bytes(tp.dataBytes);
  tp_item->set_encrypt(tp.encryption);
  tp_item->set_encryption_key_name(tp.encryptionKeyName.value_or(""));
  tp_item->set_supply(tp.supply.value_or(""));
  tp_item->mutable_created()->set_username(tp.creationLog.username);
  tp_item->mutable_created()->set_host(tp.creationLog.host);
  tp_item->mutable_created()->set_time(tp.creationLog.time);
  tp_item->mutable_modified()->set_username(tp.lastModificationLog.username);
  tp_item->mutable_modified()->set_host(tp.lastModificationLog.host);
  tp_item->mutable_modified()->set_time(tp.lastModificationLog.time);
  tp_item->set_comment(tp.comment);
  for (auto& source : tp.supply_source_set) {
    tp_item->add_supply_source(source);
  }
  for (auto& destination : tp.supply_destination_set) {
    tp_item->add_supply_destination(destination);
  }

  return data;
}

}  // namespace cta::frontend
