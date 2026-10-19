/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/sync/handlers/Handlers.hpp"
#include "common/exception/UserError.hpp"
#include "common/utils/utils.hpp"

namespace cta::admin_api::commands::sync::handlers {

using admin::OptionBoolean;
using admin::OptionString;
using admin::OptionStrList;
using admin::OptionUInt64;

void addArchiveRoute(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& scn = opts.getRequired(OptionString::STORAGE_CLASS);
  auto& cn = opts.getRequired(OptionUInt64::COPY_NUMBER);
  auto art_opt = opts.getOptional(OptionString::ARCHIVE_ROUTE_TYPE);
  auto& tapepool = opts.getRequired(OptionString::TAPE_POOL);
  auto& comment = opts.getRequired(OptionString::COMMENT);

  // Adding a new archive route without type is allowed. If so, it should default to DEFAULT.
  auto art = art_opt.has_value() ? common::dataStructures::strToArchiveRouteType(art_opt.value()) :
                                   cta::common::dataStructures::ArchiveRouteType::DEFAULT;

  ctx.catalogue.ArchiveRoute()
    ->createArchiveRoute(ctx.identity, scn, static_cast<uint32_t>(cn), art, tapepool, comment);
}

void changeArchiveRoute(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& scn = opts.getRequired(OptionString::STORAGE_CLASS);
  auto& cn = opts.getRequired(OptionUInt64::COPY_NUMBER);
  auto& art_str = opts.getRequired(OptionString::ARCHIVE_ROUTE_TYPE);

  auto art = common::dataStructures::strToArchiveRouteType(art_str);

  if (const auto comment = opts.getOptional(OptionString::COMMENT); comment) {
    ctx.catalogue.ArchiveRoute()->modifyArchiveRouteComment(ctx.identity,
                                                            scn,
                                                            static_cast<uint32_t>(cn),
                                                            art,
                                                            comment.value());
  }
  if (const auto tapepool = opts.getOptional(OptionString::TAPE_POOL); tapepool) {
    ctx.catalogue.ArchiveRoute()->modifyArchiveRouteTapePoolName(ctx.identity,
                                                                 scn,
                                                                 static_cast<uint32_t>(cn),
                                                                 art,
                                                                 tapepool.value());
  }
}

void removeArchiveRoute(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& scn = opts.getRequired(OptionString::STORAGE_CLASS);
  auto& cn = opts.getRequired(OptionUInt64::COPY_NUMBER);
  auto& art_str = opts.getRequired(OptionString::ARCHIVE_ROUTE_TYPE);

  auto art = common::dataStructures::strToArchiveRouteType(art_str);

  ctx.catalogue.ArchiveRoute()->deleteArchiveRoute(scn, static_cast<uint32_t>(cn), art);
}

void addStorageClass(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  common::dataStructures::StorageClass storageClass;

  storageClass.name = opts.getRequired(OptionString::STORAGE_CLASS);
  storageClass.nbCopies = opts.getRequired(OptionUInt64::COPY_NUMBER);
  storageClass.comment = opts.getRequired(OptionString::COMMENT);
  storageClass.vo.name = opts.getRequired(OptionString::VO);

  ctx.catalogue.StorageClass()->createStorageClass(ctx.identity, storageClass);
}

void changeStorageClass(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& scn = opts.getRequired(OptionString::STORAGE_CLASS);
  auto comment = opts.getOptional(OptionString::COMMENT);
  auto cn = opts.getOptional(OptionUInt64::COPY_NUMBER);
  auto vo = opts.getOptional(OptionString::VO);

  if (comment.has_value()) {
    ctx.catalogue.StorageClass()->modifyStorageClassComment(ctx.identity, scn, comment.value());
  }
  if (cn.has_value()) {
    ctx.catalogue.StorageClass()->modifyStorageClassNbCopies(ctx.identity, scn, cn.value());
  }
  if (vo.has_value()) {
    ctx.catalogue.StorageClass()->modifyStorageClassVo(ctx.identity, scn, vo.value());
  }
}

void removeStorageClass(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& scn = opts.getRequired(OptionString::STORAGE_CLASS);

  ctx.catalogue.StorageClass()->deleteStorageClass(scn);
}

void addTapePool(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& name = opts.getRequired(OptionString::TAPE_POOL);
  auto& vo = opts.getRequired(OptionString::VO);
  auto& ptn = opts.getRequired(OptionUInt64::PARTIAL_TAPES_NUMBER);
  auto& comment = opts.getRequired(OptionString::COMMENT);
  auto encryptionKeyName = opts.getOptional(OptionString::ENCRYPTION_KEY_NAME);
  auto supply = opts.getOptional(OptionString::SUPPLY);

  std::vector<std::string> supply_list;
  if (supply.has_value()) {
    supply_list = cta::utils::commaSeparatedStringToVector(supply.value());
  }

  if (encryptionKeyName.has_value() && encryptionKeyName.value().empty()) {
    encryptionKeyName = std::nullopt;
  }
  ctx.catalogue.TapePool()->createTapePool(ctx.identity, name, vo, ptn, encryptionKeyName, supply_list, comment);
}

void changeTapePool(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& name = opts.getRequired(OptionString::TAPE_POOL);
  auto vo = opts.getOptional(OptionString::VO);
  auto ptn = opts.getOptional(OptionUInt64::PARTIAL_TAPES_NUMBER);
  auto comment = opts.getOptional(OptionString::COMMENT);
  auto encryptionKeyName = opts.getOptional(OptionString::ENCRYPTION_KEY_NAME);
  auto supply = opts.getOptional(OptionString::SUPPLY);

  if (comment.has_value()) {
    ctx.catalogue.TapePool()->modifyTapePoolComment(ctx.identity, name, comment.value());
  }
  if (vo.has_value()) {
    ctx.catalogue.TapePool()->modifyTapePoolVo(ctx.identity, name, vo.value());
  }
  if (ptn.has_value()) {
    ctx.catalogue.TapePool()->modifyTapePoolNbPartialTapes(ctx.identity, name, ptn.value());
  }
  if (encryptionKeyName.has_value()) {
    ctx.catalogue.TapePool()->setTapePoolEncryption(ctx.identity, name, encryptionKeyName.value());
  }
  if (supply.has_value()) {
    ctx.catalogue.TapePool()->modifyTapePoolSupply(ctx.identity,
                                                   name,
                                                   cta::utils::commaSeparatedStringToVector(supply.value()));
  }
}

void removeTapePool(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& name = opts.getRequired(OptionString::TAPE_POOL);

  ctx.catalogue.TapePool()->deleteTapePool(name);
}

}  // namespace cta::admin_api::commands::sync::handlers
