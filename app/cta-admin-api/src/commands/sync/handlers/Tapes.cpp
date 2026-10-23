/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "catalogue/TapeFileSearchCriteria.hpp"
#include "commands/sync/handlers/Handlers.hpp"
#include "common/exception/UserError.hpp"
#include "common/utils/utils.hpp"

#include <list>

namespace cta::admin_api::commands::sync::handlers {

using admin::OptionBoolean;
using admin::OptionString;
using admin::OptionStrList;
using admin::OptionUInt64;

void addTape(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& vid = opts.getRequired(OptionString::VID);
  auto& mediaType = opts.getRequired(OptionString::MEDIA_TYPE);
  auto& vendor = opts.getRequired(OptionString::VENDOR);
  auto& logicallibrary = opts.getRequired(OptionString::LOGICAL_LIBRARY);
  auto& tapepool = opts.getRequired(OptionString::TAPE_POOL);
  auto& full = opts.getRequired(OptionBoolean::FULL);
  auto purchaseOrder = opts.getOptional(OptionString::MEDIA_PURCHASE_ORDER_NUMBER);
  auto state = opts.getOptional(OptionString::STATE);
  auto stateReason = opts.getOptional(OptionString::REASON);
  auto comment = opts.getOptional(OptionString::COMMENT);

  catalogue::CreateTapeAttributes tape;
  tape.vid = vid;
  tape.mediaType = mediaType;
  tape.vendor = vendor;
  tape.logicalLibraryName = logicallibrary;
  tape.tapePoolName = tapepool;
  tape.full = full;
  tape.purchaseOrder = purchaseOrder;
  tape.comment = comment.value_or("");
  if (!state.has_value()) {
    // By default, the state of the tape will be ACTIVE
    tape.state = common::dataStructures::Tape::ACTIVE;
  } else {
    // State has been provided by the user, assign it. Will throw an exception if the state provided does not exist.
    tape.state = common::dataStructures::Tape::stringToState(state.value(), true);
  }
  tape.stateReason = stateReason;
  ctx.catalogue.Tape()->createTape(ctx.identity, tape);
}

void changeTape(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& vid = opts.getRequired(OptionString::VID);
  auto mediaType = opts.getOptional(OptionString::MEDIA_TYPE);
  auto vendor = opts.getOptional(OptionString::VENDOR);
  auto logicallibrary = opts.getOptional(OptionString::LOGICAL_LIBRARY);
  auto tapepool = opts.getOptional(OptionString::TAPE_POOL);
  auto comment = opts.getOptional(OptionString::COMMENT);
  auto encryptionkeyName = opts.getOptional(OptionString::ENCRYPTION_KEY_NAME);
  auto purchaseOrder = opts.getOptional(OptionString::MEDIA_PURCHASE_ORDER_NUMBER);
  auto full = opts.getOptional(OptionBoolean::FULL);
  auto state = opts.getOptional(OptionString::STATE);
  auto stateReason = opts.getOptional(OptionString::REASON);
  auto dirty = opts.getOptional(OptionBoolean::DIRTY_BIT);
  auto verificationStatus = opts.getOptional(OptionString::VERIFICATION_STATUS);

  if (mediaType.has_value()) {
    if (ctx.catalogue.Tape()->getNbFilesOnTape(vid) != 0) {
      throw cta::exception::UserError("Unable to modify media type of tape " + vid + " because it contains files");
    }
    ctx.catalogue.Tape()->modifyTapeMediaType(ctx.identity, vid, mediaType.value());
  }
  if (vendor.has_value()) {
    ctx.catalogue.Tape()->modifyTapeVendor(ctx.identity, vid, vendor.value());
  }
  if (logicallibrary.has_value()) {
    ctx.catalogue.Tape()->modifyTapeLogicalLibraryName(ctx.identity, vid, logicallibrary.value());
  }
  if (tapepool.has_value()) {
    ctx.catalogue.Tape()->modifyTapeTapePoolName(ctx.identity, vid, tapepool.value());
  }
  if (comment.has_value()) {
    if (comment.value().empty()) {
      // If the comment is an empty string, the user meant to delete it
      comment = std::nullopt;
    }
    ctx.catalogue.Tape()->modifyTapeComment(ctx.identity, vid, comment);
  }
  if (encryptionkeyName.has_value()) {
    try {
      ctx.catalogue.Tape()->modifyTapeEncryptionKeyName(ctx.identity, vid, encryptionkeyName.value());
    } catch (const cta::catalogue::UserSpecifiedANonEmptyTapeForEncryptionKey&) {
      throw cta::exception::UserError("ERROR: forbidden to set an encryption key on a non empty tape.");
    }
    ctx.catalogue.Tape()->modifyTapeEncryptionKeyName(ctx.identity, vid, encryptionkeyName.value());
  }
  if (purchaseOrder.has_value()) {
    ctx.catalogue.Tape()->modifyPurchaseOrder(ctx.identity, vid, purchaseOrder.value());
  }
  if (full.has_value()) {
    ctx.catalogue.Tape()->setTapeFull(ctx.identity, vid, full.value());
  }
  if (state.has_value()) {
    auto stateEnumValue = common::dataStructures::Tape::stringToState(state.value(), true);
    auto operatingMode = Scheduler::OperatingMode::ALL;

    // The tape state changes allowed depend on the scheduler DB this admin API is meant for
    if (ctx.policies.admin_mode == AdminCmdMode::REPACK) {
      operatingMode = Scheduler::OperatingMode::REPACK;
    }
    if (ctx.policies.admin_mode == AdminCmdMode::NO_REPACK) {
      operatingMode = Scheduler::OperatingMode::USER;
    }

    ctx.scheduler.triggerTapeStateChange(ctx.identity, vid, stateEnumValue, stateReason, ctx.lc, operatingMode);
  }
  if (dirty.has_value()) {
    ctx.catalogue.Tape()->setTapeDirty(ctx.identity, vid, dirty.value());
  }
  if (verificationStatus.has_value()) {
    ctx.catalogue.Tape()->modifyTapeVerificationStatus(ctx.identity, vid, verificationStatus.value());
  }
}

void removeTape(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& vid = opts.getRequired(OptionString::VID);

  if (ctx.scheduler.isBeingRepacked(vid)) {
    throw exception::UserError("Cannot delete tape " + vid + " because there is a repack for that tape");
  }
  ctx.catalogue.Tape()->deleteTape(vid);
}

void reclaimTape(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& vid = opts.getRequired(OptionString::VID);

  ctx.catalogue.Tape()->reclaimTape(ctx.identity, vid, ctx.policies.tapes.recycle_log_quarantine_secs, ctx.lc);
}

void changeTapeFile(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  try {
    const auto& newStorageClassName = opts.getRequired(OptionString::STORAGE_CLASS);

    catalogue::TapeFileSearchCriteria searchCriteria;

    searchCriteria.diskFileIds = opts.getOptional(OptionStrList::FILE_ID);

    const auto diskFileId = opts.getAndValidateDiskFileIdOptional();

    if (diskFileId) {
      if (!searchCriteria.diskFileIds) {
        searchCriteria.diskFileIds.emplace();
      }

      searchCriteria.diskFileIds->push_back(*diskFileId);
    }

    searchCriteria.diskInstance = opts.getOptional(OptionString::INSTANCE);

    searchCriteria.archiveFileId = opts.getOptional(OptionUInt64::ARCHIVE_FILE_ID);

    const bool hasDiskFileIds = searchCriteria.diskFileIds && !searchCriteria.diskFileIds->empty();

    if (!hasDiskFileIds && !searchCriteria.archiveFileId) {
      throw exception::UserError("Must specify at least one of the following search options: "
                                 "fxidfile, fxid, diskfileid or id");
    }

    std::list<uint64_t> archiveFileIds;

    {
      auto archiveFiles = ctx.catalogue.ArchiveFile()->getArchiveFilesItor(searchCriteria);

      while (archiveFiles.hasMore()) {
        archiveFileIds.push_back(archiveFiles.next().archiveFileID);
      }
    }

    if (archiveFileIds.empty()) {
      throw exception::UserError("No archive files match the specified search options");
    }

    ctx.catalogue.ArchiveFile()->modifyArchiveFileStorageClassId(archiveFileIds, newStorageClassName);
  } catch (exception::UserError& ue) {
    response.set_message_txt(ue.getMessage().str());
    response.set_type(xrd::Response::RSP_ERR_USER);
  }
}

void removeTapeFile(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  auto& vid = opts.getRequired(OptionString::VID);
  auto& reason = opts.getRequired(OptionString::REASON);
  auto archiveFileId = opts.getOptional(OptionUInt64::ARCHIVE_FILE_ID);
  auto instance = opts.getOptional(OptionString::INSTANCE);
  auto diskFileIdStr = opts.getAndValidateDiskFileIdOptional();

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.vid = vid;

  if (archiveFileId.has_value()) {
    searchCriteria.archiveFileId = archiveFileId.value();
  }

  if (diskFileIdStr.has_value()) {
    searchCriteria.diskFileIds = std::vector<std::string>();
    searchCriteria.diskFileIds.value().push_back(diskFileIdStr.value());
  }

  if (instance.has_value()) {
    searchCriteria.diskInstance = instance.value();
  }

  auto archiveFile = ctx.catalogue.ArchiveFile()->getArchiveFileCopyForDeletion(searchCriteria);

  ctx.catalogue.TapeFile()->deleteTapeFileCopy(archiveFile, reason);
}

void restoreRecycleTapeFile(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  bool has_any = false;
  catalogue::RecycleTapeFileSearchCriteria searchCriteria;

  searchCriteria.vid = opts.getOptional(OptionString::VID, &has_any);

  const auto diskFileIdStr = opts.getAndValidateDiskFileIdOptional();
  if (!diskFileIdStr) {
    throw exception::UserError("Must specify at least one of the following search options: fxid, diskfileid");
  }

  // Disk instance on its own does not give a valid set of search criteria (no &has_any)
  searchCriteria.diskInstance = opts.getOptional(OptionString::INSTANCE);
  searchCriteria.archiveFileId = opts.getOptional(OptionUInt64::ARCHIVE_FILE_ID, &has_any);
  // Copy number on its own does not give a valid set of search criteria (no &has_any)
  searchCriteria.copynb = opts.getOptional(OptionUInt64::COPY_NUMBER);

  if (!has_any) {
    throw exception::UserError(
      "Must specify at least one of the following search options: vid, fxid, fxidfile or archiveFileId");
  }
  ctx.catalogue.FileRecycleLog()->restoreFileInRecycleLog(searchCriteria, diskFileIdStr.value());
}

void changeArchiveFile(const SyncCmdContext& ctx, const AdminCmdOptions& opts, xrd::Response& response) {
  try {
    std::optional<std::string> newStorageClassName = opts.getOptional(OptionString::STORAGE_CLASS);

    std::optional<std::string> diskFileIdStr = opts.getAndValidateDiskFileIdOptional();
    std::optional<std::string> diskInstance = opts.getOptional(OptionString::DISK_INSTANCE);

    auto archiveFileIds = opts.getRequired(OptionStrList::ARCHIVE_FILE_ID);

    // call is from cta-change-storageclass
    if (newStorageClassName) {
      std::list<uint64_t> archiveFileIdsUint64;

      for (const auto& id : archiveFileIds) {
        archiveFileIdsUint64.push_back(utils::toUint64(id));
      }
      ctx.catalogue.ArchiveFile()->modifyArchiveFileStorageClassId(archiveFileIdsUint64, *newStorageClassName);
    }
    // call is from cta-eos-namespace-inject
    else if (diskFileIdStr && diskInstance) {
      if (archiveFileIds.size() != 1) {
        throw exception::UserError(
          "Exactly one archive file ID must be specified when using Disk File ID and Disk Instance");
      }

      ctx.catalogue.ArchiveFile()->modifyArchiveFileFxIdAndDiskInstance(utils::toUint64(archiveFileIds.front()),
                                                                        *diskFileIdStr,
                                                                        *diskInstance);
    } else {
      throw exception::UserError("Must specify either Storage Class or Disk File ID and Disk Instance");
    }
  } catch (exception::UserError& ue) {
    response.set_message_txt(ue.getMessage().str());
    response.set_type(xrd::Response::RSP_ERR_USER);
  }
}

}  // namespace cta::admin_api::commands::sync::handlers
