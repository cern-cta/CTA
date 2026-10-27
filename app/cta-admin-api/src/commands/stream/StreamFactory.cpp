/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/stream/StreamFactory.hpp"

#include "commands/AdminCmdOptions.hpp"
#include "commands/stream/VectorResponseStream.hpp"
#include "commands/stream/converters/StreamConverters.hpp"
#include "commands/stream/scheduling/DriveLsResponseStream.hpp"
#include "commands/stream/scheduling/FailedRequestLsResponseStream.hpp"
#include "commands/stream/scheduling/RepackLsResponseStream.hpp"
#include "commands/stream/scheduling/ShowQueuesResponseStream.hpp"
#include "commands/stream/tapes/RecycleTapeFileLsResponseStream.hpp"
#include "commands/stream/tapes/TapeFileLsResponseStream.hpp"
#include "commands/stream/tapes/TapeLsResponseStream.hpp"
#include "common/exception/UserError.hpp"

#include <functional>
#include <map>
#include <utility>
#include <vector>

namespace cta::admin_api::commands::stream {

namespace {

using cta::admin::AdminCmd;
using cta::admin::HeaderType;

/// An admin command: the <Cmd, SubCmd> pair, as in the tables of the cta-admin parser
using CmdKey = std::pair<AdminCmd::Cmd, AdminCmd::SubCmd>;

using Creator = std::function<std::unique_ptr<CtaAdminResponseStream>(const StreamContext&, const admin::AdminCmd&)>;

/// Builds a stream of already loaded items, which are converted to their protobuf representation by toProto()
template<class Item>
std::unique_ptr<CtaAdminResponseStream>
vectorStream(HeaderType headerType, std::vector<Item>&& items, const std::string& instanceName) {
  return std::make_unique<VectorResponseStream<Item>>(headerType, instanceName, std::move(items));
}

/// The streamed admin commands and their corresponding request handlers
///
/// `vectorStream(...)` handles most of them, with others providing their custom handling code
const std::map<CmdKey, Creator> STREAM_CMD_TABLE = {
  {{AdminCmd::CMD_VERSION, AdminCmd::SUBCMD_NONE},
   [](const StreamContext& ctx, const admin::AdminCmd& cmd) {
     return vectorStream(HeaderType::VERSION_CMD,
                         std::vector {loadVersion(ctx.catalogue, ctx.scheduler, ctx.catalogueConnString, cmd)},
                         ctx.instanceName);
   }},
  {{AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd&) {
     return vectorStream(HeaderType::ADMIN_LS, ctx.catalogue.AdminUser()->getAdminUsers(), ctx.instanceName);
   }},
  {{AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd&) {
     return vectorStream(HeaderType::ARCHIVEROUTE_LS,
                         ctx.catalogue.ArchiveRoute()->getArchiveRoutes(),
                         ctx.instanceName);
   }},
  {{AdminCmd::CMD_ACTIVITYMOUNTRULE, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd&) {
     return vectorStream(HeaderType::ACTIVITYMOUNTRULE_LS,
                         ctx.catalogue.RequesterActivityMountRule()->getRequesterActivityMountRules(),
                         ctx.instanceName);
   }},
  {{AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd&) {
     return vectorStream(HeaderType::GROUPMOUNTRULE_LS,
                         ctx.catalogue.RequesterGroupMountRule()->getRequesterGroupMountRules(),
                         ctx.instanceName);
   }},
  {{AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd&) {
     return vectorStream(HeaderType::REQUESTERMOUNTRULE_LS,
                         ctx.catalogue.RequesterMountRule()->getRequesterMountRules(),
                         ctx.instanceName);
   }},
  {{AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd&) {
     return vectorStream(HeaderType::MOUNTPOLICY_LS, ctx.catalogue.MountPolicy()->getMountPolicies(), ctx.instanceName);
   }},
  {{AdminCmd::CMD_DISKINSTANCE, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd&) {
     return vectorStream(HeaderType::DISKINSTANCE_LS,
                         ctx.catalogue.DiskInstance()->getAllDiskInstances(),
                         ctx.instanceName);
   }},
  {{AdminCmd::CMD_DISKINSTANCESPACE, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd&) {
     return vectorStream(HeaderType::DISKINSTANCESPACE_LS,
                         ctx.catalogue.DiskInstanceSpace()->getAllDiskInstanceSpaces(),
                         ctx.instanceName);
   }},
  {{AdminCmd::CMD_DISKSYSTEM, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd&) {
     return vectorStream(HeaderType::DISKSYSTEM_LS, ctx.catalogue.DiskSystem()->getAllDiskSystems(), ctx.instanceName);
   }},
  {{AdminCmd::CMD_VIRTUALORGANIZATION, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd&) {
     return vectorStream(HeaderType::VIRTUALORGANIZATION_LS,
                         ctx.catalogue.VO()->getVirtualOrganizations(),
                         ctx.instanceName);
   }},
  {{AdminCmd::CMD_MEDIATYPE, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd&) {
     return vectorStream(HeaderType::MEDIATYPE_LS, ctx.catalogue.MediaType()->getMediaTypes(), ctx.instanceName);
   }},
  {{AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd& cmd) {
     return vectorStream(HeaderType::STORAGECLASS_LS, loadStorageClasses(ctx.catalogue, cmd), ctx.instanceName);
   }},
  {{AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd& cmd) {
     return vectorStream(HeaderType::LOGICALLIBRARY_LS, loadLogicalLibraries(ctx.catalogue, cmd), ctx.instanceName);
   }},
  {{AdminCmd::CMD_PHYSICALLIBRARY, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd&) {
     return vectorStream(HeaderType::PHYSICALLIBRARY_LS,
                         ctx.catalogue.PhysicalLibrary()->getPhysicalLibraries(),
                         ctx.instanceName);
   }},
  {{AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd& cmd) {
     return vectorStream(HeaderType::TAPEPOOL_LS, loadTapePools(ctx.catalogue, cmd), ctx.instanceName);
   }},

  // Streams of their own
  {{AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd& cmd) -> std::unique_ptr<CtaAdminResponseStream> {
     return std::make_unique<TapeLsResponseStream>(ctx.catalogue,
                                                   ctx.instanceName,
                                                   cmd,
                                                   ctx.missingFileCopiesMinAgeSecs,
                                                   HeaderType::TAPE_LS);
   }},
  {{AdminCmd::CMD_TAPEFILE, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd& cmd) -> std::unique_ptr<CtaAdminResponseStream> {
     return std::make_unique<TapeFileLsResponseStream>(ctx.catalogue, ctx.instanceName, cmd, HeaderType::TAPEFILE_LS);
   }},
  {{AdminCmd::CMD_RECYCLETAPEFILE, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd& cmd) -> std::unique_ptr<CtaAdminResponseStream> {
     return std::make_unique<RecycleTapeFileLsResponseStream>(ctx.catalogue,
                                                              ctx.instanceName,
                                                              cmd,
                                                              HeaderType::RECYLETAPEFILE_LS);
   }},
  {{AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd& cmd) -> std::unique_ptr<CtaAdminResponseStream> {
     return std::make_unique<DriveLsResponseStream>(ctx.catalogue,
                                                    ctx.scheduler,
                                                    ctx.instanceName,
                                                    cmd,
                                                    ctx.lc,
                                                    HeaderType::DRIVE_LS);
   }},
  {{AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd& cmd) -> std::unique_ptr<CtaAdminResponseStream> {
     return std::make_unique<RepackLsResponseStream>(ctx.catalogue,
                                                     ctx.scheduler,
                                                     ctx.instanceName,
                                                     cmd,
                                                     HeaderType::REPACK_LS);
   }},
  {{AdminCmd::CMD_SHOWQUEUES, AdminCmd::SUBCMD_NONE},
   [](const StreamContext& ctx, const admin::AdminCmd&) -> std::unique_ptr<CtaAdminResponseStream> {
     return std::make_unique<ShowQueuesResponseStream>(ctx.scheduler, ctx.instanceName, ctx.lc, HeaderType::SHOWQUEUES);
   }},
  {{AdminCmd::CMD_FAILEDREQUEST, AdminCmd::SUBCMD_LS},
   [](const StreamContext& ctx, const admin::AdminCmd& cmd) -> std::unique_ptr<CtaAdminResponseStream> {
     // The stream reports summary records instead of item records when --summary is given,
     // and those need their own header
     const bool isSummary = commands::AdminCmdOptions(cmd).hasFlag(admin::OptionBoolean::SUMMARY);
     return std::make_unique<FailedRequestLsResponseStream>(ctx.scheduler,
                                                            ctx.instanceName,
                                                            cmd,
                                                            ctx.schedDb,
                                                            ctx.lc,
                                                            isSummary ? HeaderType::FAILEDREQUEST_LS_SUMMARY :
                                                                        HeaderType::FAILEDREQUEST_LS);
   }},
};

}  // namespace

std::unique_ptr<CtaAdminResponseStream> createStream(const StreamContext& context, const admin::AdminCmd& adminCmd) {
  // Same rule as for the commands answered with one response
  if (isCommandDisabled(context.adminCommandMode, adminCmd)) {
    throw DisabledAdminCmd(adminCmd);
  }

  const auto it = STREAM_CMD_TABLE.find(CmdKey(adminCmd.cmd(), adminCmd.subcmd()));
  if (it == STREAM_CMD_TABLE.end()) {
    return nullptr;
  }
  return it->second(context, adminCmd);
}

}  // namespace cta::admin_api::commands::stream
