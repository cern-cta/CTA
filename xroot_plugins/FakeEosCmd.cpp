/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/make_unique.hpp"
#include "xroot_plugins/FakeEosCmd.hpp"
#include "xroot_plugins/FakeEosCmdLineArgs.hpp"
#include "xroot_plugins/messages/notification.pb.h"

#include <iostream>
#include <stdint.h>
#include <string>
#include <XrdCl/XrdClFileSystem.hh>

namespace cta {
namespace xroot_plugins {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
FakeEosCmd::FakeEosCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream):
CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
FakeEosCmd::~FakeEosCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int FakeEosCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const FakeEosCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  const std::string protocol = "xroot";
  const std::string fsUrl = protocol + ":" + "//" + cmdLineArgs.ctaHost + ":" + std::to_string(cmdLineArgs.ctaPort);
  XrdCl::FileSystem fs(fsUrl, false);

  eos::wfe::notification notification;
  notification.mutable_wf()->set_event("notification_workflow_event");
  notification.mutable_wf()->set_queue("notification_workflow_queue");
  notification.mutable_wf()->set_wfname("notification_workflow_wfname");
  notification.mutable_wf()->set_vpath("notification_workflow_vpath");
  notification.mutable_wf()->mutable_instance()->set_name("notification_instance_name");
  notification.mutable_wf()->mutable_instance()->set_url("notification_instance_url");
  notification.mutable_wf()->set_timestamp(1100);
  notification.set_turl("notification_turl");
  notification.mutable_cli()->mutable_user()->set_n(1111);
  notification.mutable_cli()->mutable_user()->set_name("notification_cli_user_name");
  notification.mutable_cli()->mutable_sec()->set_host("notification_cli_sec_host");
  notification.mutable_cli()->mutable_sec()->set_app("notification_cli_sec_app");
  notification.mutable_cli()->mutable_sec()->set_name("notification_cli_sec_name");
  notification.mutable_cli()->mutable_sec()->set_prot("notification_cli_sec_prot");
  notification.mutable_cli()->mutable_sec()->set_grps("notification_cli_sec_grps");
  notification.mutable_file()->set_fid(1122);
  notification.mutable_file()->set_pid(1133);
  notification.mutable_file()->mutable_ctime()->set_sec(1144);
  notification.mutable_file()->mutable_ctime()->set_nsec(1155);
  notification.mutable_file()->mutable_mtime()->set_sec(1166);
  notification.mutable_file()->mutable_mtime()->set_nsec(1177);
  notification.mutable_file()->mutable_btime()->set_sec(1188);
  notification.mutable_file()->mutable_btime()->set_nsec(1199);
  notification.mutable_file()->mutable_ttime()->set_sec(2200);
  notification.mutable_file()->mutable_ttime()->set_nsec(2211);
  notification.mutable_file()->mutable_owner()->set_n(2222);
  notification.mutable_file()->mutable_owner()->set_name("notification_file_owner_name");
  notification.mutable_file()->set_size(2233);
  notification.mutable_file()->mutable_cks()->set_value("notification_file_cks_value");
  notification.mutable_file()->mutable_cks()->set_name("notification_file_cks_name");
  notification.mutable_file()->set_mode(244);
  notification.mutable_file()->set_lpath("notification_file_lpath");
  (*notification.mutable_file()->mutable_xattr())["notification_file_xattr1"] = "file_xattr1_value";
  (*notification.mutable_file()->mutable_xattr())["notification_file_xattr2"] = "file_xattr2_value";
  notification.mutable_directory()->set_fid(1122);
  notification.mutable_directory()->set_pid(1133);
  notification.mutable_directory()->mutable_ctime()->set_sec(1144);
  notification.mutable_directory()->mutable_ctime()->set_nsec(1155);
  notification.mutable_directory()->mutable_mtime()->set_sec(1166);
  notification.mutable_directory()->mutable_mtime()->set_nsec(1177);
  notification.mutable_directory()->mutable_btime()->set_sec(1188);
  notification.mutable_directory()->mutable_btime()->set_nsec(1199);
  notification.mutable_directory()->mutable_ttime()->set_sec(2200);
  notification.mutable_directory()->mutable_ttime()->set_nsec(2211);
  notification.mutable_directory()->mutable_owner()->set_n(2222);
  notification.mutable_directory()->mutable_owner()->set_name("notification_directory");
  notification.mutable_directory()->set_size(2233);
  notification.mutable_directory()->mutable_cks()->set_value("notification_directory_cks_value");
  notification.mutable_directory()->mutable_cks()->set_name("notification_directory_cks_name");
  notification.mutable_directory()->set_mode(2244);
  notification.mutable_directory()->set_lpath("notification_directory_lpath");
  (*notification.mutable_directory()->mutable_xattr())["notification_directory_attr1"] = "directory_xattr1_value";
  (*notification.mutable_directory()->mutable_xattr())["notification_directory_attr2"] = "directory_xattr2_value";

  XrdCl::Buffer arg(cmdLineArgs.queryArg.size());
  arg.FromString(cmdLineArgs.queryArg);
  XrdCl::Buffer *response = nullptr;
  const XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);
  std::unique_ptr<XrdCl::Buffer> smartResponse(response);

  std::cout << "status.ToStr()=" << status.ToStr() << std::endl;
  std::cout << "status.IsError()=" << (status.IsError() ? "true" : "false") << std::endl;
  std::cout << "status.IsFatal()=" << (status.IsFatal() ? "true" : "false") << std::endl;
  std::cout << "status.IsOK()=" << (status.IsOK() ? "true" : "false") << std::endl;
  if(nullptr != smartResponse.get()) {
    std::cout << "response->GetSize()=" << response->GetSize() << std::endl;
    std::cout << "response->ToString()=" << response->ToString() << std::endl;
  }
  return 0;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void FakeEosCmd::printUsage(std::ostream &os) {
  FakeEosCmdLineArgs::printUsage(os);
}

} // namespace xroot_plugins
} // namespace cta
