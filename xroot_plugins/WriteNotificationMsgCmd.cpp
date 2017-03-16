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
#include "eos/messages/eos_messages.pb.h"
#include "xroot_plugins/WriteNotificationMsgCmd.hpp"
#include "xroot_plugins/WriteNotificationMsgCmdLineArgs.hpp"

#include <fstream>
#include <iostream>
#include <stdint.h>
#include <string>

#include <google/protobuf/util/json_util.h>

namespace cta {
namespace xroot_plugins {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
WriteNotificationMsgCmd::WriteNotificationMsgCmd(std::istream &inStream, std::ostream &outStream, std::ostream &errStream):
CmdLineTool(inStream, outStream, errStream) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
WriteNotificationMsgCmd::~WriteNotificationMsgCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int WriteNotificationMsgCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const WriteNotificationMsgCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  eos::wfe::Wrapper wrapper;
  wrapper.set_type(eos::wfe::Wrapper::NOTIFICATION);
  wrapper.mutable_notification()->mutable_wf()->set_event("notification_workflow_event");
  wrapper.mutable_notification()->mutable_wf()->set_queue("notification_workflow_queue");
  wrapper.mutable_notification()->mutable_wf()->set_wfname("notification_workflow_wfname");
  wrapper.mutable_notification()->mutable_wf()->set_vpath("notification_workflow_vpath");
  wrapper.mutable_notification()->mutable_wf()->mutable_instance()->set_name("notification_instance_name");
  wrapper.mutable_notification()->mutable_wf()->mutable_instance()->set_url("notification_instance_url");
  wrapper.mutable_notification()->mutable_wf()->set_timestamp(1100);
  wrapper.mutable_notification()->set_turl("notification_turl");
  wrapper.mutable_notification()->mutable_cli()->mutable_user()->set_n(1111);
  wrapper.mutable_notification()->mutable_cli()->mutable_user()->set_name("notification_cli_user_name");
  wrapper.mutable_notification()->mutable_cli()->mutable_sec()->set_host("notification_cli_sec_host");
  wrapper.mutable_notification()->mutable_cli()->mutable_sec()->set_app("notification_cli_sec_app");
  wrapper.mutable_notification()->mutable_cli()->mutable_sec()->set_name("notification_cli_sec_name");
  wrapper.mutable_notification()->mutable_cli()->mutable_sec()->set_prot("notification_cli_sec_prot");
  wrapper.mutable_notification()->mutable_cli()->mutable_sec()->set_grps("notification_cli_sec_grps");
  wrapper.mutable_notification()->mutable_file()->set_fid(1122);
  wrapper.mutable_notification()->mutable_file()->set_pid(1133);
  wrapper.mutable_notification()->mutable_file()->mutable_ctime()->set_sec(1144);
  wrapper.mutable_notification()->mutable_file()->mutable_ctime()->set_nsec(1155);
  wrapper.mutable_notification()->mutable_file()->mutable_mtime()->set_sec(1166);
  wrapper.mutable_notification()->mutable_file()->mutable_mtime()->set_nsec(1177);
  wrapper.mutable_notification()->mutable_file()->mutable_btime()->set_sec(1188);
  wrapper.mutable_notification()->mutable_file()->mutable_btime()->set_nsec(1199);
  wrapper.mutable_notification()->mutable_file()->mutable_ttime()->set_sec(2200);
  wrapper.mutable_notification()->mutable_file()->mutable_ttime()->set_nsec(2211);
  wrapper.mutable_notification()->mutable_file()->mutable_owner()->set_n(2222);
  wrapper.mutable_notification()->mutable_file()->mutable_owner()->set_name("notification_file_owner_name");
  wrapper.mutable_notification()->mutable_file()->set_size(2233);
  wrapper.mutable_notification()->mutable_file()->mutable_cks()->set_value("notification_file_cks_value");
  wrapper.mutable_notification()->mutable_file()->mutable_cks()->set_name("notification_file_cks_name");
  wrapper.mutable_notification()->mutable_file()->set_mode(244);
  wrapper.mutable_notification()->mutable_file()->set_lpath("notification_file_lpath");
  (*wrapper.mutable_notification()->mutable_file()->mutable_xattr())["notification_file_xattr1"] = "file_xattr1_value";
  (*wrapper.mutable_notification()->mutable_file()->mutable_xattr())["notification_file_xattr2"] = "file_xattr2_value";
  wrapper.mutable_notification()->mutable_directory()->set_fid(1122);
  wrapper.mutable_notification()->mutable_directory()->set_pid(1133);
  wrapper.mutable_notification()->mutable_directory()->mutable_ctime()->set_sec(1144);
  wrapper.mutable_notification()->mutable_directory()->mutable_ctime()->set_nsec(1155);
  wrapper.mutable_notification()->mutable_directory()->mutable_mtime()->set_sec(1166);
  wrapper.mutable_notification()->mutable_directory()->mutable_mtime()->set_nsec(1177);
  wrapper.mutable_notification()->mutable_directory()->mutable_btime()->set_sec(1188);
  wrapper.mutable_notification()->mutable_directory()->mutable_btime()->set_nsec(1199);
  wrapper.mutable_notification()->mutable_directory()->mutable_ttime()->set_sec(2200);
  wrapper.mutable_notification()->mutable_directory()->mutable_ttime()->set_nsec(2211);
  wrapper.mutable_notification()->mutable_directory()->mutable_owner()->set_n(2222);
  wrapper.mutable_notification()->mutable_directory()->mutable_owner()->set_name("notification_directory");
  wrapper.mutable_notification()->mutable_directory()->set_size(2233);
  wrapper.mutable_notification()->mutable_directory()->mutable_cks()->set_value("notification_directory_cks_value");
  wrapper.mutable_notification()->mutable_directory()->mutable_cks()->set_name("notification_directory_cks_name");
  wrapper.mutable_notification()->mutable_directory()->set_mode(2244);
  wrapper.mutable_notification()->mutable_directory()->set_lpath("notification_directory_lpath");
  (*wrapper.mutable_notification()->mutable_directory()->mutable_xattr())["notification_directory_attr1"] = "directory_xattr1_value";
  (*wrapper.mutable_notification()->mutable_directory()->mutable_xattr())["notification_directory_attr2"] = "directory_xattr2_value";

  if(cmdLineArgs.writeJsonToStdOut) {
    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = true;
    options.always_print_primitive_fields = true;
    std::string jsonNotification;
    google::protobuf::util::MessageToJsonString(wrapper, &jsonNotification, options);
    std::cout << jsonNotification;
    return 0;
  }

  std::ofstream messageFileStream(cmdLineArgs.filename, std::ios_base::binary);
  if(!messageFileStream) {
    m_err << "Failed to open " << cmdLineArgs.filename << std::endl;
    return 1;
  }

  wrapper.SerializeToOstream(&messageFileStream);

  return 0;
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void WriteNotificationMsgCmd::printUsage(std::ostream &os) {
  WriteNotificationMsgCmdLineArgs::printUsage(os);
}

} // namespace xroot_plugins
} // namespace cta
