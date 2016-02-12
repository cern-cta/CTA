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

#include "cmdline/CTACmd.hpp"
#include "common/admin/AdminHost.hpp"
#include "common/admin/AdminUser.hpp"
#include "common/archiveNS/ArchiveDirIterator.hpp"
#include "common/archiveNS/StorageClass.hpp"
#include "common/archiveNS/Tape.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "common/exception/Exception.hpp"
#include "common/SecurityIdentity.hpp"
#include "common/TapePool.hpp"
#include "common/UserIdentity.hpp"
#include "scheduler/ArchiveToTapeCopyRequest.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/RetrieveRequestDump.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "xroot_plugins/XrdCtaFile.hpp"

#include "XrdSec/XrdSecEntity.hh"

#include <cryptopp/base64.h>
#include <cryptopp/osrng.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <pwd.h>
#include <sstream>
#include <string>

namespace cta { namespace xrootPlugins {

//------------------------------------------------------------------------------
// checkClient
//------------------------------------------------------------------------------
cta::common::dataStructures::SecurityIdentity XrdProFile::checkClient(const XrdSecEntity *client) {
// TEMPORARILY commented out host check for demo purposes:
//  if(!client || !client->host || strncmp(client->host, "localhost", 9))
//  {
//    throw cta::exception::Exception(std::string(__FUNCTION__)+": [ERROR] operation possible only from localhost");
//  }
  cta::common::dataStructures::SecurityIdentity requester;
  struct passwd pwd;
  struct passwd *result;
  long bufsize = sysconf(_SC_GETPW_R_SIZE_MAX);
  if (bufsize == -1)
  {
    bufsize = 16384;
  }
  std::unique_ptr<char> buf((char *)malloc((size_t)bufsize));
  if(buf.get() == NULL)
  {
    throw cta::exception::Exception(std::string(__FUNCTION__)+": [ERROR] malloc of the buffer failed");
  }
  int rc = getpwnam_r(client->name, &pwd, buf.get(), bufsize, &result);
  if(result == NULL)
  {
    if (rc == 0)
    {
      throw cta::exception::Exception(std::string(__FUNCTION__)+": [ERROR] User "+client->name+" not found");
    }
    else
    {
      throw cta::exception::Exception(std::string(__FUNCTION__)+": [ERROR] getpwnam_r failed");
    }
  }
  std::cout << "Request received from client. Username: " << client->name <<
    " uid: " << pwd.pw_uid << " gid: " << pwd.pw_gid << std::endl;
  requester.setUid(pwd.pw_uid);
  requester.setGid(pwd.pw_gid);
  requester.setHost(client->host);
  return requester;
}

//------------------------------------------------------------------------------
// commandDispatcher
//------------------------------------------------------------------------------
void XrdProFile::dispatchCommand(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::string command(tokens[1]);
  
  if     ("bs"  == command || "bootstrap"            == command) {xCom_bootstrap(tokens, requester);}
  else if("ad"  == command || "admin"                == command) {xCom_admin(tokens, requester);}
  else if("ah"  == command || "adminhost"            == command) {xCom_adminhost(tokens, requester);}
  else if("tp"  == command || "tapepool"             == command) {xCom_tapepool(tokens, requester);}
  else if("ar"  == command || "archiveroute"         == command) {xCom_archiveroute(tokens, requester);}
  else if("ll"  == command || "logicallibrary"       == command) {xCom_logicallibrary(tokens, requester);}
  else if("ta"  == command || "tape"                 == command) {xCom_tape(tokens, requester);}
  else if("sc"  == command || "storageclass"         == command) {xCom_storageclass(tokens, requester);}
  else if("us"  == command || "user"                 == command) {xCom_user(tokens, requester);}
  else if("ug"  == command || "usergroup"            == command) {xCom_usergroup(tokens, requester);}
  else if("de"  == command || "dedication"           == command) {xCom_dedication(tokens, requester);}
  else if("re"  == command || "repack"               == command) {xCom_repack(tokens, requester);}
  else if("sh"  == command || "shrink"               == command) {xCom_shrink(tokens, requester);}
  else if("ve"  == command || "verify"               == command) {xCom_verify(tokens, requester);}
  else if("af"  == command || "archivefile"          == command) {xCom_archivefile(tokens, requester);}
  else if("te"  == command || "test"                 == command) {xCom_test(tokens, requester);}
  else if("dr"  == command || "drive"                == command) {xCom_drive(tokens, requester);}
  else if("rc"  == command || "reconcile"            == command) {xCom_reconcile(tokens, requester);}
  else if("lpa" == command || "listpendingarchives"  == command) {xCom_listpendingarchives(tokens, requester);}
  else if("lpr" == command || "listpendingretrieves" == command) {xCom_listpendingretrieves(tokens, requester);}
  else if("lds" == command || "listdrivestates"      == command) {xCom_listdrivestates(tokens, requester);}
  else if("a"   == command || "archive"              == command) {xCom_archive(tokens, requester);}
  else if("r"   == command || "retrieve"             == command) {xCom_retrieve(tokens, requester);}
  else if("da"  == command || "deletearchive"        == command) {xCom_deletearchive(tokens, requester);}
  else if("cr"  == command || "cancelretrieve"       == command) {xCom_cancelretrieve(tokens, requester);}
  else if("ufi" == command || "updatefileinfo"       == command) {xCom_updatefileinfo(tokens, requester);}
  else if("lsc" == command || "liststorageclass"     == command) {xCom_liststorageclass(tokens, requester);}
  
  else {m_data = getGenericHelp(tokens[0]);}
}

//------------------------------------------------------------------------------
// decode
//------------------------------------------------------------------------------
std::string XrdProFile::decode(const std::string msg) const {
  std::string ret;
  CryptoPP::StringSource ss1(msg, true, new CryptoPP::Base64Decoder(new CryptoPP::StringSink(ret)));
  return ret;
}

//------------------------------------------------------------------------------
// open
//------------------------------------------------------------------------------
int XrdProFile::open(const char *fileName, XrdSfsFileOpenMode openMode, mode_t createMode, const XrdSecEntity *client, const char *opaque) {
  try {
    const cta::common::dataStructures::SecurityIdentity requester = checkClient(client);

    if(!strlen(fileName)) { //this should never happen
      m_data = getGenericHelp("");
      return SFS_OK;
    }

    std::vector<std::string> tokens;
    std::stringstream ss(fileName+1); //let's skip the first slash which is always prepended since we are asking for an absolute path
    std::string item;
    while (std::getline(ss, item, '&')) {
      replaceAll(item, "_", "/"); 
      //need to add this because xroot removes consecutive slashes, and the 
      //cryptopp base64 algorithm may produce consecutive slashes. This is solved 
      //in cryptopp-5.6.3 (using Base64URLEncoder instead of Base64Encoder) but we 
      //currently have cryptopp-5.6.2. To be changed in the future...
      item = decode(item);
      tokens.push_back(item);
    }

    if(tokens.size() == 0) { //this should never happen
      m_data = getGenericHelp("");
      return SFS_OK;
    }
    if(tokens.size() < 2) {
      m_data = getGenericHelp(tokens[0]);
      return SFS_OK;
    }  
    dispatchCommand(tokens, requester);
    return SFS_OK;
  } catch (cta::exception::Exception &ex) {
    m_data = "[ERROR] CTA exception caught: ";
    m_data += ex.getMessageValue();
    m_data += "\n";
    return SFS_OK;
  } catch (std::exception &ex) {
    m_data = "[ERROR] Exception caught: ";
    m_data += ex.what();
    m_data += "\n";
    return SFS_OK;
  } catch (...) {
    m_data = "[ERROR] Unknown exception caught!";
    m_data += "\n";
    return SFS_OK;
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
int XrdProFile::close() {
  return SFS_OK;
}

//------------------------------------------------------------------------------
// fctl
//------------------------------------------------------------------------------
int XrdProFile::fctl(const int cmd, const char *args, XrdOucErrInfo &eInfo) {  
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// FName
//------------------------------------------------------------------------------
const char* XrdProFile::FName() {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return NULL;
}

//------------------------------------------------------------------------------
// getMmap
//------------------------------------------------------------------------------
int XrdProFile::getMmap(void **Addr, off_t &Size) {
  *Addr = const_cast<char *>(m_data.c_str());
  Size = m_data.length();
  return SFS_OK; //change to "return SFS_ERROR;" in case the read function below is wanted, in that case uncomment the lines in that function.
}

//------------------------------------------------------------------------------
// read
//------------------------------------------------------------------------------
XrdSfsXferSize XrdProFile::read(XrdSfsFileOffset offset, char *buffer, XrdSfsXferSize size) {
//  if((unsigned long)offset<m_data.length()) {
//    strncpy(buffer, m_data.c_str()+offset, size);
//    return m_data.length()-offset;
//  }
//  else {
//    return 0;
//  }
  error.setErrInfo(ENOTSUP, "Not supported.");
  return 0;
}

//------------------------------------------------------------------------------
// read
//------------------------------------------------------------------------------
XrdSfsXferSize XrdProFile::read(XrdSfsFileOffset offset, XrdSfsXferSize size) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return 0;
}

//------------------------------------------------------------------------------
// read
//------------------------------------------------------------------------------
XrdSfsXferSize XrdProFile::read(XrdSfsAio *aioparm) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return 0;
}

//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
XrdSfsXferSize XrdProFile::write(XrdSfsFileOffset offset, const char *buffer, XrdSfsXferSize size) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return 0;
}

//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
int XrdProFile::write(XrdSfsAio *aioparm) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// stat
//------------------------------------------------------------------------------
int XrdProFile::stat(struct stat *buf) {
  buf->st_size=m_data.length();
  return SFS_OK;
}

//------------------------------------------------------------------------------
// sync
//------------------------------------------------------------------------------
int XrdProFile::sync() {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// sync
//------------------------------------------------------------------------------
int XrdProFile::sync(XrdSfsAio *aiop) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// truncate
//------------------------------------------------------------------------------
int XrdProFile::truncate(XrdSfsFileOffset fsize) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// getCXinfo
//------------------------------------------------------------------------------
int XrdProFile::getCXinfo(char cxtype[4], int &cxrsz) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdProFile::XrdProFile(cta::Scheduler *scheduler, const char *user, int MonID): error(user, MonID), m_scheduler(scheduler), m_data("") {  
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdProFile::~XrdProFile() {  
}

//------------------------------------------------------------------------------
// replaceAll
//------------------------------------------------------------------------------
void XrdProFile::replaceAll(std::string& str, const std::string& from, const std::string& to) const {
  if(from.empty() || str.empty())
    return;
  size_t start_pos = 0;
  while((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos += to.length();
  }
}

//------------------------------------------------------------------------------
// getOptionValue
//------------------------------------------------------------------------------
std::string XrdProFile::getOptionValue(const std::vector<std::string> &tokens, const std::string& optionShortName, const std::string& optionLongName) {
  for(auto it=tokens.begin(); it!=tokens.end(); it++) {
    if(optionShortName == *it || optionLongName == *it) {
      auto it_next=it+1;
      if(it_next!=tokens.end() && it_next->find("-")!=0) {
        return *it_next;
      }
      else {
        return "";
      }
    }
  }
  return "";
}

//------------------------------------------------------------------------------
// hasOption
//------------------------------------------------------------------------------
bool XrdProFile::hasOption(const std::vector<std::string> &tokens, const std::string& optionShortName, const std::string& optionLongName) {
  for(auto it=tokens.begin(); it!=tokens.end(); it++) {
    if(optionShortName == *it || optionLongName == *it) {
      return true;
    }
  }
  return false;
}

//------------------------------------------------------------------------------
// formatResponse
//------------------------------------------------------------------------------
std::string XrdProFile::formatResponse(const std::vector<std::vector<std::string>> &responseTable) {
  if(responseTable.empty()||responseTable.at(0).empty()) {
    return "";
  }
  std::vector<int> columnSizes;
  for(uint i=0; i<responseTable.at(0).size(); i++) {
    uint columnSize=0;
    for(uint j=0; j<responseTable.size(); j++) {
      if(responseTable.at(i).at(j).size()>columnSize) {
        columnSize=responseTable.at(i).at(j).size();
      }
    }
    columnSize++; //add one space
    columnSizes.push_back(columnSize);
  }
  std::stringstream responseSS;
  for(auto row=responseTable.cbegin(); row!=responseTable.cend(); row++) {
    if(row==responseTable.cbegin()) responseSS << "\x1b[31;1m";
    for(uint i=0; i<row->size(); i++) {      
      responseSS << " " << std::setw(columnSizes.at(i)) << row->at(i);      
    }
    if(row==responseTable.cbegin()) responseSS << "\x1b[0m" << std::endl;
  }
  return responseSS.str();
}

//------------------------------------------------------------------------------
// addLogInfoToResponseRow
//------------------------------------------------------------------------------
void XrdProFile::addLogInfoToResponseRow(std::vector<std::string> &responseRow, const cta::common::dataStructures::EntryLog &creationLog, const cta::common::dataStructures::EntryLog &lastModificationLog) {
  time_t creationTime = creationLog.getTime();
  time_t lastModificationTime = lastModificationLog.getTime();
  std::string creationTimeString(ctime(&creationTime));
  std::string lastModificationTimeString(ctime(&lastModificationTime));
  creationTimeString=creationTimeString.substr(0,24); //remove the newline
  lastModificationTimeString=lastModificationTimeString.substr(0,24); //remove the newline
  responseRow.push_back(std::to_string((unsigned long long)creationLog.getUser().getUid()));
  responseRow.push_back(std::to_string((unsigned long long)creationLog.getUser().getGid()));
  responseRow.push_back(creationLog.getHost());
  responseRow.push_back(creationTimeString);
  responseRow.push_back(std::to_string((unsigned long long)lastModificationLog.getUser().getUid()));
  responseRow.push_back(std::to_string((unsigned long long)lastModificationLog.getUser().getGid()));
  responseRow.push_back(lastModificationLog.getHost());
  responseRow.push_back(lastModificationTimeString);
}

//------------------------------------------------------------------------------
// xCom_bootstrap
//------------------------------------------------------------------------------
void XrdProFile::xCom_bootstrap(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " bs/bootstrap --uid/-u <uid> --gid/-g <gid> --hostname/-h <host_name> --comment/-m <\"comment\">" << std::endl;
  std::string uid_s = getOptionValue(tokens, "-u", "--uid");
  std::string gid_s = getOptionValue(tokens, "-g", "--gid");
  std::string hostname = getOptionValue(tokens, "-h", "--hostname");
  std::string comment = getOptionValue(tokens, "-m", "--comment");
  if(uid_s.empty()||gid_s.empty()||hostname.empty()||comment.empty()) {
    m_data = help.str();
    return;
  }
  cta::common::dataStructures::UserIdentity adminUser;
  std::istringstream uid_ss(uid_s);
  int uid = 0;
  uid_ss >> uid;
  adminUser.setUid(uid);
  std::istringstream gid_ss(gid_s);
  int gid = 0;
  gid_ss >> gid;
  adminUser.setGid(gid);
  m_scheduler->createBootstrapAdminAndHostNoAuth(requester, adminUser, hostname, comment);
}

//------------------------------------------------------------------------------
// xCom_admin
//------------------------------------------------------------------------------
void XrdProFile::xCom_admin(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ad/admin add/ch/rm/ls:" << std::endl
       << "\tadd --uid/-u <uid> --gid/-g <gid> --comment/-m <\"comment\">" << std::endl
       << "\tch  --uid/-u <uid> --gid/-g <gid> --comment/-m <\"comment\">" << std::endl
       << "\trm  --uid/-u <uid> --gid/-g <gid>" << std::endl
       << "\tls" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string uid_s = getOptionValue(tokens, "-u", "--uid");
    std::string gid_s = getOptionValue(tokens, "-g", "--gid");
    if(uid_s.empty()||gid_s.empty()) {
      m_data = help.str();
      return;
    }
    cta::common::dataStructures::UserIdentity adminUser;
    std::istringstream uid_ss(uid_s);
    int uid = 0;
    uid_ss >> uid;
    adminUser.setUid(uid);
    std::istringstream gid_ss(gid_s);
    int gid = 0;
    gid_ss >> gid;
    adminUser.setGid(gid);
    if("add" == tokens[2] || "ch" == tokens[2]) {
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if(comment.empty()) {
        m_data = help.str();
        return;
      }
      if("add" == tokens[2]) { //add
        m_scheduler->createAdminUser(requester, adminUser, comment);
      }
      else { //ch
        m_scheduler->modifyAdminUserComment(requester, adminUser, comment);
      }
    }
    else { //rm
      m_scheduler->deleteAdminUser(requester, adminUser);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::AdminUser> list= m_scheduler->getAdminUsers(requester);
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"uid","gid","creator uid","creator gid","creation host","creation time","modifier uid","modifier gid","modification host","modification time","comment"};
      responseTable.push_back(header);    
      for(auto it = list.begin(); it != list.end(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(std::to_string((unsigned long long)it->getUser().getUid()));
        currentRow.push_back(std::to_string((unsigned long long)it->getUser().getGid()));
        addLogInfoToResponseRow(currentRow, it->getCreationLog(), it->getLastModificationLog());
        currentRow.push_back(it->getComment());
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  else {
    m_data = help.str();
  }
}

//------------------------------------------------------------------------------
// xCom_adminhost
//------------------------------------------------------------------------------
void XrdProFile::xCom_adminhost(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ah/adminhost add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <host_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <host_name> [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <host_name>" << std::endl
       << "\tls" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_tapepool
//------------------------------------------------------------------------------
void XrdProFile::xCom_tapepool(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " tp/tapepool add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <tapepool_name> --partialtapesnumber/-p <number_of_partial_tapes> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <tapepool_name> [--partialtapesnumber/-p <number_of_partial_tapes>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <tapepool_name>" << std::endl
       << "\tls" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_archiveroute
//------------------------------------------------------------------------------
void XrdProFile::xCom_archiveroute(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ar/archiveroute add/ch/rm/ls:" << std::endl
       << "\tadd --storageclass/-s <storage_class_name> --copynb/-c <copy_number> --tapepool/-t <tapepool_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --storageclass/-s <storage_class_name> --copynb/-c <copy_number> [--tapepool/-t <tapepool_name>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --storageclass/-s <storage_class_name> --copynb/-c <copy_number>" << std::endl
       << "\tls" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_logicallibrary
//------------------------------------------------------------------------------
void XrdProFile::xCom_logicallibrary(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ll/logicallibrary add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <logical_library_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <logical_library_name> [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <logical_library_name>" << std::endl
       << "\tls" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_tape
//------------------------------------------------------------------------------
void XrdProFile::xCom_tape(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ta/tape add/ch/rm/reclaim/ls/label:" << std::endl
       << "\tadd     --vid/-v <vid> --logicallibrary/-l <logical_library_name> --tapepool/-t <tapepool_name> --capacity/-c <capacity_in_bytes> [--enabled/-e or --disabled/-d] [--free/-f or --full/-F] [--comment/-m <\"comment\">] " << std::endl
       << "\tch      --vid/-v <vid> [--logicallibrary/-l <logical_library_name>] [--tapepool/-t <tapepool_name>] [--capacity/-c <capacity_in_bytes>] [--enabled/-e or --disabled/-d] [--free/-f or --full/-F] [--comment/-m <\"comment\">]" << std::endl
       << "\trm      --vid/-v <vid>" << std::endl
       << "\treclaim --vid/-v <vid>" << std::endl
       << "\tls      [--vid/-v <vid>] [--logicallibrary/-l <logical_library_name>] [--tapepool/-t <tapepool_name>] [--capacity/-c <capacity_in_bytes>] [--enabled/-e or --disabled/-d] [--free/-f or --full/-F] [--busy/-b or --notbusy/-n]" << std::endl
       << "\tlabel   --vid/-v <vid> [--force/-f] [--lbp/-l] [--tag/-t <tag_name>]" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_storageclass
//------------------------------------------------------------------------------
void XrdProFile::xCom_storageclass(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " sc/storageclass add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <storage_class_name> --copynb/-c <number_of_tape_copies> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <storage_class_name> [--copynb/-c <number_of_tape_copies>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <storage_class_name>" << std::endl
       << "\tls" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_user
//------------------------------------------------------------------------------
void XrdProFile::xCom_user(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " us/user add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <user_name> --group/-g <group_name> --usergroup/-u <user_group_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <user_name> --group/-g <group_name> [--usergroup/-u <user_group_name>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <user_name> --group/-g <group_name>" << std::endl
       << "\tls" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_usergroup
//------------------------------------------------------------------------------
void XrdProFile::xCom_usergroup(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ug/usergroup add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <usergroup_name> --archivepriority/--ap <priority_value> --minarchivefilesqueued/--af <minFilesQueued> --minarchivebytesqueued/--ab <minBytesQueued> --minarchiverequestage/--aa <minRequestAge> --retrievepriority/--rp <priority_value> --minretrievefilesqueued/--rf <minFilesQueued> --minretrievebytesqueued/--rb <minBytesQueued> --minretrieverequestage/--ra <minRequestAge> --maxdrivesallowed/-d <maxDrivesAllowed> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <usergroup_name> [--archivepriority/--ap <priority_value>] [--minarchivefilesqueued/--af <minFilesQueued>] [--minarchivebytesqueued/--ab <minBytesQueued>] [--minarchiverequestage/--aa <minRequestAge>] [--retrievepriority/--rp <priority_value>] [--minretrievefilesqueued/--rf <minFilesQueued>] [--minretrievebytesqueued/--rb <minBytesQueued>] [--minretrieverequestage/--ra <minRequestAge>] [--maxdrivesallowed/-d <maxDrivesAllowed>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <usergroup_name>" << std::endl
       << "\tls" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_dedication
//------------------------------------------------------------------------------
void XrdProFile::xCom_dedication(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " de/dedication add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <drive_name> [--readonly/-r or --writeonly/-w] [--usergroup/-u <user_group_name>] [--vid/-v <tape_vid>] [--tag/-t <tag_name>] --from/-f <DD/MM/YYYY> --until/-u <DD/MM/YYYY> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <drive_name> [--readonly/-r or --writeonly/-w] [--usergroup/-u <user_group_name>] [--vid/-v <tape_vid>] [--tag/-t <tag_name>] [--from/-f <DD/MM/YYYY>] [--until/-u <DD/MM/YYYY>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <drive_name>" << std::endl
       << "\tls" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_repack
//------------------------------------------------------------------------------
void XrdProFile::xCom_repack(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " re/repack add/rm/ls/err:" << std::endl
       << "\tadd [--vid/-v <vid>] [--file/-f <filename_with_vid_list>] [--expandandrepack/-d or --justexpand/-e or --justrepack/-r] [--tag/-t <tag_name>]" << std::endl
       << "\trm  [--vid/-v <vid>] [--file/-f <filename_with_vid_list>]" << std::endl
       << "\tls  [--vid/-v <vid>]" << std::endl
       << "\terr --vid/-v <vid>" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_shrink
//------------------------------------------------------------------------------
void XrdProFile::xCom_shrink(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " sh/shrink --tapepool/-t <tapepool_name>" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_verify
//------------------------------------------------------------------------------
void XrdProFile::xCom_verify(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ve/verify add/rm/ls/err:" << std::endl
       << "\tadd [--vid/-v <vid>] [--file/-f <filename_with_vid_list>] [--complete/-c] [--partial/-p <number_of_files_per_tape>] [--tag/-t <tag_name>]" << std::endl
       << "\trm  [--vid/-v <vid>] [--file/-f <filename_with_vid_list>]" << std::endl
       << "\tls  [--vid/-v <vid>]" << std::endl
       << "\terr --vid/-v <vid>" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_archivefile
//------------------------------------------------------------------------------
void XrdProFile::xCom_archivefile(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " af/archivefile ls [--id/-I <archive_file_id>] [--copynb/-c <copy_no>] [--vid/-v <vid>] [--tapepool/-t <tapepool>] [--owner/-o <owner>] [--group/-g <owner>] [--storageclass/-s <class>] [--path/-p <fullpath>] [--summary/-S] [--all/-a] (default gives error)" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_test
//------------------------------------------------------------------------------
void XrdProFile::xCom_test(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " te/test read/write (to be run on an empty self-dedicated drive; it is a synchronous command that returns performance stats and errors):" << std::endl
       << "\tread  --drive/-d <drive_name> --vid/-v <vid> --firstfseq/-f <first_fseq> --lastfseq/-l <last_fseq> --checkchecksum/-c --retries_per_file/-r <number_of_retries_per_file> [--outputdir/-o <output_dir>] [--null/-n] [--tag/-t <tag_name>]" << std::endl
       << "\twrite --drive/-d <drive_name> --vid/-v <vid> --number/-n <number_of_files> [--size/-s <file_size>] [--randomsize/-r] [--zero/-z] [--urandom/-u] [--file/-f <filename>] [--filelist/-f <filename_with_file_list>] [--tag/-t <tag_name>]" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_drive
//------------------------------------------------------------------------------
void XrdProFile::xCom_drive(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " dr/drive up/down (it is a synchronous command):" << std::endl
       << "\tup   --drive/-d <drive_name>" << std::endl
       << "\tdown --drive/-d <drive_name> [--force/-f]" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_reconcile
//------------------------------------------------------------------------------
void XrdProFile::xCom_reconcile(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " rc/reconcile (it is a synchronous command, with a possibly long execution time, returns the list of files unknown to EOS, to be deleted manually by the admin after proper checks)" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_listpendingarchives
//------------------------------------------------------------------------------
void XrdProFile::xCom_listpendingarchives(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " lpa/listpendingarchives [--tapepool/-t <tapepool_name>] [--extended/-x]" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_listpendingretrieves
//------------------------------------------------------------------------------
void XrdProFile::xCom_listpendingretrieves(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " lpr/listpendingretrieves [--vid/-v <vid>] [--extended/-x]" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_listdrivestates
//------------------------------------------------------------------------------
void XrdProFile::xCom_listdrivestates(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " lds/listdrivestates" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_archive
//------------------------------------------------------------------------------
void XrdProFile::xCom_archive(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " a/archive [--noencoding/-n] username groupname EOS_unique_id src_URL size checksum_type checksum_value storage_class DR_instance DR_path DR_owner DR_group DR_blob diskpool_name diskpool_throughput" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_retrieve
//------------------------------------------------------------------------------
void XrdProFile::xCom_retrieve(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " r/retrieve [--noencoding/-n] username groupname CTA_ArchiveFileID dst_URL DR_instance DR_path DR_owner DR_group DR_blob diskpool_name diskpool_throughput" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_deletearchive
//------------------------------------------------------------------------------
void XrdProFile::xCom_deletearchive(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " da/deletearchive username groupname CTA_ArchiveFileID" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_cancelretrieve
//------------------------------------------------------------------------------
void XrdProFile::xCom_cancelretrieve(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " cr/cancelretrieve [--noencoding/-n] username groupname CTA_ArchiveFileID dst_URL DR_instance DR_path DR_owner DR_group DR_blob" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_updatefileinfo
//------------------------------------------------------------------------------
void XrdProFile::xCom_updatefileinfo(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ufi/updatefileinfo [--noencoding/-n] username groupname CTA_ArchiveFileID storage_class DR_instance DR_path DR_owner DR_group DR_blob" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_liststorageclass
//------------------------------------------------------------------------------
void XrdProFile::xCom_liststorageclass(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " lsc/liststorageclass username groupname" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_admin
//------------------------------------------------------------------------------
//void XrdProFile::xCom_admin(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
//  std::stringstream help;
//  help << tokens[0] << " ad/admin add/ch/rm/ls:" << std::endl;
//  help << "\tadd --uid/-u <uid> --gid/-g <gid> --comment/-m <\"comment\">" << std::endl;
//  help << "\tch  --uid/-u <uid> --gid/-g <gid> --comment/-m <\"comment\">" << std::endl;
//  help << "\trm  --uid/-u <uid> --gid/-g <gid>" << std::endl;
//  help << "\tls" << std::endl;
//  if(tokens.size()<3){
//    m_data = help.str();
//    return;
//  }
//  if("add" == tokens[2]) {
//    std::string uid_s = getOptionValue(tokens, "-u", "--uid");
//    std::string gid_s = getOptionValue(tokens, "-g", "--gid");
//    std::string comment = getOptionValue(tokens, "-m", "--comment");
//    if(uid_s.empty()||gid_s.empty()||comment.empty()) {
//      m_data = help.str();
//      return;
//    }
//    cta::UserIdentity adminUser;
//    std::istringstream uid_ss(uid_s);
//    int uid = 0;
//    uid_ss >> uid;
//    adminUser.uid = uid;
//    std::istringstream gid_ss(gid_s);
//    int gid = 0;
//    gid_ss >> gid;
//    adminUser.gid = gid;
////    m_scheduler->createAdminUser(requester, adminUser, comment);
//  }
//  else if("ch" == tokens[2]) {
//    std::string uid_s = getOptionValue(tokens, "-u", "--uid");
//    std::string gid_s = getOptionValue(tokens, "-g", "--gid");
//    std::string comment = getOptionValue(tokens, "-m", "--comment");
//    if(uid_s.empty()||gid_s.empty()||comment.empty()) {
//      m_data = help.str();
//      return;
//    }
//    cta::UserIdentity adminUser;
//    std::istringstream uid_ss(uid_s);
//    int uid = 0;
//    uid_ss >> uid;
//    adminUser.uid = uid;
//    std::istringstream gid_ss(gid_s);
//    int gid = 0;
//    gid_ss >> gid;
//    adminUser.gid = gid;
//    //m_scheduler->modifyAdminUser(requester, adminUser, comment);
//  }
//  else if("rm" == tokens[2]) {
//    std::string uid_s = getOptionValue(tokens, "-u", "--uid");
//    std::string gid_s = getOptionValue(tokens, "-g", "--gid");
//    if(uid_s.empty()||gid_s.empty()) {
//      m_data = help.str();
//      return;
//    }
//    cta::UserIdentity adminUser;
//    std::istringstream uid_ss(uid_s);
//    int uid = 0;
//    uid_ss >> uid;
//    adminUser.uid = uid;
//    std::istringstream gid_ss(gid_s);
//    int gid = 0;
//    gid_ss >> gid;
//    adminUser.gid = gid;
////    m_scheduler->deleteAdminUser(requester, adminUser);
//  }
//  else if("ls" == tokens[2]) {
////    auto list = m_scheduler->getAdminUsers(requester);
//    std::list<common::admin::AdminUser> list;
//    std::ostringstream responseSS;
//    if(list.size()>0) {
//      responseSS << "\x1b[31;1m"
//                 << " " << std::setw(8) << "uid" 
//                 << " " << std::setw(8) << "gid"
//                 << " " << std::setw(18) << "creator uid" 
//                 << " " << std::setw(18) << "creator gid" 
//                 << " " << std::setw(30) << "creation host"
//                 << " " << std::setw(30) << "creation time"
//                 << " " << std::setw(30) << "comment"
//                 << "\x1b[0m" << std::endl;
//    }
//    for(auto it = list.begin(); it != list.end(); it++) {
//      std::string timeString(ctime(&(it->getCreationLog().time)));
//      timeString=timeString.substr(0,24);//remove the newline
//      responseSS << " " << std::setw(8) << it->getUser().uid 
//                 << " " << std::setw(8) << it->getUser().gid 
//                 << " " << std::setw(18) << it->getCreationLog().user.uid 
//                 << " " << std::setw(18) << it->getCreationLog().user.gid 
//                 << " " << std::setw(30) << it->getCreationLog().host
//                 << " " << std::setw(30) << timeString
//                 << " " << std::setw(30) << it->getCreationLog().comment << std::endl;
//    }
//    m_data = responseSS.str();
//  }
//  else {
//    m_data = help.str();
//  }
//}
  
//------------------------------------------------------------------------------
// getGenericHelp
//------------------------------------------------------------------------------
std::string XrdProFile::getGenericHelp(const std::string &programName) const {
  std::stringstream help;
  help << "CTA ADMIN commands:" << std::endl;
  help << "" << std::endl;
  help << "For each command there is a short version and a long one, example: op/operator. Subcommands (add/rm/ls/ch/reclaim) do not have short versions." << std::endl;
  help << "" << std::endl;
  help << programName << " ad/admin          add/rm/ls/ch" << std::endl;
  help << programName << " ah/adminhost      add/rm/ls/ch" << std::endl;
  help << programName << " tp/tapepool       add/rm/ls/ch" << std::endl;
  help << programName << " ar/archiveroute   add/rm/ls/ch" << std::endl;
  help << programName << " ll/logicallibrary add/rm/ls/ch" << std::endl;
  help << programName << " ta/tape           add/rm/ls/ch/reclaim" << std::endl;
  help << programName << " sc/storageclass   add/rm/ls/ch" << std::endl;
  help << programName << " lpa/listpendingarchives" << std::endl;
  help << programName << " lpr/listpendingretrieves" << std::endl;
  help << programName << " lds/listdrivestates" << std::endl;
  help << "" << std::endl;
  help << "CTA EOS commands:" << std::endl;
  help << "" << std::endl;
  help << "For most commands there is a short version and a long one." << std::endl;
  help << "" << std::endl;
  help << programName << " lsc/liststorageclass" << std::endl;
  help << programName << " ufi/updatefileinfo" << std::endl;
  help << programName << " a/archive" << std::endl;
  help << programName << " r/retrieve" << std::endl;
  help << programName << " da/deletearchive" << std::endl;
  help << programName << " cr/cancelretrieve" << std::endl;
  return help.str();
}

}}
