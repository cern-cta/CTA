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

#include "common/exception/Exception.hpp"
#include "scheduler/AdminHost.hpp"
#include "scheduler/AdminUser.hpp"
#include "scheduler/ArchivalRoute.hpp"
#include "scheduler/ArchiveToTapeCopyRequest.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/MockSchedulerDatabase.hpp"
#include "scheduler/RetrieveFromTapeCopyRequest.hpp"
#include "scheduler/StorageClass.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/Tape.hpp"
#include "scheduler/TapePool.hpp"
#include "scheduler/SecurityIdentity.hpp"
#include "scheduler/UserIdentity.hpp"
#include "xroot_plugins/XrdProFile.hpp"

#include "XrdSec/XrdSecEntity.hh"
#include "cmdline/CTACmd.hpp"
#include "scheduler/DirIterator.hpp"

#include <iostream>
#include <pwd.h>
#include <sstream>
#include <string>

//------------------------------------------------------------------------------
// checkClient
//------------------------------------------------------------------------------
int XrdProFile::checkClient(const XrdSecEntity *client, cta::SecurityIdentity &requester) {
  if(!client || !client->host || strncmp(client->host, "localhost", 9))
  {
    m_data = "[ERROR] operation possible only from localhost";
    return SFS_ERROR;
  }
  struct passwd pwd;
  struct passwd *result;
  char *buf;
  long bufsize = sysconf(_SC_GETPW_R_SIZE_MAX);
  if (bufsize == -1)
  {
    bufsize = 16384;
  }
  buf = (char *)malloc((size_t)bufsize);
  if(buf == NULL)
  {
    m_data = "[ERROR] malloc of the buffer failed";
    free(buf);
    return SFS_ERROR;
  }
  int rc = getpwnam_r(client->name, &pwd, buf, bufsize, &result);
  if(result == NULL)
  {
    if (rc == 0)
    {
      m_data = "[ERROR] User ";
      m_data += client->name;
      m_data += " not found";
      free(buf);
      return SFS_ERROR;
    }
    else
    {
      m_data = "[ERROR] getpwnam_r failed";
      free(buf);
      return SFS_ERROR;
    }
  }
  std::cout << "Request received from client. Username: " << client->name <<
    " uid: " << pwd.pw_uid << " gid: " << pwd.pw_gid << std::endl;
  requester = cta::SecurityIdentity(cta::UserIdentity(pwd.pw_uid, pwd.pw_gid),
    client->host);
  free(buf);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// commandDispatcher
//------------------------------------------------------------------------------
void XrdProFile::commandDispatcher(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::string command(tokens[1]);
  
  if     ("ad"    == command || "admin"                 == command) {xCom_admin(tokens, requester);}
  else if("ah"    == command || "adminhost"             == command) {xCom_adminhost(tokens, requester);}
  else if("us"    == command || "user"                  == command) {xCom_user(tokens, requester);}
  else if("tp"    == command || "tapepool"              == command) {xCom_tapepool(tokens, requester);}
  else if("ar"    == command || "archiveroute"          == command) {xCom_archiveroute(tokens, requester);}
  else if("ll"    == command || "logicallibrary"        == command) {xCom_logicallibrary(tokens, requester);}
  else if("ta"    == command || "tape"                  == command) {xCom_tape(tokens, requester);}
  else if("sc"    == command || "storageclass"          == command) {xCom_storageclass(tokens, requester);}
  else if("loa"   == command || "listongoingarchivals"  == command) {xCom_listongoingarchivals(tokens, requester);}
  else if("lor"   == command || "listongoingretrievals" == command) {xCom_listongoingretrievals(tokens, requester);}
  else if("lpa"   == command || "listpendingarchivals"  == command) {xCom_listpendingarchivals(tokens, requester);}
  else if("lpr"   == command || "listpendingretrievals" == command) {xCom_listpendingretrievals(tokens, requester);}
  else if("lds"   == command || "listdrivestates"       == command) {xCom_listdrivestates(tokens, requester);}
  
  else if("lsc"   == command || "liststorageclass"      == command) {xCom_liststorageclass(tokens, requester);}
  else if("ssc"   == command || "setstorageclass"       == command) {xCom_setstorageclass(tokens, requester);}
  else if("csc"   == command || "clearstorageclass"     == command) {xCom_clearstorageclass(tokens, requester);}
  else if("mkdir" == command)                                       {xCom_mkdir(tokens, requester);}
  else if("rmdir" == command)                                       {xCom_rmdir(tokens, requester);}
  else if("ls"    == command)                                       {xCom_ls(tokens, requester);}
  else if("a"     == command || "archive"               == command) {xCom_archive(tokens, requester);}
  else if("r"     == command || "retrieve"              == command) {xCom_retrieve(tokens, requester);}
  else if("da"    == command || "deletearchive"         == command) {xCom_deletearchive(tokens, requester);}
  else if("cr"    == command || "cancelretrieval"       == command) {xCom_cancelretrieval(tokens, requester);}
  
  else {m_data = getGenericHelp(tokens[0]);}
}

//------------------------------------------------------------------------------
// open
//------------------------------------------------------------------------------
int XrdProFile::open(const char *fileName, XrdSfsFileOpenMode openMode, mode_t createMode, const XrdSecEntity *client, const char *opaque) {
  
  cta::SecurityIdentity requester;
  int checkResult = checkClient(client, requester);
  if(SFS_OK!=checkResult) {
    return checkResult;
  }
  
  std::vector<std::string> tokens;
  std::stringstream ss(fileName);
  std::string item;
  while (std::getline(ss, item, '&')) {
    replaceAll(item, "_#_and_#_", "&");
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
  
  try {
    commandDispatcher(tokens, requester);
    return SFS_OK;
  } catch (cta::exception::Exception &ex) {
    m_data = "[ERROR] CTA exception caught: ";
    m_data += ex.what();
    return SFS_OK;
  } catch (std::exception &ex) {
    m_data = "[ERROR] Exception caught: ";
    m_data += ex.what();
    return SFS_OK;
  } catch (...) {
    m_data = "[ERROR] Unknown exception caught!";
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
  return SFS_OK; //change to "return SFS_ERROR;" in case the read function below is wanted
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
      if(it_next!=tokens.end()) {
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
// xCom_admin
//------------------------------------------------------------------------------
void XrdProFile::xCom_admin(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ad/admin add/ch/rm/ls:" << std::endl;
  help << "\tadd --uid/-u <uid> --gid/-g <gid> --comment/-m <\"comment\">" << std::endl;
  help << "\tch  --uid/-u <uid> --gid/-g <gid> --comment/-m <\"comment\">" << std::endl;
  help << "\trm  --uid/-u <uid> --gid/-g <gid>" << std::endl;
  help << "\tls" << std::endl;
  if(tokens.size()<3){
    m_data = help.str();
    return;
  }
  if("add" == tokens[2]) {
    std::string uid_s = getOptionValue(tokens, "-u", "--uid");
    std::string gid_s = getOptionValue(tokens, "-g", "--gid");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    if(uid_s.empty()||gid_s.empty()||comment.empty()) {
      m_data = help.str();
      return;
    }
    cta::UserIdentity adminUser;
    std::istringstream uid_ss(uid_s);
    int uid = 0;
    uid_ss >> uid;
    adminUser.uid = uid;
    std::istringstream gid_ss(gid_s);
    int gid = 0;
    gid_ss >> gid;
    adminUser.gid = gid;
    m_scheduler->createAdminUser(requester, adminUser, comment);
  }
  else if("ch" == tokens[2]) {
    std::string uid_s = getOptionValue(tokens, "-u", "--uid");
    std::string gid_s = getOptionValue(tokens, "-g", "--gid");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    if(uid_s.empty()||gid_s.empty()||comment.empty()) {
      m_data = help.str();
      return;
    }
    cta::UserIdentity adminUser;
    std::istringstream uid_ss(uid_s);
    int uid = 0;
    uid_ss >> uid;
    adminUser.uid = uid;
    std::istringstream gid_ss(gid_s);
    int gid = 0;
    gid_ss >> gid;
    adminUser.gid = gid;
    //m_scheduler->modifyAdminUser(requester, adminUser, comment);
  }
  else if("rm" == tokens[2]) {
    std::string uid_s = getOptionValue(tokens, "-u", "--uid");
    std::string gid_s = getOptionValue(tokens, "-g", "--gid");
    if(uid_s.empty()||gid_s.empty()) {
      m_data = help.str();
      return;
    }
    cta::UserIdentity adminUser;
    std::istringstream uid_ss(uid_s);
    int uid = 0;
    uid_ss >> uid;
    adminUser.uid = uid;
    std::istringstream gid_ss(gid_s);
    int gid = 0;
    gid_ss >> gid;
    adminUser.gid = gid;
    m_scheduler->deleteAdminUser(requester, adminUser);
  }
  else if("ls" == tokens[2]) {
    auto list = m_scheduler->getAdminUsers(requester);
    std::ostringstream responseSS;
    for(auto it = list.begin(); it != list.end(); it++) {
      responseSS << it->getUser().uid 
                 << " " << it->getUser().gid 
                 << " " << it->getCreator().uid 
                 << " " << it->getCreator().gid 
                 << " " << it->getCreationTime() 
                 << " " << it->getComment() << std::endl;
    }
    m_data = responseSS.str();
  }
  else {
    m_data = help.str();
  }
}
  
//------------------------------------------------------------------------------
// xCom_adminhost
//------------------------------------------------------------------------------
void XrdProFile::xCom_adminhost(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ah/adminhost add/ch/rm/ls:" << std::endl;
  help << "\tadd --name/-n <host_name> --comment/-m <\"comment\">" << std::endl;
  help << "\tch  --name/-n <host_name> --comment/-m <\"comment\">" << std::endl;
  help << "\trm  --name/-n <host_name>" << std::endl;
  help << "\tls" << std::endl;
  if(tokens.size()<3){
    m_data = help.str();
    return;
  }
  if("add" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    if(name.empty()||comment.empty()) {
      m_data = help.str();
      return;
    }
    m_scheduler->createAdminHost(requester, name, comment);
  }
  else if("ch" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    if(name.empty()||comment.empty()) {
      m_data = help.str();
      return;
    }
    //m_scheduler->modifyAdminHost(requester, name, comment);
  }
  else if("rm" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    if(name.empty()) {
      m_data = help.str();
      return;
    }
    m_scheduler->deleteAdminHost(requester, name);
  }
  else if("ls" == tokens[2]) {
    auto list = m_scheduler->getAdminHosts(requester);
    std::ostringstream responseSS;
    for(auto it = list.begin(); it != list.end(); it++) {
      responseSS << it->getName() 
                 << " " << it->getCreator().uid 
                 << " " << it->getCreator().gid 
                 << " " << it->getCreationTime() 
                 << " " << it->getComment() << std::endl;
    }
    m_data = responseSS.str();
  }
  else {
    m_data = help.str();
  }
}
  
//------------------------------------------------------------------------------
// xCom_user
//------------------------------------------------------------------------------
void XrdProFile::xCom_user(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " us/user add/ch/rm/ls:" << std::endl;
  help << "\tadd --uid/-u <uid> --gid/-g <gid> --comment/-m <\"comment\">" << std::endl;
  help << "\tch  --uid/-u <uid> --gid/-g <gid> --comment/-m <\"comment\">" << std::endl;
  help << "\trm  --uid/-u <uid> --gid/-g <gid>" << std::endl;
  help << "\tls" << std::endl;
  if(tokens.size()<3){
    m_data = help.str();
    return;
  }
  if("add" == tokens[2]) {
    std::string uid_s = getOptionValue(tokens, "-u", "--uid");
    std::string gid_s = getOptionValue(tokens, "-g", "--gid");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    if(uid_s.empty()||gid_s.empty()||comment.empty()) {
      m_data = help.str();
      return;
    }
    cta::UserIdentity user;
    std::istringstream uid_ss(uid_s);
    int uid = 0;
    uid_ss >> uid;
    user.uid = uid;
    std::istringstream gid_ss(gid_s);
    int gid = 0;
    gid_ss >> gid;
    user.gid = gid;
//    m_scheduler->createUser(requester, user, comment);
  }
  else if("ch" == tokens[2]) {
    std::string uid_s = getOptionValue(tokens, "-u", "--uid");
    std::string gid_s = getOptionValue(tokens, "-g", "--gid");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    if(uid_s.empty()||gid_s.empty()||comment.empty()) {
      m_data = help.str();
      return;
    }
    cta::UserIdentity user;
    std::istringstream uid_ss(uid_s);
    int uid = 0;
    uid_ss >> uid;
    user.uid = uid;
    std::istringstream gid_ss(gid_s);
    int gid = 0;
    gid_ss >> gid;
    user.gid = gid;
//    m_scheduler->modifyUser(requester, user, comment);
  }
  else if("rm" == tokens[2]) {
    std::string uid_s = getOptionValue(tokens, "-u", "--uid");
    std::string gid_s = getOptionValue(tokens, "-g", "--gid");
    if(uid_s.empty()||gid_s.empty()) {
      m_data = help.str();
      return;
    }
    cta::UserIdentity user;
    std::istringstream uid_ss(uid_s);
    int uid = 0;
    uid_ss >> uid;
    user.uid = uid;
    std::istringstream gid_ss(gid_s);
    int gid = 0;
    gid_ss >> gid;
    user.gid = gid;
//    m_scheduler->deleteUser(requester, user);
  }
  else if("ls" == tokens[2]) {
//    auto list = m_scheduler->getUsers(requester);
//    std::ostringstream responseSS;
//    for(auto it = list.begin(); it != list.end(); it++) {
//      responseSS << it->getUser().uid 
//                 << " " << it->getUser().gid 
//                 << " " << it->getCreator().uid 
//                 << " " << it->getCreator().gid 
//                 << " " << it->getCreationTime() 
//                 << " " << it->getComment() << std::endl;
//    }
//    m_data = responseSS.str();
  }
  else {
    m_data = help.str();
  }
}
  
//------------------------------------------------------------------------------
// xCom_tapepool
//------------------------------------------------------------------------------
void XrdProFile::xCom_tapepool(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " tp/tapepool add/ch/rm/ls:" << std::endl;
  help << "\tadd --name/-n <tapepool_name> --partialtapesnumber/-p <number_of_partial_tapes> --comment/-m <\"comment\">" << std::endl;
  help << "\tch  --name/-n <tapepool_name> --partialtapesnumber/-p <number_of_partial_tapes> --comment/-m <\"comment\">" << std::endl;
  help << "\trm  --name/-n <tapepool_name>" << std::endl;
  help << "\tls" << std::endl;
  if(tokens.size()<3){
    m_data = help.str();
    return;
  }
  if("add" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    std::string partialtapes_s = getOptionValue(tokens, "-p", "--partialtapesnumber");
    if(name.empty()||comment.empty()||partialtapes_s.empty()) {
      m_data = help.str();
      return;
    }
    std::istringstream partialtapes_ss(partialtapes_s);
    int partialtapes = 0;
    partialtapes_ss >> partialtapes;
    m_scheduler->createTapePool(requester, name, partialtapes, comment);
  }
  else if("ch" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    std::string partialtapes_s = getOptionValue(tokens, "-p", "--partialtapesnumber");
    if(name.empty()||comment.empty()||partialtapes_s.empty()) {
      m_data = help.str();
      return;
    }
    std::istringstream partialtapes_ss(partialtapes_s);
    int partialtapes = 0;
    partialtapes_ss >> partialtapes;
    //m_scheduler->modifyTapePool(requester, name, partialtapes, comment);
  }
  else if("rm" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    if(name.empty()) {
      m_data = help.str();
      return;
    }
    m_scheduler->deleteTapePool(requester, name);
  }
  else if("ls" == tokens[2]) {
    auto list = m_scheduler->getTapePools(requester);
    std::ostringstream responseSS;
    for(auto it = list.begin(); it != list.end(); it++) {
      responseSS << it->getName()  
                 << " " << it->getNbPartialTapes()
                 << " " << it->getCreationLog().user.uid 
                 << " " << it->getCreationLog().user.gid 
                 << " " << it->getCreationLog().host
                 << " " << it->getCreationLog().time
                 << " " << it->getCreationLog().comment;
    }
    m_data = responseSS.str();
  }
  else {
    m_data = help.str();
  }  
}
  
//------------------------------------------------------------------------------
// xCom_archiveroute
//------------------------------------------------------------------------------
void XrdProFile::xCom_archiveroute(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ar/archiveroute add/ch/rm/ls:" << std::endl;
  help << "\tadd --storageclass/-s <storage_class_name> --copynb/-c <copy_number> --tapepool/-t <tapepool_name> --comment/-m <\"comment\">" << std::endl;
  help << "\tch  --storageclass/-s <storage_class_name> --copynb/-c <copy_number> --tapepool/-t <tapepool_name> --comment/-m <\"comment\">" << std::endl;
  help << "\trm  --storageclass/-s <storage_class_name> --copynb/-c <copy_number>" << std::endl;
  help << "\tls" << std::endl;
  if(tokens.size()<3){
    m_data = help.str();
    return;
  }
  if("add" == tokens[2]) {
    std::string storageClass = getOptionValue(tokens, "-s", "--storageclass");
    std::string tapePool = getOptionValue(tokens, "-t", "--tapepool");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    std::string copynb_s = getOptionValue(tokens, "-c", "--copynb");
    if(storageClass.empty()||tapePool.empty()||comment.empty()||copynb_s.empty()) {
      m_data = help.str();
      return;
    }
    std::istringstream copynb_ss(copynb_s);
    int copynb = 0;
    copynb_ss >> copynb;
    m_scheduler->createArchivalRoute(requester, storageClass, copynb, tapePool, comment);
  }
  else if("ch" == tokens[2]) {
    std::string storageClass = getOptionValue(tokens, "-s", "--storageclass");
    std::string tapePool = getOptionValue(tokens, "-t", "--tapepool");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    std::string copynb_s = getOptionValue(tokens, "-c", "--copynb");
    if(storageClass.empty()||tapePool.empty()||comment.empty()||copynb_s.empty()) {
      m_data = help.str();
      return;
    }
    std::istringstream copynb_ss(copynb_s);
    int copynb = 0;
    copynb_ss >> copynb;
//    m_scheduler->modifyArchivalRoute(requester, storageClass, copynb, tapePool, comment);
  }
  else if("rm" == tokens[2]) {
    std::string storageClass = getOptionValue(tokens, "-s", "--storageclass");
    std::string copynb_s = getOptionValue(tokens, "-c", "--copynb");
    if(storageClass.empty()||copynb_s.empty()) {
      m_data = help.str();
      return;
    }
    std::istringstream copynb_ss(copynb_s);
    int copynb = 0;
    copynb_ss >> copynb;
    m_scheduler->deleteArchivalRoute(requester, storageClass, copynb);
  }
  else if("ls" == tokens[2]) {
    auto list = m_scheduler->getArchivalRoutes(requester);
    std::ostringstream responseSS;
    for(auto it = list.begin(); it != list.end(); it++) {
      responseSS << it->getStorageClassName()
                 << " " << it->getCopyNb()
                 << " " << it->getTapePoolName()
                 << " " << it->getCreationLog().user.uid
                 << " " << it->getCreationLog().user.gid
                 << " " << it->getCreationLog().host
                 << " " << it->getCreationLog().time
                 << " " << it->getCreationLog().comment << std::endl;
    }
    m_data = responseSS.str();
  }
  else {
    m_data = help.str();
  }  
}
  
//------------------------------------------------------------------------------
// xCom_logicallibrary
//------------------------------------------------------------------------------
void XrdProFile::xCom_logicallibrary(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ll/logicallibrary add/ch/rm/ls:" << std::endl;
  help << "\tadd --name/-n <logical_library_name> --comment/-m <\"comment\">" << std::endl;
  help << "\tch  --name/-n <logical_library_name> --comment/-m <\"comment\">" << std::endl;
  help << "\trm  --name/-n <logical_library_name>" << std::endl;
  help << "\tls" << std::endl;
  if(tokens.size()<3){
    m_data = help.str();
    return;
  }
  if("add" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    if(name.empty()||comment.empty()) {
      m_data = help.str();
      return;
    }
    m_scheduler->createLogicalLibrary(requester, name, comment);
  }
  else if("ch" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    if(name.empty()||comment.empty()) {
      m_data = help.str();
      return;
    }
//    m_scheduler->modifyLogicalLibrary(requester, name, comment);
  }
  else if("rm" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    if(name.empty()) {
      m_data = help.str();
      return;
    }
    m_scheduler->deleteLogicalLibrary(requester, name);
  }
  else if("ls" == tokens[2]) {
    auto list = m_scheduler->getLogicalLibraries(requester);
    std::ostringstream responseSS;
    for(auto it = list.begin(); it != list.end(); it++) {
      responseSS << it->getName()
                 << " " << it->getCreationLog().user.uid 
                 << " " << it->getCreationLog().user.gid
                 << " " << it->getCreationLog().host
                 << " " << it->getCreationLog().time
                 << " " << it->getCreationLog().comment;
    }
    m_data = responseSS.str();
  }
  else {
    m_data = help.str();
  }
}
  
//------------------------------------------------------------------------------
// xCom_tape
//------------------------------------------------------------------------------
void XrdProFile::xCom_tape(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ta/tape add/ch/rm/reclaim/ls:" << std::endl;
  help << "\tadd     --vid/-v <vid> --logicallibrary/-l <logical_library_name> --tapepool/-t <tapepool_name> --capacity/-c <capacity_in_bytes> --comment/-m <\"comment\">" << std::endl;
  help << "\tch      --vid/-v <vid> --logicallibrary/-l <logical_library_name> --tapepool/-t <tapepool_name> --capacity/-c <capacity_in_bytes> --comment/-m <\"comment\">" << std::endl;
  help << "\trm      --vid/-v <vid>" << std::endl;
  help << "\treclaim --vid/-v <vid>" << std::endl;
  help << "\tls" << std::endl;
  if(tokens.size()<3){
    m_data = help.str();
    return;
  }
  if("add" == tokens[2]) {
    std::string vid = getOptionValue(tokens, "-v", "--vid");
    std::string logicalLibrary = getOptionValue(tokens, "-l", "--logicallibrary");
    std::string tapePool = getOptionValue(tokens, "-t", "--tapepool");
    std::string capacity_s = getOptionValue(tokens, "-c", "--capacity");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    if(logicalLibrary.empty()||tapePool.empty()||comment.empty()||capacity_s.empty()||vid.empty()) {
      m_data = help.str();
      return;
    }
    std::istringstream capacity_ss(capacity_s);
    uint64_t capacity = 0;
    capacity_ss >> capacity;
    m_scheduler->createTape(requester, vid, logicalLibrary, tapePool, capacity, comment);
  }
  else if("ch" == tokens[2]) {
    std::string vid = getOptionValue(tokens, "-v", "--vid");
    std::string logicalLibrary = getOptionValue(tokens, "-l", "--logicallibrary");
    std::string tapePool = getOptionValue(tokens, "-t", "--tapepool");
    std::string capacity_s = getOptionValue(tokens, "-c", "--capacity");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    if(logicalLibrary.empty()||tapePool.empty()||comment.empty()||capacity_s.empty()||vid.empty()) {
      m_data = help.str();
      return;
    }
    std::istringstream capacity_ss(capacity_s);
    uint64_t capacity = 0;
    capacity_ss >> capacity;
//    m_scheduler->modifyTape(requester, vid, logicalLibrary, tapePool, capacity, comment);
  }
  else if("rm" == tokens[2]) {
    std::string vid = getOptionValue(tokens, "-v", "--vid");
    if(vid.empty()) {
      m_data = help.str();
      return;
    }
    m_scheduler->deleteTape(requester, vid);
  }
  else if("ls" == tokens[2]) {
    auto list = m_scheduler->getTapes(requester);
    std::ostringstream responseSS;
    for(auto it = list.begin(); it != list.end(); it++) {
      responseSS << it->getVid()
                 << " " << it->getLogicalLibraryName()
                 << " " << it->getCapacityInBytes()
                 << " " << it->getTapePoolName()
                 << " " << it->getDataOnTapeInBytes()
                 << " " << it->getCreator().uid 
                 << " " << it->getCreator().gid 
                 << " " << it->getCreationTime() 
                 << " " << it->getComment() << std::endl;
    }
    m_data = responseSS.str();
  }
  else {
    m_data = help.str();
  }
}
  
//------------------------------------------------------------------------------
// xCom_storageclass
//------------------------------------------------------------------------------
void XrdProFile::xCom_storageclass(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " sc/storageclass add/ch/rm/ls:" << std::endl;
  help << "\tadd --name/-n <storage_class_name> --copynb/-c <number_of_tape_copies> --comment/-m <\"comment\">" << std::endl;
  help << "\tch  --name/-n <storage_class_name> --copynb/-c <number_of_tape_copies> --comment/-m <\"comment\">" << std::endl;
  help << "\trm  --name/-n <storage_class_name>" << std::endl;
  help << "\tls" << std::endl;
  if(tokens.size()<3){
    m_data = help.str();
    return;
  }
  if("add" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    std::string copynb_s = getOptionValue(tokens, "-c", "--copynb");
    if(name.empty()||comment.empty()||copynb_s.empty()) {
      m_data = help.str();
      return;
    }
    std::istringstream copynb_ss(copynb_s);
    int copynb = 0;
    copynb_ss >> copynb;
    m_scheduler->createStorageClass(requester, name, copynb, comment);
  }
  else if("ch" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    std::string copynb_s = getOptionValue(tokens, "-c", "--copynb");
    if(name.empty()||comment.empty()||copynb_s.empty()) {
      m_data = help.str();
      return;
    }
    std::istringstream copynb_ss(copynb_s);
    int copynb = 0;
    copynb_ss >> copynb;
//    m_scheduler->modifyStorageClass(requester, name, copynb, comment);
  }
  else if("rm" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    if(name.empty()) {
      m_data = help.str();
      return;
    }
    m_scheduler->deleteStorageClass(requester, name);
  }
  else if("ls" == tokens[2]) {
    auto list = m_scheduler->getStorageClasses(requester);
    std::ostringstream responseSS;
    for(auto it = list.begin(); it != list.end(); it++) {
      responseSS << it->getName()
                 << " " << it->getNbCopies()
                 << " " << it->getCreationLog().user.uid
                 << " " << it->getCreationLog().user.gid
                 << " " << it->getCreationLog().host
                 << " " << it->getCreationLog().time
                 << " " << it->getCreationLog().comment << std::endl;
    }
    m_data = responseSS.str();
  }
  else {
    m_data = help.str();
  }
}
  
//------------------------------------------------------------------------------
// xCom_listongoingarchivals
//------------------------------------------------------------------------------
void XrdProFile::xCom_listongoingarchivals(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " loa/listongoingarchivals --tapepool/-t <tapepool_name>" << std::endl;
  std::string tapePool = getOptionValue(tokens, "-t", "--tapepool");
  std::ostringstream responseSS;
  if(tapePool.empty()) {
    auto poolList = m_scheduler->getArchiveRequests(requester);  
    for(auto pool = poolList.begin(); pool != poolList.end(); pool++) {
      for(auto request = pool->second.begin(); request!=pool->second.end(); request++) {
        responseSS << pool->first.getName()
                   << " " << request->getRemoteFile()
                   << " " << request->getArchiveFile()
                   << " " << request->getCopyNb()
                   << " " << request->getPriority()
                   << " " << request->getRequester().getUser().uid
                   << " " << request->getRequester().getUser().gid
                   << " " << request->getRequester().getHost()
                   << " " << request->getCreationTime() << std::endl;
      }    
    }  
  }
  else {
    auto requestList = m_scheduler->getArchiveRequests(requester, tapePool);    
    for(auto request = requestList.begin(); request!=requestList.end(); request++) {
      responseSS << tapePool
                 << " " << request->getRemoteFile()
                 << " " << request->getArchiveFile()
                 << " " << request->getCopyNb()
                 << " " << request->getPriority()
                 << " " << request->getRequester().getUser().uid
                 << " " << request->getRequester().getUser().gid
                 << " " << request->getRequester().getHost()
                 << " " << request->getCreationTime() << std::endl;
    }
  }
  m_data = responseSS.str();
}
  
//------------------------------------------------------------------------------
// xCom_listongoingretrievals
//------------------------------------------------------------------------------
void XrdProFile::xCom_listongoingretrievals(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " lor/listongoingretrievals --vid/-v <vid>" << std::endl;
  std::string tapeVid = getOptionValue(tokens, "-v", "--vid");
  std::ostringstream responseSS;
  if(tapeVid.empty()) {
    auto vidList = m_scheduler->getRetrieveRequests(requester);  
    for(auto vid = vidList.begin(); vid != vidList.end(); vid++) {
      for(auto request = vid->second.begin(); request!=vid->second.end(); request++) {
        responseSS << vid->first.getVid()
                   << " " << request->getArchiveFile()
                   << " " << request->getRemoteFile()
                   << " " << request->getCopyNb()
                   << " " << request->getTapeCopy().getVid()
                   << " " << request->getTapeCopy().getBlockId()
                   << " " << request->getTapeCopy().getFseq()
                   << " " << request->getPriority()
                   << " " << request->getRequester().getUser().uid
                   << " " << request->getRequester().getUser().gid
                   << " " << request->getRequester().getHost()
                   << " " << request->getCreationTime() << std::endl;
      }    
    }  
  }
  else {
    auto requestList = m_scheduler->getRetrieveRequests(requester, tapeVid);    
    for(auto request = requestList.begin(); request!=requestList.end(); request++) {
      responseSS << tapeVid
                 << " " << request->getArchiveFile()
                 << " " << request->getRemoteFile()
                 << " " << request->getCopyNb()
                 << " " << request->getTapeCopy().getVid()
                 << " " << request->getTapeCopy().getBlockId()
                 << " " << request->getTapeCopy().getFseq()
                 << " " << request->getPriority()
                 << " " << request->getRequester().getUser().uid
                 << " " << request->getRequester().getUser().gid
                 << " " << request->getRequester().getHost()
                 << " " << request->getCreationTime() << std::endl;
    }
  }
  m_data = responseSS.str();
}
  
//------------------------------------------------------------------------------
// xCom_listpendingarchivals
//------------------------------------------------------------------------------
void XrdProFile::xCom_listpendingarchivals(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " lpa/listpendingarchivals --tapepool/-t <tapepool_name>" << std::endl;
  m_data = "Not implemented yet!";
}
  
//------------------------------------------------------------------------------
// xCom_listpendingretrievals
//------------------------------------------------------------------------------
void XrdProFile::xCom_listpendingretrievals(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " lpr/listpendingretrievals --vid/-v <vid>" << std::endl;
  m_data = "Not implemented yet!";
}
  
//------------------------------------------------------------------------------
// xCom_listdrivestates
//------------------------------------------------------------------------------
void XrdProFile::xCom_listdrivestates(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " lds/listdrivestates" << std::endl;
  m_data = "Not implemented yet!";
}
  
//------------------------------------------------------------------------------
// xCom_liststorageclass
//------------------------------------------------------------------------------
void XrdProFile::xCom_liststorageclass(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " lsc/liststorageclass" << std::endl;
  auto list = m_scheduler->getStorageClasses(requester);
  std::ostringstream responseSS;
  for(auto it = list.begin(); it != list.end(); it++) {
    responseSS << it->getName()
               << " " << it->getNbCopies() << std::endl;
  }
  m_data = responseSS.str();
}
  
//------------------------------------------------------------------------------
// xCom_setstorageclass
//------------------------------------------------------------------------------
void XrdProFile::xCom_setstorageclass(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ssc/setstorageclass <dirpath> <storage_class_name>" << std::endl;
  if(tokens.size()!=4){
    m_data = help.str();
    return;
  }
  m_scheduler->setDirStorageClass(requester, tokens[2], tokens[3]);
}
  
//------------------------------------------------------------------------------
// xCom_clearstorageclass
//------------------------------------------------------------------------------
void XrdProFile::xCom_clearstorageclass(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " csc/clearstorageclass <dirpath>" << std::endl;
  if(tokens.size()!=3){
    m_data = help.str();
    return;
  }
  m_scheduler->clearDirStorageClass(requester, tokens[2]);
}
  
//------------------------------------------------------------------------------
// xCom_mkdir
//------------------------------------------------------------------------------
void XrdProFile::xCom_mkdir(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " mkdir <dirpath>" << std::endl;
  if(tokens.size()!=3){
    m_data = help.str();
    return;
  }
  m_scheduler->createDir(requester, tokens[2], 0777); // we need to set mode appropriately NOT 0777!
}
  
//------------------------------------------------------------------------------
// xCom_rmdir
//------------------------------------------------------------------------------
void XrdProFile::xCom_rmdir(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " rmdir <dirpath>" << std::endl;
  if(tokens.size()!=3){
    m_data = help.str();
    return;
  }
  m_scheduler->deleteDir(requester, tokens[2]);
}
  
//------------------------------------------------------------------------------
// xCom_ls
//------------------------------------------------------------------------------
void XrdProFile::xCom_ls(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ls <dirpath>" << std::endl;
  if(tokens.size()!=3){
    m_data = help.str();
    return;
  }
  auto dirIterator = m_scheduler->getDirContents(requester, tokens[2]);
  std::ostringstream responseSS;
  while(dirIterator.hasMore()) {
    auto dirEntry = dirIterator.next();
    responseSS << (S_ISDIR(dirEntry.getMode()) ? "d" : "-")
               << ((dirEntry.getMode() & S_IRUSR) ? "r" : "-")
               << ((dirEntry.getMode() & S_IWUSR) ? "w" : "-")
               << ((dirEntry.getMode() & S_IXUSR) ? "x" : "-")
               << ((dirEntry.getMode() & S_IRGRP) ? "r" : "-")
               << ((dirEntry.getMode() & S_IWGRP) ? "w" : "-")
               << ((dirEntry.getMode() & S_IXGRP) ? "x" : "-")
               << ((dirEntry.getMode() & S_IROTH) ? "r" : "-")
               << ((dirEntry.getMode() & S_IWOTH) ? "w" : "-")
               << ((dirEntry.getMode() & S_IXOTH) ? "x" : "-")
               << " " << dirEntry.getOwnerId()
               << " " << dirEntry.getGroupId()
               << " " << dirEntry.getStorageClassName()
               << " " << dirEntry.getName() << std::endl;
  }
  m_data = responseSS.str();
}
  
//------------------------------------------------------------------------------
// xCom_archive
//------------------------------------------------------------------------------
void XrdProFile::xCom_archive(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " a/archive <source_file1> [<source_file2> [<source_file3> [...]]] <destination_path>" << std::endl;
  if(tokens.size()<4){
    m_data = help.str();
    return;
  }
  auto src_begin = tokens.begin() + 2; //exclude the program name and the archive command
  auto src_end = tokens.end() - 2; //exclude the destination
  const std::list<std::string> source_files(src_begin, src_end);
  m_scheduler->queueArchiveRequest(requester, source_files, tokens[tokens.size()-1]);
}
  
//------------------------------------------------------------------------------
// xCom_retrieve
//------------------------------------------------------------------------------
void XrdProFile::xCom_retrieve(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " r/retrieve <source_file1> [<source_file2> [<source_file3> [...]]] <destination_path>" << std::endl;
  if(tokens.size()<4){
    m_data = help.str();
    return;
  }
  auto src_begin = tokens.begin() + 2; //exclude the program name and the archive command
  auto src_end = tokens.end() - 2; //exclude the destination
  const std::list<std::string> source_files(src_begin, src_end);
  m_scheduler->queueRetrieveRequest(requester, source_files, tokens[tokens.size()-1]);
}
  
//------------------------------------------------------------------------------
// xCom_deletearchive
//------------------------------------------------------------------------------
void XrdProFile::xCom_deletearchive(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " da/deletearchive <destination_path>" << std::endl;
  if(tokens.size()!=3){
    m_data = help.str();
    return;
  }
  m_scheduler->deleteArchiveRequest(requester, tokens[2]);
}
  
//------------------------------------------------------------------------------
// xCom_cancelretrieval
//------------------------------------------------------------------------------
void XrdProFile::xCom_cancelretrieval(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " cr/cancelretrieval <destination_path>" << std::endl;
  if(tokens.size()!=3){
    m_data = help.str();
    return;
  }
  m_scheduler->deleteRetrieveRequest(requester, tokens[2]);
}

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
  help << programName << " us/user           add/rm/ls/ch" << std::endl;
  help << programName << " tp/tapepool       add/rm/ls/ch" << std::endl;
  help << programName << " ar/archiveroute   add/rm/ls/ch" << std::endl;
  help << programName << " ll/logicallibrary add/rm/ls/ch" << std::endl;
  help << programName << " ta/tape           add/rm/ls/ch/reclaim" << std::endl;
  help << programName << " sc/storageclass   add/rm/ls/ch" << std::endl;
  help << programName << " loa/listongoingarchivals" << std::endl;
  help << programName << " lor/listongoingretrievals" << std::endl;
  help << programName << " lpa/listpendingarchivals" << std::endl;
  help << programName << " lpr/listpendingretrievals" << std::endl;
  help << programName << " lds/listdrivestates" << std::endl;
  help << "" << std::endl;
  help << "CTA USER commands:" << std::endl;
  help << "" << std::endl;
  help << "For most commands there is a short version and a long one." << std::endl;
  help << "" << std::endl;
  help << programName << " lsc/liststorageclass" << std::endl;
  help << programName << " ssc/setstorageclass" << std::endl;
  help << programName << " csc/clearstorageclass" << std::endl;
  help << programName << " mkdir" << std::endl;
  help << programName << " rmdir" << std::endl;
  help << programName << " ls" << std::endl;
  help << programName << " a/archive" << std::endl;
  help << programName << " r/retrieve" << std::endl;
  help << programName << " da/deletearchive" << std::endl;
  help << programName << " cr/cancelretrieval" << std::endl;
  return help.str();
}