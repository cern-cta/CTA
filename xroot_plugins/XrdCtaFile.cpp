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
cta::SecurityIdentity XrdProFile::checkClient(const XrdSecEntity *client) {
// TEMPORARILY commented out host check for demo purposes:
//  if(!client || !client->host || strncmp(client->host, "localhost", 9))
//  {
//    throw cta::exception::Exception(std::string(__FUNCTION__)+": [ERROR] operation possible only from localhost");
//  }
  cta::SecurityIdentity requester;
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
  requester = cta::SecurityIdentity(cta::UserIdentity(pwd.pw_uid, pwd.pw_gid),
    client->host);
  return requester;
}

//------------------------------------------------------------------------------
// commandDispatcher
//------------------------------------------------------------------------------
void XrdProFile::dispatchCommand(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::string command(tokens[1]);
  
  if     ("ad"    == command || "admin"                 == command) {xCom_admin(tokens, requester);}
  else if("ah"    == command || "adminhost"             == command) {xCom_adminhost(tokens, requester);}
  else if("tp"    == command || "tapepool"              == command) {xCom_tapepool(tokens, requester);}
  else if("ar"    == command || "archiveroute"          == command) {xCom_archiveroute(tokens, requester);}
  else if("ll"    == command || "logicallibrary"        == command) {xCom_logicallibrary(tokens, requester);}
  else if("ta"    == command || "tape"                  == command) {xCom_tape(tokens, requester);}
  else if("sc"    == command || "storageclass"          == command) {xCom_storageclass(tokens, requester);}
  else if("lpa"   == command || "listpendingarchives"   == command) {xCom_listpendingarchives(tokens, requester);}
  else if("lpr"   == command || "listpendingretrieves"  == command) {xCom_listpendingretrieves(tokens, requester);}
  else if("lds"   == command || "listdrivestates"       == command) {xCom_listdrivestates(tokens, requester);}
  
  else if("lsc"   == command || "liststorageclass"      == command) {xCom_liststorageclass(tokens, requester);}
  else if("ufi"   == command || "updatefileinfo"        == command) {xCom_updatefileinfo(tokens, requester);}
  else if("a"     == command || "archive"               == command) {xCom_archive(tokens, requester);}
  else if("r"     == command || "retrieve"              == command) {xCom_retrieve(tokens, requester);}
  else if("da"    == command || "deletearchive"         == command) {xCom_deletearchive(tokens, requester);}
  else if("cr"    == command || "cancelretrieve"        == command) {xCom_cancelretrieve(tokens, requester);}
  
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
    const cta::SecurityIdentity requester = checkClient(client);

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
    if(list.size()>0) {
      responseSS << "\x1b[31;1m"
                 << " " << std::setw(8) << "uid" 
                 << " " << std::setw(8) << "gid"
                 << " " << std::setw(18) << "creator uid" 
                 << " " << std::setw(18) << "creator gid" 
                 << " " << std::setw(30) << "creation host"
                 << " " << std::setw(30) << "creation time"
                 << " " << std::setw(30) << "comment"
                 << "\x1b[0m" << std::endl;
    }
    for(auto it = list.begin(); it != list.end(); it++) {
      std::string timeString(ctime(&(it->getCreationLog().time)));
      timeString=timeString.substr(0,24);//remove the newline
      responseSS << " " << std::setw(8) << it->getUser().uid 
                 << " " << std::setw(8) << it->getUser().gid 
                 << " " << std::setw(18) << it->getCreationLog().user.uid 
                 << " " << std::setw(18) << it->getCreationLog().user.gid 
                 << " " << std::setw(30) << it->getCreationLog().host
                 << " " << std::setw(30) << timeString
                 << " " << std::setw(30) << it->getCreationLog().comment << std::endl;
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
    if(list.size()>0) {
      responseSS << "\x1b[31;1m"
                 << " " << std::setw(17) << "name" 
                 << " " << std::setw(18) << "creator uid" 
                 << " " << std::setw(18) << "creator gid" 
                 << " " << std::setw(30) << "creation host"
                 << " " << std::setw(30) << "creation time"
                 << " " << std::setw(30) << "comment"
                 << "\x1b[0m" << std::endl;
    }
    for(auto it = list.begin(); it != list.end(); it++) {
      std::string timeString(ctime(&(it->creationLog.time)));
      timeString=timeString.substr(0,24);//remove the newline
      responseSS << " " << std::setw(17) << it->name 
                 << " " << std::setw(18) << it->creationLog.user.uid 
                 << " " << std::setw(18) << it->creationLog.user.gid
                 << " " << std::setw(30) << it->creationLog.host 
                 << " " << std::setw(30) << timeString 
                 << " " << std::setw(30) << it->creationLog.comment << std::endl;
    }
    m_data = responseSS.str();
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
    
    //TODO: The following hardcoded parameters should really be given by the user from the CLI. However before doing this we must clarify the scheduler interface and change it accordingly
    cta::MountCriteria immediateMount;
    immediateMount.maxAge = 0;
    immediateMount.maxBytesQueued = 1;
    immediateMount.maxFilesQueued = 1;
    immediateMount.quota = 10;
    m_scheduler->setTapePoolMountCriteria(name, cta::MountCriteriaByDirection(immediateMount, immediateMount));
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
    if(list.size()>0) {
      responseSS << "\x1b[31;1m"
                 << " " << std::setw(18) << "name" 
                 << " " << std::setw(25) << "# partially used tapes"
                 << " " << std::setw(18) << "creator uid" 
                 << " " << std::setw(18) << "creator gid" 
                 << " " << std::setw(30) << "creation host"
                 << " " << std::setw(30) << "creation time"
                 << " " << std::setw(30) << "comment"
                 << "\x1b[0m" << std::endl;
    }
    for(auto it = list.begin(); it != list.end(); it++) {
      std::string timeString(ctime(&(it->creationLog.time)));
      timeString=timeString.substr(0,24);//remove the newline
      responseSS << " " << std::setw(18) << it->name  
                 << " " << std::setw(25) << it->nbPartialTapes
                 << " " << std::setw(18) << it->creationLog.user.uid 
                 << " " << std::setw(18) << it->creationLog.user.gid 
                 << " " << std::setw(30) << it->creationLog.host
                 << " " << std::setw(30) << timeString
                 << " " << std::setw(30) << it->creationLog.comment << std::endl;
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
    m_scheduler->createArchiveRoute(requester, storageClass, copynb, tapePool, comment);
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
//    m_scheduler->modifyArchiveRoute(requester, storageClass, copynb, tapePool, comment);
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
    m_scheduler->deleteArchiveRoute(requester, storageClass, copynb);
  }
  else if("ls" == tokens[2]) {
    auto list = m_scheduler->getArchiveRoutes(requester);
    std::ostringstream responseSS;
    if(list.size()>0) {
      responseSS << "\x1b[31;1m"
                 << " " << std::setw(18) << "name" 
                 << " " << std::setw(8) << "copynb"
                 << " " << std::setw(16) << "tapepool"
                 << " " << std::setw(18) << "creator uid" 
                 << " " << std::setw(18) << "creator gid" 
                 << " " << std::setw(30) << "creation host"
                 << " " << std::setw(30) << "creation time"
                 << " " << std::setw(30) << "comment"
                 << "\x1b[0m" << std::endl;
    }
    for(auto it = list.begin(); it != list.end(); it++) {
      std::string timeString(ctime(&(it->creationLog.time)));
      timeString=timeString.substr(0,24);//remove the newline
      responseSS << " " << std::setw(18) << it->storageClassName
                 << " " << std::setw(8) << it->copyNb
                 << " " << std::setw(16) << it->tapePoolName
                 << " " << std::setw(18) << it->creationLog.user.uid
                 << " " << std::setw(18) << it->creationLog.user.gid
                 << " " << std::setw(30) << it->creationLog.host
                 << " " << std::setw(30) << timeString
                 << " " << std::setw(30) << it->creationLog.comment << std::endl;
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
    if(list.size()>0) {
      responseSS << "\x1b[31;1m"
                 << " " << std::setw(17) << "name" 
                 << " " << std::setw(18) << "creator uid" 
                 << " " << std::setw(18) << "creator gid" 
                 << " " << std::setw(30) << "creation host"
                 << " " << std::setw(30) << "creation time"
                 << " " << std::setw(30) << "comment"
                 << "\x1b[0m" << std::endl;
    }
    for(auto it = list.begin(); it != list.end(); it++) {
      std::string timeString(ctime(&(it->creationLog.time)));
      timeString=timeString.substr(0,24);//remove the newline
      responseSS << " " << std::setw(17) << it->name
                 << " " << std::setw(18) << it->creationLog.user.uid 
                 << " " << std::setw(18) << it->creationLog.user.gid
                 << " " << std::setw(30) << it->creationLog.host
                 << " " << std::setw(30) << timeString
                 << " " << std::setw(30) << it->creationLog.comment << std::endl;
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
  help << "\tadd     --vid/-v <vid> --logicallibrary/-l <logical_library_name> --tapepool/-t <tapepool_name> \\"<<std::endl
       <<"\t\t--capacity/-c <capacity_in_bytes> --comment/-m <\"comment\">" << std::endl;
  help << "\tch      --vid/-v <vid> --logicallibrary/-l <logical_library_name> --tapepool/-t <tapepool_name> \\"<<std::endl
       <<"\t\t--capacity/-c <capacity_in_bytes> --comment/-m <\"comment\">" << std::endl;
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
    if(list.size()>0) {
      responseSS << "\x1b[31;1m"
                 << " " << std::setw(8) << "vid" 
                 << " " << std::setw(18) << "logical library"
                 << " " << std::setw(18) << "capacity in bytes" 
                 << " " << std::setw(18) << "tapepool" 
                 << " " << std::setw(18) << "used capacity"
                 << " " << std::setw(18) << "creator uid" 
                 << " " << std::setw(18) << "creator gid" 
                 << " " << std::setw(30) << "creation host"
                 << " " << std::setw(30) << "creation time"
                 << " " << std::setw(30) << "comment"
                 << "\x1b[0m" << std::endl;
    }
    for(auto it = list.begin(); it != list.end(); it++) {
      std::string timeString(ctime(&(it->creationLog.time)));
      timeString=timeString.substr(0,24);//remove the newline
      responseSS << " " << std::setw(8) << it->vid
                 << " " << std::setw(18) << it->logicalLibraryName
                 << " " << std::setw(18) << it->capacityInBytes
                 << " " << std::setw(18) << it->tapePoolName
                 << " " << std::setw(18) << it->dataOnTapeInBytes
                 << " " << std::setw(18) << it->creationLog.user.uid 
                 << " " << std::setw(18) << it->creationLog.user.gid 
                 << " " << std::setw(30) << it->creationLog.host
                 << " " << std::setw(30) << timeString
                 << " " << std::setw(30) << it->creationLog.comment << std::endl;
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
  help << "\tadd --name/-n <storage_class_name> --copynb/-c <number_of_tape_copies> --id/-i <unique_class_id> --comment/-m <\"comment\">" << std::endl;
  help << "\tch  --name/-n <storage_class_name> --copynb/-c <number_of_tape_copies> --id/-i <unique_class_id> --comment/-m <\"comment\">" << std::endl;
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
    std::string id_s = getOptionValue(tokens, "-i", "--id");
    if(name.empty()||comment.empty()||copynb_s.empty()||id_s.empty()) {
      m_data = help.str();
      return;
    }
    std::istringstream copynb_ss(copynb_s);
    int copynb = 0;
    copynb_ss >> copynb;
    std::istringstream id_ss(id_s);
    int id = 0;
    id_ss >> id;
    m_scheduler->createStorageClass(requester, name, copynb, id, comment);
  }
  else if("ch" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    std::string comment = getOptionValue(tokens, "-m", "--comment");
    std::string copynb_s = getOptionValue(tokens, "-c", "--copynb");
    std::string id_s = getOptionValue(tokens, "-i", "--id");
    if(name.empty()||comment.empty()||copynb_s.empty()||id_s.empty()) {
      m_data = help.str();
      return;
    }
    std::istringstream copynb_ss(copynb_s);
    int copynb = 0;
    copynb_ss >> copynb;
    std::istringstream id_ss(id_s);
    int id = 0;
    id_ss >> id;
//    m_scheduler->modifyStorageClass(requester, name, copynb, id, comment);
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
    if(list.size()>0) {
      responseSS << "\x1b[31;1m"
                 << " " << std::setw(17) << "name" 
                 << " " << std::setw(8) << "# copies"
                 << " " << std::setw(18) << "creator uid" 
                 << " " << std::setw(18) << "creator gid" 
                 << " " << std::setw(30) << "creation host"
                 << " " << std::setw(30) << "creation time"
                 << " " << std::setw(30) << "comment"
                 << "\x1b[0m" << std::endl;
    }
    for(auto it = list.begin(); it != list.end(); it++) {
      std::string timeString(ctime(&(it->creationLog.time)));
      timeString=timeString.substr(0,24);//remove the newline
      responseSS << " " << std::setw(17) << it->name
                 << " " << std::setw(8) << it->nbCopies
                 << " " << std::setw(18) << it->creationLog.user.uid
                 << " " << std::setw(18) << it->creationLog.user.gid
                 << " " << std::setw(30) << it->creationLog.host
                 << " " << std::setw(30) << timeString
                 << " " << std::setw(30) << it->creationLog.comment << std::endl;
    }
    m_data = responseSS.str();
  }
  else {
    m_data = help.str();
  }
}
  
//------------------------------------------------------------------------------
// xCom_listpendingarchives
//------------------------------------------------------------------------------
void XrdProFile::xCom_listpendingarchives(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " lpa/listpendingarchives [--tapepool/-t <tapepool_name>] [--extended/-x]" << std::endl;
  std::string tapePool = getOptionValue(tokens, "-t", "--tapepool");
  bool extended = hasOption(tokens, "-x", "--extended");
  std::ostringstream responseSS;
  if(tapePool.empty()) {
    auto poolList = m_scheduler->getArchiveRequests(requester);
    if(poolList.size()>0) {
      if(extended) {
        responseSS << "\x1b[31;1m"
                   << " " << std::setw(18) << "tapepool" 
                   << " " << std::setw(30) << "remote filepath" 
                   << " " << std::setw(16) << "size"
                   << " " << std::setw(30) << "cta filepath"
                   << " " << std::setw(8) << "copynb"
                   << " " << std::setw(8) << "priority"
                   << " " << std::setw(18) << "creator uid" 
                   << " " << std::setw(18) << "creator gid" 
                   << " " << std::setw(30) << "creation host"
                   << " " << std::setw(30) << "creation time"
                   << " " << std::setw(30) << "comment"
                   << "\x1b[0m" << std::endl;
      }
      else {
        responseSS << "\x1b[31;1m"
                   << " " << std::setw(18) << "tapepool" 
                   << " " << std::setw(25) << "number of requests" 
                   << " " << std::setw(16) << "total size"
                   << "\x1b[0m" << std::endl;
      }
    }  
    for(auto pool = poolList.begin(); pool != poolList.end(); pool++) {
      uint64_t numberOfRequests=0;
      uint64_t totalSize=0;
      for(auto request = pool->second.begin(); request!=pool->second.end(); request++) {
        if(extended) {
          std::string timeString(ctime(&(request->creationLog.time)));
          timeString=timeString.substr(0,24);//remove the newline
          responseSS << " " << std::setw(18) << pool->first.name
                     << " " << std::setw(30) << request->remoteFile.path.getRaw()
                     << " " << std::setw(16) << request->remoteFile.status.size
                     << " " << std::setw(30) << request->archiveFile
                     << " " << std::setw(8) << request->copyNb
                     << " " << std::setw(8) << request->priority
                     << " " << std::setw(18) << request->creationLog.user.uid
                     << " " << std::setw(18) << request->creationLog.user.gid
                     << " " << std::setw(30) << request->creationLog.host
                     << " " << std::setw(30) << timeString 
                     << " " << std::setw(30) << request->creationLog.comment << std::endl;
        }
        numberOfRequests++;
        totalSize+=request->remoteFile.status.size;
      }
      if(!extended) {
        responseSS << " " << std::setw(18) << pool->first.name
                   << " " << std::setw(25) << numberOfRequests 
                   << " " << std::setw(16) << totalSize << std::endl;
      }
    }  
  }
  else {
    uint64_t numberOfRequests=0;
    uint64_t totalSize=0;
    auto requestList = m_scheduler->getArchiveRequests(requester, tapePool); 
    if(requestList.size()>0) {
      if(extended) {
        responseSS << "\x1b[31;1m"
                   << " " << std::setw(18) << "tapepool" 
                   << " " << std::setw(30) << "remote filepath" 
                   << " " << std::setw(16) << "size"
                   << " " << std::setw(30) << "cta filepath"
                   << " " << std::setw(8) << "copynb"
                   << " " << std::setw(8) << "priority"
                   << " " << std::setw(18) << "creator uid" 
                   << " " << std::setw(18) << "creator gid" 
                   << " " << std::setw(30) << "creation host"
                   << " " << std::setw(30) << "creation time"
                   << " " << std::setw(30) << "comment"
                   << "\x1b[0m" << std::endl;
      }
      else {
        responseSS << "\x1b[31;1m"
                   << " " << std::setw(18) << "tapepool" 
                   << " " << std::setw(25) << "number of requests" 
                   << " " << std::setw(16) << "total size"
                   << "\x1b[0m" << std::endl;
      }
    }     
    for(auto request = requestList.begin(); request!=requestList.end(); request++) {
      if(extended) {
        std::string timeString(ctime(&(request->creationLog.time)));
        timeString=timeString.substr(0,24);//remove the newline
        responseSS << " " << std::setw(18) << tapePool
                   << " " << std::setw(30) << request->remoteFile.path.getRaw()
                   << " " << std::setw(16) << request->remoteFile.status.size
                   << " " << std::setw(30) << request->archiveFile
                   << " " << std::setw(8) << request->copyNb
                   << " " << std::setw(8) << request->priority
                   << " " << std::setw(18) << request->creationLog.user.uid
                   << " " << std::setw(18) << request->creationLog.user.gid
                   << " " << std::setw(30) << request->creationLog.host
                   << " " << std::setw(30) << timeString 
                   << " " << std::setw(30) << request->creationLog.comment << std::endl;
      }
      numberOfRequests++;
      totalSize+=request->remoteFile.status.size;
    }
    if(!extended) {
      responseSS << " " << std::setw(18) << tapePool
                 << " " << std::setw(25) << numberOfRequests 
                 << " " << std::setw(16) << totalSize << std::endl;
    }
  }
  m_data = responseSS.str();
}
  
//------------------------------------------------------------------------------
// xCom_listpendingretrieves
//------------------------------------------------------------------------------
void XrdProFile::xCom_listpendingretrieves(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " lpr/listpendingretrieves [--vid/-v <vid>] [--extended/-x]" << std::endl;
  std::string tapeVid = getOptionValue(tokens, "-v", "--vid");
  bool extended = hasOption(tokens, "-x", "--extended");
  std::ostringstream responseSS;
  if(tapeVid.empty()) {
    auto vidList = m_scheduler->getRetrieveRequests(requester);
    if(vidList.size()>0) {
      if(extended) {
        responseSS << "\x1b[31;1m"
                   << " " << std::setw(8) << "vid" 
                   << " " << std::setw(30) << "cta filepath" 
                   << " " << std::setw(16) << "size"
                   << " " << std::setw(50) << "remote filepath"
                   << " " << std::setw(8) << "copynb"
                   << " " << std::setw(8) << "block id"
                   << " " << std::setw(8) << "fseq"
                   << " " << std::setw(8) << "priority"
                   << " " << std::setw(18) << "creator uid" 
                   << " " << std::setw(18) << "creator gid" 
                   << " " << std::setw(30) << "creation host"
                   << " " << std::setw(30) << "creation time"
                   << " " << std::setw(30) << "comment"
                   << "\x1b[0m" << std::endl;
      }
      else {
        responseSS << "\x1b[31;1m"
                   << " " << std::setw(8) << "vid" 
                   << " " << std::setw(25) << "number of requests" 
                   << " " << std::setw(16) << "total size"
                   << "\x1b[0m" << std::endl;
      }
    }  
    for(auto vid = vidList.begin(); vid != vidList.end(); vid++) {
      uint64_t numberOfRequests=0;
      uint64_t totalSize=0;
      for(auto request = vid->second.begin(); request!=vid->second.end(); request++) {
        if(extended) {
          std::stringstream blockIdSS;
          std::stringstream fseqSS;
          // Find the tape copy for the active copy number
          for (auto l=request->tapeCopies.begin(); l!=request->tapeCopies.end(); l++) {
            if (l->copyNb == request->activeCopyNb) {
              blockIdSS << l->blockId;
              fseqSS << l->fSeq;
            }
          }
          std::string timeString(ctime(&(request->creationLog.time)));
          timeString=timeString.substr(0,24);//remove the newline
          responseSS << " " << std::setw(8) << vid->first.vid
                     << " " << std::setw(30) << request->archiveFile.path
                     << " " << std::setw(16) << request->archiveFile.size
                     << " " << std::setw(50) << request->remoteFile
                     << " " << std::setw(8) << request->activeCopyNb
                     << " " << std::setw(8) << blockIdSS.str()
                     << " " << std::setw(8) << fseqSS.str()
                     << " " << std::setw(8) << request->priority
                     << " " << std::setw(18) << request->creationLog.user.uid
                     << " " << std::setw(18) << request->creationLog.user.gid
                     << " " << std::setw(30) << request->creationLog.host
                     << " " << std::setw(30) << timeString 
                     << " " << std::setw(30) << request->creationLog.comment << std::endl;
        }
        numberOfRequests++;
        totalSize+=request->archiveFile.size;
      }
      if(!extended) {
        responseSS << " " << std::setw(8) << vid->first.vid
                   << " " << std::setw(25) << numberOfRequests
                   << " " << std::setw(16) << totalSize << std::endl;
      }
    }  
  }
  else {
    uint64_t numberOfRequests=0;
    uint64_t totalSize=0;    
    auto requestList = m_scheduler->getRetrieveRequests(requester, tapeVid);
    if(requestList.size()>0) {
      if(extended) {
        responseSS << "\x1b[31;1m"
                   << " " << std::setw(8) << "vid" 
                   << " " << std::setw(30) << "cta filepath" 
                   << " " << std::setw(16) << "size"
                   << " " << std::setw(50) << "remote filepath"
                   << " " << std::setw(8) << "copynb"
                   << " " << std::setw(8) << "block id"
                   << " " << std::setw(8) << "fseq"
                   << " " << std::setw(8) << "priority"
                   << " " << std::setw(18) << "creator uid" 
                   << " " << std::setw(18) << "creator gid" 
                   << " " << std::setw(30) << "creation host"
                   << " " << std::setw(30) << "creation time"
                   << " " << std::setw(30) << "comment"
                   << "\x1b[0m" << std::endl;
      }
      else {
        responseSS << "\x1b[31;1m"
                   << " " << std::setw(8) << "vid" 
                   << " " << std::setw(25) << "number of requests" 
                   << " " << std::setw(16) << "total size"
                   << "\x1b[0m" << std::endl;
      }
    }
    for(auto request = requestList.begin(); request!=requestList.end(); request++) {
      if(extended) {
        std::stringstream blockIdSS;
        std::stringstream fseqSS;
        // Find the tape copy for the active copy number
        for (auto l=request->tapeCopies.begin(); l!=request->tapeCopies.end(); l++) {
          if (l->copyNb == request->activeCopyNb) {
            blockIdSS << l->blockId;
            fseqSS << l->fSeq;
          }
        }
        std::string timeString(ctime(&(request->creationLog.time)));
        timeString=timeString.substr(0,24);//remove the newline
        responseSS << " " << std::setw(8) << tapeVid
                   << " " << std::setw(30) << request->archiveFile.path
                   << " " << std::setw(16) << request->archiveFile.size
                   << " " << std::setw(50) << request->remoteFile
                   << " " << std::setw(8) << request->activeCopyNb
                   << " " << std::setw(8) << blockIdSS.str()
                   << " " << std::setw(8) << fseqSS.str()
                   << " " << std::setw(8) << request->priority
                   << " " << std::setw(18) << request->creationLog.user.uid
                   << " " << std::setw(18) << request->creationLog.user.gid
                   << " " << std::setw(30) << request->creationLog.host
                   << " " << std::setw(30) << timeString 
                   << " " << std::setw(30) << request->creationLog.comment << std::endl;
      }
      numberOfRequests++;
      totalSize+=request->archiveFile.size;
    }
    if(!extended) {
      responseSS << " " << std::setw(8) << tapeVid
                 << " " << std::setw(25) << numberOfRequests
                 << " " << std::setw(16) << totalSize << std::endl;
    }
  }
  m_data = responseSS.str();
}

std::string fromDriveStatusToString(cta::common::DriveStatus status) {
  using cta::common::DriveStatus;
  switch(status) {
    case DriveStatus::CleaningUp:
      return "CleaningUp";
    case DriveStatus::Down:
      return "Down";
    case DriveStatus::DrainingToDisk:
      return "DrainingToDisk";
    case DriveStatus::Mounting:
      return "Mounting";
    case DriveStatus::Starting:
      return "Starting";
    case DriveStatus::Transfering:
      return "Transferring";
    case DriveStatus::Unloading:
      return "Unloading";
    case DriveStatus::Unmounting:
      return "Unmounting";
    case DriveStatus::Up:
      return "Up";
    default:
      return "UnknownState";
  }
}

time_t getDurationSinceStatusBegin(const cta::common::DriveState &state) {
  time_t now = time(0);
  switch(state.status) {
  using cta::common::DriveStatus;
    case DriveStatus::CleaningUp:
      return now-state.cleanupStartTime;
    case DriveStatus::Down:
      return now-state.downOrUpStartTime;
    case DriveStatus::DrainingToDisk:
      return now-state.drainingStartTime;
    case DriveStatus::Mounting:
      return now-state.mountStartTime;
    case DriveStatus::Starting:
      return now-state.startStartTime;
    case DriveStatus::Transfering:
      return now-state.transferStartTime;
    case DriveStatus::Unloading:
      return now-state.unloadStartTime;
    case DriveStatus::Unmounting:
      return now-state.unmountStartTime;
    case DriveStatus::Up:
      return now-state.downOrUpStartTime;
    default:
      return 0;
  }
}
  
//------------------------------------------------------------------------------
// xCom_listdrivestates
//------------------------------------------------------------------------------
void XrdProFile::xCom_listdrivestates(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " lds/listdrivestates" << std::endl;
  auto list = m_scheduler->getDriveStates(requester);
  std::ostringstream responseSS;
  if(list.size()>0) {
    responseSS << "\x1b[31;1m"
               << " " << std::setw(18) << "logical library"
               << " " << std::setw(15) << "drive name"
               << " " << std::setw(15) << "status"
               << " " << std::setw(10) << "#secs"
               << " " << std::setw(12) << "mount type"
               << " " << std::setw(8) << "vid"
//               << " " << std::setw(18) << "bytes transfered"
//               << " " << std::setw(18) << "files transfered"
//               << " " << std::setw(15) << "bandwidth"
//               << " " << std::setw(12) << "session id"
               << "\x1b[0m" << std::endl;
  }
  for(auto it = list.begin(); it != list.end(); it++) {
    responseSS << " " << std::setw(18) << it->logicalLibrary
               << " " << std::setw(15) << it->name  
               << " " << std::setw(15) << fromDriveStatusToString(it->status)
               << " " << std::setw(10) << getDurationSinceStatusBegin(*it)
               << " " << std::setw(12) << cta::MountType::toString(it->mountType)
               << " " << std::setw(8) << it->currentVid
//               << " " << std::setw(18) << it->bytesTransferedInSession
//               << " " << std::setw(18) << it->filesTransferedInSession
//               << " " << std::setw(15) << it->latestBandwidth
//               << " " << std::setw(12) << it->sessionId 
               << std::endl;
  }
  m_data = responseSS.str();
}
  
//------------------------------------------------------------------------------
// xCom_liststorageclass
//------------------------------------------------------------------------------
void XrdProFile::xCom_liststorageclass(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " lsc/liststorageclass <VO>" << std::endl;
  if(tokens.size()!=3){
    m_data = help.str();
    return;
  }
  auto list = m_scheduler->getStorageClasses(requester);
  std::ostringstream responseSS;
  if(list.size()>0) {
    responseSS << "\x1b[31;1m"
               << " " << std::setw(17) << "name" 
               << " " << std::setw(8) << "# copies"
               << "\x1b[0m" << std::endl;
  }
  for(auto it = list.begin(); it != list.end(); it++) {
    std::string timeString(ctime(&(it->creationLog.time)));
    timeString=timeString.substr(0,24);//remove the newline
    responseSS << " " << std::setw(17) << it->name
               << " " << std::setw(8) << it->nbCopies << std::endl;
  }
  m_data = responseSS.str();
}
  
//------------------------------------------------------------------------------
// xCom_setstorageclass
//------------------------------------------------------------------------------
void XrdProFile::xCom_updatefileinfo(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ufi/updatefileinfo [--noencoding/-n] <CTA_ArchiveFileID> <storage_class> <DR_instance> <DR_path> <DR_owner> <DR_group> <DR_blob>" << std::endl;
  if((hasOption(tokens, "-n", "--noencoding") && tokens.size()!=10) || (!hasOption(tokens, "-n", "--noencoding") && tokens.size()!=9)){
    m_data = help.str();
    return;
  }
  m_scheduler->setDirStorageClass(requester, tokens[2], tokens[3]);
}
  
//------------------------------------------------------------------------------
// xCom_archive
//------------------------------------------------------------------------------
void XrdProFile::xCom_archive(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " a/archive [--noencoding/-n] <src_URL> <size> <checksum> <storage_class> <DR_instance> <DR_path> <DR_owner> <DR_group> <DR_blob>" << std::endl;
  if((hasOption(tokens, "-n", "--noencoding") && tokens.size()!=12) || (!hasOption(tokens, "-n", "--noencoding") && tokens.size()!=11)){
    m_data = help.str();
    return;
  }
  auto src_begin = tokens.begin() + 2; //exclude the program name and the archive command
  auto src_end = tokens.end() - 1; //exclude the destination
  const std::list<std::string> source_files(src_begin, src_end);
  m_scheduler->queueArchiveRequest(requester, source_files, tokens[tokens.size()-1]);
}
  
//------------------------------------------------------------------------------
// xCom_retrieve
//------------------------------------------------------------------------------
void XrdProFile::xCom_retrieve(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " r/retrieve [--noencoding/-n] <CTA_ArchiveFileID> <dst_URL> <DR_instance> <DR_path> <DR_owner> <DR_group> <DR_blob>" << std::endl;
  if((hasOption(tokens, "-n", "--noencoding") && tokens.size()!=10) || (!hasOption(tokens, "-n", "--noencoding") && tokens.size()!=9)){
    m_data = help.str();
    return;
  }
  auto src_begin = tokens.begin() + 2; //exclude the program name and the archive command
  auto src_end = tokens.end() - 1; //exclude the destination
  const std::list<std::string> source_files(src_begin, src_end);
  m_scheduler->queueRetrieveRequest(requester, source_files, tokens[tokens.size()-1]);
}
  
//------------------------------------------------------------------------------
// xCom_deletearchive
//------------------------------------------------------------------------------
void XrdProFile::xCom_deletearchive(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " da/deletearchive <CTA_ArchiveFileID>" << std::endl;
  if(tokens.size()!=3){
    m_data = help.str();
    return;
  }
  m_scheduler->deleteArchiveRequest(requester, tokens[2]);
}
  
//------------------------------------------------------------------------------
// xCom_cancelretrieve
//------------------------------------------------------------------------------
void XrdProFile::xCom_cancelretrieve(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " cr/cancelretrieve [--noencoding/-n] <CTA_ArchiveFileID> <dst_URL> <DR_instance> <DR_path> <DR_owner> <DR_group> <DR_blob>" << std::endl;
  if((hasOption(tokens, "-n", "--noencoding") && tokens.size()!=10) || (!hasOption(tokens, "-n", "--noencoding") && tokens.size()!=9)){
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
