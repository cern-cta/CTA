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
#include <time.h>

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
  std::cout << "Request received from client. Username: " << client->name << " uid: " << pwd.pw_uid << " gid: " << pwd.pw_gid << std::endl;
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
  for(auto it=tokens.cbegin(); it!=tokens.cend(); it++) {
    if(optionShortName == *it || optionLongName == *it) {
      auto it_next=it+1;
      if(it_next!=tokens.cend() && it_next->find("-")!=0) {
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
  for(auto it=tokens.cbegin(); it!=tokens.cend(); it++) {
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
  std::stringstream uid_ss(uid_s);
  int uid = 0;
  uid_ss >> uid;
  adminUser.setUid(uid);
  std::stringstream gid_ss(gid_s);
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
    std::stringstream uid_ss(uid_s);
    int uid = 0;
    uid_ss >> uid;
    adminUser.setUid(uid);
    std::stringstream gid_ss(gid_s);
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
      std::vector<std::string> header = {"uid","gid","c.uid","c.gid","c.host","c.time","m.uid","m.gid","m.host","m.time","comment"};
      responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
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
       << "\tch  --name/-n <host_name> --comment/-m <\"comment\">" << std::endl
       << "\trm  --name/-n <host_name>" << std::endl
       << "\tls" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string hostname = getOptionValue(tokens, "-n", "--name");
    if(hostname.empty()) {
      m_data = help.str();
      return;
    }
    if("add" == tokens[2] || "ch" == tokens[2]) {
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if(comment.empty()) {
        m_data = help.str();
        return;
      }
      if("add" == tokens[2]) { //add
        m_scheduler->createAdminHost(requester, hostname, comment);
      }
      else { //ch
        m_scheduler->modifyAdminHostComment(requester, hostname, comment);
      }
    }
    else { //rm
      m_scheduler->deleteAdminHost(requester, hostname);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::AdminHost> list= m_scheduler->getAdminHosts(requester);
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"hostname","c.uid","c.gid","c.host","c.time","m.uid","m.gid","m.host","m.time","comment"};
      responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->getName());
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
// xCom_tapepool
//------------------------------------------------------------------------------
void XrdProFile::xCom_tapepool(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " tp/tapepool add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <tapepool_name> --partialtapesnumber/-p <number_of_partial_tapes> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <tapepool_name> [--partialtapesnumber/-p <number_of_partial_tapes>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <tapepool_name>" << std::endl
       << "\tls" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name");
    if(name.empty()) {
      m_data = help.str();
      return;
    }
    if("add" == tokens[2]) { //add
      std::string ptn_s = getOptionValue(tokens, "-p", "--partialtapesnumber");
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if(comment.empty()||ptn_s.empty()) {
        m_data = help.str();
        return;
      }
      std::stringstream ptn_ss(ptn_s);
      uint64_t ptn = 0;
      ptn_ss >> ptn;
      m_scheduler->createTapePool(requester, name, ptn, comment);
    }
    else if("ch" == tokens[2]) { //ch
      std::string ptn_s = getOptionValue(tokens, "-p", "--partialtapesnumber");
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if(comment.empty()&&ptn_s.empty()) {
        m_data = help.str();
        return;
      }
      if(!comment.empty()) {
        m_scheduler->modifyTapePoolComment(requester, name, comment);
      }
      if(!ptn_s.empty()) {
        std::stringstream ptn_ss(ptn_s);
        uint64_t ptn = 0;
        ptn_ss >> ptn;
        m_scheduler->modifyTapePoolNbPartialTapes(requester, name, ptn);
      }
    }
    else { //rm
      m_scheduler->deleteTapePool(requester, name);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::TapePool> list= m_scheduler->getTapePools(requester);
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"name","# partial tapes","c.uid","c.gid","c.host","c.time","m.uid","m.gid","m.host","m.time","comment"};
      responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->getName());
        currentRow.push_back(std::to_string((unsigned long long)it->getNbPartialTapes()));
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
// xCom_archiveroute
//------------------------------------------------------------------------------
void XrdProFile::xCom_archiveroute(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ar/archiveroute add/ch/rm/ls:" << std::endl
       << "\tadd --storageclass/-s <storage_class_name> --copynb/-c <copy_number> --tapepool/-t <tapepool_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --storageclass/-s <storage_class_name> --copynb/-c <copy_number> [--tapepool/-t <tapepool_name>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --storageclass/-s <storage_class_name> --copynb/-c <copy_number>" << std::endl
       << "\tls" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string scn = getOptionValue(tokens, "-s", "--storageclass");
    std::string cn_s = getOptionValue(tokens, "-c", "--copynb");
    if(scn.empty()||cn_s.empty()) {
      m_data = help.str();
      return;
    }    
    std::stringstream cn_ss(cn_s);
    uint64_t cn = 0;
    cn_ss >> cn;
    if("add" == tokens[2]) { //add
      std::string tapepool = getOptionValue(tokens, "-t", "--tapepool");
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if(comment.empty()||tapepool.empty()) {
        m_data = help.str();
        return;
      }
      m_scheduler->createArchiveRoute(requester, scn, cn, tapepool, comment);
    }
    else if("ch" == tokens[2]) { //ch
      std::string tapepool = getOptionValue(tokens, "-t", "--tapepool");
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if(comment.empty()&&tapepool.empty()) {
        m_data = help.str();
        return;
      }
      if(!comment.empty()) {
        m_scheduler->modifyArchiveRouteComment(requester, scn, cn, comment);
      }
      if(!tapepool.empty()) {
        m_scheduler->modifyArchiveRouteTapePoolName(requester, scn, cn, tapepool);
      }
    }
    else { //rm
      m_scheduler->deleteArchiveRoute(requester, scn, cn);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::ArchiveRoute> list= m_scheduler->getArchiveRoutes(requester);
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"storage class","copy number","tapepool","c.uid","c.gid","c.host","c.time","m.uid","m.gid","m.host","m.time","comment"};
      responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->getStorageClassName());
        currentRow.push_back(std::to_string((unsigned long long)it->getCopyNb()));
        currentRow.push_back(it->getTapePoolName());
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
// xCom_logicallibrary
//------------------------------------------------------------------------------
void XrdProFile::xCom_logicallibrary(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ll/logicallibrary add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <logical_library_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <logical_library_name> --comment/-m <\"comment\">" << std::endl
       << "\trm  --name/-n <logical_library_name>" << std::endl
       << "\tls" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string hostname = getOptionValue(tokens, "-n", "--name");
    if(hostname.empty()) {
      m_data = help.str();
      return;
    }
    if("add" == tokens[2] || "ch" == tokens[2]) {
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if(comment.empty()) {
        m_data = help.str();
        return;
      }
      if("add" == tokens[2]) { //add
        m_scheduler->createLogicalLibrary(requester, hostname, comment);
      }
      else { //ch
        m_scheduler->modifyLogicalLibraryComment(requester, hostname, comment);
      }
    }
    else { //rm
      m_scheduler->deleteLogicalLibrary(requester, hostname);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::LogicalLibrary> list= m_scheduler->getLogicalLibraries(requester);
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"name","c.uid","c.gid","c.host","c.time","m.uid","m.gid","m.host","m.time","comment"};
      responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->getName());
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
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2] || "reclaim" == tokens[2] || "label" == tokens[2]) {
    std::string vid = getOptionValue(tokens, "-v", "--vid");
    if(vid.empty()) {
      m_data = help.str();
      return;
    }
    if("add" == tokens[2]) { //add
      std::string logicallibrary = getOptionValue(tokens, "-l", "--logicallibrary");
      std::string tapepool = getOptionValue(tokens, "-t", "--tapepool");
      std::string capacity_s = getOptionValue(tokens, "-c", "--capacity");
      if(logicallibrary.empty()||tapepool.empty()||capacity_s.empty()) {
        m_data = help.str();
        return;
      }
      std::stringstream capacity_ss(capacity_s);
      int capacity = 0;
      capacity_ss >> capacity;
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      bool disabled=false;
      bool full=false;
      if((hasOption(tokens, "-e", "--enabled") && hasOption(tokens, "-d", "--disabled")) || (hasOption(tokens, "-f", "--free") && hasOption(tokens, "-F", "--full"))) {
        m_data = help.str();
        return;
      }
      disabled=hasOption(tokens, "-d", "--disabled");
      full=hasOption(tokens, "-F", "--full");
      m_scheduler->createTape(requester, vid, logicallibrary, tapepool, capacity, disabled, full, comment);
    }
    else if("ch" == tokens[2]) { //ch
      std::string logicallibrary = getOptionValue(tokens, "-l", "--logicallibrary");
      std::string tapepool = getOptionValue(tokens, "-t", "--tapepool");
      std::string capacity_s = getOptionValue(tokens, "-c", "--capacity");
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if(comment.empty() && logicallibrary.empty() && tapepool.empty() && capacity_s.empty() && !hasOption(tokens, "-e", "--enabled")
              && !hasOption(tokens, "-d", "--disabled") && !hasOption(tokens, "-f", "--free") && !hasOption(tokens, "-F", "--full")) {
        m_data = help.str();
        return;
      }
      if((hasOption(tokens, "-e", "--enabled") && hasOption(tokens, "-d", "--disabled")) || (hasOption(tokens, "-f", "--free") && hasOption(tokens, "-F", "--full"))) {
        m_data = help.str();
        return;
      }
      if(!logicallibrary.empty()) {
        m_scheduler->modifyTapeLogicalLibraryName(requester, vid, logicallibrary);
      }
      if(!tapepool.empty()) {
        m_scheduler->modifyTapeTapePoolName(requester, vid, tapepool);
      }
      if(!capacity_s.empty()) {
        std::stringstream capacity_ss(capacity_s);
        uint64_t capacity = 0;
        capacity_ss >> capacity;
        m_scheduler->modifyTapeCapacityInBytes(requester, vid, capacity);
      }
      if(!comment.empty()) {
        m_scheduler->modifyTapeComment(requester, vid, comment);
      }
      if(hasOption(tokens, "-e", "--enabled")) {
        m_scheduler->setTapeDisabled(requester, vid, false);
      }
      if(hasOption(tokens, "-d", "--disabled")) {
        m_scheduler->setTapeDisabled(requester, vid, true);
      }
      if(hasOption(tokens, "-f", "--free")) {
        m_scheduler->setTapeFull(requester, vid, false);
      }
      if(hasOption(tokens, "-F", "--full")) {
        m_scheduler->setTapeFull(requester, vid, true);
      }
    }
    else if("reclaim" == tokens[2]) { //reclaim
      m_scheduler->reclaimTape(requester, vid);
    }
    else if("label" == tokens[2]) { //label
      m_scheduler->labelTape(requester, vid, hasOption(tokens, "-f", "--force"), hasOption(tokens, "-l", "--lbp"), getOptionValue(tokens, "-t", "--tag"));
    }
    else { //rm
      m_scheduler->deleteTape(requester, vid);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::string vid = getOptionValue(tokens, "-v", "--vid");
    std::string logicallibrary = getOptionValue(tokens, "-l", "--logicallibrary");
    std::string tapepool = getOptionValue(tokens, "-t", "--tapepool");
    std::string capacity = getOptionValue(tokens, "-c", "--capacity");
    if((hasOption(tokens, "-e", "--enabled") && hasOption(tokens, "-d", "--disabled")) 
            || (hasOption(tokens, "-f", "--free") && hasOption(tokens, "-F", "--full")) 
            || (hasOption(tokens, "-b", "--busy") && hasOption(tokens, "-n", "--notbusy"))) {
      m_data = help.str();
      return;
    }
    std::string disabled="";
    std::string full="";
    std::string busy="";
    if(hasOption(tokens, "-e", "--enabled")) {
      disabled = "false";
    }
    if(hasOption(tokens, "-d", "--disabled")) {
      disabled = "true";
    }
    if(hasOption(tokens, "-f", "--free")) {
      full = "false";
    }
    if(hasOption(tokens, "-F", "--full")) {
      full = "true";
    }
    if(hasOption(tokens, "-b", "--busy")) {
      busy = "false";
    }
    if(hasOption(tokens, "-n", "--notbusy")) {
      busy = "true";
    }
    std::list<cta::common::dataStructures::Tape> list= m_scheduler->getTapes(requester, vid, logicallibrary, tapepool, capacity, disabled, full, busy);
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"vid","logical library","tapepool","capacity","occupancy","last fseq","busy","full","disabled","c.uid","c.gid","c.host","c.time","m.uid","m.gid","m.host","m.time","comment"};
      responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->getVid());
        currentRow.push_back(it->getLogicalLibraryName());
        currentRow.push_back(it->getTapePoolName());
        currentRow.push_back(std::to_string((unsigned long long)it->getCapacityInBytes()));
        currentRow.push_back(std::to_string((unsigned long long)it->getDataOnTapeInBytes()));
        currentRow.push_back(std::to_string((unsigned long long)it->getLastFSeq()));
        if(it->getBusy()) currentRow.push_back("true"); else currentRow.push_back("false");
        if(it->getFull()) currentRow.push_back("true"); else currentRow.push_back("false");
        if(it->getDisabled()) currentRow.push_back("true"); else currentRow.push_back("false");
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
// xCom_storageclass
//------------------------------------------------------------------------------
void XrdProFile::xCom_storageclass(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " sc/storageclass add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <storage_class_name> --copynb/-c <number_of_tape_copies> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <storage_class_name> [--copynb/-c <number_of_tape_copies>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <storage_class_name>" << std::endl
       << "\tls" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string scn = getOptionValue(tokens, "-n", "--name");
    if(scn.empty()) {
      m_data = help.str();
      return;
    }  
    if("add" == tokens[2]) { //add
      std::string cn_s = getOptionValue(tokens, "-c", "--copynb");
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if(comment.empty()||cn_s.empty()) {
        m_data = help.str();
        return;
      }  
      std::stringstream cn_ss(cn_s);
      uint64_t cn = 0;
      cn_ss >> cn;
      m_scheduler->createStorageClass(requester, scn, cn, comment);
    }
    else if("ch" == tokens[2]) { //ch
      std::string cn_s = getOptionValue(tokens, "-c", "--copynb");
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if(comment.empty()&&cn_s.empty()) {
        m_data = help.str();
        return;
      }
      if(!comment.empty()) {
        m_scheduler->modifyStorageClassComment(requester, scn, comment);
      }
      if(!cn_s.empty()) {  
        std::stringstream cn_ss(cn_s);
        uint64_t cn = 0;
        cn_ss >> cn;
        m_scheduler->modifyStorageClassNbCopies(requester, scn, cn);
      }
    }
    else { //rm
      m_scheduler->deleteStorageClass(requester, scn);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::StorageClass> list= m_scheduler->getStorageClasses(requester);
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"storage class","number of copies","c.uid","c.gid","c.host","c.time","m.uid","m.gid","m.host","m.time","comment"};
      responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->getName());
        currentRow.push_back(std::to_string((unsigned long long)it->getNbCopies()));
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
// xCom_user
//------------------------------------------------------------------------------
void XrdProFile::xCom_user(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " us/user add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <user_name> --group/-g <group_name> --usergroup/-u <user_group_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <user_name> --group/-g <group_name> [--usergroup/-u <user_group_name>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <user_name> --group/-g <group_name>" << std::endl
       << "\tls" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string user = getOptionValue(tokens, "-n", "--name");
    std::string group = getOptionValue(tokens, "-g", "--group");
    if(user.empty()||group.empty()) {
      m_data = help.str();
      return;
    }  
    if("add" == tokens[2]) { //add
      std::string usergroup = getOptionValue(tokens, "-u", "--usergroup");
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if(comment.empty()||usergroup.empty()) {
        m_data = help.str();
        return;
      }
      m_scheduler->createUser(requester, user, group, usergroup, comment);
    }
    else if("ch" == tokens[2]) { //ch
      std::string usergroup = getOptionValue(tokens, "-u", "--usergroup");
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if(comment.empty()&&usergroup.empty()) {
        m_data = help.str();
        return;
      }
      if(!comment.empty()) {
        m_scheduler->modifyUserComment(requester, user, group, comment);
      }
      if(!usergroup.empty()) {
        m_scheduler->modifyUserUserGroup(requester, user, group, usergroup);
      }
    }
    else { //rm
      m_scheduler->deleteUser(requester, user, group);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::User> list= m_scheduler->getUsers(requester);
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"user","group","cta group","c.uid","c.gid","c.host","c.time","m.uid","m.gid","m.host","m.time","comment"};
      responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->getName());
        currentRow.push_back(it->getGroup());
        currentRow.push_back(it->getUserGroupName());
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
// xCom_usergroup
//------------------------------------------------------------------------------
void XrdProFile::xCom_usergroup(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " ug/usergroup add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <usergroup_name> --archivepriority/--ap <priority_value> --minarchivefilesqueued/--af <minFilesQueued> --minarchivebytesqueued/--ab <minBytesQueued> " << std::endl
       << "\t    --minarchiverequestage/--aa <minRequestAge> --retrievepriority/--rp <priority_value> --minretrievefilesqueued/--rf <minFilesQueued> " << std::endl
       << "\t    --minretrievebytesqueued/--rb <minBytesQueued> --minretrieverequestage/--ra <minRequestAge> --maxdrivesallowed/-d <maxDrivesAllowed> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <usergroup_name> [--archivepriority/--ap <priority_value>] [--minarchivefilesqueued/--af <minFilesQueued>] [--minarchivebytesqueued/--ab <minBytesQueued>] " << std::endl
       << "\t   [--minarchiverequestage/--aa <minRequestAge>] [--retrievepriority/--rp <priority_value>] [--minretrievefilesqueued/--rf <minFilesQueued>] " << std::endl
       << "\t   [--minretrievebytesqueued/--rb <minBytesQueued>] [--minretrieverequestage/--ra <minRequestAge>] [--maxdrivesallowed/-d <maxDrivesAllowed>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <usergroup_name>" << std::endl
       << "\tls" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string group = getOptionValue(tokens, "-n", "--name");
    if(group.empty()) {
      m_data = help.str();
      return;
    }
    if("add" == tokens[2] || "ch" == tokens[2]) {      
      std::string archivepriority_s = getOptionValue(tokens, "--ap", "--archivepriority");
      std::string minarchivefilesqueued_s = getOptionValue(tokens, "--af", "--minarchivefilesqueued");
      std::string minarchivebytesqueued_s = getOptionValue(tokens, "--ab", "--minarchivebytesqueued");
      std::string minarchiverequestage_s = getOptionValue(tokens, "--aa", "--minarchiverequestage");
      std::string retrievepriority_s = getOptionValue(tokens, "--rp", "--retrievepriority");
      std::string minretrievefilesqueued_s = getOptionValue(tokens, "--rf", "--minretrievefilesqueued");
      std::string minretrievebytesqueued_s = getOptionValue(tokens, "--rb", "--minretrievebytesqueued");
      std::string minretrieverequestage_s = getOptionValue(tokens, "--ra", "--minretrieverequestage");
      std::string maxdrivesallowed_s = getOptionValue(tokens, "-d", "--maxdrivesallowed");
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if("add" == tokens[2]) { //add
        if(archivepriority_s.empty()||minarchivefilesqueued_s.empty()||minarchivebytesqueued_s.empty()||minarchiverequestage_s.empty()||retrievepriority_s.empty()
                ||minretrievefilesqueued_s.empty()||minretrievebytesqueued_s.empty()||minretrieverequestage_s.empty()||maxdrivesallowed_s.empty()||comment.empty()) {
          m_data = help.str();
          return;
        }
        uint64_t archivepriority; std::stringstream archivepriority_ss; archivepriority_ss << archivepriority_s; archivepriority_ss >> archivepriority;
        uint64_t minarchivefilesqueued; std::stringstream minarchivefilesqueued_ss; minarchivefilesqueued_ss << minarchivefilesqueued_s; minarchivefilesqueued_ss >> minarchivefilesqueued;
        uint64_t minarchivebytesqueued; std::stringstream minarchivebytesqueued_ss; minarchivebytesqueued_ss << minarchivebytesqueued_s; minarchivebytesqueued_ss >> minarchivebytesqueued;
        uint64_t minarchiverequestage; std::stringstream minarchiverequestage_ss; minarchiverequestage_ss << minarchiverequestage_s; minarchiverequestage_ss >> minarchiverequestage;
        uint64_t retrievepriority; std::stringstream retrievepriority_ss; retrievepriority_ss << retrievepriority_s; retrievepriority_ss >> retrievepriority;
        uint64_t minretrievefilesqueued; std::stringstream minretrievefilesqueued_ss; minretrievefilesqueued_ss << minretrievefilesqueued_s; minretrievefilesqueued_ss >> minretrievefilesqueued;
        uint64_t minretrievebytesqueued; std::stringstream minretrievebytesqueued_ss; minretrievebytesqueued_ss << minretrievebytesqueued_s; minretrievebytesqueued_ss >> minretrievebytesqueued;
        uint64_t minretrieverequestage; std::stringstream minretrieverequestage_ss; minretrieverequestage_ss << minretrieverequestage_s; minretrieverequestage_ss >> minretrieverequestage;
        uint64_t maxdrivesallowed; std::stringstream maxdrivesallowed_ss; maxdrivesallowed_ss << maxdrivesallowed_s; maxdrivesallowed_ss >> maxdrivesallowed;
        m_scheduler->createUserGroup(requester, group, archivepriority, minarchivefilesqueued, minarchivebytesqueued, minarchiverequestage, retrievepriority, minretrievefilesqueued, minretrievebytesqueued, minretrieverequestage, maxdrivesallowed, comment);
      }
      else if("ch" == tokens[2]) { //ch
        if(archivepriority_s.empty()&&minarchivefilesqueued_s.empty()&&minarchivebytesqueued_s.empty()&&minarchiverequestage_s.empty()&&retrievepriority_s.empty()
                &&minretrievefilesqueued_s.empty()&&minretrievebytesqueued_s.empty()&&minretrieverequestage_s.empty()&&maxdrivesallowed_s.empty()&&comment.empty()) {
          m_data = help.str();
          return;
        }
        if(!archivepriority_s.empty()) {
          uint64_t archivepriority; std::stringstream archivepriority_ss; archivepriority_ss << archivepriority_s; archivepriority_ss >> archivepriority;
          m_scheduler->modifyUserGroupArchivePriority(requester, group, archivepriority);
        }
        if(!minarchivefilesqueued_s.empty()) {
          uint64_t minarchivefilesqueued; std::stringstream minarchivefilesqueued_ss; minarchivefilesqueued_ss << minarchivefilesqueued_s; minarchivefilesqueued_ss >> minarchivefilesqueued;
          m_scheduler->modifyUserGroupArchiveMinFilesQueued(requester, group, minarchivefilesqueued);
        }
        if(!minarchivebytesqueued_s.empty()) {
          uint64_t minarchivebytesqueued; std::stringstream minarchivebytesqueued_ss; minarchivebytesqueued_ss << minarchivebytesqueued_s; minarchivebytesqueued_ss >> minarchivebytesqueued;
          m_scheduler->modifyUserGroupArchiveMinBytesQueued(requester, group, minarchivebytesqueued);
        }
        if(!minarchiverequestage_s.empty()) {
          uint64_t minarchiverequestage; std::stringstream minarchiverequestage_ss; minarchiverequestage_ss << minarchiverequestage_s; minarchiverequestage_ss >> minarchiverequestage;
          m_scheduler->modifyUserGroupArchiveMinRequestAge(requester, group, minarchiverequestage);
        }
        if(!retrievepriority_s.empty()) {
          uint64_t retrievepriority; std::stringstream retrievepriority_ss; retrievepriority_ss << retrievepriority_s; retrievepriority_ss >> retrievepriority;
          m_scheduler->modifyUserGroupRetrievePriority(requester, group, retrievepriority);
        }
        if(!minretrievefilesqueued_s.empty()) {
          uint64_t minretrievefilesqueued; std::stringstream minretrievefilesqueued_ss; minretrievefilesqueued_ss << minretrievefilesqueued_s; minretrievefilesqueued_ss >> minretrievefilesqueued;
          m_scheduler->modifyUserGroupRetrieveMinFilesQueued(requester, group, minretrievefilesqueued);
        }
        if(!minretrievebytesqueued_s.empty()) {
          uint64_t minretrievebytesqueued; std::stringstream minretrievebytesqueued_ss; minretrievebytesqueued_ss << minretrievebytesqueued_s; minretrievebytesqueued_ss >> minretrievebytesqueued;
          m_scheduler->modifyUserGroupRetrieveMinBytesQueued(requester, group, minretrievebytesqueued);
        }
        if(!minretrieverequestage_s.empty()) {
          uint64_t minretrieverequestage; std::stringstream minretrieverequestage_ss; minretrieverequestage_ss << minretrieverequestage_s; minretrieverequestage_ss >> minretrieverequestage;
          m_scheduler->modifyUserGroupRetrieveMinRequestAge(requester, group, minretrieverequestage);
        }
        if(!maxdrivesallowed_s.empty()) {
          uint64_t maxdrivesallowed; std::stringstream maxdrivesallowed_ss; maxdrivesallowed_ss << maxdrivesallowed_s; maxdrivesallowed_ss >> maxdrivesallowed;
          m_scheduler->modifyUserGroupMaxDrivesAllowed(requester, group, maxdrivesallowed);
        }
        if(!comment.empty()) {
          m_scheduler->modifyUserGroupComment(requester, group, comment);
        }
      }
    }
    else { //rm
      m_scheduler->deleteUserGroup(requester, group);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::UserGroup> list= m_scheduler->getUserGroups(requester);
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"cta group","a.priority","a.minFiles","a.minBytes","a.minAge","r.priority","r.minFiles","r.minBytes","r.minAge","MaxDrives","c.uid","c.gid","c.host","c.time","m.uid","m.gid","m.host","m.time","comment"};
      responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->getName());
        currentRow.push_back(std::to_string((unsigned long long)it->getArchive_priority()));
        currentRow.push_back(std::to_string((unsigned long long)it->getArchive_minFilesQueued()));
        currentRow.push_back(std::to_string((unsigned long long)it->getArchive_minBytesQueued()));
        currentRow.push_back(std::to_string((unsigned long long)it->getArchive_minRequestAge()));
        currentRow.push_back(std::to_string((unsigned long long)it->getRetrieve_priority()));
        currentRow.push_back(std::to_string((unsigned long long)it->getRetrieve_minFilesQueued()));
        currentRow.push_back(std::to_string((unsigned long long)it->getRetrieve_minBytesQueued()));
        currentRow.push_back(std::to_string((unsigned long long)it->getRetrieve_minRequestAge()));
        currentRow.push_back(std::to_string((unsigned long long)it->getMaxDrivesAllowed()));
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
// xCom_dedication
//------------------------------------------------------------------------------
void XrdProFile::xCom_dedication(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " de/dedication add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <drive_name> [--readonly/-r or --writeonly/-w] [--usergroup/-u <user_group_name>] [--vid/-v <tape_vid>] [--tag/-t <tag_name>] --from/-f <DD/MM/YYYY> --until/-u <DD/MM/YYYY> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <drive_name> [--readonly/-r or --writeonly/-w] [--usergroup/-u <user_group_name>] [--vid/-v <tape_vid>] [--tag/-t <tag_name>] [--from/-f <DD/MM/YYYY>] [--until/-u <DD/MM/YYYY>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <drive_name>" << std::endl
       << "\tls" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string drive = getOptionValue(tokens, "-n", "--name");
    if(drive.empty()) {
      m_data = help.str();
      return;
    } 
    if("add" == tokens[2] || "ch" == tokens[2]) {
      bool readonly = hasOption(tokens, "-r", "--readonly");
      bool writeonly = hasOption(tokens, "-w", "--writeonly");
      std::string usergroup = getOptionValue(tokens, "-u", "--usergroup");
      std::string vid = getOptionValue(tokens, "-v", "--vid");
      std::string tag = getOptionValue(tokens, "-t", "--tag");
      std::string from_s = getOptionValue(tokens, "-f", "--from");
      std::string until_s = getOptionValue(tokens, "-u", "--until");
      std::string comment = getOptionValue(tokens, "-m", "--comment");
      if("add" == tokens[2]) { //add
        if(comment.empty()||from_s.empty()||until_s.empty()||(usergroup.empty()&&vid.empty()&&tag.empty()&&!readonly&&!writeonly)||(readonly&&writeonly)) {
          m_data = help.str();
          return;
        }
        struct tm time;
        if(NULL==strptime(from_s.c_str(), "%d/%m/%Y", &time)) {
          m_data = help.str();
          return;
        }
        time_t from = mktime(&time);  // timestamp in current timezone
        if(NULL==strptime(until_s.c_str(), "%d/%m/%Y", &time)) {
          m_data = help.str();
          return;
        }
        time_t until = mktime(&time);  // timestamp in current timezone
        cta::common::dataStructures::DedicationType type=cta::common::dataStructures::DedicationType::readwrite;
        if(readonly) {
          type=cta::common::dataStructures::DedicationType::readonly;
        }
        else if(writeonly) {
          type=cta::common::dataStructures::DedicationType::writeonly;
        }
        m_scheduler->createDedication(requester, drive, type, usergroup, tag, vid, from, until, comment);
      }
      else if("ch" == tokens[2]) { //ch
        if((comment.empty()&&from_s.empty()&&until_s.empty()&&usergroup.empty()&&vid.empty()&&tag.empty()&&!readonly&&!writeonly)||(readonly&&writeonly)) {
          m_data = help.str();
          return;
        }
        if(!comment.empty()) {
          m_scheduler->modifyDedicationComment(requester, drive, comment);
        }
        if(!from_s.empty()) {
          struct tm time;
          if(NULL==strptime(from_s.c_str(), "%d/%m/%Y", &time)) {
            m_data = help.str();
            return;
          }
          time_t from = mktime(&time);  // timestamp in current timezone
          m_scheduler->modifyDedicationFrom(requester, drive, from);
        }
        if(!until_s.empty()) {
          struct tm time;
          if(NULL==strptime(until_s.c_str(), "%d/%m/%Y", &time)) {
            m_data = help.str();
            return;
          }
          time_t until = mktime(&time);  // timestamp in current timezone
          m_scheduler->modifyDedicationUntil(requester, drive, until);
        }
        if(!usergroup.empty()) {
          m_scheduler->modifyDedicationUserGroup(requester, drive, usergroup);
        }
        if(!vid.empty()) {
          m_scheduler->modifyDedicationVid(requester, drive, vid);
        }
        if(!tag.empty()) {
          m_scheduler->modifyDedicationTag(requester, drive, tag);
        }
        if(readonly) {
          m_scheduler->modifyDedicationType(requester, drive, cta::common::dataStructures::DedicationType::readonly);          
        }
        if(writeonly) {
          m_scheduler->modifyDedicationType(requester, drive, cta::common::dataStructures::DedicationType::writeonly);
        }
      }
    }
    else { //rm
      m_scheduler->deleteDedication(requester, drive);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::Dedication> list= m_scheduler->getDedications(requester);
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"drive","type","vid","user group","tag","from","until","c.uid","c.gid","c.host","c.time","m.uid","m.gid","m.host","m.time","comment"};
      responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        std::string type_s;
        switch(it->getDedicationType()) {
          case cta::common::dataStructures::DedicationType::readonly:
            type_s = "readonly";
            break;
          case cta::common::dataStructures::DedicationType::writeonly:
            type_s = "writeonly";
            break;
          default:
            type_s = "readwrite";
            break;
        }
        char timebuffer[100];
        time_t fromtimeStamp=it->getFromTimestamp();
        time_t untiltimeStamp=it->getUntilTimestamp();
        struct tm *tm = localtime(&fromtimeStamp);
        if(strftime(timebuffer, 100, "%d/%m/%Y", tm)==0) {
          m_data = "Error converting \"from\" date!\n";
        }
        std::string fromTime_s(timebuffer);
        tm = localtime(&untiltimeStamp);
        if(strftime(timebuffer, 100, "%d/%m/%Y", tm)==0) {
          m_data = "Error converting \"to\" date!\n";
        }
        std::string untilTime_s(timebuffer);        
        currentRow.push_back(it->getDriveName());
        currentRow.push_back(type_s);
        currentRow.push_back(it->getVid());
        currentRow.push_back(it->getUserGroup());
        currentRow.push_back(it->getTag());
        currentRow.push_back(fromTime_s);
        currentRow.push_back(untilTime_s);
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
// xCom_repack
//------------------------------------------------------------------------------
void XrdProFile::xCom_repack(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " re/repack add/rm/ls/err:" << std::endl
       << "\tadd --vid/-v <vid> [--expandandrepack/-d or --justexpand/-e or --justrepack/-r] [--tag/-t <tag_name>]" << std::endl
       << "\trm  --vid/-v <vid>" << std::endl
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
       << "\tadd [--vid/-v <vid>] [--complete/-c] [--partial/-p <number_of_files_per_tape>] [--tag/-t <tag_name>]" << std::endl
       << "\trm  [--vid/-v <vid>]" << std::endl
       << "\tls  [--vid/-v <vid>]" << std::endl
       << "\terr --vid/-v <vid>" << std::endl;
}

//------------------------------------------------------------------------------
// xCom_archivefile
//------------------------------------------------------------------------------
void XrdProFile::xCom_archivefile(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " af/archivefile ls [--id/-I <archive_file_id>] [--copynb/-c <copy_no>] [--vid/-v <vid>] [--tapepool/-t <tapepool>] "
          "[--owner/-o <owner>] [--group/-g <group>] [--storageclass/-s <class>] [--path/-p <fullpath>] [--summary/-S] [--all/-a] (default gives error)" << std::endl;
  if("ls" == tokens[2]) { //ls
    std::string id = getOptionValue(tokens, "-I", "--id");
    std::string copynb = getOptionValue(tokens, "-c", "--copynb");
    std::string tapepool = getOptionValue(tokens, "-t", "--tapepool");
    std::string vid = getOptionValue(tokens, "-v", "--vid");
    std::string owner = getOptionValue(tokens, "-o", "--owner");
    std::string group = getOptionValue(tokens, "-g", "--group");
    std::string storageclass = getOptionValue(tokens, "-s", "--storageclass");
    std::string path = getOptionValue(tokens, "-p", "--path");
    bool summary = hasOption(tokens, "-S", "--summary");
    bool all = hasOption(tokens, "-a", "--all");
    if(!all && (id.empty() && copynb.empty() && tapepool.empty() && vid.empty() && owner.empty() && group.empty() && storageclass.empty() && path.empty())) {
      m_data = help.str();
      return;
    }
    if(!summary) {
      std::list<cta::common::dataStructures::ArchiveFile> list=m_scheduler->getArchiveFiles(requester, id, copynb, tapepool, vid, owner, group, storageclass, path);
      if(list.size()>0) {
        std::vector<std::vector<std::string>> responseTable;
        std::vector<std::string> header = {"id","copy no","vid","fseq","block id","EOS id","size","checksum type","checksum value","storage class","owner","group","instance","path"};
        responseTable.push_back(header);    
        for(auto it = list.cbegin(); it != list.cend(); it++) {
          for(auto jt = it->getTapeCopies().cbegin(); jt != it->getTapeCopies().cend(); jt++) {
            std::vector<std::string> currentRow;
            currentRow.push_back(it->getArchiveFileID());
            currentRow.push_back(std::to_string((unsigned long long)jt->first));
            currentRow.push_back(jt->second.getVid());
            currentRow.push_back(std::to_string((unsigned long long)jt->second.getFSeq()));
            currentRow.push_back(std::to_string((unsigned long long)jt->second.getBlockId()));
            currentRow.push_back(it->getEosFileID());
            currentRow.push_back(std::to_string((unsigned long long)it->getFileSize()));
            currentRow.push_back(it->getChecksumType());
            currentRow.push_back(it->getChecksumValue());
            currentRow.push_back(it->getStorageClass());
            currentRow.push_back(it->getDrData().getDrOwner());
            currentRow.push_back(it->getDrData().getDrGroup());
            currentRow.push_back(it->getDrData().getDrInstance());
            currentRow.push_back(it->getDrData().getDrPath());          
            responseTable.push_back(currentRow);
          }
        }
        m_data = formatResponse(responseTable);
      }
    }
    else { //summary
      cta::common::dataStructures::ArchiveFileSummary summary=m_scheduler->getArchiveFileSummary(requester, id, copynb, tapepool, vid, owner, group, storageclass, path);
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"total number of files","total size"};
      std::vector<std::string> row = {std::to_string((unsigned long long)summary.getTotalFiles()),std::to_string((unsigned long long)summary.getTotalBytes())};
      responseTable.push_back(header);
      responseTable.push_back(row);
    }
  }
  else {
    m_data = help.str();
  }
}

//------------------------------------------------------------------------------
// xCom_test
//------------------------------------------------------------------------------
void XrdProFile::xCom_test(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &requester) {
  std::stringstream help;
  help << tokens[0] << " te/test read/write (to be run on an empty self-dedicated drive; it is a synchronous command that returns performance stats and errors; all locations are local to the tapeserver):" << std::endl
       << "\tread  --drive/-d <drive_name> --vid/-v <vid> --firstfseq/-f <first_fseq> --lastfseq/-l <last_fseq> --checkchecksum/-c --retries_per_file/-r <number_of_retries_per_file> [--outputdir/-o <output_dir> or --null/-n] [--tag/-t <tag_name>]" << std::endl
       << "\twrite --drive/-d <drive_name> --vid/-v <vid> --number/-n <number_of_files> [--size/-s <file_size> or --randomsize/-r] [--zero/-z or --urandom/-u] [--tag/-t <tag_name>]" << std::endl;
       << "\twrite --drive/-d <drive_name> --vid/-v <vid> [--file/-f <filename> or --filelist/-f <filename_with_file_list>] [--tag/-t <tag_name>]" << std::endl;
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
// getGenericHelp
//------------------------------------------------------------------------------
std::string XrdProFile::getGenericHelp(const std::string &programName) const {
  std::stringstream help;
  help << "CTA ADMIN commands:" << std::endl;
  help << "" << std::endl;
  help << "For each command there is a short version and a long one. Subcommands (add/rm/ls/ch/reclaim) do not have short versions." << std::endl;
  help << "" << std::endl;
  help << programName << " admin/ad          add/ch/rm/ls"               << std::endl;
  help << programName << " adminhost/ah      add/ch/rm/ls"               << std::endl;
  help << programName << " archivefile/af    ls"                         << std::endl;
  help << programName << " archiveroute/ar   add/ch/rm/ls"               << std::endl;
  help << programName << " bootstrap/bs"                                 << std::endl;
  help << programName << " dedication/de     add/ch/rm/ls"               << std::endl;
  help << programName << " drive/dr          up/down"                    << std::endl;
  help << programName << " listdrivestates/lds"                          << std::endl;
  help << programName << " listpendingarchives/lpa"                      << std::endl;
  help << programName << " listpendingretrieves/lpr"                     << std::endl;
  help << programName << " logicallibrary/ll add/ch/rm/ls"               << std::endl;
  help << programName << " reconcile/rc"                                 << std::endl;
  help << programName << " repack/re         add/rm/ls/err"              << std::endl;
  help << programName << " shrink/sh"                                    << std::endl;
  help << programName << " storageclass/sc   add/ch/rm/ls"               << std::endl;
  help << programName << " tape/ta           add/ch/rm/reclaim/ls/label" << std::endl;
  help << programName << " tapepool/tp       add/ch/rm/ls"               << std::endl;
  help << programName << " test/te           read/write"                 << std::endl;
  help << programName << " user/us           add/ch/rm/ls"               << std::endl;
  help << programName << " usergroup/ug      add/ch/rm/ls"               << std::endl;
  help << programName << " verify/ve         add/rm/ls/err"              << std::endl;
  help << "" << std::endl;
  help << "CTA EOS commands:" << std::endl;
  help << "" << std::endl;
  help << "For each command there is a short version and a long one." << std::endl;
  help << "" << std::endl;
  help << programName << " archive/a"                                    << std::endl;
  help << programName << " cancelretrieve/cr"                            << std::endl;
  help << programName << " deletearchive/da"                             << std::endl;
  help << programName << " liststorageclass/lsc"                         << std::endl;
  help << programName << " retrieve/r"                                   << std::endl;
  help << programName << " updatefileinfo/ufi"                           << std::endl;
  return help.str();
}

}}
