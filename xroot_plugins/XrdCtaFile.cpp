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
#include "common/Configuration.hpp"

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
cta::common::dataStructures::SecurityIdentity XrdCtaFile::checkClient(const XrdSecEntity *client) {
  cta::common::dataStructures::SecurityIdentity cliIdentity;
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
  cta::common::dataStructures::UserIdentity user;
  user.name=client->name;
  user.group=client->grps;
  cliIdentity.user=user;
  cliIdentity.host=client->host;
  return cliIdentity;
}

//------------------------------------------------------------------------------
// commandDispatcher
//------------------------------------------------------------------------------
int XrdCtaFile::dispatchCommand(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::string command(tokens[1]);
  
  std::vector<std::string> adminCommands = {"bs","bootstrap","ad","admin","ah","adminhost","tp","tapepool","ar","archiveroute","ll","logicallibrary",
          "ta","tape","sc","storageclass","us","user","mg","mountpolicy","de","dedication","re","repack","sh","shrink","ve","verify",
          "af","archivefile","te","test","dr","drive","rc","reconcile","lpa","listpendingarchives","lpr","listpendingretrieves","lds","listdrivestates"};
  
  if (std::find(adminCommands.begin(), adminCommands.end(), command) != adminCommands.end()) {
    m_scheduler->authorizeCliIdentity(cliIdentity);
  }
  
  if     ("bs"   == command || "bootstrap"              == command) {return xCom_bootstrap(tokens, cliIdentity);}
  else if("ad"   == command || "admin"                  == command) {return xCom_admin(tokens, cliIdentity);}
  else if("ah"   == command || "adminhost"              == command) {return xCom_adminhost(tokens, cliIdentity);}
  else if("tp"   == command || "tapepool"               == command) {return xCom_tapepool(tokens, cliIdentity);}
  else if("ar"   == command || "archiveroute"           == command) {return xCom_archiveroute(tokens, cliIdentity);}
  else if("ll"   == command || "logicallibrary"         == command) {return xCom_logicallibrary(tokens, cliIdentity);}
  else if("ta"   == command || "tape"                   == command) {return xCom_tape(tokens, cliIdentity);}
  else if("sc"   == command || "storageclass"           == command) {return xCom_storageclass(tokens, cliIdentity);}
  else if("us"   == command || "user"                   == command) {return xCom_user(tokens, cliIdentity);}
  else if("mg"   == command || "mountpolicy"            == command) {return xCom_mountpolicy(tokens, cliIdentity);}
  else if("de"   == command || "dedication"             == command) {return xCom_dedication(tokens, cliIdentity);}
  else if("re"   == command || "repack"                 == command) {return xCom_repack(tokens, cliIdentity);}
  else if("sh"   == command || "shrink"                 == command) {return xCom_shrink(tokens, cliIdentity);}
  else if("ve"   == command || "verify"                 == command) {return xCom_verify(tokens, cliIdentity);}
  else if("af"   == command || "archivefile"            == command) {return xCom_archivefile(tokens, cliIdentity);}
  else if("te"   == command || "test"                   == command) {return xCom_test(tokens, cliIdentity);}
  else if("dr"   == command || "drive"                  == command) {return xCom_drive(tokens, cliIdentity);}
  else if("rc"   == command || "reconcile"              == command) {return xCom_reconcile(tokens, cliIdentity);}
  else if("lpa"  == command || "listpendingarchives"    == command) {return xCom_listpendingarchives(tokens, cliIdentity);}
  else if("lpr"  == command || "listpendingretrieves"   == command) {return xCom_listpendingretrieves(tokens, cliIdentity);}
  else if("lds"  == command || "listdrivestates"        == command) {return xCom_listdrivestates(tokens, cliIdentity);}
  else if("a"    == command || "archive"                == command) {return xCom_archive(tokens, cliIdentity);}
  else if("r"    == command || "retrieve"               == command) {return xCom_retrieve(tokens, cliIdentity);}
  else if("da"   == command || "deletearchive"          == command) {return xCom_deletearchive(tokens, cliIdentity);}
  else if("cr"   == command || "cancelretrieve"         == command) {return xCom_cancelretrieve(tokens, cliIdentity);}
  else if("ufi"  == command || "updatefileinfo"         == command) {return xCom_updatefileinfo(tokens, cliIdentity);}
  else if("ufsc" == command || "updatefilestorageclass" == command) {return xCom_updatefilestorageclass(tokens, cliIdentity);}
  else if("lsc"  == command || "liststorageclass"       == command) {return xCom_liststorageclass(tokens, cliIdentity);}
  
  else {
    m_data = getGenericHelp(tokens[0]);
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
}

//------------------------------------------------------------------------------
// decode
//------------------------------------------------------------------------------
std::string XrdCtaFile::decode(const std::string msg) const {
  std::string ret;
  CryptoPP::StringSource ss1(msg, true, new CryptoPP::Base64Decoder(new CryptoPP::StringSink(ret)));
  return ret;
}

//------------------------------------------------------------------------------
// open
//------------------------------------------------------------------------------
int XrdCtaFile::open(const char *fileName, XrdSfsFileOpenMode openMode, mode_t createMode, const XrdSecEntity *client, const char *opaque) {
  try {
    const cta::common::dataStructures::SecurityIdentity cliIdentity = checkClient(client);

    if(!strlen(fileName)) { //this should never happen
      m_data = getGenericHelp("");
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
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
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }
    if(tokens.size() < 2) {
      m_data = getGenericHelp(tokens[0]);
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }    
    return dispatchCommand(tokens, cliIdentity);
  } catch (cta::exception::Exception &ex) {
    m_data = "[ERROR] CTA exception caught: ";
    m_data += ex.getMessageValue();
    m_data += "\n"; 
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  } catch (std::exception &ex) {
    m_data = "[ERROR] Exception caught: ";
    m_data += ex.what();
    m_data += "\n"; 
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  } catch (...) {
    m_data = "[ERROR] Unknown exception caught!";
    m_data += "\n"; 
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
int XrdCtaFile::close() {
  return SFS_OK;
}

//------------------------------------------------------------------------------
// fctl
//------------------------------------------------------------------------------
int XrdCtaFile::fctl(const int cmd, const char *args, XrdOucErrInfo &eInfo) {  
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// FName
//------------------------------------------------------------------------------
const char* XrdCtaFile::FName() {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return NULL;
}

//------------------------------------------------------------------------------
// getMmap
//------------------------------------------------------------------------------
int XrdCtaFile::getMmap(void **Addr, off_t &Size) {
  *Addr = const_cast<char *>(m_data.c_str());
  Size = m_data.length();
  return SFS_OK; //change to "return SFS_ERROR;" in case the read function below is wanted, in that case uncomment the lines in that function.
}

//------------------------------------------------------------------------------
// read
//------------------------------------------------------------------------------
XrdSfsXferSize XrdCtaFile::read(XrdSfsFileOffset offset, char *buffer, XrdSfsXferSize size) {
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
XrdSfsXferSize XrdCtaFile::read(XrdSfsFileOffset offset, XrdSfsXferSize size) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return 0;
}

//------------------------------------------------------------------------------
// read
//------------------------------------------------------------------------------
XrdSfsXferSize XrdCtaFile::read(XrdSfsAio *aioparm) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return 0;
}

//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
XrdSfsXferSize XrdCtaFile::write(XrdSfsFileOffset offset, const char *buffer, XrdSfsXferSize size) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return 0;
}

//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
int XrdCtaFile::write(XrdSfsAio *aioparm) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// stat
//------------------------------------------------------------------------------
int XrdCtaFile::stat(struct stat *buf) {
  buf->st_size=m_data.length();
  return SFS_OK;
}

//------------------------------------------------------------------------------
// sync
//------------------------------------------------------------------------------
int XrdCtaFile::sync() {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// sync
//------------------------------------------------------------------------------
int XrdCtaFile::sync(XrdSfsAio *aiop) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// truncate
//------------------------------------------------------------------------------
int XrdCtaFile::truncate(XrdSfsFileOffset fsize) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// getCXinfo
//------------------------------------------------------------------------------
int XrdCtaFile::getCXinfo(char cxtype[4], int &cxrsz) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdCtaFile::XrdCtaFile(
  cta::catalogue::Catalogue *catalogue,
  cta::Scheduler *scheduler,
  const char *user,
  int MonID):
  error(user, MonID),
  m_catalogue(catalogue),
  m_scheduler(scheduler),
  m_data("") {  
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdCtaFile::~XrdCtaFile() {  
}

//------------------------------------------------------------------------------
// replaceAll
//------------------------------------------------------------------------------
void XrdCtaFile::replaceAll(std::string& str, const std::string& from, const std::string& to) const {
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
std::string XrdCtaFile::getOptionValue(const std::vector<std::string> &tokens, const std::string& optionShortName, const std::string& optionLongName, const bool encoded) {
  for(auto it=tokens.cbegin(); it!=tokens.cend(); it++) {
    if(optionShortName == *it || optionLongName == *it) {
      auto it_next=it+1;
      if(it_next!=tokens.cend()) {
        if(!encoded) return *it_next;
        else return decode(*it_next);
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
bool XrdCtaFile::hasOption(const std::vector<std::string> &tokens, const std::string& optionShortName, const std::string& optionLongName) {
  for(auto it=tokens.cbegin(); it!=tokens.cend(); it++) {
    if(optionShortName == *it || optionLongName == *it) {
      return true;
    }
  }
  return false;
}

//------------------------------------------------------------------------------
// timeToString
//------------------------------------------------------------------------------
std::string XrdCtaFile::timeToString(const time_t &time) {
  std::string timeString(ctime(&time));
  timeString=timeString.substr(0,timeString.size()-1); //remove newline
  return timeString;
}

//------------------------------------------------------------------------------
// formatResponse
//------------------------------------------------------------------------------
std::string XrdCtaFile::formatResponse(const std::vector<std::vector<std::string>> &responseTable) {
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
void XrdCtaFile::addLogInfoToResponseRow(std::vector<std::string> &responseRow, const cta::common::dataStructures::EntryLog &creationLog, const cta::common::dataStructures::EntryLog &lastModificationLog) {
  responseRow.push_back(creationLog.user.name);
  responseRow.push_back(creationLog.user.group);
  responseRow.push_back(creationLog.host);
  responseRow.push_back(timeToString(creationLog.time));
  responseRow.push_back(lastModificationLog.user.name);
  responseRow.push_back(lastModificationLog.user.group);
  responseRow.push_back(lastModificationLog.host);
  responseRow.push_back(timeToString(lastModificationLog.time));
}

//------------------------------------------------------------------------------
// stringParameterToUint64
//------------------------------------------------------------------------------
uint64_t XrdCtaFile::stringParameterToUint64(const std::string &parameterName, const std::string &parameterValue) const {
  try {
    return stoull(parameterValue);
  } catch(std::invalid_argument &ex) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" - Parameter: "+parameterName+" ("+parameterValue+") has an invalid argument");
  } catch(std::out_of_range &ex) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" - The value of parameter: "+parameterName+" ("+parameterValue+") is out of range");
  } catch(...) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" - Unknown error while converting parameter: "+parameterName+" ("+parameterValue+") to a uint64_t");
  }
  return 0;
}

//------------------------------------------------------------------------------
// xCom_bootstrap
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_bootstrap(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " bs/bootstrap --user/-u <user> --group/-g <group> --hostname/-h <host_name> --comment/-m <\"comment\">" << std::endl;
  std::string user = getOptionValue(tokens, "-u", "--user", false);
  std::string group = getOptionValue(tokens, "-g", "--group", false);
  std::string hostname = getOptionValue(tokens, "-h", "--hostname", false);
  std::string comment = getOptionValue(tokens, "-m", "--comment", false);
  if(user.empty()||group.empty()||hostname.empty()||comment.empty()) {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  cta::common::dataStructures::UserIdentity adminUser;
  adminUser.name=user;
  adminUser.group=group;
  m_catalogue->createBootstrapAdminAndHostNoAuth(cliIdentity, adminUser, hostname, comment);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_admin
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_admin(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " ad/admin add/ch/rm/ls:" << std::endl
       << "\tadd --user/-u <user> --group/-g <group> --comment/-m <\"comment\">" << std::endl
       << "\tch  --user/-u <user> --group/-g <group> --comment/-m <\"comment\">" << std::endl
       << "\trm  --user/-u <user> --group/-g <group>" << std::endl
       << "\tls  [--header/-h]" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string user = getOptionValue(tokens, "-u", "--user", false);
    std::string group = getOptionValue(tokens, "-g", "--group", false);
    if(user.empty()||group.empty()) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }
    cta::common::dataStructures::UserIdentity adminUser;
    adminUser.name=user;
    adminUser.group=group;
    if("add" == tokens[2] || "ch" == tokens[2]) {
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      if(comment.empty()) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      if("add" == tokens[2]) { //add
        m_catalogue->createAdminUser(cliIdentity, adminUser, comment);
      }
      else { //ch
        m_catalogue->modifyAdminUserComment(cliIdentity, adminUser, comment);
      }
    }
    else { //rm
      m_catalogue->deleteAdminUser(adminUser);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::AdminUser> list= m_catalogue->getAdminUsers();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"user","group","c.user","c.group","c.host","c.time","m.user","m.group","m.host","m.time","comment"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        currentRow.push_back("N/A");
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_adminhost
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_adminhost(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " ah/adminhost add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <host_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <host_name> --comment/-m <\"comment\">" << std::endl
       << "\trm  --name/-n <host_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string hostname = getOptionValue(tokens, "-n", "--name", false);
    if(hostname.empty()) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }
    if("add" == tokens[2] || "ch" == tokens[2]) {
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      if(comment.empty()) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      if("add" == tokens[2]) { //add
        m_catalogue->createAdminHost(cliIdentity, hostname, comment);
      }
      else { //ch
        m_catalogue->modifyAdminHostComment(cliIdentity, hostname, comment);
      }
    }
    else { //rm
      m_catalogue->deleteAdminHost(hostname);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::AdminHost> list= m_catalogue->getAdminHosts();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"hostname","c.name","c.group","c.host","c.time","m.name","m.group","m.host","m.time","comment"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_tapepool
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_tapepool(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " tp/tapepool add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <tapepool_name> --partialtapesnumber/-p <number_of_partial_tapes> [--encryption/-e or --clear/-c] --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <tapepool_name> [--partialtapesnumber/-p <number_of_partial_tapes>] [--encryption/-e or --clear/-c] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <tapepool_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name", false);
    if(name.empty()) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }
    if("add" == tokens[2]) { //add
      std::string ptn_s = getOptionValue(tokens, "-p", "--partialtapesnumber", false);
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      if(comment.empty()||ptn_s.empty()) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      uint64_t ptn = stringParameterToUint64("--partialtapesnumber", ptn_s);
      bool encryption=false;
      if((hasOption(tokens, "-e", "--encryption") && hasOption(tokens, "-c", "--clear"))) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      encryption=hasOption(tokens, "-e", "--encryption");
      m_catalogue->createTapePool(cliIdentity, name, ptn, encryption, comment);
    }
    else if("ch" == tokens[2]) { //ch
      std::string ptn_s = getOptionValue(tokens, "-p", "--partialtapesnumber", false);
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      if(comment.empty()&&ptn_s.empty()) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      if(!comment.empty()) {
        m_catalogue->modifyTapePoolComment(cliIdentity, name, comment);
      }
      if(!ptn_s.empty()) {
        uint64_t ptn = stringParameterToUint64("--partialtapesnumber", ptn_s);
        m_catalogue->modifyTapePoolNbPartialTapes(cliIdentity, name, ptn);
      }
      if(hasOption(tokens, "-e", "--encryption")) {
        m_catalogue->setTapePoolEncryption(cliIdentity, name, true);
      }
      if(hasOption(tokens, "-c", "--clear")) {
        m_catalogue->setTapePoolEncryption(cliIdentity, name, false);
      }
    }
    else { //rm
      m_catalogue->deleteTapePool(name);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::TapePool> list= m_catalogue->getTapePools();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"name","# partial tapes","encrypt","c.name","c.group","c.host","c.time","m.name","m.group","m.host","m.time","comment"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        currentRow.push_back(std::to_string((unsigned long long)it->nbPartialTapes));
        if(it->encryption) currentRow.push_back("true"); else currentRow.push_back("false");
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_archiveroute
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_archiveroute(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " ar/archiveroute add/ch/rm/ls:" << std::endl
       << "\tadd --storageclass/-s <storage_class_name> --copynb/-c <copy_number> --tapepool/-t <tapepool_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --storageclass/-s <storage_class_name> --copynb/-c <copy_number> [--tapepool/-t <tapepool_name>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --storageclass/-s <storage_class_name> --copynb/-c <copy_number>" << std::endl
       << "\tls  [--header/-h]" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string scn = getOptionValue(tokens, "-s", "--storageclass", false);
    std::string cn_s = getOptionValue(tokens, "-c", "--copynb", false);
    if(scn.empty()||cn_s.empty()) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }    
    uint64_t cn = stringParameterToUint64("--copynb", cn_s);
    if("add" == tokens[2]) { //add
      std::string tapepool = getOptionValue(tokens, "-t", "--tapepool", false);
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      if(comment.empty()||tapepool.empty()) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      m_catalogue->createArchiveRoute(cliIdentity, scn, cn, tapepool, comment);
    }
    else if("ch" == tokens[2]) { //ch
      std::string tapepool = getOptionValue(tokens, "-t", "--tapepool", false);
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      if(comment.empty()&&tapepool.empty()) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      if(!comment.empty()) {
        m_catalogue->modifyArchiveRouteComment(cliIdentity, scn, cn, comment);
      }
      if(!tapepool.empty()) {
        m_catalogue->modifyArchiveRouteTapePoolName(cliIdentity, scn, cn, tapepool);
      }
    }
    else { //rm
      m_catalogue->deleteArchiveRoute(scn, cn);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::ArchiveRoute> list= m_catalogue->getArchiveRoutes();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"storage class","copy number","tapepool","c.name","c.group","c.host","c.time","m.name","m.group","m.host","m.time","comment"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->storageClassName);
        currentRow.push_back(std::to_string((unsigned long long)it->copyNb));
        currentRow.push_back(it->tapePoolName);
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_logicallibrary
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_logicallibrary(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " ll/logicallibrary add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <logical_library_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <logical_library_name> --comment/-m <\"comment\">" << std::endl
       << "\trm  --name/-n <logical_library_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string name = getOptionValue(tokens, "-n", "--name", false);
    if(name.empty()) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }
    if("add" == tokens[2] || "ch" == tokens[2]) {
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      if("add" == tokens[2]) { //add
        if(comment.empty()) {
          m_data = help.str();
          error.setErrInfo(EPERM, m_data.c_str());
          return SFS_ERROR;
        }
        m_catalogue->createLogicalLibrary(cliIdentity, name, comment);
      }
      else { //ch
        if(comment.empty()) {
          m_data = help.str();
          error.setErrInfo(EPERM, m_data.c_str());
          return SFS_ERROR;
        }
        m_catalogue->modifyLogicalLibraryComment(cliIdentity, name, comment);
      }
    }
    else { //rm
      m_catalogue->deleteLogicalLibrary(name);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::LogicalLibrary> list= m_catalogue->getLogicalLibraries();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"name","c.name","c.group","c.host","c.time","m.name","m.group","m.host","m.time","comment"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_tape
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_tape(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " ta/tape add/ch/rm/reclaim/ls/label:" << std::endl
       << "\tadd     --vid/-v <vid> --logicallibrary/-l <logical_library_name> --tapepool/-t <tapepool_name> --capacity/-c <capacity_in_bytes> [--encryptionkey/-k <encryption_key>]" << std::endl
       << "\t        [--enabled/-e or --disabled/-d] [--free/-f or --full/-F] [--comment/-m <\"comment\">] " << std::endl
       << "\tch      --vid/-v <vid> [--logicallibrary/-l <logical_library_name>] [--tapepool/-t <tapepool_name>] [--capacity/-c <capacity_in_bytes>] [--encryptionkey/-k <encryption_key>]" << std::endl
       << "\t        [--enabled/-e or --disabled/-d] [--free/-f or --full/-F] [--comment/-m <\"comment\">]" << std::endl
       << "\trm      --vid/-v <vid>" << std::endl
       << "\treclaim --vid/-v <vid>" << std::endl
       << "\tls      [--header/-h] [--vid/-v <vid>] [--logicallibrary/-l <logical_library_name>] [--tapepool/-t <tapepool_name>] [--capacity/-c <capacity_in_bytes>]" << std::endl
       << "\t        [--lbp/-p or --nolbp/-P] [--enabled/-e or --disabled/-d] [--free/-f or --full/-F] [--busy/-b or --notbusy/-n]" << std::endl
       << "\tlabel   --vid/-v <vid> [--force/-f] [--lbp/-l] [--tag/-t <tag_name>]" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2] || "reclaim" == tokens[2] || "label" == tokens[2]) {
    std::string vid = getOptionValue(tokens, "-v", "--vid", false);
    if(vid.empty()) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }
    if("add" == tokens[2]) { //add
      std::string logicallibrary = getOptionValue(tokens, "-l", "--logicallibrary", false);
      std::string tapepool = getOptionValue(tokens, "-t", "--tapepool", false);
      std::string capacity_s = getOptionValue(tokens, "-c", "--capacity", false);
      if(logicallibrary.empty()||tapepool.empty()||capacity_s.empty()) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      uint64_t capacity = stringParameterToUint64("--capacity", capacity_s);
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      bool disabled=false;
      bool full=false;
      if((hasOption(tokens, "-e", "--enabled") && hasOption(tokens, "-d", "--disabled")) || (hasOption(tokens, "-f", "--free") && hasOption(tokens, "-F", "--full"))) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      disabled=hasOption(tokens, "-d", "--disabled");
      full=hasOption(tokens, "-F", "--full");
      std::string encryptionkey = getOptionValue(tokens, "-k", "--encryptionkey", false);
      m_catalogue->createTape(cliIdentity, vid, logicallibrary, tapepool, encryptionkey, capacity, disabled, full, comment);
    }
    else if("ch" == tokens[2]) { //ch
      std::string logicallibrary = getOptionValue(tokens, "-l", "--logicallibrary", false);
      std::string tapepool = getOptionValue(tokens, "-t", "--tapepool", false);
      std::string capacity_s = getOptionValue(tokens, "-c", "--capacity", false);
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      std::string encryptionkey = getOptionValue(tokens, "-k", "--encryptionkey", false);
      if(comment.empty() && logicallibrary.empty() && tapepool.empty() && capacity_s.empty() && encryptionkey.empty() && !hasOption(tokens, "-e", "--enabled")
              && !hasOption(tokens, "-d", "--disabled") && !hasOption(tokens, "-f", "--free") && !hasOption(tokens, "-F", "--full")) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      if((hasOption(tokens, "-e", "--enabled") && hasOption(tokens, "-d", "--disabled")) || (hasOption(tokens, "-f", "--free") && hasOption(tokens, "-F", "--full"))) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      if(!logicallibrary.empty()) {
        m_catalogue->modifyTapeLogicalLibraryName(cliIdentity, vid, logicallibrary);
      }
      if(!tapepool.empty()) {
        m_catalogue->modifyTapeTapePoolName(cliIdentity, vid, tapepool);
      }
      if(!capacity_s.empty()) {
        uint64_t capacity = stringParameterToUint64("--capacity", capacity_s);
        m_catalogue->modifyTapeCapacityInBytes(cliIdentity, vid, capacity);
      }
      if(!comment.empty()) {
        m_catalogue->modifyTapeComment(cliIdentity, vid, comment);
      }
      if(!encryptionkey.empty()) {
        m_catalogue->modifyTapeEncryptionKey(cliIdentity, vid, encryptionkey);
      }
      if(hasOption(tokens, "-e", "--enabled")) {
        m_scheduler->setTapeDisabled(cliIdentity, vid, false);
      }
      if(hasOption(tokens, "-d", "--disabled")) {
        m_scheduler->setTapeDisabled(cliIdentity, vid, true);
      }
      if(hasOption(tokens, "-f", "--free")) {
        m_scheduler->setTapeFull(cliIdentity, vid, false);
      }
      if(hasOption(tokens, "-F", "--full")) {
        m_scheduler->setTapeFull(cliIdentity, vid, true);
      }
    }
    else if("reclaim" == tokens[2]) { //reclaim
      m_catalogue->reclaimTape(cliIdentity, vid);
    }
    else if("label" == tokens[2]) { //label
      m_scheduler->labelTape(cliIdentity, vid, hasOption(tokens, "-f", "--force"), hasOption(tokens, "-l", "--lbp"), getOptionValue(tokens, "-t", "--tag", false));
    }
    else { //rm
      m_catalogue->deleteTape(vid);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::string vid = getOptionValue(tokens, "-v", "--vid", false);
    std::string logicallibrary = getOptionValue(tokens, "-l", "--logicallibrary", false);
    std::string tapepool = getOptionValue(tokens, "-t", "--tapepool", false);
    std::string capacity = getOptionValue(tokens, "-c", "--capacity", false);
    if((hasOption(tokens, "-e", "--enabled") && hasOption(tokens, "-d", "--disabled")) 
            || (hasOption(tokens, "-f", "--free") && hasOption(tokens, "-F", "--full")) 
            || (hasOption(tokens, "-b", "--busy") && hasOption(tokens, "-n", "--notbusy"))
            || (hasOption(tokens, "-p", "--lbp") && hasOption(tokens, "-P", "--nolbp"))) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }
    std::string disabled="";
    std::string full="";
    std::string busy="";
    std::string lbp="";
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
    if(hasOption(tokens, "-p", "--lbp")) {
      lbp = "false";
    }
    if(hasOption(tokens, "-P", "--nolbp")) {
      lbp = "true";
    }
    std::list<cta::common::dataStructures::Tape> list= m_catalogue->getTapes(vid, logicallibrary, tapepool, capacity, disabled, full, busy, lbp);
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"vid","logical library","tapepool","encription key","capacity","occupancy","last fseq","busy","full","disabled","lpb","label drive","label time",
                                         "last w drive","last w time","last r drive","last r time","c.name","c.group","c.host","c.time","m.name","m.group","m.host","m.time","comment"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->vid);
        currentRow.push_back(it->logicalLibraryName);
        currentRow.push_back(it->tapePoolName);
        currentRow.push_back(it->encryptionKey);
        currentRow.push_back(std::to_string((unsigned long long)it->capacityInBytes));
        currentRow.push_back(std::to_string((unsigned long long)it->dataOnTapeInBytes));
        currentRow.push_back(std::to_string((unsigned long long)it->lastFSeq));
        if(it->busy) currentRow.push_back("true"); else currentRow.push_back("false");
        if(it->full) currentRow.push_back("true"); else currentRow.push_back("false");
        if(it->disabled) currentRow.push_back("true"); else currentRow.push_back("false");
        if(it->lbp) currentRow.push_back("true"); else currentRow.push_back("false");
        currentRow.push_back(it->labelLog.drive);
        currentRow.push_back(std::to_string((unsigned long long)it->labelLog.time));
        currentRow.push_back(it->lastWriteLog.drive);
        currentRow.push_back(std::to_string((unsigned long long)it->lastWriteLog.time));
        currentRow.push_back(it->lastReadLog.drive);
        currentRow.push_back(std::to_string((unsigned long long)it->lastReadLog.time));
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_storageclass
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_storageclass(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " sc/storageclass add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <storage_class_name> --copynb/-c <number_of_tape_copies> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <storage_class_name> [--copynb/-c <number_of_tape_copies>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <storage_class_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string scn = getOptionValue(tokens, "-n", "--name", false);
    if(scn.empty()) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }  
    if("add" == tokens[2]) { //add
      std::string cn_s = getOptionValue(tokens, "-c", "--copynb", false);
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      if(comment.empty()||cn_s.empty()) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }  
      uint64_t cn = stringParameterToUint64("--copynb", cn_s);
      m_catalogue->createStorageClass(cliIdentity, scn, cn, comment);
    }
    else if("ch" == tokens[2]) { //ch
      std::string cn_s = getOptionValue(tokens, "-c", "--copynb", false);
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      if(comment.empty()&&cn_s.empty()) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      if(!comment.empty()) {
        m_catalogue->modifyStorageClassComment(cliIdentity, scn, comment);
      }
      if(!cn_s.empty()) {  
        uint64_t cn = stringParameterToUint64("--copynb", cn_s);
        m_catalogue->modifyStorageClassNbCopies(cliIdentity, scn, cn);
      }
    }
    else { //rm
      m_catalogue->deleteStorageClass(scn);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::StorageClass> list= m_catalogue->getStorageClasses();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"storage class","number of copies","c.name","c.group","c.host","c.time","m.name","m.group","m.host","m.time","comment"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        currentRow.push_back(std::to_string((unsigned long long)it->nbCopies));
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_user
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_user(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " us/user add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <user_name> --group/-g <group_name> --mountpolicy/-u <mount_group_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <user_name> --group/-g <group_name> [--mountpolicy/-u <mount_group_name>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <user_name> --group/-g <group_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string user = getOptionValue(tokens, "-n", "--name", false);
    std::string group = getOptionValue(tokens, "-g", "--group", false);
    if(user.empty()||group.empty()) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }
    cta::common::dataStructures::UserIdentity userIdentity;
    userIdentity.name=user;
    userIdentity.group=group;
    if("add" == tokens[2]) { //add
      std::string mountpolicy = getOptionValue(tokens, "-u", "--mountpolicy", false);
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      if(comment.empty()||mountpolicy.empty()) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      m_catalogue->createRequester(cliIdentity, userIdentity, mountpolicy, comment);
    }
    else if("ch" == tokens[2]) { //ch
      std::string mountpolicy = getOptionValue(tokens, "-u", "--mountpolicy", false);
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      if(comment.empty()&&mountpolicy.empty()) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      if(!comment.empty()) {
        m_catalogue->modifyRequesterComment(cliIdentity, userIdentity, comment);
      }
      if(!mountpolicy.empty()) {
        m_catalogue->modifyRequesterMountPolicy(cliIdentity, userIdentity, mountpolicy);
      }
    }
    else { //rm
      m_catalogue->deleteRequester(userIdentity);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::Requester> list= m_catalogue->getRequesters();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"user","group","cta group","c.name","c.group","c.host","c.time","m.name","m.group","m.host","m.time","comment"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        currentRow.push_back(it->group);
        currentRow.push_back(it->mountPolicy);
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_mountpolicy
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_mountpolicy(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " mg/mountpolicy add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <mountpolicy_name> --archivepriority/--ap <priority_value> --minarchiverequestage/--aa <minRequestAge> --retrievepriority/--rp <priority_value>" << std::endl
       << "\t    --minretrieverequestage/--ra <minRequestAge> --maxdrivesallowed/-d <maxDrivesAllowed> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <mountpolicy_name> [--archivepriority/--ap <priority_value>] [--minarchiverequestage/--aa <minRequestAge>] [--retrievepriority/--rp <priority_value>]" << std::endl
       << "\t   [--minretrieverequestage/--ra <minRequestAge>] [--maxdrivesallowed/-d <maxDrivesAllowed>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <mountpolicy_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string group = getOptionValue(tokens, "-n", "--name", false);
    if(group.empty()) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }
    if("add" == tokens[2] || "ch" == tokens[2]) {      
      std::string archivepriority_s = getOptionValue(tokens, "--ap", "--archivepriority", false);
      std::string minarchiverequestage_s = getOptionValue(tokens, "--aa", "--minarchiverequestage", false);
      std::string retrievepriority_s = getOptionValue(tokens, "--rp", "--retrievepriority", false);
      std::string minretrieverequestage_s = getOptionValue(tokens, "--ra", "--minretrieverequestage", false);
      std::string maxdrivesallowed_s = getOptionValue(tokens, "-d", "--maxdrivesallowed", false);
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      if("add" == tokens[2]) { //add
        if(archivepriority_s.empty()||minarchiverequestage_s.empty()||retrievepriority_s.empty()
                ||minretrieverequestage_s.empty()||maxdrivesallowed_s.empty()||comment.empty()) {
          m_data = help.str();
          error.setErrInfo(EPERM, m_data.c_str());
          return SFS_ERROR;
        }
        uint64_t archivepriority = stringParameterToUint64("--archivepriority", archivepriority_s);
        uint64_t minarchiverequestage = stringParameterToUint64("--minarchiverequestage", minarchiverequestage_s);
        uint64_t retrievepriority = stringParameterToUint64("--retrievepriority", retrievepriority_s);
        uint64_t minretrieverequestage = stringParameterToUint64("--minretrieverequestage", minretrieverequestage_s);
        uint64_t maxdrivesallowed = stringParameterToUint64("--maxdrivesallowed", maxdrivesallowed_s);
        m_catalogue->createMountPolicy(cliIdentity, group, archivepriority, minarchiverequestage, retrievepriority, minretrieverequestage, maxdrivesallowed, comment);
      }
      else if("ch" == tokens[2]) { //ch
        if(archivepriority_s.empty()&&minarchiverequestage_s.empty()&&retrievepriority_s.empty()
                &&minretrieverequestage_s.empty()&&maxdrivesallowed_s.empty()&&comment.empty()) {
          m_data = help.str();
          error.setErrInfo(EPERM, m_data.c_str());
          return SFS_ERROR;
        }
        if(!archivepriority_s.empty()) {
          uint64_t archivepriority = stringParameterToUint64("--archivepriority", archivepriority_s);
          m_catalogue->modifyMountPolicyArchivePriority(cliIdentity, group, archivepriority);
        }
        if(!minarchiverequestage_s.empty()) {
          uint64_t minarchiverequestage = stringParameterToUint64("--minarchiverequestage", minarchiverequestage_s);
          m_catalogue->modifyMountPolicyArchiveMinRequestAge(cliIdentity, group, minarchiverequestage);
        }
        if(!retrievepriority_s.empty()) {
          uint64_t retrievepriority = stringParameterToUint64("--retrievepriority", retrievepriority_s);
          m_catalogue->modifyMountPolicyRetrievePriority(cliIdentity, group, retrievepriority);
        }
        if(!minretrieverequestage_s.empty()) {
          uint64_t minretrieverequestage = stringParameterToUint64("--minretrieverequestage", minretrieverequestage_s);
          m_catalogue->modifyMountPolicyRetrieveMinRequestAge(cliIdentity, group, minretrieverequestage);
        }
        if(!maxdrivesallowed_s.empty()) {
          uint64_t maxdrivesallowed = stringParameterToUint64("--maxdrivesallowed", maxdrivesallowed_s);
          m_catalogue->modifyMountPolicyMaxDrivesAllowed(cliIdentity, group, maxdrivesallowed);
        }
        if(!comment.empty()) {
          m_catalogue->modifyMountPolicyComment(cliIdentity, group, comment);
        }
      }
    }
    else { //rm
      m_catalogue->deleteMountPolicy(group);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::MountPolicy> list= m_catalogue->getMountPolicies();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"cta group","a.priority","a.minFiles","a.minBytes","a.minAge","r.priority","r.minFiles","r.minBytes","r.minAge","MaxDrives","c.name","c.group","c.host","c.time","m.name","m.group","m.host","m.time","comment"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        currentRow.push_back(std::to_string((unsigned long long)it->archivePriority));
        currentRow.push_back(std::to_string((unsigned long long)it->archiveMinRequestAge));
        currentRow.push_back(std::to_string((unsigned long long)it->retrievePriority));
        currentRow.push_back(std::to_string((unsigned long long)it->retrieveMinRequestAge));
        currentRow.push_back(std::to_string((unsigned long long)it->maxDrivesAllowed));
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_dedication
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_dedication(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " de/dedication add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <drive_name> [--readonly/-r or --writeonly/-w] [--vid/-v <tape_vid>] [--tag/-t <tag_name>] --from/-f <DD/MM/YYYY> --until/-u <DD/MM/YYYY> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <drive_name> [--readonly/-r or --writeonly/-w] [--vid/-v <tape_vid>] [--tag/-t <tag_name>] [--from/-f <DD/MM/YYYY>] [--until/-u <DD/MM/YYYY>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <drive_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;
  if("add" == tokens[2] || "ch" == tokens[2] || "rm" == tokens[2]) {
    std::string drive = getOptionValue(tokens, "-n", "--name", false);
    if(drive.empty()) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    } 
    if("add" == tokens[2] || "ch" == tokens[2]) {
      bool readonly = hasOption(tokens, "-r", "--readonly");
      bool writeonly = hasOption(tokens, "-w", "--writeonly");
      std::string vid = getOptionValue(tokens, "-v", "--vid", false);
      std::string tag = getOptionValue(tokens, "-t", "--tag", false);
      std::string from_s = getOptionValue(tokens, "-f", "--from", false);
      std::string until_s = getOptionValue(tokens, "-u", "--until", false);
      std::string comment = getOptionValue(tokens, "-m", "--comment", false);
      if("add" == tokens[2]) { //add
        if(comment.empty()||from_s.empty()||until_s.empty()||(vid.empty()&&tag.empty()&&!readonly&&!writeonly)||(readonly&&writeonly)) {
          m_data = help.str();
          error.setErrInfo(EPERM, m_data.c_str());
          return SFS_ERROR;
        }
        struct tm time;
        if(NULL==strptime(from_s.c_str(), "%d/%m/%Y", &time)) {
          m_data = help.str();
          error.setErrInfo(EPERM, m_data.c_str());
          return SFS_ERROR;
        }
        time_t from = mktime(&time);  // timestamp in current timezone
        if(NULL==strptime(until_s.c_str(), "%d/%m/%Y", &time)) {
          m_data = help.str();
          error.setErrInfo(EPERM, m_data.c_str());
          return SFS_ERROR;
        }
        time_t until = mktime(&time);  // timestamp in current timezone
        cta::common::dataStructures::DedicationType type=cta::common::dataStructures::DedicationType::readwrite;
        if(readonly) {
          type=cta::common::dataStructures::DedicationType::readonly;
        }
        else if(writeonly) {
          type=cta::common::dataStructures::DedicationType::writeonly;
        }
        m_catalogue->createDedication(cliIdentity, drive, type, tag, vid, from, until, comment);
      }
      else if("ch" == tokens[2]) { //ch
        if((comment.empty()&&from_s.empty()&&until_s.empty()&&vid.empty()&&tag.empty()&&!readonly&&!writeonly)||(readonly&&writeonly)) {
          m_data = help.str();
          error.setErrInfo(EPERM, m_data.c_str());
          return SFS_ERROR;
        }
        if(!comment.empty()) {
          m_catalogue->modifyDedicationComment(cliIdentity, drive, comment);
        }
        if(!from_s.empty()) {
          struct tm time;
          if(NULL==strptime(from_s.c_str(), "%d/%m/%Y", &time)) {
            m_data = help.str();
            error.setErrInfo(EPERM, m_data.c_str());
            return SFS_ERROR;
          }
          time_t from = mktime(&time);  // timestamp in current timezone
          m_catalogue->modifyDedicationFrom(cliIdentity, drive, from);
        }
        if(!until_s.empty()) {
          struct tm time;
          if(NULL==strptime(until_s.c_str(), "%d/%m/%Y", &time)) {
            m_data = help.str();
            error.setErrInfo(EPERM, m_data.c_str());
            return SFS_ERROR;
          }
          time_t until = mktime(&time);  // timestamp in current timezone
          m_catalogue->modifyDedicationUntil(cliIdentity, drive, until);
        }
        if(!vid.empty()) {
          m_catalogue->modifyDedicationVid(cliIdentity, drive, vid);
        }
        if(!tag.empty()) {
          m_catalogue->modifyDedicationTag(cliIdentity, drive, tag);
        }
        if(readonly) {
          m_catalogue->modifyDedicationType(cliIdentity, drive, cta::common::dataStructures::DedicationType::readonly);          
        }
        if(writeonly) {
          m_catalogue->modifyDedicationType(cliIdentity, drive, cta::common::dataStructures::DedicationType::writeonly);
        }
      }
    }
    else { //rm
      m_catalogue->deleteDedication(drive);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::Dedication> list= m_catalogue->getDedications();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"drive","type","vid","tag","from","until","c.name","c.group","c.host","c.time","m.name","m.group","m.host","m.time","comment"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        std::string type_s;
        switch(it->dedicationType) {
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
        currentRow.push_back(it->driveName);
        currentRow.push_back(type_s);
        currentRow.push_back(it->vid);
        currentRow.push_back(it->tag);
        currentRow.push_back(timeToString(it->fromTimestamp));
        currentRow.push_back(timeToString(it->untilTimestamp));
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_repack
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_repack(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " re/repack add/rm/ls/err:" << std::endl
       << "\tadd --vid/-v <vid> [--justexpand/-e or --justrepack/-r] [--tag/-t <tag_name>]" << std::endl
       << "\trm  --vid/-v <vid>" << std::endl
       << "\tls  [--header/-h] [--vid/-v <vid>]" << std::endl
       << "\terr --vid/-v <vid>" << std::endl;
  if("add" == tokens[2] || "err" == tokens[2] || "rm" == tokens[2]) {
    std::string vid = getOptionValue(tokens, "-v", "--vid", false);
    if(vid.empty()) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }  
    if("add" == tokens[2]) { //add
      std::string tag = getOptionValue(tokens, "-t", "--tag", false);
      bool justexpand = hasOption(tokens, "-e", "--justexpand");
      bool justrepack = hasOption(tokens, "-r", "--justrepack");
      if(justexpand&&justrepack) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      cta::common::dataStructures::RepackType type=cta::common::dataStructures::RepackType::expandandrepack;
      if(justexpand) {
        type=cta::common::dataStructures::RepackType::justexpand;
      }
      if(justrepack) {
        type=cta::common::dataStructures::RepackType::justrepack;
      }
      m_scheduler->repack(cliIdentity, vid, tag, type);
    }
    else if("err" == tokens[2]) { //err
      cta::common::dataStructures::RepackInfo info = m_scheduler->getRepack(cliIdentity, vid);
      if(info.errors.size()>0) {
        std::vector<std::vector<std::string>> responseTable;
        std::vector<std::string> header = {"fseq","error message"};
        if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
        for(auto it = info.errors.cbegin(); it != info.errors.cend(); it++) {
          std::vector<std::string> currentRow;
          currentRow.push_back(std::to_string((unsigned long long)it->first));
          currentRow.push_back(it->second);
          responseTable.push_back(currentRow);
        }
        m_data = formatResponse(responseTable);
      }
    }
    else { //rm
      m_scheduler->cancelRepack(cliIdentity, vid);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::RepackInfo> list;
    std::string vid = getOptionValue(tokens, "-v", "--vid", false);
    if(vid.empty()) {      
      list = m_scheduler->getRepacks(cliIdentity);
    }
    else {
      list.push_back(m_scheduler->getRepack(cliIdentity, vid));
    }
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"vid","files","size","type","tag","to retrieve","to archive","failed","archived","status","name","group","host","time"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::string type_s;
        switch(it->repackType) {
          case cta::common::dataStructures::RepackType::expandandrepack:
            type_s = "expandandrepack";
            break;
          case cta::common::dataStructures::RepackType::justexpand:
            type_s = "justexpand";
            break;
          case cta::common::dataStructures::RepackType::justrepack:
            type_s = "justrepack";
            break;
        }
        std::vector<std::string> currentRow;
        currentRow.push_back(it->vid);
        currentRow.push_back(std::to_string((unsigned long long)it->totalFiles));
        currentRow.push_back(std::to_string((unsigned long long)it->totalSize));
        currentRow.push_back(type_s);
        currentRow.push_back(it->tag);
        currentRow.push_back(std::to_string((unsigned long long)it->filesToRetrieve));//change names
        currentRow.push_back(std::to_string((unsigned long long)it->filesToArchive));
        currentRow.push_back(std::to_string((unsigned long long)it->filesFailed));
        currentRow.push_back(std::to_string((unsigned long long)it->filesArchived));
        currentRow.push_back(it->repackStatus);
        currentRow.push_back(it->creationLog.user.name);
        currentRow.push_back(it->creationLog.user.group);
        currentRow.push_back(it->creationLog.host);        
        currentRow.push_back(timeToString(it->creationLog.time));
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_shrink
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_shrink(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " sh/shrink --tapepool/-t <tapepool_name>" << std::endl;
  std::string tapepool = getOptionValue(tokens, "-t", "--tapepool", false);
  if(tapepool.empty()) {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  m_scheduler->shrink(cliIdentity, tapepool);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_verify
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_verify(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " ve/verify add/rm/ls/err:" << std::endl
       << "\tadd [--vid/-v <vid>] [--complete/-c or --partial/-p <number_of_files_per_tape>] [--tag/-t <tag_name>]" << std::endl
       << "\trm  [--vid/-v <vid>]" << std::endl
       << "\tls  [--header/-h] [--vid/-v <vid>]" << std::endl
       << "\terr --vid/-v <vid>" << std::endl;
  if("add" == tokens[2] || "err" == tokens[2] || "rm" == tokens[2]) {
    std::string vid = getOptionValue(tokens, "-v", "--vid", false);
    if(vid.empty()) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }  
    if("add" == tokens[2]) { //add
      std::string tag = getOptionValue(tokens, "-t", "--tag", false);
      std::string numberOfFiles_s = getOptionValue(tokens, "-p", "--partial", false);
      bool complete = hasOption(tokens, "-c", "--complete");
      if(complete&&!numberOfFiles_s.empty()) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      uint64_t numberOfFiles=0; //0 means do a complete verification
      if(!numberOfFiles_s.empty()) {
        numberOfFiles = stringParameterToUint64("--partial", numberOfFiles_s);
      }
      m_scheduler->verify(cliIdentity, vid, tag, numberOfFiles);
    }
    else if("err" == tokens[2]) { //err
      cta::common::dataStructures::VerifyInfo info = m_scheduler->getVerify(cliIdentity, vid);
      if(info.errors.size()>0) {
        std::vector<std::vector<std::string>> responseTable;
        std::vector<std::string> header = {"fseq","error message"};
        if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
        for(auto it = info.errors.cbegin(); it != info.errors.cend(); it++) {
          std::vector<std::string> currentRow;
          currentRow.push_back(std::to_string((unsigned long long)it->first));
          currentRow.push_back(it->second);
          responseTable.push_back(currentRow);
        }
        m_data = formatResponse(responseTable);
      }
    }
    else { //rm
      m_scheduler->cancelVerify(cliIdentity, vid);
    }
  }
  else if("ls" == tokens[2]) { //ls
    std::list<cta::common::dataStructures::VerifyInfo> list;
    std::string vid = getOptionValue(tokens, "-v", "--vid", false);
    if(vid.empty()) {      
      list = m_scheduler->getVerifys(cliIdentity);
    }
    else {
      list.push_back(m_scheduler->getVerify(cliIdentity, vid));
    }
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"vid","files","size","tag","to verify","failed","verified","status","name","group","host","time"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->vid);
        currentRow.push_back(std::to_string((unsigned long long)it->totalFiles));
        currentRow.push_back(std::to_string((unsigned long long)it->totalSize));
        currentRow.push_back(it->tag);
        currentRow.push_back(std::to_string((unsigned long long)it->filesToVerify));
        currentRow.push_back(std::to_string((unsigned long long)it->filesFailed));
        currentRow.push_back(std::to_string((unsigned long long)it->filesVerified));
        currentRow.push_back(it->verifyStatus);
        currentRow.push_back(it->creationLog.user.name);
        currentRow.push_back(it->creationLog.user.group);
        currentRow.push_back(it->creationLog.host);       
        currentRow.push_back(timeToString(it->creationLog.time));
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_archivefile
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_archivefile(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " af/archivefile ls [--header/-h] [--id/-I <archive_file_id>] [--eosid/-e <eos_id>] [--copynb/-c <copy_no>] [--vid/-v <vid>] [--tapepool/-t <tapepool>] "
          "[--owner/-o <owner>] [--group/-g <group>] [--storageclass/-s <class>] [--path/-p <fullpath>] [--summary/-S] [--all/-a] (default gives error)" << std::endl;
  if("ls" == tokens[2]) { //ls
    std::string id_s = getOptionValue(tokens, "-I", "--id", false);
    std::string eosid = getOptionValue(tokens, "-e", "--eosid", false);
    std::string copynb = getOptionValue(tokens, "-c", "--copynb", false);
    std::string tapepool = getOptionValue(tokens, "-t", "--tapepool", false);
    std::string vid = getOptionValue(tokens, "-v", "--vid", false);
    std::string owner = getOptionValue(tokens, "-o", "--owner", false);
    std::string group = getOptionValue(tokens, "-g", "--group", false);
    std::string storageclass = getOptionValue(tokens, "-s", "--storageclass", false);
    std::string path = getOptionValue(tokens, "-p", "--path", false);
    bool summary = hasOption(tokens, "-S", "--summary");
    bool all = hasOption(tokens, "-a", "--all");
    if(!all && (id_s.empty() && eosid.empty() && copynb.empty() && tapepool.empty() && vid.empty() && owner.empty() && group.empty() && storageclass.empty() && path.empty())) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }
    if(!summary) {
      std::list<cta::common::dataStructures::ArchiveFile> list=m_catalogue->getArchiveFiles(id_s, eosid, copynb, tapepool, vid, owner, group, storageclass, path);
      if(list.size()>0) {
        std::vector<std::vector<std::string>> responseTable;
        std::vector<std::string> header = {"id","copy no","vid","fseq","block id","disk id","size","checksum type","checksum value","storage class","owner","group","instance","path","creation time"};
        if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
        for(auto it = list.cbegin(); it != list.cend(); it++) {
          for(auto jt = it->tapeFiles.cbegin(); jt != it->tapeFiles.cend(); jt++) {
            std::vector<std::string> currentRow;
            currentRow.push_back(std::to_string((unsigned long long)it->archiveFileID));
            currentRow.push_back(std::to_string((unsigned long long)jt->first));
            currentRow.push_back(jt->second.vid);
            currentRow.push_back(std::to_string((unsigned long long)jt->second.fSeq));
            currentRow.push_back(std::to_string((unsigned long long)jt->second.blockId));
            currentRow.push_back(it->diskFileID);
            currentRow.push_back(it->diskInstance);
            currentRow.push_back(std::to_string((unsigned long long)it->fileSize));
            currentRow.push_back(it->checksumType);
            currentRow.push_back(it->checksumValue);
            currentRow.push_back(it->storageClass);
            currentRow.push_back(it->drData.drOwner);
            currentRow.push_back(it->drData.drGroup);
            currentRow.push_back(it->drData.drPath);    
            currentRow.push_back(std::to_string((unsigned long long)it->creationTime));          
            responseTable.push_back(currentRow);
          }
        }
        m_data = formatResponse(responseTable);
      }
    }
    else { //summary
      cta::common::dataStructures::ArchiveFileSummary summary=m_catalogue->getArchiveFileSummary(id_s, eosid, copynb, tapepool, vid, owner, group, storageclass, path);
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"total number of files","total size"};
      std::vector<std::string> row = {std::to_string((unsigned long long)summary.totalFiles),std::to_string((unsigned long long)summary.totalBytes)};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);
      responseTable.push_back(row);
    }
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_test
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_test(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " te/test read/write/write_auto (to be run on an empty self-dedicated drive; it is a synchronous command that returns performance stats and errors; all locations are local to the tapeserver):" << std::endl
       << "\tread  --drive/-d <drive_name> --vid/-v <vid> --firstfseq/-f <first_fseq> --lastfseq/-l <last_fseq> --checkchecksum/-c --output/-o <\"null\" or output_dir> [--tag/-t <tag_name>]" << std::endl
       << "\twrite --drive/-d <drive_name> --vid/-v <vid> --file/-f <filename> [--tag/-t <tag_name>]" << std::endl
       << "\twrite_auto --drive/-d <drive_name> --vid/-v <vid> --number/-n <number_of_files> --size/-s <file_size> --input/-i <\"zero\" or \"urandom\"> [--tag/-t <tag_name>]" << std::endl;
  std::string drive = getOptionValue(tokens, "-d", "--drive", false);
  std::string vid = getOptionValue(tokens, "-v", "--vid", false);
  if(vid.empty() || drive.empty()) {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  std::string tag = getOptionValue(tokens, "-t", "--tag", false);
  if("read" == tokens[2]) {
    std::string firstfseq_s = getOptionValue(tokens, "-f", "--firstfseq", false);
    std::string lastfseq_s = getOptionValue(tokens, "-l", "--lastfseq", false);
    std::string output = getOptionValue(tokens, "-o", "--output", false);
    if(firstfseq_s.empty() || lastfseq_s.empty() || output.empty()) {
      m_data = help.str();
      error.setErrInfo(EPERM, m_data.c_str());
      return SFS_ERROR;
    }    
    bool checkchecksum = hasOption(tokens, "-c", "--checkchecksum");
    uint64_t firstfseq = stringParameterToUint64("--firstfseq", firstfseq_s);
    uint64_t lastfseq = stringParameterToUint64("--lastfseq", lastfseq_s);
    cta::common::dataStructures::ReadTestResult res = m_scheduler->readTest(cliIdentity, drive, vid, firstfseq, lastfseq, checkchecksum, output, tag);   
    std::vector<std::vector<std::string>> responseTable;
    std::vector<std::string> header = {"fseq","checksum type","checksum value","error"};
    responseTable.push_back(header);
    for(auto it = res.checksums.cbegin(); it != res.checksums.cend(); it++) {
      std::vector<std::string> currentRow;
      currentRow.push_back(std::to_string((unsigned long long)it->first));
      currentRow.push_back(it->second.first);
      currentRow.push_back(it->second.second);
      if(res.errors.find(it->first) != res.errors.cend()) {
        currentRow.push_back(res.errors.at(it->first));
      }
      else {
        currentRow.push_back("-");
      }
      responseTable.push_back(currentRow);
    }
    m_data = formatResponse(responseTable);
    std::stringstream ss;
    ss << std::endl << "Drive: " << res.driveName << " Vid: " << res.vid << " #Files: " << res.totalFilesRead << " #Bytes: " << res.totalBytesRead 
       << " Time: " << res.totalTimeInSeconds << " s Speed(avg): " << (long double)res.totalBytesRead/(long double)res.totalTimeInSeconds << " B/s" <<std::endl;
    m_data += ss.str();   
  }
  else if("write" == tokens[2] || "write_auto" == tokens[2]) {
    cta::common::dataStructures::WriteTestResult res;
    if("write" == tokens[2]) { //write
      std::string file = getOptionValue(tokens, "-f", "--file", false);
      if(file.empty()) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }  
      res = m_scheduler->writeTest(cliIdentity, drive, vid, file, tag);
    }
    else { //write_auto
      std::string number_s = getOptionValue(tokens, "-n", "--number", false);
      std::string size_s = getOptionValue(tokens, "-s", "--size", false);
      std::string input = getOptionValue(tokens, "-i", "--input", false);
      if(number_s.empty()||size_s.empty()||(input!="zero"&&input!="urandom")) {
        m_data = help.str();
        error.setErrInfo(EPERM, m_data.c_str());
        return SFS_ERROR;
      }
      uint64_t number = stringParameterToUint64("--number", number_s);
      uint64_t size = stringParameterToUint64("--size", size_s);
      cta::common::dataStructures::TestSourceType type;
      if(input=="zero") { //zero
        type = cta::common::dataStructures::TestSourceType::devzero;
      }
      else { //urandom
        type = cta::common::dataStructures::TestSourceType::devurandom;
      }
      res = m_scheduler->write_autoTest(cliIdentity, drive, vid, number, size, type, tag);
    }
    std::vector<std::vector<std::string>> responseTable;
    std::vector<std::string> header = {"fseq","checksum type","checksum value","error"};
    responseTable.push_back(header);
    for(auto it = res.checksums.cbegin(); it != res.checksums.cend(); it++) {
      std::vector<std::string> currentRow;
      currentRow.push_back(std::to_string((unsigned long long)it->first));
      currentRow.push_back(it->second.first);
      currentRow.push_back(it->second.second);
      if(res.errors.find(it->first) != res.errors.cend()) {
        currentRow.push_back(res.errors.at(it->first));
      }
      else {
        currentRow.push_back("-");
      }
      responseTable.push_back(currentRow);
    }
    m_data = formatResponse(responseTable);
    std::stringstream ss;
    ss << std::endl << "Drive: " << res.driveName << " Vid: " << res.vid << " #Files: " << res.totalFilesWritten << " #Bytes: " << res.totalBytesWritten 
       << " Time: " << res.totalTimeInSeconds << " s Speed(avg): " << (long double)res.totalBytesWritten/(long double)res.totalTimeInSeconds << " B/s" <<std::endl;
    m_data += ss.str();    
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_drive
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_drive(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " dr/drive up/down (it is a synchronous command):" << std::endl
       << "\tup   --drive/-d <drive_name>" << std::endl
       << "\tdown --drive/-d <drive_name> [--force/-f]" << std::endl;
  std::string drive = getOptionValue(tokens, "-d", "--drive", false);
  if(drive.empty()) {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  if("up" == tokens[2]) {
    m_scheduler->setDriveStatus(cliIdentity, drive, true, false);
  }
  else if("down" == tokens[2]) {
    m_scheduler->setDriveStatus(cliIdentity, drive, false, hasOption(tokens, "-f", "--force"));
  }
  else {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_reconcile
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_reconcile(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " rc/reconcile (it is a synchronous command, with a possibly long execution time, returns the list of files unknown to EOS, to be deleted manually by the admin after proper checks)" << std::endl;
  std::list<cta::common::dataStructures::ArchiveFile> list = m_scheduler->reconcile(cliIdentity);  
  if(list.size()>0) {
    std::vector<std::vector<std::string>> responseTable;
    std::vector<std::string> header = {"id","copy no","vid","fseq","block id","disk id","instance","size","checksum type","checksum value","storage class","owner","group","path"};
    responseTable.push_back(header);    
    for(auto it = list.cbegin(); it != list.cend(); it++) {
      for(auto jt = it->tapeFiles.cbegin(); jt != it->tapeFiles.cend(); jt++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(std::to_string((unsigned long long)it->archiveFileID));
        currentRow.push_back(std::to_string((unsigned long long)jt->first));
        currentRow.push_back(jt->second.vid);
        currentRow.push_back(std::to_string((unsigned long long)jt->second.fSeq));
        currentRow.push_back(std::to_string((unsigned long long)jt->second.blockId));
        currentRow.push_back(it->diskFileID);
        currentRow.push_back(it->diskInstance);
        currentRow.push_back(std::to_string((unsigned long long)it->fileSize));
        currentRow.push_back(it->checksumType);
        currentRow.push_back(it->checksumValue);
        currentRow.push_back(it->storageClass);
        currentRow.push_back(it->drData.drOwner);
        currentRow.push_back(it->drData.drGroup);
        currentRow.push_back(it->drData.drPath);          
        responseTable.push_back(currentRow);
      }
    }
    m_data = formatResponse(responseTable);
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_listpendingarchives
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_listpendingarchives(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " lpa/listpendingarchives [--header/-h] [--tapepool/-t <tapepool_name>] [--extended/-x]" << std::endl;
  std::string tapepool = getOptionValue(tokens, "-t", "--tapepool", false);
  bool extended = hasOption(tokens, "-x", "--extended");
  std::map<std::string, std::list<cta::common::dataStructures::ArchiveJob> > result;
  if(tapepool.empty()) {
    result = m_scheduler->getPendingArchiveJobs(cliIdentity);
  }
  else {
    std::list<cta::common::dataStructures::ArchiveJob> list = m_scheduler->getPendingArchiveJobs(cliIdentity, tapepool);
    if(list.size()>0) {
      result[tapepool] = list;
    }
  }
  if(result.size()>0) {
    if(extended) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"tapepool","id","storage class","copy no.","disk id","instance","checksum type","checksum value","size","user","group","path","diskpool","diskpool throughput"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = result.cbegin(); it != result.cend(); it++) {
        for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++) {
          std::vector<std::string> currentRow;
          currentRow.push_back(it->first);
          currentRow.push_back(std::to_string((unsigned long long)jt->archiveFileID));
          currentRow.push_back(jt->request.storageClass);
          currentRow.push_back(std::to_string((unsigned long long)jt->copyNumber));
          currentRow.push_back(jt->request.diskFileID);
          currentRow.push_back(jt->request.instance);
          currentRow.push_back(jt->request.checksumType);
          currentRow.push_back(jt->request.checksumValue);         
          currentRow.push_back(std::to_string((unsigned long long)jt->request.fileSize));
          currentRow.push_back(jt->request.requester.name);
          currentRow.push_back(jt->request.requester.group);
          currentRow.push_back(jt->request.drData.drPath);
          currentRow.push_back(jt->request.diskpoolName);
          currentRow.push_back(std::to_string((unsigned long long)jt->request.diskpoolThroughput));
          responseTable.push_back(currentRow);
        }
      }
      m_data = formatResponse(responseTable);
    }
    else {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"tapepool","total files","total size"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = result.cbegin(); it != result.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->first);
        currentRow.push_back(std::to_string((unsigned long long)it->second.size()));
        uint64_t size=0;
        for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++) {
          size += jt->request.fileSize;
        }
        currentRow.push_back(std::to_string((unsigned long long)size));
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_listpendingretrieves
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_listpendingretrieves(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " lpr/listpendingretrieves [--header/-h] [--vid/-v <vid>] [--extended/-x]" << std::endl;
  std::string vid = getOptionValue(tokens, "-v", "--vid", false);
  bool extended = hasOption(tokens, "-x", "--extended");
  std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob> > result;
  if(vid.empty()) {
    result = m_scheduler->getPendingRetrieveJobs(cliIdentity);
  }
  else {
    std::list<cta::common::dataStructures::RetrieveJob> list = m_scheduler->getPendingRetrieveJobs(cliIdentity, vid);
    if(list.size()>0) {
      result[vid] = list;
    }
  }
  if(result.size()>0) {
    if(extended) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"vid","id","copy no.","fseq","block id","size","user","group","path","diskpool","diskpool throughput"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = result.cbegin(); it != result.cend(); it++) {
        for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++) {
          std::vector<std::string> currentRow;
          currentRow.push_back(it->first);
          currentRow.push_back(std::to_string((unsigned long long)jt->request.archiveFileID));
          cta::common::dataStructures::ArchiveFile file = m_catalogue->getArchiveFileById(jt->request.archiveFileID);
          currentRow.push_back(std::to_string((unsigned long long)(jt->tapeCopies.at(it->first).first)));
          currentRow.push_back(std::to_string((unsigned long long)(jt->tapeCopies.at(it->first).second.fSeq)));
          currentRow.push_back(std::to_string((unsigned long long)(jt->tapeCopies.at(it->first).second.blockId)));
          currentRow.push_back(std::to_string((unsigned long long)file.fileSize));
          currentRow.push_back(jt->request.requester.name);
          currentRow.push_back(jt->request.requester.group);
          currentRow.push_back(jt->request.drData.drPath);
          currentRow.push_back(jt->request.diskpoolName);
          currentRow.push_back(std::to_string((unsigned long long)jt->request.diskpoolThroughput));
          responseTable.push_back(currentRow);
        }
      }
      m_data = formatResponse(responseTable);
    }
    else {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"vid","total files","total size"};
      if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
      for(auto it = result.cbegin(); it != result.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->first);
        currentRow.push_back(std::to_string((unsigned long long)it->second.size()));
        uint64_t size=0;
        for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++) {
          cta::common::dataStructures::ArchiveFile file = m_catalogue->getArchiveFileById(jt->request.archiveFileID);
          size += file.fileSize;
        }
        currentRow.push_back(std::to_string((unsigned long long)size));
        responseTable.push_back(currentRow);
      }
      m_data = formatResponse(responseTable);
    }
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_listdrivestates
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_listdrivestates(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " lds/listdrivestates [--header/-h]" << std::endl;
  std::list<cta::common::dataStructures::DriveState> result = m_scheduler->getDriveStates(cliIdentity);  
  if(result.size()>0) {
    std::vector<std::vector<std::string>> responseTable;
    std::vector<std::string> header = {"logical library","host","drive","status","since","mount","vid","tapepool","session id","since","files","bytes","latest speed"};
    if(hasOption(tokens, "-h", "--header")) responseTable.push_back(header);    
    for(auto it = result.cbegin(); it != result.cend(); it++) {
      std::vector<std::string> currentRow;
      currentRow.push_back(it->logicalLibrary);
      currentRow.push_back(it->host);
      currentRow.push_back(it->name);
      currentRow.push_back(cta::common::dataStructures::toString(it->status));
      currentRow.push_back(std::to_string((unsigned long long)(time(NULL)-it->currentStateStartTime)));
      currentRow.push_back(cta::common::dataStructures::toString(it->mountType));
      currentRow.push_back(it->currentVid);
      currentRow.push_back(it->currentTapePool);
      currentRow.push_back(std::to_string((unsigned long long)it->sessionId));
      currentRow.push_back(std::to_string((unsigned long long)(time(NULL)-it->sessionStartTime)));
      currentRow.push_back(std::to_string((unsigned long long)it->filesTransferedInSession));
      currentRow.push_back(std::to_string((unsigned long long)it->bytesTransferedInSession));
      currentRow.push_back(std::to_string((long double)it->latestBandwidth));
      responseTable.push_back(currentRow);
    }
    m_data = formatResponse(responseTable);
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_archive
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_archive(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " a/archive --encoded <\"true\" or \"false\"> --user <user> --group <group> --diskid <disk_id> --instance <instance> --srcurl <src_URL> --size <size> --checksumtype <checksum_type>" << std::endl
                    << "\t--checksumvalue <checksum_value> --storageclass <storage_class> --diskfilepath <disk_filepath> --diskfileowner <disk_fileowner>" << std::endl
                    << "\t--diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob> --diskpool <diskpool_name> --throughput <diskpool_throughput>" << std::endl;
  std::string encoded_s = getOptionValue(tokens, "", "--encoded", false);
  if(encoded_s!="true" && encoded_s!="false") {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  bool encoded = encoded_s=="true"?true:false;
  std::string user = getOptionValue(tokens, "", "--user", encoded);
  std::string group = getOptionValue(tokens, "", "--group", encoded);
  std::string diskid = getOptionValue(tokens, "", "--diskid", encoded);
  std::string instance = getOptionValue(tokens, "", "--instance", encoded);
  std::string srcurl = getOptionValue(tokens, "", "--srcurl", encoded);
  std::string size_s = getOptionValue(tokens, "", "--size", encoded);
  std::string checksumtype = getOptionValue(tokens, "", "--checksumtype", encoded);
  std::string checksumvalue = getOptionValue(tokens, "", "--checksumvalue", encoded);
  std::string storageclass = getOptionValue(tokens, "", "--storageclass", encoded);
  std::string diskfilepath = getOptionValue(tokens, "", "--diskfilepath", encoded);
  std::string diskfileowner = getOptionValue(tokens, "", "--diskfileowner", encoded);
  std::string diskfilegroup = getOptionValue(tokens, "", "--diskfilegroup", encoded);
  std::string recoveryblob = getOptionValue(tokens, "", "--recoveryblob", encoded);
  std::string diskpool = getOptionValue(tokens, "", "--diskpool", encoded);
  std::string throughput_s = getOptionValue(tokens, "", "--throughput", encoded);
  if(user.empty() || group.empty() || diskid.empty() || srcurl.empty() || size_s.empty() || checksumtype.empty() || checksumvalue.empty()
          || storageclass.empty() || instance.empty() || diskfilepath.empty() || diskfileowner.empty() || diskfilegroup.empty() || recoveryblob.empty() || diskpool.empty() || throughput_s.empty()) {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  uint64_t size = stringParameterToUint64("--size", size_s);
  uint64_t throughput = stringParameterToUint64("--throughput", throughput_s);
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user;
  originator.group=group;
  cta::common::dataStructures::DRData drData;
  drData.drBlob=recoveryblob;
  drData.drGroup=diskfilegroup;
  drData.drOwner=diskfileowner;
  drData.drPath=diskfilepath;
  cta::common::dataStructures::ArchiveRequest request;
  request.checksumType=checksumtype;
  request.checksumValue=checksumvalue;
  request.diskpoolName=diskpool;
  request.diskpoolThroughput=throughput;
  request.drData=drData;
  request.diskFileID=diskid;
  request.instance=instance;
  request.fileSize=size;
  request.requester=originator;
  request.srcURL=srcurl;
  request.storageClass=storageclass;  
  uint64_t archiveFileId = m_scheduler->queueArchive(cliIdentity, request);
  std::stringstream res_ss;
  res_ss << archiveFileId << std::endl;
  m_data = res_ss.str();
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_retrieve
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_retrieve(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " r/retrieve --encoded <\"true\" or \"false\"> --user <user> --group <group> --id <CTA_ArchiveFileID> --dsturl <dst_URL> --diskfilepath <disk_filepath>" << std::endl
                    << "\t--diskfileowner <disk_fileowner> --diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob> --diskpool <diskpool_name> --throughput <diskpool_throughput>" << std::endl;
  std::string encoded_s = getOptionValue(tokens, "", "--encoded", false);
  if(encoded_s!="true" && encoded_s!="false") {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  bool encoded = encoded_s=="true"?true:false;
  std::string user = getOptionValue(tokens, "", "--user", encoded);
  std::string group = getOptionValue(tokens, "", "--group", encoded);
  std::string id_s = getOptionValue(tokens, "", "--id", encoded);
  std::string dsturl = getOptionValue(tokens, "", "--dsturl", encoded);
  std::string diskfilepath = getOptionValue(tokens, "", "--diskfilepath", encoded);
  std::string diskfileowner = getOptionValue(tokens, "", "--diskfileowner", encoded);
  std::string diskfilegroup = getOptionValue(tokens, "", "--diskfilegroup", encoded);
  std::string recoveryblob = getOptionValue(tokens, "", "--recoveryblob", encoded);
  std::string diskpool = getOptionValue(tokens, "", "--diskpool", encoded);
  std::string throughput_s = getOptionValue(tokens, "", "--throughput", encoded);
  if(user.empty() || group.empty() || id_s.empty() || dsturl.empty() || diskfilepath.empty() || diskfileowner.empty() || diskfilegroup.empty() || recoveryblob.empty() || diskpool.empty() || throughput_s.empty()) {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  uint64_t id = stringParameterToUint64("--id", id_s);
  uint64_t throughput = stringParameterToUint64("--throughput", throughput_s);
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user;
  originator.group=group;
  cta::common::dataStructures::DRData drData;
  drData.drBlob=recoveryblob;
  drData.drGroup=diskfilegroup;
  drData.drOwner=diskfileowner;
  drData.drPath=diskfilepath;
  cta::common::dataStructures::RetrieveRequest request;
  request.diskpoolName=diskpool;
  request.diskpoolThroughput=throughput;
  request.drData=drData;
  request.archiveFileID=id;
  request.requester=originator;
  request.dstURL=dsturl;
  m_scheduler->queueRetrieve(cliIdentity, request);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_deletearchive
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_deletearchive(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " da/deletearchive --encoded <\"true\" or \"false\"> --user <user> --group <group> --id <CTA_ArchiveFileID>" << std::endl;
  std::string encoded_s = getOptionValue(tokens, "", "--encoded", false);
  if(encoded_s!="true" && encoded_s!="false") {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  bool encoded = encoded_s=="true"?true:false;
  std::string user = getOptionValue(tokens, "", "--user", encoded);
  std::string group = getOptionValue(tokens, "", "--group", encoded);
  std::string id_s = getOptionValue(tokens, "", "--id", encoded);
  if(user.empty() || group.empty() || id_s.empty()) {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  uint64_t id = stringParameterToUint64("--id", id_s);
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user;
  originator.group=group;
  cta::common::dataStructures::DeleteArchiveRequest request;
  request.archiveFileID=id;
  request.requester=originator;
  m_scheduler->deleteArchive(cliIdentity, request);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_cancelretrieve
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_cancelretrieve(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " cr/cancelretrieve --encoded <\"true\" or \"false\"> --user <user> --group <group> --id <CTA_ArchiveFileID> --dsturl <dst_URL> --diskfilepath <disk_filepath>" << std::endl
                    << "\t--diskfileowner <disk_fileowner> --diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob>" << std::endl;
  std::string encoded_s = getOptionValue(tokens, "", "--encoded", false);
  if(encoded_s!="true" && encoded_s!="false") {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  bool encoded = encoded_s=="true"?true:false;
  std::string user = getOptionValue(tokens, "", "--user", encoded);
  std::string group = getOptionValue(tokens, "", "--group", encoded);
  std::string id_s = getOptionValue(tokens, "", "--id", encoded);
  std::string dsturl = getOptionValue(tokens, "", "--dsturl", encoded);
  std::string diskfilepath = getOptionValue(tokens, "", "--diskfilepath", encoded);
  std::string diskfileowner = getOptionValue(tokens, "", "--diskfileowner", encoded);
  std::string diskfilegroup = getOptionValue(tokens, "", "--diskfilegroup", encoded);
  std::string recoveryblob = getOptionValue(tokens, "", "--recoveryblob", encoded);
  if(user.empty() || group.empty() || id_s.empty() || dsturl.empty() || diskfilepath.empty() || diskfileowner.empty() || diskfilegroup.empty() || recoveryblob.empty()) {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  uint64_t id = stringParameterToUint64("--id", id_s);
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user;
  originator.group=group;
  cta::common::dataStructures::DRData drData;
  drData.drBlob=recoveryblob;
  drData.drGroup=diskfilegroup;
  drData.drOwner=diskfileowner;
  drData.drPath=diskfilepath;
  cta::common::dataStructures::CancelRetrieveRequest request;
  request.drData=drData;
  request.archiveFileID=id;
  request.requester=originator;
  request.dstURL=dsturl;
  m_scheduler->cancelRetrieve(cliIdentity, request);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_updatefilestorageclass
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_updatefilestorageclass(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " ufsc/updatefilestorageclass --encoded <\"true\" or \"false\"> --user <user> --group <group> --id <CTA_ArchiveFileID> --storageclass <storage_class> --diskfilepath <disk_filepath>" << std::endl
                    << "\t--diskfileowner <disk_fileowner> --diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob>" << std::endl;
  std::string encoded_s = getOptionValue(tokens, "", "--encoded", false);
  if(encoded_s!="true" && encoded_s!="false") {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  bool encoded = encoded_s=="true"?true:false;
  std::string user = getOptionValue(tokens, "", "--user", encoded);
  std::string group = getOptionValue(tokens, "", "--group", encoded);
  std::string id_s = getOptionValue(tokens, "", "--id", encoded);
  std::string storageclass = getOptionValue(tokens, "", "--storageclass", encoded);
  std::string diskfilepath = getOptionValue(tokens, "", "--diskfilepath", encoded);
  std::string diskfileowner = getOptionValue(tokens, "", "--diskfileowner", encoded);
  std::string diskfilegroup = getOptionValue(tokens, "", "--diskfilegroup", encoded);
  std::string recoveryblob = getOptionValue(tokens, "", "--recoveryblob", encoded);
  if(user.empty() || group.empty() || id_s.empty() || storageclass.empty() || diskfilepath.empty() || diskfileowner.empty() || diskfilegroup.empty() || recoveryblob.empty()) {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  uint64_t id = stringParameterToUint64("--id", id_s);
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user;
  originator.group=group;
  cta::common::dataStructures::DRData drData;
  drData.drBlob=recoveryblob;
  drData.drGroup=diskfilegroup;
  drData.drOwner=diskfileowner;
  drData.drPath=diskfilepath;
  cta::common::dataStructures::UpdateFileStorageClassRequest request;
  request.drData=drData;
  request.archiveFileID=id;
  request.requester=originator;
  request.storageClass=storageclass;
  m_scheduler->updateFileStorageClass(cliIdentity, request);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_updatefileinfo
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_updatefileinfo(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " ufi/updatefileinfo --encoded <\"true\" or \"false\"> --id <CTA_ArchiveFileID> --diskfilepath <disk_filepath>" << std::endl
                    << "\t--diskfileowner <disk_fileowner> --diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob>" << std::endl;
  std::string encoded_s = getOptionValue(tokens, "", "--encoded", false);
  if(encoded_s!="true" && encoded_s!="false") {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  bool encoded = encoded_s=="true"?true:false;
  std::string id_s = getOptionValue(tokens, "", "--id", encoded);
  std::string diskfilepath = getOptionValue(tokens, "", "--diskfilepath", encoded);
  std::string diskfileowner = getOptionValue(tokens, "", "--diskfileowner", encoded);
  std::string diskfilegroup = getOptionValue(tokens, "", "--diskfilegroup", encoded);
  std::string recoveryblob = getOptionValue(tokens, "", "--recoveryblob", encoded);
  if(id_s.empty() || diskfilepath.empty() || diskfileowner.empty() || diskfilegroup.empty() || recoveryblob.empty()) {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  uint64_t id = stringParameterToUint64("--id", id_s);
  cta::common::dataStructures::DRData drData;
  drData.drBlob=recoveryblob;
  drData.drGroup=diskfilegroup;
  drData.drOwner=diskfileowner;
  drData.drPath=diskfilepath;
  cta::common::dataStructures::UpdateFileInfoRequest request;
  request.drData=drData;
  request.archiveFileID=id;
  m_scheduler->updateFileInfo(cliIdentity, request);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// xCom_liststorageclass
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_liststorageclass(const std::vector<std::string> &tokens, const cta::common::dataStructures::SecurityIdentity &cliIdentity) {
  std::stringstream help;
  help << tokens[0] << " lsc/liststorageclass --encoded <\"true\" or \"false\"> --user <user> --group <group>" << std::endl;
  std::string encoded_s = getOptionValue(tokens, "", "--encoded", false);
  if(encoded_s!="true" && encoded_s!="false") {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  bool encoded = encoded_s=="true"?true:false;
  std::string user = getOptionValue(tokens, "", "--user", encoded);
  std::string group = getOptionValue(tokens, "", "--group", encoded);
  if(user.empty() || group.empty()) {
    m_data = help.str();
    error.setErrInfo(EPERM, m_data.c_str());
    return SFS_ERROR;
  }
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user;
  originator.group=group;
  cta::common::dataStructures::ListStorageClassRequest request;
  request.requester=originator;
  m_scheduler->listStorageClass(cliIdentity, request);
  return SFS_OK;
}
  
//------------------------------------------------------------------------------
// getGenericHelp
//------------------------------------------------------------------------------
std::string XrdCtaFile::getGenericHelp(const std::string &programName) const {
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
  help << programName << " mountpolicy/mp    add/ch/rm/ls"               << std::endl;
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
  help << programName << " updatefilestorageclass/ufsc"                  << std::endl;
  return help.str();
}

}}
