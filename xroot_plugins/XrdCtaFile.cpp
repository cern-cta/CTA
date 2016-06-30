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

#include "scheduler/SchedulerDatabase.hpp"
#include "xroot_plugins/XrdCtaFile.hpp"

#include "XrdSec/XrdSecEntity.hh"

#include "catalogue/ArchiveFileSearchCriteria.hpp"
#include "common/Configuration.hpp"
#include "common/utils/utils.hpp"

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
void XrdCtaFile::checkClient(const XrdSecEntity *client) {
  if(client==NULL || client->name==NULL || client->host==NULL) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+": [ERROR] XrdSecEntity from xroot contains invalid information (NULL pointer detected!)");
  }
  std::cout << "FILE Request received from client. Username: " << client->name << " Host: " << client->host << std::endl;
  m_cliIdentity.username=client->name;
  m_cliIdentity.host=client->host;
}

//------------------------------------------------------------------------------
// logRequestAndSetCmdlineResult
//------------------------------------------------------------------------------
int XrdCtaFile::logRequestAndSetCmdlineResult(const cta::common::dataStructures::FrontendReturnCode rc, const std::string &returnString) {
  m_cmdlineOutput = returnString;
  m_cmdlineReturnCode = rc;
  
  std::list<log::Param> params;
  params.push_back(log::Param("USERNAME", m_cliIdentity.username));
  params.push_back(log::Param("HOST", m_cliIdentity.host));
  params.push_back(log::Param("RETURN_CODE", toString(m_cmdlineReturnCode)));
  std::stringstream originalRequest;
  for(auto it=m_requestTokens.begin(); it!=m_requestTokens.end(); it++) {
    originalRequest << *it << " ";
  }
  params.push_back(log::Param("REQUEST", originalRequest.str()));
  
  switch(m_cmdlineReturnCode) {
    case cta::common::dataStructures::FrontendReturnCode::ok:
      m_log(log::INFO, "Successful Request", params);
      break;
    case cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry:
      m_log(log::USERERR, "Syntax error or missing argument(s) in request", params);
      break;
    default:
      params.push_back(log::Param("ERROR", returnString));
      m_log(log::ERR, "Unsuccessful Request", params);
      break;
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// commandDispatcher
//------------------------------------------------------------------------------
int XrdCtaFile::dispatchCommand() {
  std::string command(m_requestTokens.at(1));
  
  std::vector<std::string> adminCommands = {"bs","bootstrap","ad","admin","ah","adminhost","tp","tapepool","ar","archiveroute","ll","logicallibrary",
          "ta","tape","sc","storageclass","us","user","mg","mountpolicy","de","dedication","re","repack","sh","shrink","ve","verify",
          "af","archivefile","te","test","dr","drive","rc","reconcile","lpa","listpendingarchives","lpr","listpendingretrieves","lds","listdrivestates"};
  
  if (std::find(adminCommands.begin(), adminCommands.end(), command) != adminCommands.end()) {
    m_scheduler->authorizeCliIdentity(m_cliIdentity);
  }
  
  if     ("bs"   == command || "bootstrap"              == command) {return xCom_bootstrap();}
  else if("ad"   == command || "admin"                  == command) {return xCom_admin();}
  else if("ah"   == command || "adminhost"              == command) {return xCom_adminhost();}
  else if("tp"   == command || "tapepool"               == command) {return xCom_tapepool();}
  else if("ar"   == command || "archiveroute"           == command) {return xCom_archiveroute();}
  else if("ll"   == command || "logicallibrary"         == command) {return xCom_logicallibrary();}
  else if("ta"   == command || "tape"                   == command) {return xCom_tape();}
  else if("sc"   == command || "storageclass"           == command) {return xCom_storageclass();}
  else if("rmr"  == command || "requestermountrule"     == command) {return xCom_requestermountrule();}
  else if("gmr"  == command || "groupmountrule"         == command) {return xCom_groupmountrule();}
  else if("mg"   == command || "mountpolicy"            == command) {return xCom_mountpolicy();}
  else if("de"   == command || "dedication"             == command) {return xCom_dedication();}
  else if("re"   == command || "repack"                 == command) {return xCom_repack();}
  else if("sh"   == command || "shrink"                 == command) {return xCom_shrink();}
  else if("ve"   == command || "verify"                 == command) {return xCom_verify();}
  else if("af"   == command || "archivefile"            == command) {return xCom_archivefile();}
  else if("te"   == command || "test"                   == command) {return xCom_test();}
  else if("dr"   == command || "drive"                  == command) {return xCom_drive();}
  else if("rc"   == command || "reconcile"              == command) {return xCom_reconcile();}
  else if("lpa"  == command || "listpendingarchives"    == command) {return xCom_listpendingarchives();}
  else if("lpr"  == command || "listpendingretrieves"   == command) {return xCom_listpendingretrieves();}
  else if("lds"  == command || "listdrivestates"        == command) {return xCom_listdrivestates();}
  else if("a"    == command || "archive"                == command) {return xCom_archive();}
  else if("r"    == command || "retrieve"               == command) {return xCom_retrieve();}
  else if("da"   == command || "deletearchive"          == command) {return xCom_deletearchive();}
  else if("cr"   == command || "cancelretrieve"         == command) {return xCom_cancelretrieve();}
  else if("ufi"  == command || "updatefileinfo"         == command) {return xCom_updatefileinfo();}
  else if("ufsc" == command || "updatefilestorageclass" == command) {return xCom_updatefilestorageclass();}
  else if("lsc"  == command || "liststorageclass"       == command) {return xCom_liststorageclass();}
  
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, getGenericHelp(m_requestTokens.at(0)));
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
    checkClient(client);
    if(!strlen(fileName)) { //this should never happen
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, getGenericHelp(""));
    }
    
    std::stringstream ss(fileName+1); //let's skip the first slash which is always prepended since we are asking for an absolute path
    std::string item;
    while (std::getline(ss, item, '&')) {
      replaceAll(item, "_", "/"); 
      //need to add this because xroot removes consecutive slashes, and the 
      //cryptopp base64 algorithm may produce consecutive slashes. This is solved 
      //in cryptopp-5.6.3 (using Base64URLEncoder instead of Base64Encoder) but we 
      //currently have cryptopp-5.6.2. To be changed in the future...
      item = decode(item);
      m_requestTokens.push_back(item);
    }

    if(m_requestTokens.size() == 0) { //this should never happen
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, getGenericHelp(""));
    }
    if(m_requestTokens.size() < 2) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, getGenericHelp(m_requestTokens.at(0)));
    }    
    return dispatchCommand();
  } catch (cta::exception::Exception &ex) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ctaErrorNoRetry, ex.getMessageValue()+"\n");
  } catch (std::exception &ex) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ctaErrorNoRetry, std::string(ex.what())+"\n");
  } catch (...) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ctaErrorNoRetry, "Unknown exception caught!\n");
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
  m_cmdlineOutput = std::to_string(m_cmdlineReturnCode) + m_cmdlineOutput;
  *Addr = const_cast<char *>(m_cmdlineOutput.c_str());
  Size = m_cmdlineOutput.length();
  return SFS_OK; //change to "return SFS_ERROR;" in case the read function below is wanted, in that case uncomment the lines in that function.
}

//------------------------------------------------------------------------------
// read
//------------------------------------------------------------------------------
XrdSfsXferSize XrdCtaFile::read(XrdSfsFileOffset offset, char *buffer, XrdSfsXferSize size) {
//  if((unsigned long)offset<m_cmdlineOutput.length()) {
//    strncpy(buffer, m_cmdlineOutput.c_str()+offset, size);
//    return m_cmdlineOutput.length()-offset;
//  }
//  else {
//    return SFS_OK;
//  }
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// read
//------------------------------------------------------------------------------
XrdSfsXferSize XrdCtaFile::read(XrdSfsFileOffset offset, XrdSfsXferSize size) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// read
//------------------------------------------------------------------------------
XrdSfsXferSize XrdCtaFile::read(XrdSfsAio *aioparm) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
XrdSfsXferSize XrdCtaFile::write(XrdSfsFileOffset offset, const char *buffer, XrdSfsXferSize size) {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
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
  buf->st_size=m_cmdlineOutput.length();
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
  cta::log::Logger *log,
  const char *user,
  int MonID):
  error(user, MonID),
  m_catalogue(catalogue),
  m_scheduler(scheduler),
  m_log(*log),
  m_cmdlineOutput(""),
  m_cmdlineReturnCode(cta::common::dataStructures::FrontendReturnCode::ok) {  
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
// getOption
//------------------------------------------------------------------------------
std::string XrdCtaFile::getOption(const std::string& optionShortName, const std::string& optionLongName, const bool encoded) {
  for(auto it=m_requestTokens.cbegin(); it!=m_requestTokens.cend(); it++) {
    if(optionShortName == *it || optionLongName == *it) {
      auto it_next=it+1;
      if(it_next!=m_requestTokens.cend()) {
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
// getOptionStringValue
//------------------------------------------------------------------------------
optional<std::string> XrdCtaFile::getOptionStringValue(const std::string& optionShortName, const std::string& optionLongName, const bool encoded) {
  std::string option = getOption(optionShortName, optionLongName, encoded);
  if(option.empty()) {
    return optional<std::string>();
  }
  return optional<std::string>(option);
}

//------------------------------------------------------------------------------
// getOptionUint64Value
//------------------------------------------------------------------------------
optional<uint64_t> XrdCtaFile::getOptionUint64Value(const std::string& optionShortName, const std::string& optionLongName, const bool encoded) {
  std::string option = getOption(optionShortName, optionLongName, encoded);
  if(option.empty()) {
    return optional<uint64_t>();
  }
  return optional<uint64_t>(stringParameterToUint64(optionLongName, option));
}

//------------------------------------------------------------------------------
// getOptionBoolValue
//------------------------------------------------------------------------------
optional<bool> XrdCtaFile::getOptionBoolValue(const std::string& optionShortName, const std::string& optionLongName, const bool encoded) {
  std::string option = getOption(optionShortName, optionLongName, encoded);
  if(option.empty()) {
    return optional<bool>();
  }
  return optional<bool>(stringParameterToBool(optionLongName, option));
}

//------------------------------------------------------------------------------
// getOptionTimeValue
//------------------------------------------------------------------------------
optional<time_t> XrdCtaFile::getOptionTimeValue(const std::string& optionShortName, const std::string& optionLongName, const bool encoded) {
  std::string option = getOption(optionShortName, optionLongName, encoded);
  if(option.empty()) {
    return optional<time_t>();
  }
  return optional<time_t>(stringParameterToTime(optionLongName, option));
}

//------------------------------------------------------------------------------
// hasOption
//------------------------------------------------------------------------------
bool XrdCtaFile::hasOption(const std::string& optionShortName, const std::string& optionLongName) {
  for(auto it=m_requestTokens.cbegin(); it!=m_requestTokens.cend(); it++) {
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
std::string XrdCtaFile::formatResponse(const std::vector<std::vector<std::string>> &responseTable, const bool withHeader) {
  if(responseTable.empty()||responseTable.at(0).empty()) {
    return "";
  }
  std::vector<int> columnSizes;
  for(uint j=0; j<responseTable.at(0).size(); j++) { //for each column j
    uint columnSize=0;
    for(uint i=0; i<responseTable.size(); i++) { //for each row i
      if(responseTable.at(i).at(j).size()>columnSize) {
        columnSize=responseTable.at(i).at(j).size();
      }
    }
    columnSize++; //add one space
    columnSizes.push_back(columnSize);//loops here
  }
  std::stringstream responseSS;
  for(auto row=responseTable.cbegin(); row!=responseTable.cend(); row++) {
    if(withHeader && row==responseTable.cbegin()) responseSS << "\x1b[31;1m";
    for(uint i=0; i<row->size(); i++) {
      responseSS << " " << std::setw(columnSizes.at(i)) << row->at(i);
    }
    if(withHeader && row==responseTable.cbegin()) responseSS << "\x1b[0m" << std::endl;
    else responseSS << std::endl;
  }
  return responseSS.str();
}

//------------------------------------------------------------------------------
// addLogInfoToResponseRow
//------------------------------------------------------------------------------
void XrdCtaFile::addLogInfoToResponseRow(std::vector<std::string> &responseRow, const cta::common::dataStructures::EntryLog &creationLog, const cta::common::dataStructures::EntryLog &lastModificationLog) {
  responseRow.push_back(creationLog.username);
  responseRow.push_back(creationLog.host);
  responseRow.push_back(timeToString(creationLog.time));
  responseRow.push_back(lastModificationLog.username);
  responseRow.push_back(lastModificationLog.host);
  responseRow.push_back(timeToString(lastModificationLog.time));
}

//------------------------------------------------------------------------------
// stringParameterToUint64
//------------------------------------------------------------------------------
uint64_t XrdCtaFile::stringParameterToUint64(const std::string &parameterName, const std::string &parameterValue) const {
  try {
    return cta::utils::toUint64(parameterValue);
  } catch(cta::exception::Exception &ex) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" - Parameter: "+parameterName+" ("+parameterValue+
            ") could not be converted to uint64_t because: " + ex.getMessageValue());
  }
}

//------------------------------------------------------------------------------
// stringParameterToBool
//------------------------------------------------------------------------------
bool XrdCtaFile::stringParameterToBool(const std::string &parameterName, const std::string &parameterValue) const {
  if(parameterValue == "true") {
    return true;
  } 
  else if(parameterValue == "false") {
    return false;
  } 
  else {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" - Parameter: "+parameterName+" ("+parameterValue+
            ") could not be converted to bool: the only values allowed are \"true\" and \"false\"");
  }
}

//------------------------------------------------------------------------------
// stringParameterToTime
//------------------------------------------------------------------------------
time_t XrdCtaFile::stringParameterToTime(const std::string &parameterName, const std::string &parameterValue) const {
  struct tm time;
  if(NULL==strptime(parameterValue.c_str(), "%d/%m/%Y", &time)) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" - Parameter: "+parameterName+" ("+parameterValue+
            ") could not be interpreted as a time, the allowed format is: <DD/MM/YYYY>");
  }
  return mktime(&time);
}

//------------------------------------------------------------------------------
// xCom_bootstrap
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_bootstrap() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " bs/bootstrap --username/-u <user_name> --hostname/-h <host_name> --comment/-m <\"comment\">" << std::endl;
  optional<std::string> username = getOptionStringValue("-u", "--username", false);
  optional<std::string> hostname = getOptionStringValue("-h", "--hostname", false);
  optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
  if(!username||!hostname||!comment) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  m_catalogue->createBootstrapAdminAndHostNoAuth(m_cliIdentity, username.value(), hostname.value(), comment.value());
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_admin
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_admin() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ad/admin add/ch/rm/ls:" << std::endl
       << "\tadd --username/-u <user_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --username/-u <user_name> --comment/-m <\"comment\">" << std::endl
       << "\trm  --username/-u <user_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> username = getOptionStringValue("-u", "--username", false);
    if(!username) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }
    if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2)) {
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if(!comment) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      if("add" == m_requestTokens.at(2)) { //add
        m_catalogue->createAdminUser(m_cliIdentity, username.value(), comment.value());
      }
      else { //ch
        m_catalogue->modifyAdminUserComment(m_cliIdentity, username.value(), comment.value());
      }
    }
    else { //rm
      m_catalogue->deleteAdminUser(username.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::AdminUser> list= m_catalogue->getAdminUsers();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"user","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_adminhost
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_adminhost() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ah/adminhost add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <host_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <host_name> --comment/-m <\"comment\">" << std::endl
       << "\trm  --name/-n <host_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> hostname = getOptionStringValue("-n", "--name", false);
    if(!hostname) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }
    if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2)) {
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if(!comment) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      if("add" == m_requestTokens.at(2)) { //add
        m_catalogue->createAdminHost(m_cliIdentity, hostname.value(), comment.value());
      }
      else { //ch
        m_catalogue->modifyAdminHostComment(m_cliIdentity, hostname.value(), comment.value());
      }
    }
    else { //rm
      m_catalogue->deleteAdminHost(hostname.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::AdminHost> list= m_catalogue->getAdminHosts();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"hostname","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_tapepool
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_tapepool() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " tp/tapepool add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <tapepool_name> --partialtapesnumber/-p <number_of_partial_tapes> [--encryption/-e or --clear/-c] --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <tapepool_name> [--partialtapesnumber/-p <number_of_partial_tapes>] [--encryption/-e or --clear/-c] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <tapepool_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> name = getOptionStringValue("-n", "--name", false);
    if(!name) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }
    if("add" == m_requestTokens.at(2)) { //add
      optional<uint64_t> ptn = getOptionUint64Value("-p", "--partialtapesnumber", false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if(!comment||!ptn) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      bool encryption=false;
      if((hasOption("-e", "--encryption") && hasOption("-c", "--clear"))) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      encryption=hasOption("-e", "--encryption");
      m_catalogue->createTapePool(m_cliIdentity, name.value(), ptn.value(), encryption, comment.value());
    }
    else if("ch" == m_requestTokens.at(2)) { //ch
      optional<uint64_t> ptn = getOptionUint64Value("-p", "--partialtapesnumber", false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if(!comment&&!ptn) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      if(comment) {
        m_catalogue->modifyTapePoolComment(m_cliIdentity, name.value(), comment.value());
      }
      if(ptn) {
        m_catalogue->modifyTapePoolNbPartialTapes(m_cliIdentity, name.value(), ptn.value());
      }
      if(hasOption("-e", "--encryption")) {
        m_catalogue->setTapePoolEncryption(m_cliIdentity, name.value(), true);
      }
      if(hasOption("-c", "--clear")) {
        m_catalogue->setTapePoolEncryption(m_cliIdentity, name.value(), false);
      }
    }
    else { //rm
      m_catalogue->deleteTapePool(name.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::TapePool> list= m_catalogue->getTapePools();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"name","# partial tapes","encrypt","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        currentRow.push_back(std::to_string((unsigned long long)it->nbPartialTapes));
        if(it->encryption) currentRow.push_back("true"); else currentRow.push_back("false");
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_archiveroute
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_archiveroute() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ar/archiveroute add/ch/rm/ls:" << std::endl
       << "\tadd --storageclass/-s <storage_class_name> --copynb/-c <copy_number> --tapepool/-t <tapepool_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --storageclass/-s <storage_class_name> --copynb/-c <copy_number> [--tapepool/-t <tapepool_name>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --storageclass/-s <storage_class_name> --copynb/-c <copy_number>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> scn = getOptionStringValue("-s", "--storageclass", false);
    optional<uint64_t> cn = getOptionUint64Value("-c", "--copynb", false);
    if(!scn||!cn) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }
    if("add" == m_requestTokens.at(2)) { //add
      optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if(!comment||!tapepool) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      m_catalogue->createArchiveRoute(m_cliIdentity, scn.value(), cn.value(), tapepool.value(), comment.value());
    }
    else if("ch" == m_requestTokens.at(2)) { //ch
      optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if(!comment&&!tapepool) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      if(comment) {
        m_catalogue->modifyArchiveRouteComment(m_cliIdentity, scn.value(), cn.value(), comment.value());
      }
      if(tapepool) {
        m_catalogue->modifyArchiveRouteTapePoolName(m_cliIdentity, scn.value(), cn.value(), tapepool.value());
      }
    }
    else { //rm
      m_catalogue->deleteArchiveRoute(scn.value(), cn.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::ArchiveRoute> list= m_catalogue->getArchiveRoutes();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"storage class","copy number","tapepool","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->storageClassName);
        currentRow.push_back(std::to_string((unsigned long long)it->copyNb));
        currentRow.push_back(it->tapePoolName);
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_logicallibrary
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_logicallibrary() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ll/logicallibrary add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <logical_library_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <logical_library_name> --comment/-m <\"comment\">" << std::endl
       << "\trm  --name/-n <logical_library_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> name = getOptionStringValue("-n", "--name", false);
    if(!name) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }
    if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2)) {
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if("add" == m_requestTokens.at(2)) { //add
        if(!comment) {
          return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
        }
        m_catalogue->createLogicalLibrary(m_cliIdentity, name.value(), comment.value());
      }
      else { //ch
        if(!comment) {
          return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
        }
        m_catalogue->modifyLogicalLibraryComment(m_cliIdentity, name.value(), comment.value());
      }
    }
    else { //rm
      m_catalogue->deleteLogicalLibrary(name.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::LogicalLibrary> list= m_catalogue->getLogicalLibraries();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"name","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_tape
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_tape() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ta/tape add/ch/rm/reclaim/ls/label:" << std::endl
       << "\tadd     --vid/-v <vid> --logicallibrary/-l <logical_library_name> --tapepool/-t <tapepool_name> --capacity/-c <capacity_in_bytes> [--encryptionkey/-k <encryption_key>]" << std::endl
       << "\t        [--enabled/-e or --disabled/-d] [--free/-f or --full/-F] [--comment/-m <\"comment\">] " << std::endl
       << "\tch      --vid/-v <vid> [--logicallibrary/-l <logical_library_name>] [--tapepool/-t <tapepool_name>] [--capacity/-c <capacity_in_bytes>] [--encryptionkey/-k <encryption_key>]" << std::endl
       << "\t        [--enabled/-e or --disabled/-d] [--free/-f or --full/-F] [--comment/-m <\"comment\">]" << std::endl
       << "\trm      --vid/-v <vid>" << std::endl
       << "\treclaim --vid/-v <vid>" << std::endl
       << "\tls      [--header/-h] [--vid/-v <vid>] [--logicallibrary/-l <logical_library_name>] [--tapepool/-t <tapepool_name>] [--capacity/-c <capacity_in_bytes>]" << std::endl
       << "\t        [--lbp/-p or --nolbp/-P] [--enabled/-e or --disabled/-d] [--free/-f or --full/-F] [--busy/-b or --notbusy/-n]" << std::endl
       << "\tlabel   --vid/-v <vid> [--force/-f] [--lbp/-l] [--tag/-t <tag_name>]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2) || "reclaim" == m_requestTokens.at(2) || "label" == m_requestTokens.at(2)) {
    optional<std::string> vid = getOptionStringValue("-v", "--vid", false);
    if(!vid) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }
    if("add" == m_requestTokens.at(2)) { //add
      optional<std::string> logicallibrary = getOptionStringValue("-l", "--logicallibrary", false);
      optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", false);
      optional<uint64_t> capacity = getOptionUint64Value("-c", "--capacity", false);
      if(!logicallibrary||!tapepool||!capacity) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      bool disabled=false;
      bool full=false;
      if((hasOption("-e", "--enabled") && hasOption("-d", "--disabled")) || (hasOption("-f", "--free") && hasOption("-F", "--full"))) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      disabled=hasOption("-d", "--disabled");
      full=hasOption("-F", "--full");
      optional<std::string> encryptionkey = getOptionStringValue("-k", "--encryptionkey", false);
      m_catalogue->createTape(m_cliIdentity, vid.value(), logicallibrary.value(), tapepool.value(), encryptionkey.value(), capacity.value(), disabled, full, comment.value());
    }
    else if("ch" == m_requestTokens.at(2)) { //ch
      optional<std::string> logicallibrary = getOptionStringValue("-l", "--logicallibrary", false);
      optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", false);
      optional<uint64_t> capacity = getOptionUint64Value("-c", "--capacity", false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      optional<std::string> encryptionkey = getOptionStringValue("-k", "--encryptionkey", false);
      if(!comment && !logicallibrary && !tapepool && !capacity && !encryptionkey && !hasOption("-e", "--enabled")
              && !hasOption("-d", "--disabled") && !hasOption("-f", "--free") && !hasOption("-F", "--full")) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      if((hasOption("-e", "--enabled") && hasOption("-d", "--disabled")) || (hasOption("-f", "--free") && hasOption("-F", "--full"))) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      if(logicallibrary) {
        m_catalogue->modifyTapeLogicalLibraryName(m_cliIdentity, vid.value(), logicallibrary.value());
      }
      if(tapepool) {
        m_catalogue->modifyTapeTapePoolName(m_cliIdentity, vid.value(), tapepool.value());
      }
      if(capacity) {
        m_catalogue->modifyTapeCapacityInBytes(m_cliIdentity, vid.value(), capacity.value());
      }
      if(comment) {
        m_catalogue->modifyTapeComment(m_cliIdentity, vid.value(), comment.value());
      }
      if(encryptionkey) {
        m_catalogue->modifyTapeEncryptionKey(m_cliIdentity, vid.value(), encryptionkey.value());
      }
      if(hasOption("-e", "--enabled")) {
        m_scheduler->setTapeDisabled(m_cliIdentity, vid.value(), false);
      }
      if(hasOption("-d", "--disabled")) {
        m_scheduler->setTapeDisabled(m_cliIdentity, vid.value(), true);
      }
      if(hasOption("-f", "--free")) {
        m_scheduler->setTapeFull(m_cliIdentity, vid.value(), false);
      }
      if(hasOption("-F", "--full")) {
        m_scheduler->setTapeFull(m_cliIdentity, vid.value(), true);
      }
    }
    else if("reclaim" == m_requestTokens.at(2)) { //reclaim
      m_catalogue->reclaimTape(m_cliIdentity, vid.value());
    }
    else if("label" == m_requestTokens.at(2)) { //label
      optional<std::string> tag = getOptionStringValue("-t", "--tag", false);
      m_scheduler->labelTape(m_cliIdentity, vid.value(), hasOption("-f", "--force"), hasOption("-l", "--lbp"), tag.value());
    }
    else { //rm
      m_catalogue->deleteTape(vid.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    optional<std::string> vid = getOptionStringValue("-v", "--vid", false);
    optional<std::string> logicallibrary = getOptionStringValue("-l", "--logicallibrary", false);
    optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", false);
    optional<uint64_t> capacity = getOptionUint64Value("-c", "--capacity", false);
    if((hasOption("-e", "--enabled") && hasOption("-d", "--disabled")) 
            || (hasOption("-f", "--free") && hasOption("-F", "--full")) 
            // TODO BUSY IS GONE!
            || (hasOption("-b", "--busy") && hasOption("-n", "--notbusy"))
            || (hasOption("-p", "--lbp") && hasOption("-P", "--nolbp"))) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }
    cta::catalogue::TapeSearchCriteria searchCriteria;
    if(hasOption("-e", "--enabled")) {
      searchCriteria.disabled = "false";
    }
    if(hasOption("-d", "--disabled")) {
      searchCriteria.disabled = "true";
    }
    if(hasOption("-f", "--free")) {
      searchCriteria.full = "false";
    }
    if(hasOption("-F", "--full")) {
      searchCriteria.full = "true";
    }
    if(hasOption("-p", "--lbp")) {
      searchCriteria.lbp = "true";
    }
    if(hasOption("-P", "--nolbp")) {
      searchCriteria.lbp = "false";
    }
    std::list<cta::common::dataStructures::Tape> list= m_catalogue->getTapes(searchCriteria);
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"vid","logical library","tapepool","encription key","capacity","occupancy","last fseq","busy","full","disabled","lpb","label drive","label time",
                                         "last w drive","last w time","last r drive","last r time","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
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
        // TODO currentRow.push_back(it->labelLog.drive);
        currentRow.push_back("TODO");
        // TODO currentRow.push_back(std::to_string((unsigned long long)it->labelLog.time));
        currentRow.push_back("TODO");
        // TODO currentRow.push_back(it->lastWriteLog.drive);
        currentRow.push_back("TODO");
        // TODO currentRow.push_back(std::to_string((unsigned long long)it->lastWriteLog.time));
        currentRow.push_back("TODO");
        // TODO currentRow.push_back(it->lastReadLog.drive);
        currentRow.push_back("TODO");
        // TODO currentRow.push_back(std::to_string((unsigned long long)it->lastReadLog.time));
        currentRow.push_back("TODO");
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_storageclass
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_storageclass() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " sc/storageclass add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <storage_class_name> --copynb/-c <number_of_tape_copies> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <storage_class_name> [--copynb/-c <number_of_tape_copies>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <storage_class_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> scn = getOptionStringValue("-n", "--name", false);
    if(!scn) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }  
    if("add" == m_requestTokens.at(2)) { //add
      optional<uint64_t> cn = getOptionUint64Value("-c", "--copynb", false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if(!comment||!cn) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      m_catalogue->createStorageClass(m_cliIdentity, scn.value(), cn.value(), comment.value());
    }
    else if("ch" == m_requestTokens.at(2)) { //ch
      optional<uint64_t> cn = getOptionUint64Value("-c", "--copynb", false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if(!comment&&!cn) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      if(comment) {
        m_catalogue->modifyStorageClassComment(m_cliIdentity, scn.value(), comment.value());
      }
      if(cn) {
        m_catalogue->modifyStorageClassNbCopies(m_cliIdentity, scn.value(), cn.value());
      }
    }
    else { //rm
      m_catalogue->deleteStorageClass(scn.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::StorageClass> list= m_catalogue->getStorageClasses();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"storage class","number of copies","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        currentRow.push_back(std::to_string((unsigned long long)it->nbCopies));
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_requestermountrule
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_requestermountrule() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " rmr/requestermountrule add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <user_name> --mountpolicy/-u <policy_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <user_name> [--mountpolicy/-u <policy_name>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <user_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> name = getOptionStringValue("-n", "--name", false);
    if(!name) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }
    if("add" == m_requestTokens.at(2)) { //add
      optional<std::string> mountpolicy = getOptionStringValue("-u", "--mountpolicy", false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if(!comment||!mountpolicy) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      m_catalogue->createRequesterMountRule(m_cliIdentity, mountpolicy.value(), name.value(), comment.value());
    }
    else if("ch" == m_requestTokens.at(2)) { //ch
      optional<std::string> mountpolicy = getOptionStringValue("-u", "--mountpolicy", false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if(!comment&&!mountpolicy) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      if(comment) {
        m_catalogue->modifyRequesterComment(m_cliIdentity, name.value(), comment.value());
      }
      if(mountpolicy) {
        m_catalogue->modifyRequesterMountPolicy(m_cliIdentity, name.value(), mountpolicy.value());
      }
    }
    else { //rm
      m_catalogue->deleteRequesterMountRule(name.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::RequesterMountRule> list= m_catalogue->getRequesterMountRules();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"username","policy","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        currentRow.push_back(it->mountPolicy);
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_groupmountrule
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_groupmountrule() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " gmr/groupmountrule add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <user_name> --mountpolicy/-u <policy_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <user_name> [--mountpolicy/-u <policy_name>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <user_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> name = getOptionStringValue("-n", "--name", false);
    if(!name) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }
    if("add" == m_requestTokens.at(2)) { //add
      optional<std::string> mountpolicy = getOptionStringValue("-u", "--mountpolicy", false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if(!comment||!mountpolicy) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      m_catalogue->createRequesterGroupMountRule(m_cliIdentity, mountpolicy.value(), name.value(), comment.value());
    }
    else if("ch" == m_requestTokens.at(2)) { //ch
      optional<std::string> mountpolicy = getOptionStringValue("-u", "--mountpolicy", false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if(!comment&&!mountpolicy) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      if(comment) {
        m_catalogue->modifyRequesterGroupComment(m_cliIdentity, name.value(), comment.value());
      }
      if(mountpolicy) {
        m_catalogue->modifyRequesterGroupMountPolicy(m_cliIdentity, name.value(), mountpolicy.value());
      }
    }
    else { //rm
      m_catalogue->deleteRequesterGroupMountRule(name.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::RequesterGroupMountRule> list= m_catalogue->getRequesterGroupMountRules();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"group","policy","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->name);
        currentRow.push_back(it->mountPolicy);
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_mountpolicy
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_mountpolicy() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " mg/mountpolicy add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <mountpolicy_name> --archivepriority/--ap <priority_value> --minarchiverequestage/--aa <minRequestAge> --retrievepriority/--rp <priority_value>" << std::endl
       << "\t    --minretrieverequestage/--ra <minRequestAge> --maxdrivesallowed/-d <maxDrivesAllowed> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <mountpolicy_name> [--archivepriority/--ap <priority_value>] [--minarchiverequestage/--aa <minRequestAge>] [--retrievepriority/--rp <priority_value>]" << std::endl
       << "\t   [--minretrieverequestage/--ra <minRequestAge>] [--maxdrivesallowed/-d <maxDrivesAllowed>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <mountpolicy_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> group = getOptionStringValue("-n", "--name", false);
    if(!group) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }
    if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2)) {      
      optional<uint64_t> archivepriority = getOptionUint64Value("--ap", "--archivepriority", false);
      optional<uint64_t> minarchiverequestage = getOptionUint64Value("--aa", "--minarchiverequestage", false);
      optional<uint64_t> retrievepriority = getOptionUint64Value("--rp", "--retrievepriority", false);
      optional<uint64_t> minretrieverequestage = getOptionUint64Value("--ra", "--minretrieverequestage", false);
      optional<uint64_t> maxdrivesallowed = getOptionUint64Value("-d", "--maxdrivesallowed", false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if("add" == m_requestTokens.at(2)) { //add
        if(!archivepriority||!minarchiverequestage||!retrievepriority
                ||!minretrieverequestage||!maxdrivesallowed||!comment) {
          return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
        }
        m_catalogue->createMountPolicy(m_cliIdentity, group.value(), archivepriority.value(), minarchiverequestage.value(), retrievepriority.value(), minretrieverequestage.value(), maxdrivesallowed.value(), comment.value());
      }
      else if("ch" == m_requestTokens.at(2)) { //ch
        if(!archivepriority&&!minarchiverequestage&&!retrievepriority
                &&!minretrieverequestage&&!maxdrivesallowed&&!comment) {
          return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
        }
        if(archivepriority) {
          m_catalogue->modifyMountPolicyArchivePriority(m_cliIdentity, group.value(), archivepriority.value());
        }
        if(minarchiverequestage) {
          m_catalogue->modifyMountPolicyArchiveMinRequestAge(m_cliIdentity, group.value(), minarchiverequestage.value());
        }
        if(retrievepriority) {
          m_catalogue->modifyMountPolicyRetrievePriority(m_cliIdentity, group.value(), retrievepriority.value());
        }
        if(minretrieverequestage) {
          m_catalogue->modifyMountPolicyRetrieveMinRequestAge(m_cliIdentity, group.value(), minretrieverequestage.value());
        }
        if(maxdrivesallowed) {
          m_catalogue->modifyMountPolicyMaxDrivesAllowed(m_cliIdentity, group.value(), maxdrivesallowed.value());
        }
        if(comment) {
          m_catalogue->modifyMountPolicyComment(m_cliIdentity, group.value(), comment.value());
        }
      }
    }
    else { //rm
      m_catalogue->deleteMountPolicy(group.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::MountPolicy> list= m_catalogue->getMountPolicies();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"cta group","a.priority","a.minFiles","a.minBytes","a.minAge","r.priority","r.minFiles","r.minBytes","r.minAge","MaxDrives","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
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
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_dedication
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_dedication() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " de/dedication add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <drive_name> [--readonly/-r or --writeonly/-w] [--vid/-v <tape_vid>] [--tag/-t <tag_name>] --from/-f <DD/MM/YYYY> --until/-u <DD/MM/YYYY> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <drive_name> [--readonly/-r or --writeonly/-w] [--vid/-v <tape_vid>] [--tag/-t <tag_name>] [--from/-f <DD/MM/YYYY>] [--until/-u <DD/MM/YYYY>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <drive_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> drive = getOptionStringValue("-n", "--name", false);
    if(!drive) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    } 
    if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2)) {
      bool readonly = hasOption("-r", "--readonly");
      bool writeonly = hasOption("-w", "--writeonly");
      optional<std::string> vid = getOptionStringValue("-v", "--vid", false);
      optional<std::string> tag = getOptionStringValue("-t", "--tag", false);
      optional<time_t> from = getOptionTimeValue("-f", "--from", false);
      optional<time_t> until = getOptionTimeValue("-u", "--until", false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false);
      if("add" == m_requestTokens.at(2)) { //add
        if(!comment||!from||!until||(!vid&&!tag&&!readonly&&!writeonly)||(readonly&&writeonly)) {
          return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
        }
        cta::common::dataStructures::DedicationType type=cta::common::dataStructures::DedicationType::readwrite;
        if(readonly) {
          type=cta::common::dataStructures::DedicationType::readonly;
        }
        else if(writeonly) {
          type=cta::common::dataStructures::DedicationType::writeonly;
        }
        m_catalogue->createDedication(m_cliIdentity, drive.value(), type, tag.value(), vid.value(), from.value(), until.value(), comment.value());
      }
      else if("ch" == m_requestTokens.at(2)) { //ch
        if((!comment&&!from&&!until&&!vid&&!tag&&!readonly&&!writeonly)||(readonly&&writeonly)) {
          return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
        }
        if(comment) {
          m_catalogue->modifyDedicationComment(m_cliIdentity, drive.value(), comment.value());
        }
        if(from) {
          m_catalogue->modifyDedicationFrom(m_cliIdentity, drive.value(), from.value());
        }
        if(until) {
          m_catalogue->modifyDedicationUntil(m_cliIdentity, drive.value(), until.value());
        }
        if(vid) {
          m_catalogue->modifyDedicationVid(m_cliIdentity, drive.value(), vid.value());
        }
        if(tag) {
          m_catalogue->modifyDedicationTag(m_cliIdentity, drive.value(), tag.value());
        }
        if(readonly) {
          m_catalogue->modifyDedicationType(m_cliIdentity, drive.value(), cta::common::dataStructures::DedicationType::readonly);          
        }
        if(writeonly) {
          m_catalogue->modifyDedicationType(m_cliIdentity, drive.value(), cta::common::dataStructures::DedicationType::writeonly);
        }
      }
    }
    else { //rm
      m_catalogue->deleteDedication(drive.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::Dedication> list= m_catalogue->getDedications();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"drive","type","vid","tag","from","until","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
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
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_repack
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_repack() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " re/repack add/rm/ls/err:" << std::endl
       << "\tadd --vid/-v <vid> [--justexpand/-e or --justrepack/-r] [--tag/-t <tag_name>]" << std::endl
       << "\trm  --vid/-v <vid>" << std::endl
       << "\tls  [--header/-h] [--vid/-v <vid>]" << std::endl
       << "\terr --vid/-v <vid>" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("add" == m_requestTokens.at(2) || "err" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> vid = getOptionStringValue("-v", "--vid", false);
    if(!vid) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }  
    if("add" == m_requestTokens.at(2)) { //add
      optional<std::string> tag = getOptionStringValue("-t", "--tag", false);
      bool justexpand = hasOption("-e", "--justexpand");
      bool justrepack = hasOption("-r", "--justrepack");
      if(justexpand&&justrepack) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      cta::common::dataStructures::RepackType type=cta::common::dataStructures::RepackType::expandandrepack;
      if(justexpand) {
        type=cta::common::dataStructures::RepackType::justexpand;
      }
      if(justrepack) {
        type=cta::common::dataStructures::RepackType::justrepack;
      }
      m_scheduler->repack(m_cliIdentity, vid.value(), tag.value(), type);
    }
    else if("err" == m_requestTokens.at(2)) { //err
      cta::common::dataStructures::RepackInfo info = m_scheduler->getRepack(m_cliIdentity, vid.value());
      if(info.errors.size()>0) {
        std::vector<std::vector<std::string>> responseTable;
        std::vector<std::string> header = {"fseq","error message"};
        if(hasOption("-h", "--header")) responseTable.push_back(header);    
        for(auto it = info.errors.cbegin(); it != info.errors.cend(); it++) {
          std::vector<std::string> currentRow;
          currentRow.push_back(std::to_string((unsigned long long)it->first));
          currentRow.push_back(it->second);
          responseTable.push_back(currentRow);
        }
        cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
      }
    }
    else { //rm
      m_scheduler->cancelRepack(m_cliIdentity, vid.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::RepackInfo> list;
    optional<std::string> vid = getOptionStringValue("-v", "--vid", false);
    if(!vid) {      
      list = m_scheduler->getRepacks(m_cliIdentity);
    }
    else {
      list.push_back(m_scheduler->getRepack(m_cliIdentity, vid.value()));
    }
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"vid","files","size","type","tag","to retrieve","to archive","failed","archived","status","name","host","time"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
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
        currentRow.push_back(it->creationLog.username);
        currentRow.push_back(it->creationLog.host);        
        currentRow.push_back(timeToString(it->creationLog.time));
        responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_shrink
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_shrink() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " sh/shrink --tapepool/-t <tapepool_name>" << std::endl;
  optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", false);
  if(!tapepool) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  m_scheduler->shrink(m_cliIdentity, tapepool.value());
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_verify
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_verify() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ve/verify add/rm/ls/err:" << std::endl
       << "\tadd [--vid/-v <vid>] [--complete/-c or --partial/-p <number_of_files_per_tape>] [--tag/-t <tag_name>]" << std::endl
       << "\trm  [--vid/-v <vid>]" << std::endl
       << "\tls  [--header/-h] [--vid/-v <vid>]" << std::endl
       << "\terr --vid/-v <vid>" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("add" == m_requestTokens.at(2) || "err" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> vid = getOptionStringValue("-v", "--vid", false);
    if(!vid) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }  
    if("add" == m_requestTokens.at(2)) { //add
      optional<std::string> tag = getOptionStringValue("-t", "--tag", false);
      optional<uint64_t> numberOfFiles = getOptionUint64Value("-p", "--partial", false); //0 means do a complete verification
      bool complete = hasOption("-c", "--complete");
      if(complete&&numberOfFiles) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      m_scheduler->verify(m_cliIdentity, vid.value(), tag.value(), numberOfFiles.value());
    }
    else if("err" == m_requestTokens.at(2)) { //err
      cta::common::dataStructures::VerifyInfo info = m_scheduler->getVerify(m_cliIdentity, vid.value());
      if(info.errors.size()>0) {
        std::vector<std::vector<std::string>> responseTable;
        std::vector<std::string> header = {"fseq","error message"};
        if(hasOption("-h", "--header")) responseTable.push_back(header);    
        for(auto it = info.errors.cbegin(); it != info.errors.cend(); it++) {
          std::vector<std::string> currentRow;
          currentRow.push_back(std::to_string((unsigned long long)it->first));
          currentRow.push_back(it->second);
          responseTable.push_back(currentRow);
        }
        cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
      }
    }
    else { //rm
      m_scheduler->cancelVerify(m_cliIdentity, vid.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::VerifyInfo> list;
    optional<std::string> vid = getOptionStringValue("-v", "--vid", false);
    if(!vid) {      
      list = m_scheduler->getVerifys(m_cliIdentity);
    }
    else {
      list.push_back(m_scheduler->getVerify(m_cliIdentity, vid.value()));
    }
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"vid","files","size","tag","to verify","failed","verified","status","name","host","time"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
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
        currentRow.push_back(it->creationLog.username);
        currentRow.push_back(it->creationLog.host);       
        currentRow.push_back(timeToString(it->creationLog.time));
        responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_archivefile
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_archivefile() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " af/archivefile ls [--header/-h] [--id/-I <archive_file_id>] [--diskid/-d <disk_id>] [--copynb/-c <copy_no>] [--vid/-v <vid>] [--tapepool/-t <tapepool>] "
          "[--owner/-o <owner>] [--group/-g <group>] [--storageclass/-s <class>] [--path/-p <fullpath>] [--instance/-i <instance>] [--summary/-S] [--all/-a] (default gives error)" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("ls" == m_requestTokens.at(2)) { //ls
    optional<uint64_t> id = getOptionUint64Value("-I", "--id", false);
    optional<std::string> diskid = getOptionStringValue("-d", "--diskid", false);
    optional<uint64_t> copynb = getOptionUint64Value("-c", "--copynb", false);
    optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", false);
    optional<std::string> vid = getOptionStringValue("-v", "--vid", false);
    optional<std::string> owner = getOptionStringValue("-o", "--owner", false);
    optional<std::string> group = getOptionStringValue("-g", "--group", false);
    optional<std::string> storageclass = getOptionStringValue("-s", "--storageclass", false);
    optional<std::string> path = getOptionStringValue("-p", "--path", false);
    optional<std::string> instance = getOptionStringValue("-i", "--instance", false);
    bool summary = hasOption("-S", "--summary");
    bool all = hasOption("-a", "--all");
    if(!all && (!instance && !id && !diskid && !copynb && !tapepool && !vid && !owner && !group && !storageclass && !path)) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }
    cta::catalogue::ArchiveFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = id;
    searchCriteria.diskFileGroup = group;
    searchCriteria.diskFileId = diskid;
    searchCriteria.diskFilePath = path;
    searchCriteria.diskFileUser = owner;
    searchCriteria.diskInstance = instance;
    searchCriteria.storageClass = storageclass;
    searchCriteria.tapeFileCopyNb = copynb;
    searchCriteria.tapePool = tapepool;
    searchCriteria.vid = vid;
    if(!summary) {
      std::unique_ptr<cta::catalogue::ArchiveFileItor> itor = m_catalogue->getArchiveFileItor(searchCriteria);
      if(itor->hasMore()) {
        std::vector<std::vector<std::string>> responseTable;
        std::vector<std::string> header = {"id","copy no","vid","fseq","block id","disk id","size","checksum type","checksum value","storage class","owner","group","instance","path","creation time"};
        if(hasOption("-h", "--header")) responseTable.push_back(header);    
        while(itor->hasMore()) {
          const cta::common::dataStructures::ArchiveFile archiveFile = itor->next();
          for(auto jt = archiveFile.tapeFiles.cbegin(); jt != archiveFile.tapeFiles.cend(); jt++) {
            std::vector<std::string> currentRow;
            currentRow.push_back(std::to_string((unsigned long long)archiveFile.archiveFileID));
            currentRow.push_back(std::to_string((unsigned long long)jt->first));
            currentRow.push_back(jt->second.vid);
            currentRow.push_back(std::to_string((unsigned long long)jt->second.fSeq));
            currentRow.push_back(std::to_string((unsigned long long)jt->second.blockId));
            currentRow.push_back(archiveFile.diskFileId);
            currentRow.push_back(archiveFile.diskInstance);
            currentRow.push_back(std::to_string((unsigned long long)archiveFile.fileSize));
            currentRow.push_back(archiveFile.checksumType);
            currentRow.push_back(archiveFile.checksumValue);
            currentRow.push_back(archiveFile.storageClass);
            currentRow.push_back(archiveFile.diskFileInfo.owner);
            currentRow.push_back(archiveFile.diskFileInfo.group);
            currentRow.push_back(archiveFile.diskFileInfo.path);    
            currentRow.push_back(std::to_string((unsigned long long)archiveFile.creationTime));          
            responseTable.push_back(currentRow);
          }
        }
        cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
      }
    }
    else { //summary
      cta::common::dataStructures::ArchiveFileSummary summary=m_catalogue->getArchiveFileSummary(searchCriteria);
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"total number of files","total size"};
      std::vector<std::string> row = {std::to_string((unsigned long long)summary.totalFiles),std::to_string((unsigned long long)summary.totalBytes)};
      if(hasOption("-h", "--header")) responseTable.push_back(header);
      responseTable.push_back(row);
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_test
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_test() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " te/test read/write/write_auto (to be run on an empty self-dedicated drive; it is a synchronous command that returns performance stats and errors; all locations are local to the tapeserver):" << std::endl
       << "\tread  --drive/-d <drive_name> --vid/-v <vid> --firstfseq/-f <first_fseq> --lastfseq/-l <last_fseq> --checkchecksum/-c --output/-o <\"null\" or output_dir> [--tag/-t <tag_name>]" << std::endl
       << "\twrite --drive/-d <drive_name> --vid/-v <vid> --file/-f <filename> [--tag/-t <tag_name>]" << std::endl
       << "\twrite_auto --drive/-d <drive_name> --vid/-v <vid> --number/-n <number_of_files> --size/-s <file_size> --input/-i <\"zero\" or \"urandom\"> [--tag/-t <tag_name>]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  optional<std::string> drive = getOptionStringValue("-d", "--drive", false);
  optional<std::string> vid = getOptionStringValue("-v", "--vid", false);
  if(!vid || !drive) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  optional<std::string> tag = getOptionStringValue("-t", "--tag", false);
  if("read" == m_requestTokens.at(2)) {
    optional<uint64_t> firstfseq = getOptionUint64Value("-f", "--firstfseq", false);
    optional<uint64_t> lastfseq = getOptionUint64Value("-l", "--lastfseq", false);
    optional<std::string> output = getOptionStringValue("-o", "--output", false);
    if(!firstfseq || !lastfseq || !output) {
      return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
    }    
    bool checkchecksum = hasOption("-c", "--checkchecksum");
    cta::common::dataStructures::ReadTestResult res = m_scheduler->readTest(m_cliIdentity, drive.value(), vid.value(), firstfseq.value(), lastfseq.value(), checkchecksum, output.value(), tag.value());   
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
    cmdlineOutput << formatResponse(responseTable, true)
           << std::endl << "Drive: " << res.driveName << " Vid: " << res.vid << " #Files: " << res.totalFilesRead << " #Bytes: " << res.totalBytesRead 
           << " Time: " << res.totalTimeInSeconds << " s Speed(avg): " << (long double)res.totalBytesRead/(long double)res.totalTimeInSeconds << " B/s" <<std::endl;
  }
  else if("write" == m_requestTokens.at(2) || "write_auto" == m_requestTokens.at(2)) {
    cta::common::dataStructures::WriteTestResult res;
    if("write" == m_requestTokens.at(2)) { //write
      optional<std::string> file = getOptionStringValue("-f", "--file", false);
      if(!file) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }  
      res = m_scheduler->writeTest(m_cliIdentity, drive.value(), vid.value(), file.value(), tag.value());
    }
    else { //write_auto
      optional<uint64_t> number = getOptionUint64Value("-n", "--number", false);
      optional<uint64_t> size = getOptionUint64Value("-s", "--size", false);
      optional<std::string> input = getOptionStringValue("-i", "--input", false);
      if(!number||!size||(input.value()!="zero"&&input.value()!="urandom")) {
        return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
      }
      cta::common::dataStructures::TestSourceType type;
      if(input.value()=="zero") { //zero
        type = cta::common::dataStructures::TestSourceType::devzero;
      }
      else { //urandom
        type = cta::common::dataStructures::TestSourceType::devurandom;
      }
      res = m_scheduler->write_autoTest(m_cliIdentity, drive.value(), vid.value(), number.value(), size.value(), type, tag.value());
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
    cmdlineOutput << formatResponse(responseTable, true)
           << std::endl << "Drive: " << res.driveName << " Vid: " << res.vid << " #Files: " << res.totalFilesWritten << " #Bytes: " << res.totalBytesWritten 
           << " Time: " << res.totalTimeInSeconds << " s Speed(avg): " << (long double)res.totalBytesWritten/(long double)res.totalTimeInSeconds << " B/s" <<std::endl;
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_drive
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_drive() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " dr/drive up/down (it is a synchronous command):" << std::endl
       << "\tup   --drive/-d <drive_name>" << std::endl
       << "\tdown --drive/-d <drive_name> [--force/-f]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  optional<std::string> drive = getOptionStringValue("-d", "--drive", false);
  if(!drive) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  if("up" == m_requestTokens.at(2)) {
    m_scheduler->setDriveStatus(m_cliIdentity, drive.value(), true, false);
  }
  else if("down" == m_requestTokens.at(2)) {
    m_scheduler->setDriveStatus(m_cliIdentity, drive.value(), false, hasOption("-f", "--force"));
  }
  else {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_reconcile
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_reconcile() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " rc/reconcile (it is a synchronous command, with a possibly long execution time, returns the list of files unknown to EOS, to be deleted manually by the admin after proper checks)" << std::endl;
  std::list<cta::common::dataStructures::ArchiveFile> list = m_scheduler->reconcile(m_cliIdentity);  
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
        currentRow.push_back(it->diskFileId);
        currentRow.push_back(it->diskInstance);
        currentRow.push_back(std::to_string((unsigned long long)it->fileSize));
        currentRow.push_back(it->checksumType);
        currentRow.push_back(it->checksumValue);
        currentRow.push_back(it->storageClass);
        currentRow.push_back(it->diskFileInfo.owner);
        currentRow.push_back(it->diskFileInfo.group);
        currentRow.push_back(it->diskFileInfo.path);          
        responseTable.push_back(currentRow);
      }
    }
    cmdlineOutput << formatResponse(responseTable, true);
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_listpendingarchives
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_listpendingarchives() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " lpa/listpendingarchives [--header/-h] [--tapepool/-t <tapepool_name>] [--extended/-x]" << std::endl;
  optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", false);
  bool extended = hasOption("-x", "--extended");
  std::map<std::string, std::list<cta::common::dataStructures::ArchiveJob> > result;
  if(!tapepool) {
    result = m_scheduler->getPendingArchiveJobs();
  }
  else {
    std::list<cta::common::dataStructures::ArchiveJob> list = m_scheduler->getPendingArchiveJobs(tapepool.value());
    if(list.size()>0) {
      result[tapepool.value()] = list;
    }
  }
  if(result.size()>0) {
    if(extended) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"tapepool","id","storage class","copy no.","disk id","instance","checksum type","checksum value","size","user","group","path","diskpool","diskpool throughput"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
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
          currentRow.push_back(jt->request.diskFileInfo.path);
          currentRow.push_back(jt->request.diskpoolName);
          currentRow.push_back(std::to_string((unsigned long long)jt->request.diskpoolThroughput));
          responseTable.push_back(currentRow);
        }
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
    else {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"tapepool","total files","total size"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
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
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_listpendingretrieves
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_listpendingretrieves() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " lpr/listpendingretrieves [--header/-h] [--vid/-v <vid>] [--extended/-x]" << std::endl;
  optional<std::string> vid = getOptionStringValue("-v", "--vid", false);
  bool extended = hasOption("-x", "--extended");
  std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob> > result;
  if(!vid) {
    result = m_scheduler->getPendingRetrieveJobs();
  }
  else {
    std::list<cta::common::dataStructures::RetrieveJob> list = m_scheduler->getPendingRetrieveJobs(vid.value());
    if(list.size()>0) {
      result[vid.value()] = list;
    }
  }
  if(result.size()>0) {
    if(extended) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"vid","id","copy no.","fseq","block id","size","user","group","path","diskpool","diskpool throughput"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
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
          currentRow.push_back(jt->request.diskFileInfo.path);
          currentRow.push_back(jt->request.diskpoolName);
          currentRow.push_back(std::to_string((unsigned long long)jt->request.diskpoolThroughput));
          responseTable.push_back(currentRow);
        }
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
    else {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"vid","total files","total size"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
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
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_listdrivestates
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_listdrivestates() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " lds/listdrivestates [--header/-h]" << std::endl;
  std::list<cta::common::dataStructures::DriveState> result = m_scheduler->getDriveStates(m_cliIdentity);  
  if(result.size()>0) {
    std::vector<std::vector<std::string>> responseTable;
    std::vector<std::string> header = {"logical library","host","drive","status","since","mount","vid","tapepool","session id","since","files","bytes","latest speed"};
    if(hasOption("-h", "--header")) responseTable.push_back(header);    
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
    cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
  }
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_archive
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_archive() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " a/archive --encoded <\"true\" or \"false\"> --user <user> --group <group> --diskid <disk_id> --instance <instance> --srcurl <src_URL> --size <size> --checksumtype <checksum_type>" << std::endl
                    << "\t--checksumvalue <checksum_value> --storageclass <storage_class> --diskfilepath <disk_filepath> --diskfileowner <disk_fileowner>" << std::endl
                    << "\t--diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob> --diskpool <diskpool_name> --throughput <diskpool_throughput>" << std::endl;
  optional<bool> encoded_s = getOptionBoolValue("", "--encoded", false);
  if(!encoded_s) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  bool encoded = encoded_s.value();
  optional<std::string> user = getOptionStringValue("", "--user", encoded);
  optional<std::string> group = getOptionStringValue("", "--group", encoded);
  optional<std::string> diskid = getOptionStringValue("", "--diskid", encoded);
  optional<std::string> instance = getOptionStringValue("", "--instance", encoded);
  optional<std::string> srcurl = getOptionStringValue("", "--srcurl", encoded);
  optional<uint64_t> size = getOptionUint64Value("", "--size", encoded);
  optional<std::string> checksumtype = getOptionStringValue("", "--checksumtype", encoded);
  optional<std::string> checksumvalue = getOptionStringValue("", "--checksumvalue", encoded);
  optional<std::string> storageclass = getOptionStringValue("", "--storageclass", encoded);
  optional<std::string> diskfilepath = getOptionStringValue("", "--diskfilepath", encoded);
  optional<std::string> diskfileowner = getOptionStringValue("", "--diskfileowner", encoded);
  optional<std::string> diskfilegroup = getOptionStringValue("", "--diskfilegroup", encoded);
  optional<std::string> recoveryblob = getOptionStringValue("", "--recoveryblob", encoded);
  optional<std::string> diskpool = getOptionStringValue("", "--diskpool", encoded);
  optional<uint64_t> throughput = getOptionUint64Value("", "--throughput", encoded);
  if(!user || !group || !diskid || !srcurl || !size || !checksumtype || !checksumvalue
          || !storageclass || !instance || !diskfilepath || !diskfileowner || !diskfilegroup || !recoveryblob || !diskpool || !throughput) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user.value();
  originator.group=group.value();
  cta::common::dataStructures::DiskFileInfo diskFileInfo;
  diskFileInfo.recoveryBlob=recoveryblob.value();
  diskFileInfo.group=diskfilegroup.value();
  diskFileInfo.owner=diskfileowner.value();
  diskFileInfo.path=diskfilepath.value();
  cta::common::dataStructures::ArchiveRequest request;
  request.checksumType=checksumtype.value();
  request.checksumValue=checksumvalue.value();
  request.diskpoolName=diskpool.value();
  request.diskpoolThroughput=throughput.value();
  request.diskFileInfo=diskFileInfo;
  request.diskFileID=diskid.value();
  request.instance=instance.value();
  request.fileSize=size.value();
  request.requester=originator;
  request.srcURL=srcurl.value();
  request.storageClass=storageclass.value();  
  uint64_t archiveFileId = m_scheduler->queueArchive(m_cliIdentity, request);
  cmdlineOutput << archiveFileId << std::endl;
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_retrieve
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_retrieve() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " r/retrieve --encoded <\"true\" or \"false\"> --user <user> --group <group> --id <CTA_ArchiveFileID> --dsturl <dst_URL> --diskfilepath <disk_filepath>" << std::endl
                    << "\t--diskfileowner <disk_fileowner> --diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob> --diskpool <diskpool_name> --throughput <diskpool_throughput>" << std::endl;
  optional<bool> encoded_s = getOptionBoolValue("", "--encoded", false);
  if(!encoded_s) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  bool encoded = encoded_s.value();
  optional<std::string> user = getOptionStringValue("", "--user", encoded);
  optional<std::string> group = getOptionStringValue("", "--group", encoded);
  optional<uint64_t> id = getOptionUint64Value("", "--id", encoded);
  optional<std::string> dsturl = getOptionStringValue("", "--dsturl", encoded);
  optional<std::string> diskfilepath = getOptionStringValue("", "--diskfilepath", encoded);
  optional<std::string> diskfileowner = getOptionStringValue("", "--diskfileowner", encoded);
  optional<std::string> diskfilegroup = getOptionStringValue("", "--diskfilegroup", encoded);
  optional<std::string> recoveryblob = getOptionStringValue("", "--recoveryblob", encoded);
  optional<std::string> diskpool = getOptionStringValue("", "--diskpool", encoded);
  optional<uint64_t> throughput = getOptionUint64Value("", "--throughput", encoded);
  if(!user || !group || !id || !dsturl || !diskfilepath || !diskfileowner || !diskfilegroup || !recoveryblob || !diskpool || !throughput) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user.value();
  originator.group=group.value();
  cta::common::dataStructures::DiskFileInfo diskFileInfo;
  diskFileInfo.recoveryBlob=recoveryblob.value();
  diskFileInfo.group=diskfilegroup.value();
  diskFileInfo.owner=diskfileowner.value();
  diskFileInfo.path=diskfilepath.value();
  cta::common::dataStructures::RetrieveRequest request;
  request.diskpoolName=diskpool.value();
  request.diskpoolThroughput=throughput.value();
  request.diskFileInfo=diskFileInfo;
  request.archiveFileID=id.value();
  request.requester=originator;
  request.dstURL=dsturl.value();
  m_scheduler->queueRetrieve(m_cliIdentity, request);
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_deletearchive
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_deletearchive() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " da/deletearchive --encoded <\"true\" or \"false\"> --user <user> --group <group> --id <CTA_ArchiveFileID>" << std::endl;
  optional<bool> encoded_s = getOptionBoolValue("", "--encoded", false);
  if(!encoded_s) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  bool encoded = encoded_s.value();
  optional<std::string> user = getOptionStringValue("", "--user", encoded);
  optional<std::string> group = getOptionStringValue("", "--group", encoded);
  optional<uint64_t> id = getOptionUint64Value("", "--id", encoded);
  if(!user || !group || !id) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user.value();
  originator.group=group.value();
  cta::common::dataStructures::DeleteArchiveRequest request;
  request.archiveFileID=id.value();
  request.requester=originator;
  m_scheduler->deleteArchive(m_cliIdentity, request);
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_cancelretrieve
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_cancelretrieve() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " cr/cancelretrieve --encoded <\"true\" or \"false\"> --user <user> --group <group> --id <CTA_ArchiveFileID> --dsturl <dst_URL> --diskfilepath <disk_filepath>" << std::endl
                    << "\t--diskfileowner <disk_fileowner> --diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob>" << std::endl;
  optional<bool> encoded_s = getOptionBoolValue("", "--encoded", false);
  if(!encoded_s) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  bool encoded = encoded_s.value();
  optional<std::string> user = getOptionStringValue("", "--user", encoded);
  optional<std::string> group = getOptionStringValue("", "--group", encoded);
  optional<uint64_t> id = getOptionUint64Value("", "--id", encoded);
  optional<std::string> dsturl = getOptionStringValue("", "--dsturl", encoded);
  optional<std::string> diskfilepath = getOptionStringValue("", "--diskfilepath", encoded);
  optional<std::string> diskfileowner = getOptionStringValue("", "--diskfileowner", encoded);
  optional<std::string> diskfilegroup = getOptionStringValue("", "--diskfilegroup", encoded);
  optional<std::string> recoveryblob = getOptionStringValue("", "--recoveryblob", encoded);
  if(!user || !group || !id || !dsturl || !diskfilepath || !diskfileowner || !diskfilegroup || !recoveryblob) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user.value();
  originator.group=group.value();
  cta::common::dataStructures::DiskFileInfo diskFileInfo;
  diskFileInfo.recoveryBlob=recoveryblob.value();
  diskFileInfo.group=diskfilegroup.value();
  diskFileInfo.owner=diskfileowner.value();
  diskFileInfo.path=diskfilepath.value();
  cta::common::dataStructures::CancelRetrieveRequest request;
  request.diskFileInfo=diskFileInfo;
  request.archiveFileID=id.value();
  request.requester=originator;
  request.dstURL=dsturl.value();
  m_scheduler->cancelRetrieve(m_cliIdentity, request);
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_updatefilestorageclass
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_updatefilestorageclass() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ufsc/updatefilestorageclass --encoded <\"true\" or \"false\"> --user <user> --group <group> --id <CTA_ArchiveFileID> --storageclass <storage_class> --diskfilepath <disk_filepath>" << std::endl
                    << "\t--diskfileowner <disk_fileowner> --diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob>" << std::endl;
  optional<bool> encoded_s = getOptionBoolValue("", "--encoded", false);
  if(!encoded_s) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  bool encoded = encoded_s.value();
  optional<std::string> user = getOptionStringValue("", "--user", encoded);
  optional<std::string> group = getOptionStringValue("", "--group", encoded);
  optional<uint64_t> id = getOptionUint64Value("", "--id", encoded);
  optional<std::string> storageclass = getOptionStringValue("", "--storageclass", encoded);
  optional<std::string> diskfilepath = getOptionStringValue("", "--diskfilepath", encoded);
  optional<std::string> diskfileowner = getOptionStringValue("", "--diskfileowner", encoded);
  optional<std::string> diskfilegroup = getOptionStringValue("", "--diskfilegroup", encoded);
  optional<std::string> recoveryblob = getOptionStringValue("", "--recoveryblob", encoded);
  if(!user || !group || !id || !storageclass || !diskfilepath || !diskfileowner || !diskfilegroup || !recoveryblob) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user.value();
  originator.group=group.value();
  cta::common::dataStructures::DiskFileInfo diskFileInfo;
  diskFileInfo.recoveryBlob=recoveryblob.value();
  diskFileInfo.group=diskfilegroup.value();
  diskFileInfo.owner=diskfileowner.value();
  diskFileInfo.path=diskfilepath.value();
  cta::common::dataStructures::UpdateFileStorageClassRequest request;
  request.diskFileInfo=diskFileInfo;
  request.archiveFileID=id.value();
  request.requester=originator;
  request.storageClass=storageclass.value();
  m_scheduler->updateFileStorageClass(m_cliIdentity, request);
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_updatefileinfo
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_updatefileinfo() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ufi/updatefileinfo --encoded <\"true\" or \"false\"> --id <CTA_ArchiveFileID> --diskfilepath <disk_filepath>" << std::endl
                    << "\t--diskfileowner <disk_fileowner> --diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob>" << std::endl;
  optional<bool> encoded_s = getOptionBoolValue("", "--encoded", false);
  if(!encoded_s) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  bool encoded = encoded_s.value();
  optional<uint64_t> id = getOptionUint64Value("", "--id", encoded);
  optional<std::string> diskfilepath = getOptionStringValue("", "--diskfilepath", encoded);
  optional<std::string> diskfileowner = getOptionStringValue("", "--diskfileowner", encoded);
  optional<std::string> diskfilegroup = getOptionStringValue("", "--diskfilegroup", encoded);
  optional<std::string> recoveryblob = getOptionStringValue("", "--recoveryblob", encoded);
  if(!id || !diskfilepath || !diskfileowner || !diskfilegroup || !recoveryblob) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  cta::common::dataStructures::DiskFileInfo diskFileInfo;
  diskFileInfo.recoveryBlob=recoveryblob.value();
  diskFileInfo.group=diskfilegroup.value();
  diskFileInfo.owner=diskfileowner.value();
  diskFileInfo.path=diskfilepath.value();
  cta::common::dataStructures::UpdateFileInfoRequest request;
  request.diskFileInfo=diskFileInfo;
  request.archiveFileID=id.value();
  m_scheduler->updateFileInfo(m_cliIdentity, request);
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
}

//------------------------------------------------------------------------------
// xCom_liststorageclass
//------------------------------------------------------------------------------
int XrdCtaFile::xCom_liststorageclass() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " lsc/liststorageclass --encoded <\"true\" or \"false\"> --user <user> --group <group>" << std::endl;
  optional<bool> encoded_s = getOptionBoolValue("", "--encoded", false);
  if(!encoded_s) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  bool encoded = encoded_s.value();
  optional<std::string> user = getOptionStringValue("", "--user", encoded);
  optional<std::string> group = getOptionStringValue("", "--group", encoded);
  if(!user || !group) {
    return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, help.str());
  }
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user.value();
  originator.group=group.value();
  cta::common::dataStructures::ListStorageClassRequest request;
  request.requester=originator;
  m_scheduler->listStorageClass(m_cliIdentity, request);
  return logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput.str());
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
  help << programName << " admin/ad                 add/ch/rm/ls"               << std::endl;
  help << programName << " adminhost/ah             add/ch/rm/ls"               << std::endl;
  help << programName << " archivefile/af           ls"                         << std::endl;
  help << programName << " archiveroute/ar          add/ch/rm/ls"               << std::endl;
  help << programName << " bootstrap/bs"                                        << std::endl;
  help << programName << " dedication/de            add/ch/rm/ls"               << std::endl;
  help << programName << " drive/dr                 up/down"                    << std::endl;
  help << programName << " groupmountrule/gmr       add/rm/ls/err"              << std::endl;
  help << programName << " listdrivestates/lds"                                 << std::endl;
  help << programName << " listpendingarchives/lpa"                             << std::endl;
  help << programName << " listpendingretrieves/lpr"                            << std::endl;
  help << programName << " logicallibrary/ll        add/ch/rm/ls"               << std::endl;
  help << programName << " reconcile/rc"                                        << std::endl;
  help << programName << " repack/re                add/rm/ls/err"              << std::endl;
  help << programName << " requestermountrule/rmr   add/rm/ls/err"              << std::endl;
  help << programName << " shrink/sh"                                           << std::endl;
  help << programName << " storageclass/sc          add/ch/rm/ls"               << std::endl;
  help << programName << " tape/ta                  add/ch/rm/reclaim/ls/label" << std::endl;
  help << programName << " tapepool/tp              add/ch/rm/ls"               << std::endl;
  help << programName << " test/te                  read/write"                 << std::endl;
  help << programName << " user/us                  add/ch/rm/ls"               << std::endl;
  help << programName << " mountpolicy/mp           add/ch/rm/ls"               << std::endl;
  help << programName << " verify/ve                add/rm/ls/err"              << std::endl;
  help << "" << std::endl;
  help << "CTA EOS commands:" << std::endl;
  help << "" << std::endl;
  help << "For each command there is a short version and a long one." << std::endl;
  help << "" << std::endl;
  help << programName << " archive/a"                                           << std::endl;
  help << programName << " cancelretrieve/cr"                                   << std::endl;
  help << programName << " deletearchive/da"                                    << std::endl;
  help << programName << " liststorageclass/lsc"                                << std::endl;
  help << programName << " retrieve/r"                                          << std::endl;
  help << programName << " updatefileinfo/ufi"                                  << std::endl;
  help << programName << " updatefilestorageclass/ufsc"                         << std::endl;
  return help.str();
}

}}
