/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Command-line tool for CTA Admin commands
 * @description    CTA Admin command using Google Protocol Buffers and XRootD SSI transport
 * @copyright      Copyright 2017 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#if 0
#include "scheduler/SchedulerDatabase.hpp"
#include "xroot_plugins/XrdCtaFile.hpp"

#include "XrdSec/XrdSecEntity.hh"

#include "catalogue/TapeFileSearchCriteria.hpp"
#include "common/Configuration.hpp"
#include "common/utils/utils.hpp"
#include "common/Timer.hpp"
#include "common/utils/GetOptThreadSafe.hpp"
#include "common/exception/UserError.hpp"
#include "common/exception/NonRetryableError.hpp"
#include "common/exception/RetryableError.hpp"

#include <cryptopp/base64.h>
#include <cryptopp/osrng.h>
#include <iomanip>
#include <memory>
#include <pwd.h>
#include <string>
#include <time.h>
#endif

#include <iostream> // for debug output
#include <sstream>

#include "CtaAdminCmd.hpp"



CtaAdminCmd::CtaAdminCmd(int argc, const char *const *const argv)
{
   // Check we have at least one parameter

   if(argc < 2) throw std::runtime_error(getGenericHelp(argv[0]));

   // Tokenize the command

   for(int i = 0; i < argc; ++i)
   {
      m_requestTokens.push_back(argv[i]);
   }

   // Parse the command

   std::string &command = m_requestTokens.at(1);
  
   if     ("ad"   == command || "admin"                  == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_admin();}
   else if("ah"   == command || "adminhost"              == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_adminhost();}
   else if("tp"   == command || "tapepool"               == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_tapepool();}
   else if("ar"   == command || "archiveroute"           == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_archiveroute();}
   else if("ll"   == command || "logicallibrary"         == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_logicallibrary();}
   else if("ta"   == command || "tape"                   == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_tape();}
   else if("sc"   == command || "storageclass"           == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_storageclass();}
   else if("rmr"  == command || "requestermountrule"     == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_requestermountrule();}
   else if("gmr"  == command || "groupmountrule"         == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_groupmountrule();}
   else if("mp"   == command || "mountpolicy"            == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_mountpolicy();}
   else if("re"   == command || "repack"                 == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_repack();}
   else if("sh"   == command || "shrink"                 == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_shrink();}
   else if("ve"   == command || "verify"                 == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_verify();}
   else if("af"   == command || "archivefile"            == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_archivefile();}
   else if("te"   == command || "test"                   == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_test();}
   else if("dr"   == command || "drive"                  == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_drive();}
   else if("lpa"  == command || "listpendingarchives"    == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_listpendingarchives();}
   else if("lpr"  == command || "listpendingretrieves"   == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_listpendingretrieves();}
   else if("sq"   == command || "showqueues"             == command) xCom_notimpl(); //{authorizeAdmin(); return xCom_showqueues();}

   else if("a"    == command || "archive"                == command) xCom_notimpl(); //{return xCom_archive();}
   else if("r"    == command || "retrieve"               == command) xCom_notimpl(); //{return xCom_retrieve();}
   else if("da"   == command || "deletearchive"          == command) xCom_notimpl(); //{return xCom_deletearchive();}
   else if("cr"   == command || "cancelretrieve"         == command) xCom_notimpl(); //{return xCom_cancelretrieve();}
   else if("ufi"  == command || "updatefileinfo"         == command) xCom_notimpl(); //{return xCom_updatefileinfo();}
   else if("ufsc" == command || "updatefilestorageclass" == command) xCom_notimpl(); //{return xCom_updatefilestorageclass();}
   else if("lsc"  == command || "liststorageclass"       == command) xCom_notimpl(); //{return xCom_liststorageclass();}

   else {
     throw std::runtime_error(getGenericHelp(m_requestTokens.at(0)));
   }
}



std::string CtaAdminCmd::getGenericHelp(const std::string &programName) const
{
   std::stringstream help;

   help << "CTA ADMIN commands:"                                                 << std::endl
                                                                                 << std::endl
        << "For each command there is a short version and a long one. "
        << "Subcommands (add/rm/ls/ch/reclaim) do not have short versions."      << std::endl
                                                                                 << std::endl;

   help << programName << " admin/ad                 add/ch/rm/ls"               << std::endl
        << programName << " adminhost/ah             add/ch/rm/ls"               << std::endl
        << programName << " archivefile/af           ls"                         << std::endl
        << programName << " archiveroute/ar          add/ch/rm/ls"               << std::endl
        << programName << " drive/dr                 up/down/ls"                 << std::endl
        << programName << " groupmountrule/gmr       add/rm/ls/err"              << std::endl
        << programName << " listpendingarchives/lpa"                             << std::endl
        << programName << " listpendingretrieves/lpr"                            << std::endl
        << programName << " logicallibrary/ll        add/ch/rm/ls"               << std::endl
        << programName << " mountpolicy/mp           add/ch/rm/ls"               << std::endl
        << programName << " repack/re                add/rm/ls/err"              << std::endl
        << programName << " requestermountrule/rmr   add/rm/ls/err"              << std::endl
        << programName << " shrink/sh"                                           << std::endl
        << programName << " storageclass/sc          add/ch/rm/ls"               << std::endl
        << programName << " tape/ta                  add/ch/rm/reclaim/ls/label" << std::endl
        << programName << " tapepool/tp              add/ch/rm/ls"               << std::endl
        << programName << " test/te                  read/write"                 << std::endl
        << programName << " verify/ve                add/rm/ls/err"              << std::endl
                                                                                 << std::endl;

   help << "CTA EOS commands: [NOT IMPLEMENTED]"                                 << std::endl
                                                                                 << std::endl
        << "For each command there is a short version and a long one."           << std::endl
                                                                                 << std::endl
        << programName << " archive/a"                                           << std::endl
        << programName << " cancelretrieve/cr"                                   << std::endl
        << programName << " deletearchive/da"                                    << std::endl
        << programName << " liststorageclass/lsc"                                << std::endl
        << programName << " retrieve/r"                                          << std::endl
        << programName << " updatefileinfo/ufi"                                  << std::endl
        << programName << " updatefilestorageclass/ufsc"                         << std::endl
                                                                                 << std::endl
        << "Special option for running " << programName
        << " within the EOS workflow engine:"                                    << std::endl
                                                                                 << std::endl
        << programName << " ... --stderr" << std::endl
                                                                                 << std::endl
        << "The option tells " << programName
        << " to write results to both standard out and standard error."          << std::endl
        << "The option must be specified as the very last command-line argument of "
        << programName << "."                                                    << std::endl;

   return help.str();
}



#if 0
/*!
 * checkOptions
 */
void CtaAdminCmd::checkOptions(const std::string &helpString) {
  if(!m_missingRequiredOptions.empty()||(!m_missingOptionalOptions.empty() && m_optionalOptions.empty())) {
    throw cta::exception::UserError(helpString);
  }
}



/*!
 * logRequestAndSetCmdlineResult
 */
void CtaAdminCmd::logRequestAndSetCmdlineResult(const cta::common::dataStructures::FrontendReturnCode rc, const std::string &returnString) {
  // The return code of teh executed command is the first character of the
  // result sent back to the cta command-line tool
  //
  // Please note return codes can only be between "0" and "9".
  m_cmdlineOutput = std::to_string(rc);

  if(!m_missingRequiredOptions.empty()) {
    m_cmdlineOutput += "The following required options are missing:\n";
    for(auto it=m_missingRequiredOptions.cbegin(); it!=m_missingRequiredOptions.cend(); it++) {
      m_cmdlineOutput += "Missing option: ";
      m_cmdlineOutput += *it;
      m_cmdlineOutput += "\n";
    }
  }
  if(!m_missingOptionalOptions.empty() && m_optionalOptions.empty() && !m_suppressOptionalOptionsWarning) {
    m_cmdlineOutput += "At least one of the following options is required:\n";
    for(auto it=m_missingOptionalOptions.cbegin(); it!=m_missingOptionalOptions.cend(); it++) {
      m_cmdlineOutput += "Missing option: ";
      m_cmdlineOutput += *it;
      m_cmdlineOutput += "\n";
    }
  }
  m_cmdlineOutput += returnString;

  std::list<cta::log::Param> params;
  params.push_back({"Username", m_cliIdentity.username});
  params.push_back({"Host", m_cliIdentity.host});
  params.push_back({"ReturnCode", toString(rc)});
  params.push_back({"CommandOutput", m_cmdlineOutput});
  std::stringstream originalRequest;
  for(auto it=m_requestTokens.begin(); it!=m_requestTokens.end(); it++) {
    originalRequest << *it << " ";
  }
  params.push_back(cta::log::Param("REQUEST", originalRequest.str()));
  
  switch(rc) {
    case cta::common::dataStructures::FrontendReturnCode::ok:
      m_log(log::INFO, "Successful Request", params);
      break;
    case cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry:
      m_log(log::USERERR, "Syntax error or missing argument(s) in request", params);
      break;
    default:
      params.push_back(cta::log::Param("ERROR", returnString));
      m_log(log::ERR, "Unsuccessful Request", params);
      break;
  }
}




/*!
 * open
 */
int CtaAdminCmd::open(const char *fileName, XrdSfsFileOpenMode openMode, mode_t createMode, const XrdSecEntity *client, const char *opaque) {
  try {
    checkClient(client);
    if(!strlen(fileName)) { //this should never happen
      throw cta::exception::UserError(getGenericHelp(""));
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
      throw cta::exception::UserError(getGenericHelp(""));
    }
    if(m_requestTokens.size() < 2) {
      throw cta::exception::UserError(getGenericHelp(m_requestTokens.at(0)));
    }    
    const std::string cmdlineOutput = dispatchCommand();
    logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ok, cmdlineOutput);
  } catch (cta::exception::UserError &ex) {
    logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::userErrorNoRetry, ex.getMessageValue()+"\n");
  } catch (cta::exception::NonRetryableError &ex) {
    logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ctaErrorNoRetry, ex.getMessageValue()+"\n");
  } catch (cta::exception::RetryableError &ex) {
    logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ctaErrorRetry, ex.getMessageValue()+"\n");
  } catch (cta::exception::Exception &ex) {
    logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ctaErrorNoRetry, ex.getMessageValue()+"\n");
  } catch (std::exception &ex) {
    logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ctaErrorNoRetry, std::string(ex.what())+"\n");
  } catch (...) {
    logRequestAndSetCmdlineResult(cta::common::dataStructures::FrontendReturnCode::ctaErrorNoRetry, "Unknown exception caught!\n");
  }
  return SFS_OK;
}



/*!
 * read
 */
XrdSfsXferSize CtaAdminCmd::read(XrdSfsFileOffset offset, char *buffer, XrdSfsXferSize size) {
  if (nullptr != m_listArchiveFilesCmd.get()) {
    // Temporarily treat the "cta archive ls" command the same as all the others
    return m_listArchiveFilesCmd->read(offset, buffer, size);
  } else {
    return readFromCmdlineOutput(offset, buffer, size);
  }
}



/*!
 * readFromCmdlineOutput
 */
XrdSfsXferSize CtaAdminCmd::readFromCmdlineOutput(XrdSfsFileOffset offset, char *buffer, XrdSfsXferSize size) {
  if(0 > offset) {
    error.setErrInfo(EINVAL, "The value of offset is negative");
    return SFS_ERROR;
  }

  if(offset >= (XrdSfsFileOffset)m_cmdlineOutput.length()) {
    return SFS_OK;
  }

  const XrdSfsXferSize remainingNbBytesToRead = m_cmdlineOutput.length() - offset;
  const XrdSfsXferSize actualNbBytesToRead = remainingNbBytesToRead >= size ? size : remainingNbBytesToRead;

  if(0 < actualNbBytesToRead) {
    strncpy(buffer, m_cmdlineOutput.c_str() + offset, actualNbBytesToRead);
    return actualNbBytesToRead;
  } else {
    return SFS_OK;
  }
}



/*!
 * replaceAll
 */
void CtaAdminCmd::replaceAll(std::string& str, const std::string& from, const std::string& to) const {
  if(from.empty() || str.empty())
    return;
  size_t start_pos = 0;
  while((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos += to.length();
  }
}



/*!
 * getOption
 */
std::string CtaAdminCmd::getOption(const std::string& optionShortName, const std::string& optionLongName) {
  bool encoded = false;
  for(auto it=m_requestTokens.cbegin(); it!=m_requestTokens.cend(); it++) {
    if(optionShortName == *it || optionLongName == *it || optionLongName+":base64" == *it) {
      if(optionLongName+":base64" == *it) {
        encoded = true;
      }
      auto it_next=it+1;
      if(it_next!=m_requestTokens.cend()) {
        std::string value = *it_next;
        if(!encoded) return value;
        else return decode(value.erase(0,7)); //erasing the first 7 characters "base64:" and decoding the rest
      }
      else {
        return "";
      }
    }
  }
  return "";
}



/*!
 * getOptionStringValue
 */
optional<std::string> CtaAdminCmd::getOptionStringValue(const std::string& optionShortName, 
        const std::string& optionLongName,
        const bool required, 
        const bool useDefaultIfMissing, 
        const std::string& defaultValue) {
  std::string option = getOption(optionShortName, optionLongName);
  if(option.empty()&&useDefaultIfMissing) {
    option = defaultValue;
  }
  if(option.empty()) {
    if(required) {
      m_missingRequiredOptions.push_back(optionLongName);
    }
    else {
      m_missingOptionalOptions.push_back(optionLongName);
    }
    return optional<std::string>();
  }
  if(!required) {
    m_optionalOptions.push_back(optionLongName);
  }
  return optional<std::string>(option);
}



/*!
 * getOptionUint64Value
 */
optional<uint64_t> CtaAdminCmd::getOptionUint64Value(const std::string& optionShortName, 
        const std::string& optionLongName,
        const bool required, 
        const bool useDefaultIfMissing, 
        const std::string& defaultValue) {
  std::string option = getOption(optionShortName, optionLongName);
  if(option.empty()&&useDefaultIfMissing) {
    option = defaultValue;
  }
  if(option.empty()) {
    if(required) {
      m_missingRequiredOptions.push_back(optionLongName);
    }
    else {
      m_missingOptionalOptions.push_back(optionLongName);
    }
    return optional<uint64_t>();
  }
  if(!required) {
    m_optionalOptions.push_back(optionLongName);
  }
  return optional<uint64_t>(stringParameterToUint64(optionLongName, option));
}



/*!
 * getOptionBoolValue
 */
optional<bool> CtaAdminCmd::getOptionBoolValue(const std::string& optionShortName, 
        const std::string& optionLongName,
        const bool required, 
        const bool useDefaultIfMissing, 
        const std::string& defaultValue) {
  std::string option = getOption(optionShortName, optionLongName);
  if(option.empty()&&useDefaultIfMissing) {
    option = defaultValue;
  }
  if(option.empty()) {
    if(required) {
      m_missingRequiredOptions.push_back(optionLongName);
    }
    else {
      m_missingOptionalOptions.push_back(optionLongName);
    }
    return optional<bool>();
  }
  if(!required) {
    m_optionalOptions.push_back(optionLongName);
  }
  return optional<bool>(stringParameterToBool(optionLongName, option));
}



/*!
 * getOptionTimeValue
 */
optional<time_t> CtaAdminCmd::getOptionTimeValue(const std::string& optionShortName, 
        const std::string& optionLongName,
        const bool required, 
        const bool useDefaultIfMissing, 
        const std::string& defaultValue) {
  std::string option = getOption(optionShortName, optionLongName);
  if(option.empty()&&useDefaultIfMissing) {
    option = defaultValue;
  }
  if(option.empty()) {
    if(required) {
      m_missingRequiredOptions.push_back(optionLongName);
    }
    else {
      m_missingOptionalOptions.push_back(optionLongName);
    }
    return optional<time_t>();
  }
  if(!required) {
    m_optionalOptions.push_back(optionLongName);
  }
  return optional<time_t>(stringParameterToTime(optionLongName, option));
}



/*!
 * hasOption
 */
bool CtaAdminCmd::hasOption(const std::string& optionShortName, const std::string& optionLongName) {
  for(auto it=m_requestTokens.cbegin(); it!=m_requestTokens.cend(); it++) {
    if(optionShortName == *it || optionLongName == *it) {
      return true;
    }
  }
  return false;
}



/*!
 * timeToString
 */
std::string CtaAdminCmd::timeToString(const time_t &time) {
  std::string timeString(ctime(&time));
  timeString=timeString.substr(0,timeString.size()-1); //remove newline
  return timeString;
}



/*!
 * BytesToMbString
 */
std::string CtaAdminCmd::BytesToMbString(const uint64_t bytes) {
  std::ostringstream oss;
  const long double mBytes = (long double)bytes/1000.0/1000.0;
  oss << std::setprecision(2) << std::fixed << mBytes;
  return oss.str();
}



/*!
 * formatResponse
 */
std::string CtaAdminCmd::formatResponse(const std::vector<std::vector<std::string>> &responseTable, const bool withHeader) {
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
    columnSizes.push_back(columnSize);//loops here
  }
  std::stringstream responseSS;
  for(auto row=responseTable.cbegin(); row!=responseTable.cend(); row++) {
    if(withHeader && row==responseTable.cbegin()) responseSS << "\x1b[31;1m";
    for(uint i=0; i<row->size(); i++) {
      responseSS << std::string(i?"  ":"")<< std::setw(columnSizes.at(i)) << row->at(i);
    }
    if(withHeader && row==responseTable.cbegin()) responseSS << "\x1b[0m" << std::endl;
    else responseSS << std::endl;
  }
  return responseSS.str();
}



/*!
 * addLogInfoToResponseRow
 */
void CtaAdminCmd::addLogInfoToResponseRow(std::vector<std::string> &responseRow, const cta::common::dataStructures::EntryLog &creationLog, const cta::common::dataStructures::EntryLog &lastModificationLog) {
  responseRow.push_back(creationLog.username);
  responseRow.push_back(creationLog.host);
  responseRow.push_back(timeToString(creationLog.time));
  responseRow.push_back(lastModificationLog.username);
  responseRow.push_back(lastModificationLog.host);
  responseRow.push_back(timeToString(lastModificationLog.time));
}



/*!
 * stringParameterToUint64
 */
uint64_t CtaAdminCmd::stringParameterToUint64(const std::string &parameterName, const std::string &parameterValue) const {
  try {
    return cta::utils::toUint64(parameterValue);
  } catch(cta::exception::Exception &ex) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" - Parameter: "+parameterName+" ("+parameterValue+
            ") could not be converted to uint64_t because: " + ex.getMessageValue());
  }
}



/*!
 * stringParameterToBool
 */
bool CtaAdminCmd::stringParameterToBool(const std::string &parameterName, const std::string &parameterValue) const {
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



/*!
 * stringParameterToTime
 */
time_t CtaAdminCmd::stringParameterToTime(const std::string &parameterName, const std::string &parameterValue) const {
  struct tm time;
  if(nullptr==strptime(parameterValue.c_str(), "%d/%m/%Y", &time)) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" - Parameter: "+parameterName+" ("+parameterValue+
            ") could not be interpreted as a time, the allowed format is: <DD/MM/YYYY>");
  }
  return mktime(&time);
}



/*!
 * EOS2CTAChecksumType
 */
optional<std::string> CtaAdminCmd::EOS2CTAChecksumType(const optional<std::string> &EOSChecksumType) {
  if(EOSChecksumType && EOSChecksumType.value()=="adler") return optional<std::string>("ADLER32");
  return nullopt;
}



/*!
 * EOS2CTAChecksumValue
 */
optional<std::string> CtaAdminCmd::EOS2CTAChecksumValue(const optional<std::string> &EOSChecksumValue) {
  if(EOSChecksumValue) {
    std::string CTAChecksumValue("0X");  
    CTAChecksumValue += EOSChecksumValue.value();
    cta::utils::toUpper(CTAChecksumValue);
    return optional<std::string>(CTAChecksumValue);
  }
  return nullopt;
}



/*!
 * xCom_admin
 */
std::string CtaAdminCmd::xCom_admin() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ad/admin add/ch/rm/ls:" << std::endl
       << "\tadd --username/-u <user_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --username/-u <user_name> --comment/-m <\"comment\">" << std::endl
       << "\trm  --username/-u <user_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> username = getOptionStringValue("-u", "--username", true, false);
    if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2)) {
      optional<std::string> comment = getOptionStringValue("-m", "--comment", true, false);
      if("add" == m_requestTokens.at(2)) { //add
        checkOptions(help.str());
        m_catalogue->createAdminUser(m_cliIdentity, username.value(), comment.value());
      }
      else { //ch
        checkOptions(help.str());
        m_catalogue->modifyAdminUserComment(m_cliIdentity, username.value(), comment.value());
      }
    }
    else { //rm
      checkOptions(help.str());
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
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_adminhost
 */
std::string CtaAdminCmd::xCom_adminhost() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ah/adminhost add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <host_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <host_name> --comment/-m <\"comment\">" << std::endl
       << "\trm  --name/-n <host_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> hostname = getOptionStringValue("-n", "--name", true, false);
    if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2)) {
      optional<std::string> comment = getOptionStringValue("-m", "--comment", true, false);
      if("add" == m_requestTokens.at(2)) { //add
        checkOptions(help.str());
        m_catalogue->createAdminHost(m_cliIdentity, hostname.value(), comment.value());
      }
      else { //ch
        checkOptions(help.str());
        m_catalogue->modifyAdminHostComment(m_cliIdentity, hostname.value(), comment.value());
      }
    }
    else { //rm
      checkOptions(help.str());
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
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_tapepool
 */
std::string CtaAdminCmd::xCom_tapepool() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " tp/tapepool add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <tapepool_name> --partialtapesnumber/-p <number_of_partial_tapes> --encrypted/-e <\"true\" or \"false\"> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <tapepool_name> [--partialtapesnumber/-p <number_of_partial_tapes>] [--encrypted/-e <\"true\" or \"false\">] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <tapepool_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> name = getOptionStringValue("-n", "--name", true, false);
    if("add" == m_requestTokens.at(2)) { //add
      optional<uint64_t> ptn = getOptionUint64Value("-p", "--partialtapesnumber", true, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", true, false);
      optional<bool> encrypted = getOptionBoolValue("-e", "--encrypted", true, false);
      checkOptions(help.str());
      m_catalogue->createTapePool(m_cliIdentity, name.value(), ptn.value(), encrypted.value(), comment.value());
    }
    else if("ch" == m_requestTokens.at(2)) { //ch
      optional<uint64_t> ptn = getOptionUint64Value("-p", "--partialtapesnumber", false, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false, false);
      optional<bool> encrypted = getOptionBoolValue("-e", "--encrypted", false, false);
      checkOptions(help.str());
      if(comment) {
        m_catalogue->modifyTapePoolComment(m_cliIdentity, name.value(), comment.value());
      }
      if(ptn) {
        m_catalogue->modifyTapePoolNbPartialTapes(m_cliIdentity, name.value(), ptn.value());
      }
      if(encrypted) {
        m_catalogue->setTapePoolEncryption(m_cliIdentity, name.value(), encrypted.value());
      }
    }
    else { //rm
      checkOptions(help.str());
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
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_archiveroute
 */
std::string CtaAdminCmd::xCom_archiveroute() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ar/archiveroute add/ch/rm/ls:" << std::endl
       << "\tadd --instance/-i <instance_name> --storageclass/-s <storage_class_name> --copynb/-c <copy_number> --tapepool/-t <tapepool_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --instance/-i <instance_name> --storageclass/-s <storage_class_name> --copynb/-c <copy_number> [--tapepool/-t <tapepool_name>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --instance/-i <instance_name> --storageclass/-s <storage_class_name> --copynb/-c <copy_number>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> scn = getOptionStringValue("-s", "--storageclass", true, false);
    optional<uint64_t> cn = getOptionUint64Value("-c", "--copynb", true, false);
    optional<std::string> in = getOptionStringValue("-i", "--instance", true, false);
    if("add" == m_requestTokens.at(2)) { //add
      optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", true, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", true, false);
      checkOptions(help.str());
      m_catalogue->createArchiveRoute(m_cliIdentity, in.value(), scn.value(), cn.value(), tapepool.value(), comment.value());
    }
    else if("ch" == m_requestTokens.at(2)) { //ch
      optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", false, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false, false);
      checkOptions(help.str());
      if(comment) {
        m_catalogue->modifyArchiveRouteComment(m_cliIdentity, in.value(), scn.value(), cn.value(), comment.value());
      }
      if(tapepool) {
        m_catalogue->modifyArchiveRouteTapePoolName(m_cliIdentity, in.value(), scn.value(), cn.value(), tapepool.value());
      }
    }
    else { //rm
      checkOptions(help.str());
      m_catalogue->deleteArchiveRoute(in.value(), scn.value(), cn.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::ArchiveRoute> list= m_catalogue->getArchiveRoutes();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"instance","storage class","copy number","tapepool","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->diskInstanceName);
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
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_logicallibrary
 */
std::string CtaAdminCmd::xCom_logicallibrary() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ll/logicallibrary add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <logical_library_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <logical_library_name> --comment/-m <\"comment\">" << std::endl
       << "\trm  --name/-n <logical_library_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> name = getOptionStringValue("-n", "--name", true, false);
    if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2)) {
      optional<std::string> comment = getOptionStringValue("-m", "--comment", true, false);
      if("add" == m_requestTokens.at(2)) { //add
        checkOptions(help.str());
        m_catalogue->createLogicalLibrary(m_cliIdentity, name.value(), comment.value());
      }
      else { //ch
        checkOptions(help.str());
        m_catalogue->modifyLogicalLibraryComment(m_cliIdentity, name.value(), comment.value());
      }
    }
    else { //rm
      checkOptions(help.str());
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
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_tape
 */
std::string CtaAdminCmd::xCom_tape() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ta/tape add/ch/rm/reclaim/ls/label:" << std::endl
       << "\tadd     --vid/-v <vid> --logicallibrary/-l <logical_library_name> --tapepool/-t <tapepool_name> --capacity/-c <capacity_in_bytes>" << std::endl
       << "\t        --disabled/-d <\"true\" or \"false\"> --full/-f <\"true\" or \"false\"> [--comment/-m <\"comment\">] " << std::endl
       << "\tch      --vid/-v <vid> [--logicallibrary/-l <logical_library_name>] [--tapepool/-t <tapepool_name>] [--capacity/-c <capacity_in_bytes>] [--encryptionkey/-k <encryption_key>]" << std::endl
       << "\t        [--disabled/-d <\"true\" or \"false\">] [--full/-f <\"true\" or \"false\">] [--comment/-m <\"comment\">]" << std::endl
       << "\trm      --vid/-v <vid>" << std::endl
       << "\treclaim --vid/-v <vid>" << std::endl
       << "\tls      [--header/-h] [--all/-a] or any of: [--vid/-v <vid>] [--logicallibrary/-l <logical_library_name>] [--tapepool/-t <tapepool_name>] [--capacity/-c <capacity_in_bytes>]" << std::endl
       << "\t        [--lbp/-p <\"true\" or \"false\">] [--disabled/-d <\"true\" or \"false\">] [--full/-f <\"true\" or \"false\">]" << std::endl
       << "\tlabel   --vid/-v <vid> [--force/-f <\"true\" or \"false\">] [--lbp/-l <\"true\" or \"false\">] [--tag/-t <tag_name>]" << std::endl
       << "Where" << std::endl
       << "\tencryption_key Is the name of the encryption key used to encrypt the tape" << std::endl;
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2) || "reclaim" == m_requestTokens.at(2) || "label" == m_requestTokens.at(2)) {
    optional<std::string> vid = getOptionStringValue("-v", "--vid", true, false);
    if("add" == m_requestTokens.at(2)) { //add
      optional<std::string> logicallibrary = getOptionStringValue("-l", "--logicallibrary", true, false);
      optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", true, false);
      optional<uint64_t> capacity = getOptionUint64Value("-c", "--capacity", true, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", true, true, "-");
      optional<bool> disabled = getOptionBoolValue("-d", "--disabled", true, false);
      optional<bool> full = getOptionBoolValue("-f", "--full", true, false);
      checkOptions(help.str());
      m_catalogue->createTape(m_cliIdentity, vid.value(), logicallibrary.value(), tapepool.value(), capacity.value(), disabled.value(), full.value(), comment.value());
    }
    else if("ch" == m_requestTokens.at(2)) { //ch
      optional<std::string> logicallibrary = getOptionStringValue("-l", "--logicallibrary", false, false);
      optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", false, false);
      optional<uint64_t> capacity = getOptionUint64Value("-c", "--capacity", false, false);
      optional<std::string> encryptionkey = getOptionStringValue("-k", "--encryptionkey", false, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false, false);
      optional<bool> disabled = getOptionBoolValue("-d", "--disabled", false, false);
      optional<bool> full = getOptionBoolValue("-f", "--full", false, false);
      checkOptions(help.str());
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
      if(disabled) {
        m_catalogue->setTapeDisabled(m_cliIdentity, vid.value(), disabled.value());
      }
      if(full) {
        m_catalogue->setTapeFull(m_cliIdentity, vid.value(), full.value());
      }
    }
    else if("reclaim" == m_requestTokens.at(2)) { //reclaim
      checkOptions(help.str());
      m_catalogue->reclaimTape(m_cliIdentity, vid.value());
    }
    else if("label" == m_requestTokens.at(2)) { //label
      //the tag will be set to "-" in case it's missing from the cmdline; which means no tagging
      optional<std::string> tag = getOptionStringValue("-t", "--tag", false, false); 
      optional<bool> force = getOptionBoolValue("-f", "--force", false, true, "false");
      optional<bool> lbp = getOptionBoolValue("-l", "--lbp", false, true, "true");
      checkOptions(help.str());
      m_scheduler->queueLabel(m_cliIdentity, vid.value(), force.value(), lbp.value(), tag);
    }
    else { //rm
      checkOptions(help.str());
      m_catalogue->deleteTape(vid.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    cta::catalogue::TapeSearchCriteria searchCriteria;
    if(!hasOption("-a","--all")) {
      searchCriteria.capacityInBytes = getOptionUint64Value("-c", "--capacity", false, false);
      searchCriteria.disabled = getOptionBoolValue("-d", "--disabled", false, false);
      searchCriteria.full = getOptionBoolValue("-f", "--full", false, false);
      searchCriteria.lbp = getOptionBoolValue("-p", "--lbp", false, false);
      searchCriteria.logicalLibrary = getOptionStringValue("-l", "--logicallibrary", false, false);
      searchCriteria.tapePool = getOptionStringValue("-t", "--tapepool", false, false);
      searchCriteria.vid = getOptionStringValue("-v", "--vid", false, false);
    }
    else {
      m_suppressOptionalOptionsWarning=true;
    }
    checkOptions(help.str());
    std::list<cta::common::dataStructures::Tape> list= m_catalogue->getTapes(searchCriteria);
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"vid","logical library","tapepool","encription key","capacity","occupancy","last fseq","full","disabled","lpb","label drive","label time",
                                         "last w drive","last w time","last r drive","last r time","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->vid);
        currentRow.push_back(it->logicalLibraryName);
        currentRow.push_back(it->tapePoolName);
        currentRow.push_back((bool)it->encryptionKey ? it->encryptionKey.value() : "-");
        currentRow.push_back(std::to_string((unsigned long long)it->capacityInBytes));
        currentRow.push_back(std::to_string((unsigned long long)it->dataOnTapeInBytes));
        currentRow.push_back(std::to_string((unsigned long long)it->lastFSeq));
        if(it->full) currentRow.push_back("true"); else currentRow.push_back("false");
        if(it->disabled) currentRow.push_back("true"); else currentRow.push_back("false");
        if(it->lbp) currentRow.push_back("true"); else currentRow.push_back("false");
        if(it->labelLog) {
          currentRow.push_back(it->labelLog.value().drive);
          currentRow.push_back(std::to_string((unsigned long long)it->labelLog.value().time));
        }
        else {
          currentRow.push_back("-");
          currentRow.push_back("-");
        }
        if(it->lastWriteLog) {
          currentRow.push_back(it->lastWriteLog.value().drive);
          currentRow.push_back(std::to_string((unsigned long long)it->lastWriteLog.value().time));
        }
        else {
          currentRow.push_back("-");
          currentRow.push_back("-");
        }
        if(it->lastReadLog) {
          currentRow.push_back(it->lastReadLog.value().drive);
          currentRow.push_back(std::to_string((unsigned long long)it->lastReadLog.value().time));
        }
        else {
          currentRow.push_back("-");
          currentRow.push_back("-");
        }
        addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
        currentRow.push_back(it->comment);
        responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_storageclass
 */
std::string CtaAdminCmd::xCom_storageclass() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " sc/storageclass add/ch/rm/ls:" << std::endl
       << "\tadd --instance/-i <instance_name> --name/-n <storage_class_name> --copynb/-c <number_of_tape_copies> --comment/-m <\"comment\">" << std::endl
       << "\tch  --instance/-i <instance_name> --name/-n <storage_class_name> [--copynb/-c <number_of_tape_copies>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --instance/-i <instance_name> --name/-n <storage_class_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> scn = getOptionStringValue("-n", "--name", true, false);
    optional<std::string> in = getOptionStringValue("-i", "--instance", true, false);
    if("add" == m_requestTokens.at(2)) { //add
      optional<uint64_t> cn = getOptionUint64Value("-c", "--copynb", true, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", true, false);
      checkOptions(help.str());
      common::dataStructures::StorageClass storageClass;
      storageClass.diskInstance = in.value();
      storageClass.name = scn.value();
      storageClass.nbCopies = cn.value();
      storageClass.comment = comment.value();
      m_catalogue->createStorageClass(m_cliIdentity, storageClass);
    }
    else if("ch" == m_requestTokens.at(2)) { //ch
      optional<uint64_t> cn = getOptionUint64Value("-c", "--copynb", false, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false, false);
      checkOptions(help.str());
      if(comment) {
        m_catalogue->modifyStorageClassComment(m_cliIdentity, in.value(), scn.value(), comment.value());
      }
      if(cn) {
        m_catalogue->modifyStorageClassNbCopies(m_cliIdentity, in.value(), scn.value(), cn.value());
      }
    }
    else { //rm
      checkOptions(help.str());
      m_catalogue->deleteStorageClass(in.value(), scn.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::StorageClass> list= m_catalogue->getStorageClasses();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"instance","storage class","number of copies","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->diskInstance);
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
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_requestermountrule
 */
std::string CtaAdminCmd::xCom_requestermountrule() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " rmr/requestermountrule add/ch/rm/ls:" << std::endl
       << "\tadd --instance/-i <instance_name> --name/-n <user_name> --mountpolicy/-u <policy_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --instance/-i <instance_name> --name/-n <user_name> [--mountpolicy/-u <policy_name>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --instance/-i <instance_name> --name/-n <user_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> name = getOptionStringValue("-n", "--name", true, false);
    optional<std::string> in = getOptionStringValue("-i", "--instance", true, false);
    if("add" == m_requestTokens.at(2)) { //add
      optional<std::string> mountpolicy = getOptionStringValue("-u", "--mountpolicy", true, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", true, false);
      checkOptions(help.str());
      m_catalogue->createRequesterMountRule(m_cliIdentity, mountpolicy.value(), in.value(), name.value(),
        comment.value());
    }
    else if("ch" == m_requestTokens.at(2)) { //ch
      optional<std::string> mountpolicy = getOptionStringValue("-u", "--mountpolicy", false, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false, false);
      checkOptions(help.str());
      if(comment) {
        m_catalogue->modifyRequesteMountRuleComment(m_cliIdentity, in.value(), name.value(), comment.value());
      }
      if(mountpolicy) {
        m_catalogue->modifyRequesterMountRulePolicy(m_cliIdentity, in.value(), name.value(), mountpolicy.value());
      }
    }
    else { //rm
      checkOptions(help.str());
      m_catalogue->deleteRequesterMountRule(in.value(), name.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::RequesterMountRule> list= m_catalogue->getRequesterMountRules();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"instance","username","policy","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->diskInstance);
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
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_groupmountrule
 */
std::string CtaAdminCmd::xCom_groupmountrule() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " gmr/groupmountrule add/ch/rm/ls:" << std::endl
       << "\tadd --instance/-i <instance_name> --name/-n <user_name> --mountpolicy/-u <policy_name> --comment/-m <\"comment\">" << std::endl
       << "\tch  --instance/-i <instance_name> --name/-n <user_name> [--mountpolicy/-u <policy_name>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --instance/-i <instance_name> --name/-n <user_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> name = getOptionStringValue("-n", "--name", true, false);
    optional<std::string> in = getOptionStringValue("-i", "--instance", true, false);
    if("add" == m_requestTokens.at(2)) { //add
      optional<std::string> mountpolicy = getOptionStringValue("-u", "--mountpolicy", true, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", true, false);
      checkOptions(help.str());
      m_catalogue->createRequesterGroupMountRule(m_cliIdentity, mountpolicy.value(), in.value(), name.value(),
        comment.value());
    }
    else if("ch" == m_requestTokens.at(2)) { //ch
      optional<std::string> mountpolicy = getOptionStringValue("-u", "--mountpolicy", false, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false, false);
      checkOptions(help.str());
      if(comment) {
        m_catalogue->modifyRequesterGroupMountRuleComment(m_cliIdentity, in.value(), name.value(), comment.value());
      }
      if(mountpolicy) {
        m_catalogue->modifyRequesterGroupMountRulePolicy(m_cliIdentity, in.value(), name.value(), mountpolicy.value());
      }
    }
    else { //rm
      checkOptions(help.str());
      m_catalogue->deleteRequesterGroupMountRule(in.value(), name.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::RequesterGroupMountRule> list= m_catalogue->getRequesterGroupMountRules();
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"instance","group","policy","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->diskInstance);
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
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_mountpolicy
 */
std::string CtaAdminCmd::xCom_mountpolicy() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " mp/mountpolicy add/ch/rm/ls:" << std::endl
       << "\tadd --name/-n <mountpolicy_name> --archivepriority/--ap <priority_value> --minarchiverequestage/--aa <minRequestAge> --retrievepriority/--rp <priority_value>" << std::endl
       << "\t    --minretrieverequestage/--ra <minRequestAge> --maxdrivesallowed/-d <maxDrivesAllowed> --comment/-m <\"comment\">" << std::endl
       << "\tch  --name/-n <mountpolicy_name> [--archivepriority/--ap <priority_value>] [--minarchiverequestage/--aa <minRequestAge>] [--retrievepriority/--rp <priority_value>]" << std::endl
       << "\t   [--minretrieverequestage/--ra <minRequestAge>] [--maxdrivesallowed/-d <maxDrivesAllowed>] [--comment/-m <\"comment\">]" << std::endl
       << "\trm  --name/-n <mountpolicy_name>" << std::endl
       << "\tls  [--header/-h]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> group = getOptionStringValue("-n", "--name", true, false);
    if("add" == m_requestTokens.at(2) || "ch" == m_requestTokens.at(2)) { 
      if("add" == m_requestTokens.at(2)) { //add     
        optional<uint64_t> archivepriority = getOptionUint64Value("--ap", "--archivepriority", true, false);
        optional<uint64_t> minarchiverequestage = getOptionUint64Value("--aa", "--minarchiverequestage", true, false);
        optional<uint64_t> retrievepriority = getOptionUint64Value("--rp", "--retrievepriority", true, false);
        optional<uint64_t> minretrieverequestage = getOptionUint64Value("--ra", "--minretrieverequestage", true, false);
        optional<uint64_t> maxdrivesallowed = getOptionUint64Value("-d", "--maxdrivesallowed", true, false);
        optional<std::string> comment = getOptionStringValue("-m", "--comment", true, false);
        checkOptions(help.str());
        m_catalogue->createMountPolicy(m_cliIdentity, group.value(), archivepriority.value(), minarchiverequestage.value(), retrievepriority.value(), minretrieverequestage.value(), maxdrivesallowed.value(), comment.value());
      }
      else if("ch" == m_requestTokens.at(2)) { //ch      
        optional<uint64_t> archivepriority = getOptionUint64Value("--ap", "--archivepriority", false, false);
        optional<uint64_t> minarchiverequestage = getOptionUint64Value("--aa", "--minarchiverequestage", false, false);
        optional<uint64_t> retrievepriority = getOptionUint64Value("--rp", "--retrievepriority", false, false);
        optional<uint64_t> minretrieverequestage = getOptionUint64Value("--ra", "--minretrieverequestage", false, false);
        optional<uint64_t> maxdrivesallowed = getOptionUint64Value("-d", "--maxdrivesallowed", false, false);
        optional<std::string> comment = getOptionStringValue("-m", "--comment", false, false);
        checkOptions(help.str());
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
      std::vector<std::string> header = {"mount policy","a.priority","a.minAge","r.priority","r.minAge","max drives","c.user","c.host","c.time","m.user","m.host","m.time","comment"};
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
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_repack
 */
std::string CtaAdminCmd::xCom_repack() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " re/repack add/rm/ls/err:" << std::endl
       << "\tadd --vid/-v <vid> [--justexpand/-e or --justrepack/-r] [--tag/-t <tag_name>]" << std::endl
       << "\trm  --vid/-v <vid>" << std::endl
       << "\tls  [--header/-h] [--vid/-v <vid>]" << std::endl
       << "\terr --vid/-v <vid>" << std::endl;  
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  if("add" == m_requestTokens.at(2) || "err" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> vid = getOptionStringValue("-v", "--vid", true, false);
    if(!vid) {
      throw cta::exception::UserError(help.str());
    }  
    if("add" == m_requestTokens.at(2)) { //add
      optional<std::string> tag = getOptionStringValue("-t", "--tag", false, false);
      bool justexpand = hasOption("-e", "--justexpand");
      bool justrepack = hasOption("-r", "--justrepack");
      cta::common::dataStructures::RepackType type=cta::common::dataStructures::RepackType::expandandrepack;
      if(justexpand) {
        type=cta::common::dataStructures::RepackType::justexpand;
      }
      if(justrepack) {
        type=cta::common::dataStructures::RepackType::justrepack;
      }
      if(justexpand&&justrepack) {
        throw cta::exception::UserError(help.str());
      }
      checkOptions(help.str());
      m_scheduler->queueRepack(m_cliIdentity, vid.value(), tag, type);
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
      checkOptions(help.str());
      m_scheduler->cancelRepack(m_cliIdentity, vid.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::RepackInfo> list;
    optional<std::string> vid = getOptionStringValue("-v", "--vid", false, false);
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
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_shrink
 */
std::string CtaAdminCmd::xCom_shrink() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " sh/shrink --tapepool/-t <tapepool_name>" << std::endl;
  optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", true, false);
  checkOptions(help.str());
  m_scheduler->shrink(m_cliIdentity, tapepool.value());
  return cmdlineOutput.str();
}



/*!
 * xCom_verify
 */
std::string CtaAdminCmd::xCom_verify() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ve/verify add/rm/ls/err:" << std::endl
       << "\tadd --vid/-v <vid> [--nbfiles <number_of_files_per_tape>] [--tag/-t <tag_name>]" << std::endl
       << "\trm  --vid/-v <vid>" << std::endl
       << "\tls  [--header/-h] [--vid/-v <vid>]" << std::endl
       << "\terr --vid/-v <vid>" << std::endl;  
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  if("add" == m_requestTokens.at(2) || "err" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> vid = getOptionStringValue("-v", "--vid", true, false);
    if("add" == m_requestTokens.at(2)) { //add
      optional<std::string> tag = getOptionStringValue("-t", "--tag", false, false);
      optional<uint64_t> numberOfFiles = getOptionUint64Value("-p", "--partial", false, false); //nullopt means do a complete verification      
      checkOptions(help.str());
      m_scheduler->queueVerify(m_cliIdentity, vid.value(), tag, numberOfFiles);
    }
    else if("err" == m_requestTokens.at(2)) { //err
      checkOptions(help.str());
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
      checkOptions(help.str());
      m_scheduler->cancelVerify(m_cliIdentity, vid.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::VerifyInfo> list;
    optional<std::string> vid = getOptionStringValue("-v", "--vid", true, false);
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
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_archivefile
 */
std::string CtaAdminCmd::xCom_archivefile() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " af/archivefile ls [--header/-h] [--id/-I <archive_file_id>] [--diskid/-d <disk_id>] [--copynb/-c <copy_no>] [--vid/-v <vid>] [--tapepool/-t <tapepool>] "
          "[--owner/-o <owner>] [--group/-g <group>] [--storageclass/-s <class>] [--path/-p <fullpath>] [--instance/-i <instance>] [--summary/-S] [--all/-a] (default gives error)" << std::endl;  
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  if("ls" == m_requestTokens.at(2)) { //ls
    bool summary = hasOption("-S", "--summary");
    bool all = hasOption("-a", "--all");
    if(all) m_suppressOptionalOptionsWarning=true;
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = getOptionUint64Value("-I", "--id", false, false);
    searchCriteria.diskFileGroup = getOptionStringValue("-g", "--group", false, false);
    searchCriteria.diskFileId = getOptionStringValue("-d", "--diskid", false, false);
    searchCriteria.diskFilePath = getOptionStringValue("-p", "--path", false, false);
    searchCriteria.diskFileUser = getOptionStringValue("-o", "--owner", false, false);
    searchCriteria.diskInstance = getOptionStringValue("-i", "--instance", false, false);
    searchCriteria.storageClass = getOptionStringValue("-s", "--storageclass", false, false);
    searchCriteria.tapeFileCopyNb = getOptionUint64Value("-c", "--copynb", false, false);
    searchCriteria.tapePool = getOptionStringValue("-t", "--tapepool", false, false);
    searchCriteria.vid = getOptionStringValue("-v", "--vid", false, false);
    if(!all) {
      checkOptions(help.str());
    }
    if(!summary) {
      const bool displayHeader = hasOption("-h", "--header");
      auto archiveFileItor = m_catalogue->getArchiveFiles(searchCriteria);
      m_listArchiveFilesCmd.reset(new ListArchiveFilesCmd(m_log, error, displayHeader, std::move(archiveFileItor)));
      /*
      std::unique_ptr<cta::catalogue::ArchiveFileItor> itor = m_catalogue->getArchiveFileItor(searchCriteria);
      if(itor->hasMore()) {
        std::vector<std::vector<std::string>> responseTable;
        std::vector<std::string> header = {"id","copy no","vid","fseq","block id","instance","disk id","size","checksum type","checksum value","storage class","owner","group","path","creation time"};
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
            currentRow.push_back(archiveFile.diskInstance);
            currentRow.push_back(archiveFile.diskFileId);
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
      */
    }
    else { //summary
      cta::common::dataStructures::ArchiveFileSummary summary=m_catalogue->getTapeFileSummary(searchCriteria);
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {"total number of files","total size"};
      std::vector<std::string> row = {std::to_string((unsigned long long)summary.totalFiles),std::to_string((unsigned long long)summary.totalBytes)};
      if(hasOption("-h", "--header")) responseTable.push_back(header);
      responseTable.push_back(row);
      cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
    }
  }
  else {
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_test
 */
std::string CtaAdminCmd::xCom_test() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " te/test read/write/write_auto (to be run on an empty self-dedicated drive; it is a synchronous command that returns performance stats and errors; all locations are local to the tapeserver):" << std::endl
       << "\tread  --drive/-d <drive_name> --vid/-v <vid> --firstfseq/-f <first_fseq> --lastfseq/-l <last_fseq> --checkchecksum/-c --output/-o <\"null\" or output_dir> [--tag/-t <tag_name>]" << std::endl
       << "\twrite --drive/-d <drive_name> --vid/-v <vid> --file/-f <filename> [--tag/-t <tag_name>]" << std::endl
       << "\twrite_auto --drive/-d <drive_name> --vid/-v <vid> --number/-n <number_of_files> --size/-s <file_size> --input/-i <\"zero\" or \"urandom\"> [--tag/-t <tag_name>]" << std::endl;  
  if(m_requestTokens.size() < 3) {
    throw cta::exception::UserError(help.str());
  }
  optional<std::string> drive = getOptionStringValue("-d", "--drive", true, false);
  optional<std::string> vid = getOptionStringValue("-v", "--vid", true, false);
  if("read" == m_requestTokens.at(2)) {
    optional<uint64_t> firstfseq = getOptionUint64Value("-f", "--firstfseq", true, false);
    optional<uint64_t> lastfseq = getOptionUint64Value("-l", "--lastfseq", true, false);
    optional<std::string> output = getOptionStringValue("-o", "--output", true, false);        
    bool checkchecksum = hasOption("-c", "--checkchecksum");
    checkOptions(help.str());
    optional<std::string> tag = getOptionStringValue("-t", "--tag", false, false);
    std::string tag_value = tag? tag.value():"-";
    cta::common::dataStructures::ReadTestResult res = m_scheduler->readTest(m_cliIdentity, drive.value(), vid.value(), firstfseq.value(), lastfseq.value(), checkchecksum, output.value(), tag_value);   
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
      optional<std::string> file = getOptionStringValue("-f", "--file", true, false);
      checkOptions(help.str());
      optional<std::string> tag = getOptionStringValue("-t", "--tag", false, false);
      std::string tag_value = tag? tag.value():"-";
      res = m_scheduler->writeTest(m_cliIdentity, drive.value(), vid.value(), file.value(), tag_value);
    }
    else { //write_auto
      optional<uint64_t> number = getOptionUint64Value("-n", "--number", true, false);
      optional<uint64_t> size = getOptionUint64Value("-s", "--size", true, false);
      optional<std::string> input = getOptionStringValue("-i", "--input", true, false);
      if(input.value()!="zero"&&input.value()!="urandom") {
        m_missingRequiredOptions.push_back("--input value must be either \"zero\" or \"urandom\"");
      }
      checkOptions(help.str());
      optional<std::string> tag = getOptionStringValue("-t", "--tag", false, false);
      std::string tag_value = tag? tag.value():"-";
      cta::common::dataStructures::TestSourceType type;
      if(input.value()=="zero") { //zero
        type = cta::common::dataStructures::TestSourceType::devzero;
      }
      else { //urandom
        type = cta::common::dataStructures::TestSourceType::devurandom;
      }
      res = m_scheduler->write_autoTest(m_cliIdentity, drive.value(), vid.value(), number.value(), size.value(), type, tag_value);
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
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_drive
 */
std::string CtaAdminCmd::xCom_drive() {
  log::LogContext lc(m_log);
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " dr/drive up/down/ls (it is a synchronous command):"    << std::endl
       << "Set the requested state of the drive. The drive will complete any running mount" << std::endl
       << "unless it is preempted with the --force option."                                 << std::endl
       << "\tup <drive_name>"                                                               << std::endl
       << "\tdown <drive_name> [--force/-f]"                                                << std::endl
       << ""                                                                                << std::endl
       << "List the states for one or all drives"                                           << std::endl
       << "\tls [<drive_name>]"                                                             << std::endl;
       
  // We should have at least one sub command. {"cta", "dr/drive", "up/down/ls"}.
  if (m_requestTokens.size() < 3)
    throw cta::exception::UserError(help.str());
  if ("up" == m_requestTokens.at(2)) {
    // Here the drive name is required in addition
    if (m_requestTokens.size() != 4)
      throw cta::exception::UserError(help.str());
    m_scheduler->setDesiredDriveState(m_cliIdentity, m_requestTokens.at(3), true, false, lc);
    cmdlineOutput << "Drive " << m_requestTokens.at(3) << " set UP." << std::endl;
  } else if ("down" == m_requestTokens.at(2)) {
    // Parse the command line for option and drive name.
    cta::utils::GetOpThreadSafe::Request req;
    for (size_t i=2; i<m_requestTokens.size(); i++)
      req.argv.push_back(m_requestTokens.at(i));
    req.optstring = { "f" };
    struct ::option longOptions[] = { { "force", no_argument, 0 , 'f' }, { 0, 0, 0, 0 } };
    req.longopts = longOptions;
    auto reply = cta::utils::GetOpThreadSafe::getOpt(req);
    // We should have one and only one no-option argument, the drive name.
    if (reply.remainder.size() != 1) {
      throw cta::exception::UserError(help.str());
    }
    // Check if the force option was present.
    bool force=reply.options.size() && (reply.options.at(0).option == "f");
    m_scheduler->setDesiredDriveState(m_cliIdentity, reply.remainder.at(0), false, force, lc);
    cmdlineOutput << "Drive " <<  reply.remainder.at(0) << " set DOWN";
    if (force) {
      cmdlineOutput << " (forced down)";
    }
    cmdlineOutput << "." << std::endl;
  } else if ("ls" == m_requestTokens.at(2)) {
    if ((m_requestTokens.size() == 3) || (m_requestTokens.size() == 4)) {
      // We will dump all the drives, and select the one asked for if needed.
      bool singleDrive = (m_requestTokens.size() == 4);
      bool driveFound = false;
      auto driveStates = m_scheduler->getDriveStates(m_cliIdentity, lc);
      if (driveStates.size()) {
        std::vector<std::vector<std::string>> responseTable;
        std::vector<std::string> headers = {"library", "drive", "host", "desired", "request",
          "status", "since", "vid", "tapepool", "files", "MBytes",
          "MB/s", "session", "age"};
        responseTable.push_back(headers);
        typedef decltype(*driveStates.begin()) dStateVal;
        driveStates.sort([](const dStateVal & a, const dStateVal & b){ return a.driveName < b.driveName; });
        for (auto ds: driveStates) {
          if (singleDrive && m_requestTokens.at(3) != ds.driveName) continue;
          driveFound = true;
          std::vector<std::string> currentRow;
          currentRow.push_back(ds.logicalLibrary);
          currentRow.push_back(ds.driveName);
          currentRow.push_back(ds.host);
          currentRow.push_back(ds.desiredDriveState.up?"Up":"Down");
          currentRow.push_back(cta::common::dataStructures::toString(ds.mountType));
          currentRow.push_back(cta::common::dataStructures::toString(ds.driveStatus));
          // print the time spent in the current state
          switch(ds.driveStatus) {
          case cta::common::dataStructures::DriveStatus::Up:
          case cta::common::dataStructures::DriveStatus::Down:
            currentRow.push_back(std::to_string((unsigned long long)(time(nullptr)-ds.downOrUpStartTime)));
            break;
          case cta::common::dataStructures::DriveStatus::Starting:
            currentRow.push_back(std::to_string((unsigned long long)(time(nullptr)-ds.startStartTime)));
            break;
          case cta::common::dataStructures::DriveStatus::Mounting:
            currentRow.push_back(std::to_string((unsigned long long)(time(nullptr)-ds.mountStartTime)));
            break;
          case cta::common::dataStructures::DriveStatus::Transferring:
            currentRow.push_back(std::to_string((unsigned long long)(time(nullptr)-ds.transferStartTime)));
            break;
          case cta::common::dataStructures::DriveStatus::CleaningUp:
            currentRow.push_back(std::to_string((unsigned long long)(time(nullptr)-ds.cleanupStartTime)));
            break;
          case cta::common::dataStructures::DriveStatus::Unloading:
            currentRow.push_back(std::to_string((unsigned long long)(time(nullptr)-ds.unloadStartTime)));
            break;
          case cta::common::dataStructures::DriveStatus::Unmounting:
            currentRow.push_back(std::to_string((unsigned long long)(time(nullptr)-ds.unmountStartTime)));
            break;
          case cta::common::dataStructures::DriveStatus::DrainingToDisk:
            currentRow.push_back(std::to_string((unsigned long long)(time(nullptr)-ds.drainingStartTime)));
            break;
          case cta::common::dataStructures::DriveStatus::Unknown:
            currentRow.push_back("-");
            break;
          }
          currentRow.push_back(ds.currentVid==""?"-":ds.currentVid);
          currentRow.push_back(ds.currentTapePool==""?"-":ds.currentTapePool);
          switch (ds.driveStatus) {
            case cta::common::dataStructures::DriveStatus::Transferring:
              currentRow.push_back(std::to_string((unsigned long long)ds.filesTransferredInSession));
              currentRow.push_back(BytesToMbString(ds.bytesTransferredInSession));
              currentRow.push_back(BytesToMbString(ds.latestBandwidth));
              break;
            default:
              currentRow.push_back("-");
              currentRow.push_back("-");
              currentRow.push_back("-");
          }
          switch(ds.driveStatus) {
            case cta::common::dataStructures::DriveStatus::Up:
            case cta::common::dataStructures::DriveStatus::Down:
            case cta::common::dataStructures::DriveStatus::Unknown:
              currentRow.push_back("-");
              break;
            default:
              currentRow.push_back(std::to_string((unsigned long long)ds.sessionId));
          }
          currentRow.push_back(std::to_string((unsigned long long)(time(nullptr)-ds.lastUpdateTime)));
          responseTable.push_back(currentRow);
        }
        if (singleDrive && !driveFound) {
          throw cta::exception::UserError(std::string("No such drive: ") + m_requestTokens.at(3));
        }
        cmdlineOutput<< formatResponse(responseTable, true);
      }
    } else {
      throw cta::exception::UserError(help.str());
    } 
  } else {
    throw cta::exception::UserError(help.str());
  }
  return cmdlineOutput.str();
}



/*!
 * xCom_listpendingarchives
 */
std::string CtaAdminCmd::xCom_listpendingarchives() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " lpa/listpendingarchives [--header/-h] [--tapepool/-t <tapepool_name>] [--extended/-x]" << std::endl;
  optional<std::string> tapepool = getOptionStringValue("-t", "--tapepool", false, false);
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
      std::vector<std::string> header = {"tapepool","id","storage class","copy no.","disk id","instance","checksum type","checksum value","size","user","group","path"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = result.cbegin(); it != result.cend(); it++) {
        for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++) {
          std::vector<std::string> currentRow;
          currentRow.push_back(it->first);
          currentRow.push_back(std::to_string((unsigned long long)jt->archiveFileID));
          currentRow.push_back(jt->request.storageClass);
          currentRow.push_back(std::to_string((unsigned long long)jt->copyNumber));
          currentRow.push_back(jt->request.diskFileID);
          currentRow.push_back(jt->instanceName);
          currentRow.push_back(jt->request.checksumType);
          currentRow.push_back(jt->request.checksumValue);         
          currentRow.push_back(std::to_string((unsigned long long)jt->request.fileSize));
          currentRow.push_back(jt->request.requester.name);
          currentRow.push_back(jt->request.requester.group);
          currentRow.push_back(jt->request.diskFileInfo.path);
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
  return cmdlineOutput.str();
}



/*!
 * xCom_listpendingretrieves
 */
std::string CtaAdminCmd::xCom_listpendingretrieves() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " lpr/listpendingretrieves [--header/-h] [--vid/-v <vid>] [--extended/-x]" << std::endl;
  optional<std::string> vid = getOptionStringValue("-v", "--vid", false, false);
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
      std::vector<std::string> header = {"vid","id","copy no.","fseq","block id","size","user","group","path"};
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
  return cmdlineOutput.str();
}



/*!
 * xCom_showqueues
 */
std::string CtaAdminCmd::xCom_showqueues() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " sq/showqueues [--header/-h]" << std::endl;
  log::LogContext lc(m_log);
  auto queuesAndMounts=m_scheduler->getQueuesAndMountSummaries(lc);
  if (queuesAndMounts.size()) {
    std::vector<std::vector<std::string>> responseTable;
    std::vector<std::string> header = {"type","tapepool","vid","files queued","MBytes queued","oldest age","priority","min age","max drives",
      "cur. mounts", "cur. files", "cur. MBytes", "MB/s", "next mounts", "tapes capacity", "files on tapes", "MBytes on tapes", "full tapes", "empty tapes",
      "disabled tapes", "writables tapes"};
    if(hasOption("-h", "--header")) responseTable.push_back(header);
    for (auto & q: queuesAndMounts) {
      std::vector<std::string> currentRow;
      currentRow.push_back(common::dataStructures::toString(q.mountType));
      currentRow.push_back(q.tapePool);
      currentRow.push_back(q.vid);
      currentRow.push_back(std::to_string(q.filesQueued));
      currentRow.push_back(BytesToMbString(q.bytesQueued));
      currentRow.push_back(std::to_string(q.oldestJobAge));
      if (common::dataStructures::MountType::Archive == q.mountType) {
        currentRow.push_back(std::to_string(q.mountPolicy.archivePriority));
        currentRow.push_back(std::to_string(q.mountPolicy.archiveMinRequestAge));
        currentRow.push_back(std::to_string(q.mountPolicy.maxDrivesAllowed));
      } else if (common::dataStructures::MountType::Retrieve == q.mountType) {
        currentRow.push_back(std::to_string(q.mountPolicy.retrievePriority));
        currentRow.push_back(std::to_string(q.mountPolicy.retrieveMinRequestAge));
        currentRow.push_back(std::to_string(q.mountPolicy.maxDrivesAllowed));
      } else {
        currentRow.push_back("-");
        currentRow.push_back("-");
        currentRow.push_back("-");
      }
      currentRow.push_back(std::to_string(q.currentMounts));
      currentRow.push_back(std::to_string(q.currentFiles));
      currentRow.push_back(BytesToMbString(q.currentBytes));
      currentRow.push_back(BytesToMbString(q.latestBandwidth));
      currentRow.push_back(std::to_string(q.nextMounts));
      currentRow.push_back(std::to_string(q.tapesCapacity/(uint64_t)MBytes));
      currentRow.push_back(std::to_string(q.filesOnTapes));
      currentRow.push_back(BytesToMbString(q.dataOnTapes));
      currentRow.push_back(std::to_string(q.fullTapes));
      currentRow.push_back(std::to_string(q.emptyTapes));
      currentRow.push_back(std::to_string(q.disabledTapes));
      currentRow.push_back(std::to_string(q.writableTapes));
      responseTable.push_back(currentRow);
    }
    cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
  }
  return cmdlineOutput.str();
}




/*!
 * xCom_archive
 */
std::string CtaAdminCmd::xCom_archive() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " a/archive --user <user> --group <group> --diskid <disk_id> --srcurl <src_URL> --size <size> --checksumtype <checksum_type>" << std::endl
       << "\t--checksumvalue <checksum_value> --storageclass <storage_class> --diskfilepath <disk_filepath> --diskfileowner <disk_fileowner>" << std::endl
       << "\t--diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob> --reportURL <reportURL>" << std::endl
       << "\tNote: apply the postfix \":base64\" to long option names whose values are base64 encoded" << std::endl;
  optional<std::string> user = getOptionStringValue("", "--user", true, false);
  optional<std::string> group = getOptionStringValue("", "--group", true, false);
  optional<std::string> diskid = getOptionStringValue("", "--diskid", true, false);
  optional<std::string> srcurl = getOptionStringValue("", "--srcurl", true, false);
  optional<uint64_t> size = getOptionUint64Value("", "--size", true, false);
  optional<std::string> checksumtype = EOS2CTAChecksumType(getOptionStringValue("", "--checksumtype", true, false));
  optional<std::string> checksumvalue = EOS2CTAChecksumValue(getOptionStringValue("", "--checksumvalue", true, false));
  optional<std::string> storageclass = getOptionStringValue("", "--storageclass", true, false);
  optional<std::string> diskfilepath = getOptionStringValue("", "--diskfilepath", true, false);
  optional<std::string> diskfileowner = getOptionStringValue("", "--diskfileowner", true, false);
  optional<std::string> diskfilegroup = getOptionStringValue("", "--diskfilegroup", true, false);
  optional<std::string> recoveryblob = getOptionStringValue("", "--recoveryblob", true, false);
  optional<std::string> archiveReportURL = getOptionStringValue("", "--reportURL", false, true, "null:");
  checkOptions(help.str());
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
  request.diskFileInfo=diskFileInfo;
  request.diskFileID=diskid.value();
  request.fileSize=size.value();
  request.requester=originator;
  request.srcURL=srcurl.value();
  request.storageClass=storageclass.value();
  request.archiveReportURL=archiveReportURL.value();
  request.creationLog.host = m_cliIdentity.host;
  request.creationLog.username = m_cliIdentity.username;
  request.creationLog.time = time(nullptr);
  log::LogContext lc(m_log);
  uint64_t archiveFileId = m_scheduler->queueArchive(m_cliIdentity.username, request, lc);
  cmdlineOutput << "<eos::wfe::path::fxattr:sys.archiveFileId>" << archiveFileId << std::endl;
  return cmdlineOutput.str();
}



/*!
 * xCom_retrieve
 */
std::string CtaAdminCmd::xCom_retrieve() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " r/retrieve --user <user> --group <group> --id <CTA_ArchiveFileID> --dsturl <dst_URL> --diskfilepath <disk_filepath>" << std::endl
       << "\t--diskfileowner <disk_fileowner> --diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob>" << std::endl
       << "\tNote: apply the postfix \":base64\" to long option names whose values are base64 encoded" << std::endl;
  optional<std::string> user = getOptionStringValue("", "--user", true, false);
  optional<std::string> group = getOptionStringValue("", "--group", true, false);
  optional<uint64_t> id = getOptionUint64Value("", "--id", true, false);
  optional<std::string> dsturl = getOptionStringValue("", "--dsturl", true, false);
  optional<std::string> diskfilepath = getOptionStringValue("", "--diskfilepath", true, false);
  optional<std::string> diskfileowner = getOptionStringValue("", "--diskfileowner", true, false);
  optional<std::string> diskfilegroup = getOptionStringValue("", "--diskfilegroup", true, false);
  optional<std::string> recoveryblob = getOptionStringValue("", "--recoveryblob", true, false);
  checkOptions(help.str());
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user.value();
  originator.group=group.value();
  cta::common::dataStructures::DiskFileInfo diskFileInfo;
  diskFileInfo.recoveryBlob=recoveryblob.value();
  diskFileInfo.group=diskfilegroup.value();
  diskFileInfo.owner=diskfileowner.value();
  diskFileInfo.path=diskfilepath.value();
  cta::common::dataStructures::RetrieveRequest request;
  request.diskFileInfo=diskFileInfo;
  request.archiveFileID=id.value();
  request.requester=originator;
  request.dstURL=dsturl.value();
  request.creationLog.host = m_cliIdentity.host;
  request.creationLog.username = m_cliIdentity.username;
  request.creationLog.time = time(nullptr);
  log::LogContext lc(m_log);
  m_scheduler->queueRetrieve(m_cliIdentity.username, request, lc);
  return cmdlineOutput.str();
}



/*!
 * xCom_deletearchive
 */
std::string CtaAdminCmd::xCom_deletearchive() {
  std::stringstream help;
  help << m_requestTokens.at(0) << " da/deletearchive --user <user> --group <group> --id <CTA_ArchiveFileID>" << std::endl
       << "\tNote: apply the postfix \":base64\" to long option names whose values are base64 encoded" << std::endl;
  optional<std::string> user = getOptionStringValue("", "--user", true, false);
  optional<std::string> group = getOptionStringValue("", "--group", true, false);
  optional<uint64_t> id = getOptionUint64Value("", "--id", true, false);
  checkOptions(help.str());
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user.value();
  originator.group=group.value();
  cta::common::dataStructures::DeleteArchiveRequest request;
  request.archiveFileID=id.value();
  request.requester=originator;
  log::LogContext lc(m_log);
  m_scheduler->deleteArchive(m_cliIdentity.username, request, lc);
  log::ScopedParamContainer params(lc);
  params.add("fileId", request.archiveFileID);
  lc.log(log::INFO, "In CtaAdminCmd::xCom_deletearchive(): deleted archive file.");
  return "";
}



/*!
 * xCom_cancelretrieve
 */
std::string CtaAdminCmd::xCom_cancelretrieve() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " cr/cancelretrieve --user <user> --group <group> --id <CTA_ArchiveFileID> --dsturl <dst_URL> --diskfilepath <disk_filepath>" << std::endl
       << "\t--diskfileowner <disk_fileowner> --diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob>" << std::endl
       << "\tNote: apply the postfix \":base64\" to long option names whose values are base64 encoded" << std::endl;
  optional<std::string> user = getOptionStringValue("", "--user", true, false);
  optional<std::string> group = getOptionStringValue("", "--group", true, false);
  optional<uint64_t> id = getOptionUint64Value("", "--id", true, false);
  optional<std::string> dsturl = getOptionStringValue("", "--dsturl", true, false);
  optional<std::string> diskfilepath = getOptionStringValue("", "--diskfilepath", true, false);
  optional<std::string> diskfileowner = getOptionStringValue("", "--diskfileowner", true, false);
  optional<std::string> diskfilegroup = getOptionStringValue("", "--diskfilegroup", true, false);
  optional<std::string> recoveryblob = getOptionStringValue("", "--recoveryblob", true, false);
  checkOptions(help.str());
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
  m_scheduler->cancelRetrieve(m_cliIdentity.username, request);
  return cmdlineOutput.str();
}



/*!
 * xCom_updatefilestorageclass
 */
std::string CtaAdminCmd::xCom_updatefilestorageclass() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ufsc/updatefilestorageclass --user <user> --group <group> --id <CTA_ArchiveFileID> --storageclass <storage_class> --diskfilepath <disk_filepath>" << std::endl
       << "\t--diskfileowner <disk_fileowner> --diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob>" << std::endl
       << "\tNote: apply the postfix \":base64\" to long option names whose values are base64 encoded" << std::endl;
  optional<std::string> user = getOptionStringValue("", "--user", true, false);
  optional<std::string> group = getOptionStringValue("", "--group", true, false);
  optional<uint64_t> id = getOptionUint64Value("", "--id", true, false);
  optional<std::string> storageclass = getOptionStringValue("", "--storageclass", true, false);
  optional<std::string> diskfilepath = getOptionStringValue("", "--diskfilepath", true, false);
  optional<std::string> diskfileowner = getOptionStringValue("", "--diskfileowner", true, false);
  optional<std::string> diskfilegroup = getOptionStringValue("", "--diskfilegroup", true, false);
  optional<std::string> recoveryblob = getOptionStringValue("", "--recoveryblob", true, false);
  checkOptions(help.str());
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
  m_scheduler->updateFileStorageClass(m_cliIdentity.username, request);
  return cmdlineOutput.str();
}



/*!
 * xCom_updatefileinfo
 */
std::string CtaAdminCmd::xCom_updatefileinfo() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " ufi/updatefileinfo --id <CTA_ArchiveFileID> --diskfilepath <disk_filepath>" << std::endl
       << "\t--diskfileowner <disk_fileowner> --diskfilegroup <disk_filegroup> --recoveryblob <recovery_blob>" << std::endl
       << "\tNote: apply the postfix \":base64\" to long option names whose values are base64 encoded" << std::endl;
  optional<uint64_t> id = getOptionUint64Value("", "--id", true, false);
  optional<std::string> diskfilepath = getOptionStringValue("", "--diskfilepath", true, false);
  optional<std::string> diskfileowner = getOptionStringValue("", "--diskfileowner", true, false);
  optional<std::string> diskfilegroup = getOptionStringValue("", "--diskfilegroup", true, false);
  optional<std::string> recoveryblob = getOptionStringValue("", "--recoveryblob", true, false);
  checkOptions(help.str());
  cta::common::dataStructures::DiskFileInfo diskFileInfo;
  diskFileInfo.recoveryBlob=recoveryblob.value();
  diskFileInfo.group=diskfilegroup.value();
  diskFileInfo.owner=diskfileowner.value();
  diskFileInfo.path=diskfilepath.value();
  cta::common::dataStructures::UpdateFileInfoRequest request;
  request.diskFileInfo=diskFileInfo;
  request.archiveFileID=id.value();
  m_scheduler->updateFileInfo(m_cliIdentity.username, request);
  return cmdlineOutput.str();
}



/*!
 * xCom_liststorageclass
 */
std::string CtaAdminCmd::xCom_liststorageclass() {
  std::stringstream cmdlineOutput;
  std::stringstream help;
  help << m_requestTokens.at(0) << " lsc/liststorageclass --user <user> --group <group>" << std::endl
       << "\tNote: apply the postfix \":base64\" to long option names whose values are base64 encoded" << std::endl;
  optional<std::string> user = getOptionStringValue("", "--user", true, false);
  optional<std::string> group = getOptionStringValue("", "--group", true, false);
  checkOptions(help.str());
  cta::common::dataStructures::UserIdentity originator;
  originator.name=user.value();
  originator.group=group.value();
  cta::common::dataStructures::ListStorageClassRequest request;
  request.requester=originator;
  m_scheduler->listStorageClass(m_cliIdentity.username, request);
  return cmdlineOutput.str();
}
#endif
  
