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

#pragma once

#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/FrontendReturnCode.hpp"
#include "common/log/SyslogLogger.hpp"
#include "common/optional.hpp"
#include "scheduler/Scheduler.hpp"

#include "XrdSfs/XrdSfsInterface.hh"

#include <string>
#include <vector>

namespace cta { namespace xrootPlugins {

/**
 * This class represents the heart of the xroot server plugin: it inherits from 
 * XrdSfsFile and it is used by XrdCtaFilesystem whenever a command is executed 
 * (that is when a new file path is requested by the user). All function 
 * documentation can be found in XrdSfs/XrdSfsInterface.hh.
 */
class XrdCtaFile : public XrdSfsFile {
  
public:
  
  XrdOucErrInfo  error;
  virtual int open(const char *fileName, XrdSfsFileOpenMode openMode, mode_t createMode, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual int close();
  virtual int fctl(const int cmd, const char *args, XrdOucErrInfo &eInfo);
  virtual const char *FName();
  virtual int getMmap(void **Addr, off_t &Size);
  virtual XrdSfsXferSize read(XrdSfsFileOffset offset, XrdSfsXferSize size);
  virtual XrdSfsXferSize read(XrdSfsFileOffset offset, char *buffer, XrdSfsXferSize size);
  virtual XrdSfsXferSize read(XrdSfsAio *aioparm);
  virtual XrdSfsXferSize write(XrdSfsFileOffset offset, const char *buffer, XrdSfsXferSize size);
  virtual int write(XrdSfsAio *aioparm);
  virtual int stat(struct ::stat *buf);
  virtual int sync();
  virtual int sync(XrdSfsAio *aiop);
  virtual int truncate(XrdSfsFileOffset fsize);
  virtual int getCXinfo(char cxtype[4], int &cxrsz);
  XrdCtaFile(cta::catalogue::Catalogue *catalogue, cta::Scheduler *scheduler, cta::log::Logger *log, const char *user=0, int MonID=0);
  ~XrdCtaFile();
  
protected:

  /**
   * The catalogue object pointer.
   */
  cta::catalogue::Catalogue *m_catalogue;
  
  /**
   * The scheduler object pointer
   */
  cta::Scheduler *m_scheduler;
  
  /**
   * The scheduler object pointer
   */
  cta::log::Logger &m_log;
  
  /**
   * This is the string holding the result of the command
   */
  std::string m_cmdlineOutput;
  
  /**
   * This is the return code to be passed to the client
   */
  cta::common::dataStructures::FrontendReturnCode m_cmdlineReturnCode;
  
  /**
   * The original client request represented as a vector of strings
   */
  std::vector<std::string> m_requestTokens;
  
  /**
   * The client identity info: username and host
   */
  cta::common::dataStructures::SecurityIdentity m_cliIdentity;  
  
  /**
   * The protocol used by the xroot client
   */
  std::string m_protocol;  
  
  /**
   * Vector containing required options which are missing from the user command
   */
  std::vector<std::string> m_missingRequiredOptions;
  
  /**
   * Vector containing optional options which are missing from the user command
   */
  std::vector<std::string> m_missingOptionalOptions;
  
  /**
   * Vector containing optional options which are present in the user command
   */
  std::vector<std::string> m_optionalOptions;
  
  /**
   * Flag used to suppress missing optional options. Set to false by default (used only in file and tape listings)
   */
  bool m_suppressOptionalOptionsWarning;
  
  /**
   * Decodes a string in base 64
   * 
   * @param msg string to decode
   * @return decoded string
   */
  std::string decode(const std::string msg) const;
  
  /**
   * Checks whether client has correct permissions and fills the corresponding SecurityIdentity structure
   * 
   * @param client  The client security entity
   */
  void checkClient(const XrdSecEntity *client);
  
  /**
   * Replaces all occurrences in a string "str" of a substring "from" with the string "to"
   * 
   * @param str  The original string
   * @param from The substring to replace
   * @param to   The replacement string
   */
  void replaceAll(std::string& str, const std::string& from, const std::string& to) const;
  
  /**
   * Parses the command line and dispatches it to the relevant function
   * 
   * @param requester  The requester identity
   * @return           SFS_OK in case command succeeded, SFS_ERROR otherwise
   */
  void dispatchCommand();
  
  /**
   * Set of functions that, given the command line string vector, return the string/numerical/boolean/time value of the specified option
   * 
   * @param optionShortName      The short name of the required option
   * @param optionLongName       The long name of the required option
   * @param required             True if the option is required, false otherwise
   * @param useDefaultIfMissing  True if the default value (next parameter) is to be used if option is missing from cmdline, false otherwise
   * @param default              Value of the default in case option is missing from cmdline (and useDefaultIfMissing is true)
   * @return the option value (empty if absent)
   */
  optional<std::string> getOptionStringValue(const std::string& optionShortName, const std::string& optionLongName, 
    const bool required, const bool useDefaultIfMissing, const std::string& defaultValue="-");
  optional<uint64_t> getOptionUint64Value(const std::string& optionShortName, const std::string& optionLongName, 
    const bool required, const bool useDefaultIfMissing, const std::string& defaultValue="0");
  optional<bool> getOptionBoolValue(const std::string& optionShortName, const std::string& optionLongName, 
    const bool required, const bool useDefaultIfMissing, const std::string& defaultValue="false");
  optional<time_t> getOptionTimeValue(const std::string& optionShortName, const std::string& optionLongName, 
    const bool required, const bool useDefaultIfMissing, const std::string& defaultValue="01/01/1970");
  
  /**
   * Returns the string/numerical/boolean value of the specified option
   * 
   * @param optionShortName The short name of the required option
   * @param optionLongName  The long name of the required option
   * @return the option value (empty if absent)
   */
  std::string getOption(const std::string& optionShortName, const std::string& optionLongName);
  
  /**
   * Given the command line string vector it returns true if the specified option is present, false otherwise
   * 
   * @param optionShortName The short name of the required option
   * @param optionLongName  The long name of the required option
   * @return true if the specified option is present, false otherwise
   */
  bool hasOption(const std::string& optionShortName, const std::string& optionLongName);
  
  void xCom_admin();
  void xCom_adminhost();
  void xCom_tapepool();
  void xCom_archiveroute();
  void xCom_logicallibrary();
  void xCom_tape();
  void xCom_storageclass();
  void xCom_requestermountrule();
  void xCom_groupmountrule();
  void xCom_mountpolicy();
  void xCom_repack();
  void xCom_shrink();
  void xCom_verify();
  void xCom_archivefile();
  void xCom_test();
  void xCom_drive();
  void xCom_listpendingarchives();
  void xCom_listpendingretrieves();
  void xCom_archive();
  void xCom_retrieve();
  void xCom_deletearchive();
  void xCom_cancelretrieve();
  void xCom_updatefileinfo();
  void xCom_updatefilestorageclass();
  void xCom_liststorageclass();
  
  /**
   * Checks whether the user that issued the admin command is an authorized admin (throws an exception if it's not).
   */
  void authorizeAdmin();
  
  /**
   * Returns the response string properly formatted in a table
   * 
   * @param  responseTable The response 2-D matrix
   * @return the response string properly formatted in a table
   */
  std::string formatResponse(const std::vector<std::vector<std::string>> &responseTable, const bool withHeader);
  
  /**
   * Returns a string representation of the time
   * 
   * @param  time The input time
   * @return a string representation of the time
   */
  std::string timeToString(const time_t &time);
  
  /**
   * Adds the creation log and the last modification log to the current response row
   * 
   * @param  responseRow The current response row to modify
   * @param  creationLog the creation log
   * @param  lastModificationLog the last modification log
   */
  void addLogInfoToResponseRow(std::vector<std::string> &responseRow, const cta::common::dataStructures::EntryLog &creationLog, const cta::common::dataStructures::EntryLog &lastModificationLog);
  
  /**
   * Returns the help string
   * 
   * @param  programName The name of the client program
   * @return the help string
   */
  std::string getGenericHelp(const std::string &programName) const;
  
  /**
   * Converts a parameter string into a uint64_t (throws a cta::exception if it fails)
   * 
   * @param  parameterName The name of the parameter
   * @param  parameterValue The value of the parameter
   * @return the conversion result
   */
  uint64_t stringParameterToUint64(const std::string &parameterName, const std::string &parameterValue) const;
  
  /**
   * Converts a parameter string into a bool (throws a cta::exception if it fails)
   * 
   * @param  parameterName The name of the parameter
   * @param  parameterValue The value of the parameter
   * @return the conversion result
   */
  bool stringParameterToBool(const std::string &parameterName, const std::string &parameterValue) const;
  
  /**
   * Converts a parameter string into a time_t (throws a cta::exception if it fails)
   * 
   * @param  parameterName The name of the parameter
   * @param  parameterValue The value of the parameter
   * @return the conversion result
   */
  time_t stringParameterToTime(const std::string &parameterName, const std::string &parameterValue) const;
  
  /**
   * Sets the return code of the cmdline client and its output. Always returns SFS_OK, which is the only xroot return 
   * code that allows the copy process to happen successfully. Logs the original request and any error in processing it.
   * 
   * @param  rc The return code of the cmdline client
   * @param  returnString The output of the cmdline client
   * @return SFS_OK
   */
  int logRequestAndSetCmdlineResult(const cta::common::dataStructures::FrontendReturnCode rc, const std::string &returnString);
  
  /**
   * Checks if all needed options are present. Throws UserError otherwise.
   * 
   * @param  helpString The help string to be included in the exception message
   */
  void checkOptions(const std::string &helpString);
  
  /**
   * Converts the checksum type string format from EOS to CTA
   * 
   * @param EOSChecksumType Original EOS checksum type
   * @return the checksum type string converted to CTA format
   */
  optional<std::string> EOS2CTAChecksumType(const optional<std::string> &EOSChecksumType);
  
  /**
   * Converts the checksum value string format from EOS to CTA
   * 
   * @param EOSChecksumValue Original EOS checksum value
   * @return the checksum value string converted to CTA format
   */
  optional<std::string> EOS2CTAChecksumValue(const optional<std::string> &EOSChecksumValue);
};

}}
