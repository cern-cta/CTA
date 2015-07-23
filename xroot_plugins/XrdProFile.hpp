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

#include "scheduler/Scheduler.hpp"

#include "XrdSfs/XrdSfsInterface.hh"

#include <string>
#include <vector>

class XrdProFile : public XrdSfsFile {
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
  virtual int stat(struct stat *buf);
  virtual int sync();
  virtual int sync(XrdSfsAio *aiop);
  virtual int truncate(XrdSfsFileOffset fsize);
  virtual int getCXinfo(char cxtype[4], int &cxrsz);
  XrdProFile(cta::Scheduler *scheduler, const char *user=0, int MonID=0);
  ~XrdProFile();
protected:
  
  /**
   * The scheduler object pointer
   */
  cta::Scheduler *m_scheduler;
  
  /**
   * This is the string holding the result of the command
   */
  std::string m_data;
  
  /**
   * Decodes a string in base 64
   * 
   * @param msg string to decode
   * @return decoded string
   */
  std::string decode(const std::string msg) const;
  
  /**
   * Checks whether client has correct permissions and returns the corresponding SecurityIdentity structure
   * 
   * @param req     parsed request
   * @return The requester structure
   */
  cta::SecurityIdentity checkClient(const XrdSecEntity *client);
  
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
   * @param tokens The command line tokens
   */
  void dispatchCommand(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Given the command line string vector it returns the value of the specified option or an empty string if absent
   * 
   * @param tokens          The command line tokens 
   * @param optionShortName The short name of the required option
   * @param optionLongName  The long name of the required option
   * @return the value of the option or an empty string if absent
   */
  std::string getOptionValue(const std::vector<std::string> &tokens, const std::string& optionShortName, const std::string& optionLongName);
  
  /**
   * Executes the admin command
   * 
   * @param tokens The command line tokens
   */
  void xCom_admin(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the adminhost command
   * 
   * @param tokens The command line tokens
   */
  void xCom_adminhost(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the user command
   * 
   * @param tokens The command line tokens
   */
  void xCom_user(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the tapepool command
   * 
   * @param tokens The command line tokens
   */
  void xCom_tapepool(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the archiveroute command
   * 
   * @param tokens The command line tokens
   */
  void xCom_archiveroute(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the logicallibrary command
   * 
   * @param tokens The command line tokens
   */
  void xCom_logicallibrary(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the tape command
   * 
   * @param tokens The command line tokens
   */
  void xCom_tape(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the storageclass command
   * 
   * @param tokens The command line tokens
   */
  void xCom_storageclass(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the listongoingarchives command
   * 
   * @param tokens The command line tokens
   */
  void xCom_listongoingarchives(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the listongoingretrieves command
   * 
   * @param tokens The command line tokens
   */
  void xCom_listongoingretrieves(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the listpendingarchives command
   * 
   * @param tokens The command line tokens
   */
  void xCom_listpendingarchives(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the listpendingretrieves command
   * 
   * @param tokens The command line tokens
   */
  void xCom_listpendingretrieves(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the listdrivestates command
   * 
   * @param tokens The command line tokens
   */
  void xCom_listdrivestates(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the liststorageclass command
   * 
   * @param tokens The command line tokens
   */
  void xCom_liststorageclass(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the setstorageclass command
   * 
   * @param tokens The command line tokens
   */
  void xCom_setstorageclass(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the clearstorageclass command
   * 
   * @param tokens The command line tokens
   */
  void xCom_clearstorageclass(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the mkdir command
   * 
   * @param tokens The command line tokens
   */
  void xCom_mkdir(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the chown command
   * 
   * @param tokens The command line tokens
   */
  void xCom_chown(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the rmdir command
   * 
   * @param tokens The command line tokens
   */
  void xCom_rmdir(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the ls command
   * 
   * @param tokens The command line tokens
   */
  void xCom_ls(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the archive command
   * 
   * @param tokens The command line tokens
   */
  void xCom_archive(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the retrieve command
   * 
   * @param tokens The command line tokens
   */
  void xCom_retrieve(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the deletearchive command
   * 
   * @param tokens The command line tokens
   */
  void xCom_deletearchive(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Executes the cancelretrieve command
   * 
   * @param tokens The command line tokens
   */
  void xCom_cancelretrieve(const std::vector<std::string> &tokens, const cta::SecurityIdentity &requester);
  
  /**
   * Returns the help string
   * 
   * @param  programName The name of the client program
   * @return the help string
   */
  std::string getGenericHelp(const std::string &programName) const;
};
