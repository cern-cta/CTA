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
#include "xroot_plugins/XrdProFile.hpp"

#include <sstream>
#include <string>

//------------------------------------------------------------------------------
// commandDispatcher
//------------------------------------------------------------------------------
void XrdProFile::commandDispatcher(const std::vector<std::string> &tokens) {
  std::string command(tokens[1]);
  
  if     ("op"    == command || "operator"              == command) {xCom_operator(tokens);}
  else if("oh"    == command || "operatorhost"          == command) {xCom_operatorhost(tokens);}
  else if("us"    == command || "user"                  == command) {xCom_user(tokens);}
  else if("tp"    == command || "tapepool"              == command) {xCom_tapepool(tokens);}
  else if("ar"    == command || "archiveroute"          == command) {xCom_archiveroute(tokens);}
  else if("ll"    == command || "logicallibrary"        == command) {xCom_logicallibrary(tokens);}
  else if("ta"    == command || "tape"                  == command) {xCom_tape(tokens);}
  else if("sc"    == command || "storageclass"          == command) {xCom_storageclass(tokens);}
  else if("loa"   == command || "listongoingarchivals"  == command) {xCom_listongoingarchivals(tokens);}
  else if("lor"   == command || "listongoingretrievals" == command) {xCom_listongoingretrievals(tokens);}
  else if("lpa"   == command || "listpendingarchivals"  == command) {xCom_listpendingarchivals(tokens);}
  else if("lpr"   == command || "listpendingretrievals" == command) {xCom_listpendingretrievals(tokens);}
  else if("lds"   == command || "listdrivestates"       == command) {xCom_listdrivestates(tokens);}
  
  else if("lsc"   == command || "liststorageclass"      == command) {xCom_liststorageclass(tokens);}
  else if("ssc"   == command || "setstorageclass"       == command) {xCom_setstorageclass(tokens);}
  else if("csc"   == command || "clearstorageclass"     == command) {xCom_clearstorageclass(tokens);}
  else if("mkdir" == command)                                       {xCom_mkdir(tokens);}
  else if("rmdir" == command)                                       {xCom_rmdir(tokens);}
  else if("ls"    == command)                                       {xCom_ls(tokens);}
  else if("a"     == command || "archive"               == command) {xCom_archive(tokens);}
  else if("r"     == command || "retrieve"              == command) {xCom_retrieve(tokens);}
  else if("da"    == command || "deletearchive"         == command) {xCom_deletearchive(tokens);}
  else if("cr"    == command || "cancelretrieval"       == command) {xCom_cancelretrieval(tokens);}
  
  else {m_data = getGenericHelp(tokens[0]);}
}

//------------------------------------------------------------------------------
// open
//------------------------------------------------------------------------------
int XrdProFile::open(const char *fileName, XrdSfsFileOpenMode openMode, mode_t createMode, const XrdSecEntity *client, const char *opaque) {
  
  std::vector<std::string> tokens;
  std::stringstream ss(fileName);
  std::string item;
  while (std::getline(ss, item, '&')) {
    replaceAll(item, "_#_and_#_", "&");
    tokens.push_back(item);
  }
  
  if(tokens.size() == 1) {
    m_data = getGenericHelp(tokens[0]);
    return SFS_OK;
  }
  try {
    commandDispatcher(tokens);
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
  
  return SFS_OK;
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
XrdProFile::XrdProFile(const char *user, int MonID): error(user, MonID), m_data("") {  
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
// xCom_operator
//------------------------------------------------------------------------------
void XrdProFile::xCom_operator(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_operatorhost
//------------------------------------------------------------------------------
void XrdProFile::xCom_operatorhost(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_user
//------------------------------------------------------------------------------
void XrdProFile::xCom_user(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_tapepool
//------------------------------------------------------------------------------
void XrdProFile::xCom_tapepool(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_archiveroute
//------------------------------------------------------------------------------
void XrdProFile::xCom_archiveroute(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_logicallibrary
//------------------------------------------------------------------------------
void XrdProFile::xCom_logicallibrary(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_tape
//------------------------------------------------------------------------------
void XrdProFile::xCom_tape(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_storageclass
//------------------------------------------------------------------------------
void XrdProFile::xCom_storageclass(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_listongoingarchivals
//------------------------------------------------------------------------------
void XrdProFile::xCom_listongoingarchivals(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_listongoingretrievals
//------------------------------------------------------------------------------
void XrdProFile::xCom_listongoingretrievals(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_listpendingarchivals
//------------------------------------------------------------------------------
void XrdProFile::xCom_listpendingarchivals(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_listpendingretrievals
//------------------------------------------------------------------------------
void XrdProFile::xCom_listpendingretrievals(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_listdrivestates
//------------------------------------------------------------------------------
void XrdProFile::xCom_listdrivestates(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_liststorageclass
//------------------------------------------------------------------------------
void XrdProFile::xCom_liststorageclass(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_setstorageclass
//------------------------------------------------------------------------------
void XrdProFile::xCom_setstorageclass(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_clearstorageclass
//------------------------------------------------------------------------------
void XrdProFile::xCom_clearstorageclass(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_mkdir
//------------------------------------------------------------------------------
void XrdProFile::xCom_mkdir(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_rmdir
//------------------------------------------------------------------------------
void XrdProFile::xCom_rmdir(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_ls
//------------------------------------------------------------------------------
void XrdProFile::xCom_ls(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_archive
//------------------------------------------------------------------------------
void XrdProFile::xCom_archive(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_retrieve
//------------------------------------------------------------------------------
void XrdProFile::xCom_retrieve(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_deletearchive
//------------------------------------------------------------------------------
void XrdProFile::xCom_deletearchive(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}
  
//------------------------------------------------------------------------------
// xCom_cancelretrieval
//------------------------------------------------------------------------------
void XrdProFile::xCom_cancelretrieval(const std::vector<std::string> &tokens) {
  std::string help = "";
  
}

//------------------------------------------------------------------------------
// getGenericHelp
//------------------------------------------------------------------------------
std::string XrdProFile::getGenericHelp(const std::string &programName) const {
  std::stringstream ss;
  ss << "CTA ADMIN commands:\n";
  ss << "\n";
  ss << "For each command there is a short version and a long one, example: op/operator. Subcommands (add/rm/ls/ch/reclaim) do not have short versions.\n";
  ss << "\n";
  ss << programName << " op/operator       add/rm/ls\n";
  ss << programName << " oh/operatorhost   add/rm/ls\n";
  ss << programName << " us/user           add/rm/ls\n";
  ss << programName << " tp/tapepool       add/rm/ls/ch\n";
  ss << programName << " ar/archiveroute   add/rm/ls/ch\n";
  ss << programName << " ll/logicallibrary add/rm/ls/ch\n";
  ss << programName << " ta/tape           add/rm/ls/ch/reclaim\n";
  ss << programName << " sc/storageclass   add/rm/ls/ch\n";
  ss << programName << " loa/listongoingarchivals\n";
  ss << programName << " lor/listongoingretrievals\n";
  ss << programName << " lpa/listpendingarchivals\n";
  ss << programName << " lpr/listpendingretrievals\n";
  ss << programName << " lds/listdrivestates\n";
  ss << "\n";
  ss << "CTA USER commands:\n";
  ss << "\n";
  ss << "For most commands there is a short version and a long one.\n";
  ss << "\n";
  ss << programName << " lsc/liststorageclass\n";
  ss << programName << " ssc/setstorageclass\n";
  ss << programName << " csc/clearstorageclass\n";
  ss << programName << " mkdir\n";
  ss << programName << " rmdir\n";
  ss << programName << " ls\n";
  ss << programName << " a/archive\n";
  ss << programName << " r/retrieve\n";
  ss << programName << " da/deletearchive\n";
  ss << programName << " cr/cancelretrieval\n";
  return ss.str();
}