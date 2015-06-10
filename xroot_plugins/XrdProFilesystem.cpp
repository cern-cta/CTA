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

#include "XrdProFilesystem.hpp"
#include "XrdProFile.hpp"
#include "XrdProDir.hpp"

#include "XrdOuc/XrdOucString.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdVersion.hh"
#include "nameserver/MockNameServer.hpp"
#include "nameserver/NameServer.hpp"
#include "scheduler/AdminHost.hpp"
#include "scheduler/AdminUser.hpp"
#include "scheduler/ArchivalRoute.hpp"
#include "scheduler/ArchiveToTapeCopyRequest.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/MockSchedulerDatabase.hpp"
#include "scheduler/RetrievalJob.hpp"
#include "scheduler/StorageClass.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/Tape.hpp"
#include "scheduler/TapePool.hpp"

#include <memory>
#include <iostream>
#include <pwd.h>
#include <sstream>
#include <sys/types.h>

XrdVERSIONINFO(XrdSfsGetFileSystem,XrdPro)

extern "C"
{
  XrdSfsFileSystem *XrdSfsGetFileSystem (XrdSfsFileSystem* native_fs, XrdSysLogger* lp, const char* configfn)
  {
    return new XrdProFilesystem(new cta::MockNameServer(), new cta::MockSchedulerDatabase());
  }
}

//------------------------------------------------------------------------------
// checkClient
//------------------------------------------------------------------------------
int XrdProFilesystem::checkClient(
  const XrdSecEntity *client,
  XrdOucErrInfo &eInfo,
  cta::SecurityIdentity &requester) const {
  if(!client || !client->host || strncmp(client->host, "localhost", 9))
  {
    std::string response = "[ERROR] operation possible only from localhost";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
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
    std::string response = "[ERROR] malloc of the buffer failed";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    free(buf);
    return SFS_DATA;
  }
  int rc = getpwnam_r(client->name, &pwd, buf, bufsize, &result);
  if(result == NULL)
  {
    if (rc == 0)
    {
      std::string response = "[ERROR] User ";
      response += client->name;
      response += " not found";
      eInfo.setErrInfo(response.length()+1, response.c_str());
      free(buf);
      return SFS_DATA;
    }
    else
    {
      std::string response = "[ERROR] getpwnam_r failed";
      eInfo.setErrInfo(response.length()+1, response.c_str());
      free(buf);
      return SFS_DATA;
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
// parseArchiveRequest
//------------------------------------------------------------------------------
int XrdProFilesystem::parseRequest(const XrdSfsFSctl &args, ParsedRequest &req, XrdOucErrInfo &eInfo) const {
  std::stringstream ss(args.Arg1);
  std::string s;
  getline(ss, s, '?');
  req.cmd = s;
  while (getline(ss, s, '+')) {
    req.args.push_back(s);
  }
  if(req.cmd.empty()) {
    std::string response = "[ERROR] No command supplied";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// executeArchiveCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeArchiveCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() < 2) {
    std::string response = "[ERROR] Too few arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    std::list<std::string> sourceFiles;
    std::string destinationPath = req.args.at(req.args.size()-1);
    for(size_t i=0; i<req.args.size()-1; i++) {
      sourceFiles.push_back(req.args.at(i));
    }
    m_scheduler->queueArchiveRequest(requester, sourceFiles, destinationPath);
    std::ostringstream responseSS;
    responseSS << "[OK] Requested archival of the following files:\n";
    for(std::list<std::string>::iterator it = sourceFiles.begin(); it != sourceFiles.end(); it++) {
      responseSS << "[OK]\t" << *it << "\n";
    }
    responseSS << "[OK] To the following directory:\n";
    responseSS << "[OK]\t" << destinationPath;
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeLsArchiveJobsCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeLsArchiveJobsCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 0 && req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    if(req.args.size() == 1) {      
      std::list<cta::ArchiveToTapeCopyRequest> requests = m_scheduler->getArchiveRequests(requester, req.args.at(0));
      std::ostringstream responseSS;
      responseSS << "[OK] List of archive requests for tape pool " << req.args.at(0) << ":\n";
      for(auto it = requests.begin(); it != requests.end(); it++) {
        responseSS << "[OK]\t" << it->getRequester().getUser().getUid()
                << " " << it->getRequester().getUser().getGid() 
                << " " << it->getCreationTime()
                << " " << it->getRemoteFile() 
                << " " << it->getArchiveFile() << "\n";
      }
      eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
      return SFS_DATA;
    }
    else {
      std::map<cta::TapePool, std::list<cta::ArchiveToTapeCopyRequest> > pools = m_scheduler->getArchiveRequests(requester);
      std::ostringstream responseSS;
      for(auto pool=pools.begin(); pool!=pools.end(); pool++) {
        responseSS << "[OK] List of archive requests for tape pool " << (pool->first).getName() << ":\n";
        for(auto it = (pool->second).begin(); it != (pool->second).end(); it++) {
          responseSS << "[OK]\t" << it->getRequester().getUser().getUid()
                  << " " << it->getRequester().getUser().getGid() 
                  << " " << it->getCreationTime()
                  << " " << it->getRemoteFile() 
                  << " " << it->getArchiveFile() << "\n";
        }
      }
      eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
      return SFS_DATA;
    }
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeRetrieveCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeRetrieveCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() < 2) {
    std::string response = "[ERROR] Too few arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    std::list<std::string> sourceFiles;
    std::string destinationPath = req.args.at(req.args.size()-1);
    for(size_t i=0; i<req.args.size()-1; i++) {
      sourceFiles.push_back(req.args.at(i));
    }
    m_scheduler->queueRetrieveRequest(requester, sourceFiles, destinationPath);
    std::ostringstream responseSS;
    responseSS << "[OK] Requested retrieval of the following files:\n";
    for(auto it = sourceFiles.begin(); it != sourceFiles.end(); it++) {
      responseSS << "[OK]\t" << *it << "\n";
    }
    responseSS << "[OK] To the following directory:\n";
    responseSS << "[OK]\t" << destinationPath;
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeLsRetrieveJobsCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeLsRetrieveJobsCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 0 && req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    if(req.args.size() != 1) {      
      std::list<cta::RetrievalJob> requests = m_scheduler->getRetrievalJobs(requester, req.args.at(0));
      std::ostringstream responseSS;
      responseSS << "[OK] List of retrieve requests for vid " << req.args.at(0) << ":\n";
      for(auto it = requests.begin(); it != requests.end(); it++) {
        responseSS << "[OK]\t" << it->getCreator().getUid()
                << " " << it->getCreator().getGid() 
                << " " << it->getCreationTime()
                << " " << it->getStateStr() 
                << " " << it->getSrcPath() 
                << " " << it->getDstUrl() << "\n";
      }
      eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
      return SFS_DATA;
    }
    else {
      std::map<cta::Tape, std::list<cta::RetrievalJob> > tapes = m_scheduler->getRetrievalJobs(requester);
      std::ostringstream responseSS;
      for(auto tape=tapes.begin(); tape!=tapes.end(); tape++) {
        responseSS << "[OK] List of retrieve requests for vid " << (tape->first).getVid() << ":\n";
        for(std::list<cta::RetrievalJob>::const_iterator it = (tape->second).begin(); it != (tape->second).end(); it++) {
          responseSS << "[OK]\t" << it->getCreator().getUid()
                  << " " << it->getCreator().getGid() 
                  << " " << it->getCreationTime()
                  << " " << it->getStateStr() 
                  << " " << it->getSrcPath() 
                  << " " << it->getDstUrl() << "\n";
        }
      }
      eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
      return SFS_DATA;
    }
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeMkclassCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeMkclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 3) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    uint16_t numberOfCopies;
    std::istringstream ss(req.args.at(1));
    ss >> numberOfCopies;
    m_scheduler->createStorageClass(requester, req.args.at(0), numberOfCopies, req.args.at(2));
    std::ostringstream responseSS;
    responseSS << "[OK] Created storage class " << req.args.at(0) << " with " << req.args.at(1) << " tape copies with the following comment: \"" << req.args.at(2) << "\"" ;
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeChdirclassCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeChdirclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 2) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_scheduler->setDirStorageClass(requester, req.args.at(0), req.args.at(1));
    std::ostringstream responseSS;
    responseSS << "[OK] Changed storage class of directory " << req.args.at(0) << " to " << req.args.at(1);
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeCldirclassCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeCldirclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_scheduler->clearDirStorageClass(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Cleared storage class of directory " << req.args.at(0);
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeGetdirclassCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeGetdirclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    std::string stgClass = m_scheduler->getDirStorageClass(requester, req.args.at(0));
    std::ostringstream responseSS;
    if(stgClass.empty()) {
      responseSS << "[OK] Directory " << req.args.at(0) << " does not have a storage class";      
    }
    else {
      responseSS << "[OK] Directory " << req.args.at(0) << " has the " << stgClass << " storage class";
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeRmclassCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeRmclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_scheduler->deleteStorageClass(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Storage class " << req.args.at(0) << " deleted";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeLsclassCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeLsclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 0) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    std::list<cta::StorageClass> stgList = m_scheduler->getStorageClasses(requester);
    std::ostringstream responseSS;
    responseSS << "[OK] Listing of the storage class names and no of copies:";
    for(std::list<cta::StorageClass>::iterator it = stgList.begin(); it != stgList.end(); it++) {
      responseSS << "\n" << it->getName() << " " << it->getNbCopies() << " " 
              << it->getCreator().getUid() << " " << it->getCreator().getGid() << " " 
              << it->getCreationTime() << " \"" << it->getComment() << "\"";
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeMkdirCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeMkdirCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_scheduler->createDir(requester, req.args.at(0), 0777); //TODO set the modebits correctly
    std::ostringstream responseSS;
    responseSS << "[OK] Directory " << req.args.at(0) << " created";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeRmdirCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeRmdirCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_scheduler->deleteDir(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Directory " << req.args.at(0) << " removed";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeLsCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeLsCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    std::ostringstream responseSS;
    cta::DirIterator itor = m_scheduler->getDirContents(requester, req.args.at(0));
    while(itor.hasMore()) {
      const cta::DirEntry &entry = itor.next();
      
      responseSS << "\n";
      responseSS << ((entry.getType() == cta::DirEntry::ENTRYTYPE_DIRECTORY) ? "d" : "-");
      responseSS << ((entry.getMode() & S_IRUSR) ? "r" : "-");
      responseSS << ((entry.getMode() & S_IWUSR) ? "w" : "-");
      responseSS << ((entry.getMode() & S_IXUSR) ? "x" : "-");
      responseSS << ((entry.getMode() & S_IRGRP) ? "r" : "-");
      responseSS << ((entry.getMode() & S_IWGRP) ? "w" : "-");
      responseSS << ((entry.getMode() & S_IXGRP) ? "x" : "-");
      responseSS << ((entry.getMode() & S_IROTH) ? "r" : "-");
      responseSS << ((entry.getMode() & S_IWOTH) ? "w" : "-");
      responseSS << ((entry.getMode() & S_IXOTH) ? "x" : "-");
      responseSS << " ";
      responseSS << entry.getOwnerId();
      responseSS << " ";
      responseSS << entry.getGroupId();
      responseSS << " ";
      responseSS << entry.getName();
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeMkpoolCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeMkpoolCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 2) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    const uint32_t nbPartialTapes = 1;
    m_scheduler->createTapePool(requester, req.args.at(0), nbPartialTapes, req.args.at(1));
    std::ostringstream responseSS;
    responseSS << "[OK] Tape pool " << req.args.at(0) << " created with comment \"" << req.args.at(1) << "\"";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeRmpoolCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeRmpoolCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_scheduler->deleteTapePool(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Tape pool " << req.args.at(0) << " removed";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeLspoolCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeLspoolCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 0) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    std::list<cta::TapePool> poolList = m_scheduler->getTapePools(requester);
    std::ostringstream responseSS;
    responseSS << "[OK] Listing of the tape pools:";
    for(std::list<cta::TapePool>::iterator it = poolList.begin(); it != poolList.end(); it++) {
      responseSS << "\n" << it->getName() << " " << it->getCreator().getUid() << " " << it->getCreator().getGid() << " " 
              << it->getCreationTime() << " \"" << it->getComment() << "\"";
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeMkrouteCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeMkrouteCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 4) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    uint16_t copyNo;
    std::istringstream ss(req.args.at(1));
    ss >> copyNo;
    m_scheduler->createArchivalRoute(requester, req.args.at(0), copyNo, req.args.at(2), req.args.at(3));
    std::ostringstream responseSS;
    responseSS << "[OK] Archive route from storage class " << req.args.at(0) << " with copy number " << copyNo << " to tape pool " << req.args.at(2) << " created with comment \"" << req.args.at(3) << "\"";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeRmrouteCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeRmrouteCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 2) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    uint16_t copyNo;
    std::istringstream ss(req.args.at(1));
    ss >> copyNo;
    m_scheduler->deleteArchivalRoute(requester, req.args.at(0), copyNo);
    std::ostringstream responseSS;
    responseSS << "[OK] Archive route from storage class " << req.args.at(0) << " with copy number " << copyNo << " removed";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeLsrouteCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeLsrouteCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 0) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    std::list<cta::ArchivalRoute> routeList = m_scheduler->getArchivalRoutes(requester);
    std::ostringstream responseSS;
    responseSS << "[OK] Listing of the archive routes:";
    for(std::list<cta::ArchivalRoute>::iterator it = routeList.begin(); it != routeList.end(); it++) {
      responseSS << "\n" << it->getStorageClassName() << ":" << it->getCopyNb() 
              << " " << it->getTapePoolName()
              << " " << it->getCreator().getUid()
              << " " << it->getCreator().getGid()
              << " \"" << it->getComment() << "\"";
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeMkllibCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeMkllibCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 2) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_scheduler->createLogicalLibrary(requester, req.args.at(0), req.args.at(1));
    std::ostringstream responseSS;
    responseSS << "[OK] Logical library " << req.args.at(0) << " created with comment \"" << req.args.at(1) << "\"";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeRmllibCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeRmllibCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_scheduler->deleteLogicalLibrary(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Logical library " << req.args.at(0) << " removed";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeLsllibCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeLsllibCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 0) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    std::list<cta::LogicalLibrary> llibs = m_scheduler->getLogicalLibraries(requester);
    std::ostringstream responseSS;
    responseSS << "[OK] Listing of the logical libraries:";
    for(std::list<cta::LogicalLibrary>::iterator it = llibs.begin(); it != llibs.end(); it++) {
      responseSS  << "\n" << it->getName()
              << " " << it->getCreator().getUid()
              << " " << it->getCreator().getGid() 
              << " " << it->getCreationTime()
              << " \"" << it->getComment() << "\"";
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeMktapeCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeMktapeCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 5) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    uint64_t capacity;
    std::istringstream ss(req.args.at(3));
    ss >> capacity;
    m_scheduler->createTape(requester, req.args.at(0), req.args.at(1), req.args.at(2), capacity, req.args.at(4));
    std::ostringstream responseSS;
    responseSS << "[OK] Tape " << req.args.at(0) << " of logical library " << req.args.at(1) << " of " << capacity << " bytes of capacity " << " was created in tapepool " << req.args.at(2) << " with comment \"" << req.args.at(4) << "\"";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeRmtapeCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeRmtapeCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_scheduler->deleteTape(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Tape " << req.args.at(0) << " removed";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeLstapeCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeLstapeCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 0) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    std::list<cta::Tape> tapes = m_scheduler->getTapes(requester);
    std::ostringstream responseSS;
    responseSS << "[OK] Listing of tapes:";
    for(std::list<cta::Tape>::iterator it = tapes.begin(); it != tapes.end(); it++) {
      responseSS  << "\n" << it->getVid()
              << " " << it->getCapacityInBytes()
              << " " << it->getDataOnTapeInBytes()
              << " " << it->getLogicalLibraryName()
              << " " << it->getTapePoolName()
              << " " << it->getCreator().getUid()
              << " " << it->getCreator().getGid() 
              << " " << it->getCreationTime()
              << " \"" << it->getComment() << "\"";
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeMkadminuserCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeMkadminuserCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 2) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    cta::UserIdentity adminUser;
    std::istringstream i0(req.args.at(0));
    int uid = 0;
    i0 >> uid;
    adminUser.setUid(uid);
    std::istringstream i1(req.args.at(1));
    int gid = 0;
    i1 >> gid;
    adminUser.setGid(gid);
    const std::string comment = "TO BE DONE";
    m_scheduler->createAdminUser(requester, adminUser, comment);
    std::ostringstream responseSS;
    responseSS << "[OK] Admin user with uid " << req.args.at(0) << " and gid " << req.args.at(1) << " created";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeRmadminuserCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeRmadminuserCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 2) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    cta::UserIdentity adminUser;
    std::stringstream ssArg0(req.args.at(0));
    int uid = 0;
    ssArg0 >> uid;
    adminUser.setUid(uid);
    std::stringstream ssArg1(req.args.at(1));
    int gid = 0;
    ssArg0 >> gid;
    adminUser.setGid(gid);
    m_scheduler->deleteAdminUser(requester, adminUser);
    std::ostringstream responseSS;
    responseSS << "[OK] Admin user with uid " << req.args.at(0) << " and gid " << req.args.at(1) << " deleted";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeLsadminuserCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeLsadminuserCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 0) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    std::list<cta::AdminUser> userIdList = m_scheduler->getAdminUsers(requester);
    std::ostringstream responseSS;
    responseSS << "[OK] Listing of the admin user uids and gids:";
    for(std::list<cta::AdminUser>::iterator it = userIdList.begin(); it != userIdList.end(); it++) {
      responseSS << "\n" << it->getUser().getUid() << " " << it->getUser().getGid();
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeMkadminhostCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeMkadminhostCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    const std::string comment = "TO BE DONE";
    m_scheduler->createAdminHost(requester, req.args.at(0), comment);
    std::ostringstream responseSS;
    responseSS << "[OK] Admin host " << req.args.at(0) << " created";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeRmadminhostCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeRmadminhostCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_scheduler->deleteAdminHost(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Admin host " << req.args.at(0) << " removed";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeLsadminhostCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeLsadminhostCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 0) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    std::list<cta::AdminHost> hostList = m_scheduler->getAdminHosts(requester);
    std::ostringstream responseSS;
    responseSS << "[OK] Listing of the admin hosts:";
    for(std::list<cta::AdminHost>::iterator it = hostList.begin(); it != hostList.end(); it++) {
      responseSS << "\n" << it->getName();
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::exception::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// dispatchRequest
//------------------------------------------------------------------------------
int XrdProFilesystem::dispatchRequest(const XrdSfsFSctl &args, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) {
  ParsedRequest req;
  int checkParse = parseRequest(args, req, eInfo);
  if(SFS_OK!=checkParse) {
    return checkParse;
  }
  if(strcmp(req.cmd.c_str(), "/archive") == 0)
  {  
    return executeArchiveCommand(req, eInfo, requester);
  }
  else if(strcmp(req.cmd.c_str(), "/lsarchiverequests") == 0)
  {  
    return executeLsArchiveJobsCommand(req, eInfo, requester);
  }
  if(strcmp(req.cmd.c_str(), "/retrieve") == 0)
  {  
    return executeRetrieveCommand(req, eInfo, requester);
  }
  else if(strcmp(req.cmd.c_str(), "/lsretrieverequests") == 0)
  {  
    return executeLsRetrieveJobsCommand(req, eInfo, requester);
  }
  else if(strcmp(req.cmd.c_str(), "/mkclass") == 0)
  {  
    return executeMkclassCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/chdirclass") == 0)
  {  
    return executeChdirclassCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/cldirclass") == 0)
  {  
    return executeCldirclassCommand(req, eInfo, requester);
  }    
  else if(strcmp(req.cmd.c_str(), "/getdirclass") == 0)
  {  
    return executeGetdirclassCommand(req, eInfo, requester);
  }
  else if(strcmp(req.cmd.c_str(), "/rmclass") == 0)
  {  
    return executeRmclassCommand(req, eInfo, requester);
  }
  else if(strcmp(req.cmd.c_str(), "/lsclass") == 0)
  {  
    return executeLsclassCommand(req, eInfo, requester);
  }
  else if(strcmp(req.cmd.c_str(), "/mkdir") == 0)
  {  
    return executeMkdirCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/rmdir") == 0)
  {  
    return executeRmdirCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/ls") == 0)
  {  
    return executeLsCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/mkpool") == 0)
  {  
    return executeMkpoolCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/rmpool") == 0)
  {  
    return executeRmpoolCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/lspool") == 0)
  {  
    return executeLspoolCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/mkroute") == 0)
  {  
    return executeMkrouteCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/rmroute") == 0)
  {  
    return executeRmrouteCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/lsroute") == 0)
  {  
    return executeLsrouteCommand(req, eInfo, requester);
  } 
  else if(strcmp(req.cmd.c_str(), "/mkllib") == 0)
  {  
    return executeMkllibCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/rmllib") == 0)
  {  
    return executeRmllibCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/lsllib") == 0)
  {  
    return executeLsllibCommand(req, eInfo, requester);
  }   
  else if(strcmp(req.cmd.c_str(), "/mktape") == 0)
  {  
    return executeMktapeCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/rmtape") == 0)
  {  
    return executeRmtapeCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/lstape") == 0)
  {  
    return executeLstapeCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/mkadminuser") == 0)
  {  
    return executeMkadminuserCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/rmadminuser") == 0)
  {  
    return executeRmadminuserCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/lsadminuser") == 0)
  {  
    return executeLsadminuserCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/mkadminhost") == 0)
  {  
    return executeMkadminhostCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/rmadminhost") == 0)
  {  
    return executeRmadminhostCommand(req, eInfo, requester);
  }  
  else if(strcmp(req.cmd.c_str(), "/lsadminhost") == 0)
  {  
    return executeLsadminhostCommand(req, eInfo, requester);
  }
  else
  {
    std::string response = "[ERROR] Unknown command received";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// FSctl
//------------------------------------------------------------------------------
int XrdProFilesystem::FSctl(const int cmd, XrdSfsFSctl &args, XrdOucErrInfo &eInfo, const XrdSecEntity *client)
{
  cta::SecurityIdentity requester;
  int checkResult = checkClient(client, eInfo, requester);
  if(SFS_OK!=checkResult) {
    return checkResult;
  }
  if(cmd == SFS_FSCTL_PLUGIO)
  {
    return dispatchRequest(args, eInfo, requester);
  }
  else
  {
    std::string response = "[ERROR] Unknown query request received";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// newFile
//------------------------------------------------------------------------------
XrdSfsFile * XrdProFilesystem::newFile(char *user, int MonID)
{  
  return new XrdProFile(user, MonID);
}

//------------------------------------------------------------------------------
// newDir
//------------------------------------------------------------------------------
XrdSfsDirectory * XrdProFilesystem::newDir(char *user, int MonID)
{
  return new XrdProDir(*m_scheduler, user, MonID);
}

//------------------------------------------------------------------------------
// fsctl
//------------------------------------------------------------------------------
int XrdProFilesystem::fsctl(const int cmd, const char *args, XrdOucErrInfo &eInfo, const XrdSecEntity *client)
{
  (void)cmd; (void)args; (void)eInfo; (void)client;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// getStats
//------------------------------------------------------------------------------
int XrdProFilesystem::getStats(char *buff, int blen)
{
  (void)buff; (void)blen;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// getVersion
//------------------------------------------------------------------------------
const char * XrdProFilesystem::getVersion()
{
  return NULL;
}

//------------------------------------------------------------------------------
// exists
//------------------------------------------------------------------------------
int XrdProFilesystem::exists(const char *path, XrdSfsFileExistence &eFlag, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)eFlag; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// mkdir
//------------------------------------------------------------------------------
int XrdProFilesystem::mkdir(const char *path, XrdSfsMode mode, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)mode; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// prepare
//------------------------------------------------------------------------------
int XrdProFilesystem::prepare(XrdSfsPrep &pargs, XrdOucErrInfo &eInfo, const XrdSecEntity *client)
{
  (void)pargs; (void)eInfo; (void)client;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// rem
//------------------------------------------------------------------------------
int XrdProFilesystem::rem(const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// remdir
//------------------------------------------------------------------------------
int XrdProFilesystem::remdir(const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// rename
//------------------------------------------------------------------------------
int XrdProFilesystem::rename(const char *oPath, const char *nPath, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaqueO, const char *opaqueN)
{
  (void)oPath; (void)nPath; (void)eInfo; (void)client; (void)opaqueO; (void)opaqueN;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// stat
//------------------------------------------------------------------------------
int XrdProFilesystem::stat(const char *Name, struct stat *buf, XrdOucErrInfo &eInfo, const XrdSecEntity *client,const char *opaque)
{
  (void)Name; (void)buf; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// stat
//------------------------------------------------------------------------------
int XrdProFilesystem::stat(const char *path, mode_t &mode, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)mode; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// truncate
//------------------------------------------------------------------------------
int XrdProFilesystem::truncate(const char *path, XrdSfsFileOffset fsize, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)fsize; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// chksum
//------------------------------------------------------------------------------
int XrdProFilesystem::chksum(csFunc Func, const char *csName, const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client,const char *opaque)
{
  (void)Func; (void)csName; (void)path; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// chmod
//------------------------------------------------------------------------------
int XrdProFilesystem::chmod(
  const char *path,
  XrdSfsMode mode,
  XrdOucErrInfo &eInfo,
  const XrdSecEntity *client,
  const char *opaque) {
  (void)path; (void)mode; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// Disc
//------------------------------------------------------------------------------
void XrdProFilesystem::Disc(const XrdSecEntity *client)
{
  (void)client;
}

//------------------------------------------------------------------------------
// EnvInfo
//------------------------------------------------------------------------------
void XrdProFilesystem::EnvInfo(XrdOucEnv *envP)
{
  (void)envP;
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
XrdProFilesystem::XrdProFilesystem(cta::NameServer *ns, cta::SchedulerDatabase *scheddb): m_ns(ns), m_scheddb(scheddb), m_scheduler(new cta::Scheduler(*ns, *scheddb)) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
XrdProFilesystem::~XrdProFilesystem() {
  delete m_scheduler;
  delete m_ns;
  delete m_scheddb;
}
