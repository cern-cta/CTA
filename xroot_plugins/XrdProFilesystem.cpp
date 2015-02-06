#include "XrdProFilesystem.hpp"

#include "XrdOuc/XrdOucString.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdVersion.hh"
#include "libs/client/Exception.hpp"

#include <iostream>
#include <pwd.h>
#include <sstream>
#include <sys/types.h>

XrdVERSIONINFO(XrdSfsGetFileSystem,XrdPro)

extern "C"
{
  XrdSfsFileSystem *XrdSfsGetFileSystem (XrdSfsFileSystem* native_fs, XrdSysLogger* lp, const char* configfn)
  {
    return new XrdProFilesystem;
  }
}

//------------------------------------------------------------------------------
// checkClient
//------------------------------------------------------------------------------
int XrdProFilesystem::checkClient(const XrdSecEntity *client, XrdOucErrInfo &eInfo, cta::SecurityIdentity &requester) const {
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
  std::cout << "Request received from client. Username: " << client->name << " uid: " << pwd.pw_uid << " gid: " << pwd.pw_gid << std::endl;
  requester.host = client->host;
  requester.user.uid = pwd.pw_uid;
  requester.user.gid = pwd.pw_gid;
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
int XrdProFilesystem::executeArchiveCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
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
    std::string jobID = m_clientAPI->archiveToTape(requester, sourceFiles, destinationPath);
    std::ostringstream responseSS;
    responseSS << "[OK] Requested archival of the following files:\n";
    for(std::list<std::string>::iterator it = sourceFiles.begin(); it != sourceFiles.end(); it++) {
      responseSS << "[OK]\t" << *it << "\n";
    }
    responseSS << "[OK] To the following directory:\n";
    responseSS << "[OK]\t" << destinationPath << "\n";
    responseSS << "[OK] JobID: " << jobID;
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeMkclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 3) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    uint8_t numberOfCopies;
    std::istringstream ss(req.args.at(1));
    ss >> numberOfCopies;
    m_clientAPI->createStorageClass(requester, req.args.at(0), numberOfCopies, req.args.at(2));
    std::ostringstream responseSS;
    responseSS << "[OK] Created storage class " << req.args.at(0) << " with " << req.args.at(1) << " tape copies with the following comment: \"" << req.args.at(2) << "\"" ;
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeChdirclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 2) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_clientAPI->setDirectoryStorageClass(requester, req.args.at(0), req.args.at(1));
    std::ostringstream responseSS;
    responseSS << "[OK] Changed storage class of directory " << req.args.at(0) << " to " << req.args.at(1);
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeCldirclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_clientAPI->clearDirectoryStorageClass(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Cleared storage class of directory " << req.args.at(0);
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
    std::string stgClass = m_clientAPI->getDirectoryStorageClass(requester, req.args.at(0));
    std::ostringstream responseSS;
    if(stgClass.empty()) {
      responseSS << "[OK] Directory " << req.args.at(0) << " does not have a storage class";      
    }
    else {
      responseSS << "[OK] Directory " << req.args.at(0) << " has the " << stgClass << " storage class";
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeRmclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_clientAPI->deleteStorageClass(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Storage class " << req.args.at(0) << " deleted";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
    std::list<cta::StorageClass> stgList = m_clientAPI->getStorageClasses(requester);
    std::ostringstream responseSS;
    responseSS << "[OK] Listing of the storage class names and no of copies:";
    for(std::list<cta::StorageClass>::iterator it = stgList.begin(); it != stgList.end(); it++) {
      responseSS << "\n" << it->getName() << " " << it->getNbCopies() << " " 
              << it->getCreator().uid << " " << it->getCreator().gid << " " 
              << it->getCreationTime() << " \"" << it->getComment() << "\"";
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeMkdirCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_clientAPI->createDirectory(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Directory " << req.args.at(0) << " created";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeRmdirCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_clientAPI->deleteDirectory(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Directory " << req.args.at(0) << " removed";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
    cta::DirectoryIterator itor = m_clientAPI->getDirectoryContents(requester, req.args.at(0));
    while(itor.hasMore()) {
      const cta::DirectoryEntry &entry = itor.next();
      
      responseSS << "\n";
      responseSS << ((entry.getEntryType() == cta::DirectoryEntry::ENTRYTYPE_DIRECTORY) ? "d" : "-");
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
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeMkpoolCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 2) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_clientAPI->createTapePool(requester, req.args.at(0), req.args.at(1));
    std::ostringstream responseSS;
    responseSS << "[OK] Tape pool " << req.args.at(0) << " created with comment \"" << req.args.at(1) << "\"";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeRmpoolCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_clientAPI->deleteTapePool(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Tape pool " << req.args.at(0) << " removed";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
    std::list<cta::TapePool> poolList = m_clientAPI->getTapePools(requester);
    std::ostringstream responseSS;
    responseSS << "[OK] Listing of the tape pools:";
    for(std::list<cta::TapePool>::iterator it = poolList.begin(); it != poolList.end(); it++) {
      responseSS << "\n" << it->getName() << " " << it->getCreator().uid << " " << it->getCreator().gid << " " 
              << it->getCreationTime() << " \"" << it->getComment() << "\"";
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeMkrouteCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 4) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    uint8_t copyNo;
    std::istringstream ss(req.args.at(1));
    ss >> copyNo;
    m_clientAPI->createMigrationRoute(requester, req.args.at(0), copyNo, req.args.at(2), req.args.at(3));
    std::ostringstream responseSS;
    responseSS << "[OK] Migration route from storage class " << req.args.at(0) << " with copy number " << copyNo << " to tape pool " << req.args.at(2) << " created with comment \"" << req.args.at(3) << "\"";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeRmrouteCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 2) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    uint8_t copyNo;
    std::istringstream ss(req.args.at(1));
    ss >> copyNo;
    m_clientAPI->deleteMigrationRoute(requester, req.args.at(0), copyNo);
    std::ostringstream responseSS;
    responseSS << "[OK] Migration route from storage class " << req.args.at(0) << " with copy number " << copyNo << " removed";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
    std::list<cta::MigrationRoute> routeList = m_clientAPI->getMigrationRoutes(requester);
    std::ostringstream responseSS;
    responseSS << "[OK] Listing of the migration routes:";
    for(std::list<cta::MigrationRoute>::iterator it = routeList.begin(); it != routeList.end(); it++) {
      responseSS << "\n" << it->getStorageClassName() << ":" << it->getCopyNb() 
              << " " << it->getTapePoolName()
              << " " << it->getCreator().uid 
              << " " << it->getCreator().gid 
              << " \"" << it->getComment() << "\"";
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeMkadminuserCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 2) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    cta::UserIdentity adminUser;
    std::istringstream i0(req.args.at(0)); i0 >> adminUser.uid;
    std::istringstream i1(req.args.at(1)); i1 >> adminUser.gid;
    m_clientAPI->createAdminUser(requester, adminUser);
    std::ostringstream responseSS;
    responseSS << "[OK] Admin user with uid " << req.args.at(0) << " and gid " << req.args.at(1) << " created";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeRmadminuserCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 2) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    cta::UserIdentity adminUser;
    std::stringstream ssArg0(req.args.at(0));
    ssArg0 >> adminUser.uid;
    std::stringstream ssArg1(req.args.at(1));
    ssArg0 >> adminUser.gid;
    m_clientAPI->deleteAdminUser(requester, adminUser);
    std::ostringstream responseSS;
    responseSS << "[OK] Admin user with uid " << req.args.at(0) << " and gid " << req.args.at(1) << " deleted";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
    std::list<cta::UserIdentity> userIdList = m_clientAPI->getAdminUsers(requester);
    std::ostringstream responseSS;
    responseSS << "[OK] Listing of the admin user uids and gids:";
    for(std::list<cta::UserIdentity>::iterator it = userIdList.begin(); it != userIdList.end(); it++) {
      responseSS << "\n" << it->uid << " " << it->gid;
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeMkadminhostCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_clientAPI->createAdminHost(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Admin host " << req.args.at(0) << " created";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::executeRmadminhostCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length()+1, response.c_str());
    return SFS_DATA;
  }
  try {
    m_clientAPI->deleteAdminHost(requester, req.args.at(0));
    std::ostringstream responseSS;
    responseSS << "[OK] Admin host " << req.args.at(0) << " removed";
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
    std::list<std::string> hostList = m_clientAPI->getAdminHosts(requester);
    std::ostringstream responseSS;
    responseSS << "[OK] Listing of the admin hosts:";
    for(std::list<std::string>::iterator it = hostList.begin(); it != hostList.end(); it++) {
      responseSS << "\n" << *it;
    }
    eInfo.setErrInfo(responseSS.str().length()+1, responseSS.str().c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
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
int XrdProFilesystem::dispatchRequest(const XrdSfsFSctl &args, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const {
  ParsedRequest req;
  int checkParse = parseRequest(args, req, eInfo);
  if(SFS_OK!=checkParse) {
    return checkParse;
  }
  if(strcmp(req.cmd.c_str(), "/archive") == 0)
  {  
    return executeArchiveCommand(req, eInfo, requester);
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
// newDir
//------------------------------------------------------------------------------
XrdSfsDirectory * XrdProFilesystem::newDir(char *user, int MonID)
{
  (void)user; (void)MonID;
  return NULL;
}

//------------------------------------------------------------------------------
// newFile
//------------------------------------------------------------------------------
XrdSfsFile * XrdProFilesystem::newFile(char *user, int MonID)
{
  (void)user; (void)MonID;
  return NULL;
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
int XrdProFilesystem::chmod(const char *path, XrdSfsMode mode, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
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
XrdProFilesystem::XrdProFilesystem() {
  m_clientAPI = new cta::MockClientAPI();
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
XrdProFilesystem::~XrdProFilesystem() {
  delete m_clientAPI;
}
