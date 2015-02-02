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
int XrdProFilesystem::checkClient(const XrdSecEntity *client, XrdOucErrInfo &eInfo) {
  if(!client || !client->host || strncmp(client->host, "localhost", 9))
  {
    std::string response = "[ERROR] operation possible only from localhost";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
  struct passwd pwd;
  struct passwd *result;
  char *buf;
  long bufsize;
  bufsize = sysconf(_SC_GETPW_R_SIZE_MAX);
  if (bufsize == -1)
  {
    bufsize = 16384;
  }
  buf = (char *)malloc((size_t)bufsize);
  if(buf == NULL)
  {
    std::string response = "[ERROR] malloc of the buffer failed";
    eInfo.setErrInfo(response.length(), response.c_str());
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
      eInfo.setErrInfo(response.length(), response.c_str());
      free(buf);
      return SFS_DATA;
    }
    else
    {
      std::string response = "[ERROR] getpwnam_r failed";
      eInfo.setErrInfo(response.length(), response.c_str());
      free(buf);
      return SFS_DATA;
    }
  }
  std::cout << "Request received from client. Username: " << client->name << " uid: " << pwd.pw_uid << " gid: " << pwd.pw_gid << std::endl;
  free(buf);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// parseArchiveRequest
//------------------------------------------------------------------------------
int XrdProFilesystem::parseRequest(const XrdSfsFSctl &args, ParsedRequest &req, XrdOucErrInfo &eInfo) {
  std::stringstream ss(args.Arg1);
  std::string s;
  getline(ss, s, '?');
  req.cmd = s;
  while (getline(ss, s, '+')) {
    req.args.push_back(s);
  }
  if(req.cmd.empty()) {
    std::string response = "[ERROR] No command supplied";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// executeArchiveCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeArchiveCommand(ParsedRequest &req, XrdOucErrInfo &eInfo) {
  if(req.args.size() < 2) {
    std::string response = "[ERROR] Too few arguments provided";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
  try {
    std::list<std::string> sourceFiles;
    std::string destinationPath = req.args.at(req.args.size()-1);
    for(size_t i=0; i<req.args.size()-1; i++) {
      sourceFiles.push_back(req.args.at(i));
    }
    cta::UserIdentity requester;
    std::string jobID = m_clientAPI->archiveToTape(requester, sourceFiles, destinationPath);
    std::string response = "[OK] Requested archival of the following files:\n";
    for(std::list<std::string>::iterator it = sourceFiles.begin(); it != sourceFiles.end(); it++) {
      response += "[OK]\t";
      response += *it;
      response += "\n";
    }
    response += "[OK] To the following directory:\n";
    response += "[OK]\t";
    response += destinationPath;   
    response += "\n[OK] JobID: ";
    response += jobID;
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeCreateStorageClassCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeMkclassCommand(ParsedRequest &req, XrdOucErrInfo &eInfo) {
  if(req.args.size() != 2) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
  try {
    uint8_t numberOfCopies;
    std::istringstream ss(req.args.at(1));
    ss >> numberOfCopies;
    cta::UserIdentity requester;
    m_clientAPI->createStorageClass(requester, req.args.at(0), numberOfCopies);
    std::string response = "[OK] Created storage class ";
    response += req.args.at(0);
    response += " with ";
    response += req.args.at(1);
    response += " tape copies";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeCreateStorageClassCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeChclassCommand(ParsedRequest &req, XrdOucErrInfo &eInfo) {
  if(req.args.size() != 2) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
  try {
    std::string response = "[OK] Changed storage class of directory ";
    response += req.args.at(0);
    response += " to ";
    response += req.args.at(1);
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeDeleteStorageClassCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeRmclassCommand(ParsedRequest &req, XrdOucErrInfo &eInfo) {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
  try {
    cta::UserIdentity requester;
    m_clientAPI->deleteStorageClass(requester, req.args.at(0));
    std::string response = "[OK] Storage class ";
    response += req.args.at(0);
    response += " deleted";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeListStorageClassCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeLsclassCommand(ParsedRequest &req, XrdOucErrInfo &eInfo) {
  if(req.args.size() != 0) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
  try {
    cta::UserIdentity requester;
    std::list<cta::StorageClass> stgList = m_clientAPI->getStorageClasses(requester);
    std::string response = "[OK] Listing of the storage class names and no of copies:";
    for(std::list<cta::StorageClass>::iterator it = stgList.begin(); it != stgList.end(); it++) {
      response += "\n";
      response += it->name;
      response += " ";
      response += it->nbCopies;
    }
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeMkdirCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeMkdirCommand(ParsedRequest &req, XrdOucErrInfo &eInfo) {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
  try {
    cta::UserIdentity requester;
    m_clientAPI->createDirectory(requester, req.args.at(0));
    std::string response = "[OK] Directory ";
    response += req.args.at(0);
    response += " created";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeRmdirCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeRmdirCommand(ParsedRequest &req, XrdOucErrInfo &eInfo) {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
  try {
    std::string response = "[OK] Directory ";
    response += req.args.at(0);
    response += " removed";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// executeLsCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeLsCommand(ParsedRequest &req, XrdOucErrInfo &eInfo) {
  if(req.args.size() != 1) {
    std::string response = "[ERROR] Wrong number of arguments provided";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
  try {
    cta::UserIdentity requester;
    std::string response;
    cta::DirectoryIterator itor = m_clientAPI->getDirectoryContents(requester, req.args.at(0));
    while(itor.hasMore()) {
      const cta::DirectoryEntry &entry = itor.next();
      response += "\n";
      response += (S_ISDIR(entry.mode)) ? "d" : "-";
      response += (entry.mode & S_IRUSR) ? "r" : "-";
      response += (entry.mode & S_IWUSR) ? "w" : "-";
      response += (entry.mode & S_IXUSR) ? "x" : "-";
      response += (entry.mode & S_IRGRP) ? "r" : "-";
      response += (entry.mode & S_IWGRP) ? "w" : "-";
      response += (entry.mode & S_IXGRP) ? "x" : "-";
      response += (entry.mode & S_IROTH) ? "r" : "-";
      response += (entry.mode & S_IWOTH) ? "w" : "-";
      response += (entry.mode & S_IXOTH) ? "x" : "-";
      response += " ";
      response += entry.ownerId;
      response += " ";
      response += entry.groupId;
      response += " ";
      response += entry.name;
    }
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (cta::Exception &ex) {
    std::string response = "[ERROR] CTA exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (std::exception &ex) {
    std::string response = "[ERROR] Exception caught: ";
    response += ex.what();
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  } catch (...) {
    std::string response = "[ERROR] Unknown exception caught!";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// dispatchRequest
//------------------------------------------------------------------------------
int XrdProFilesystem::dispatchRequest(XrdSfsFSctl &args, XrdOucErrInfo &eInfo) {
  ParsedRequest req;
  int checkParse = parseRequest(args, req, eInfo);
  if(SFS_OK!=checkParse) {
    return checkParse;
  }
  if(strcmp(req.cmd.c_str(), "/archive") == 0)
  {  
    return executeArchiveCommand(req, eInfo);
  }
  else if(strcmp(req.cmd.c_str(), "/mkclass") == 0)
  {  
    return executeMkclassCommand(req, eInfo);
  }  
  else if(strcmp(req.cmd.c_str(), "/chclass") == 0)
  {  
    return executeChclassCommand(req, eInfo);
  }
  else if(strcmp(req.cmd.c_str(), "/rmclass") == 0)
  {  
    return executeRmclassCommand(req, eInfo);
  }
  else if(strcmp(req.cmd.c_str(), "/lsclass") == 0)
  {  
    return executeLsclassCommand(req, eInfo);
  }
  else if(strcmp(req.cmd.c_str(), "/mkdir") == 0)
  {  
    return executeMkdirCommand(req, eInfo);
  }  
  else if(strcmp(req.cmd.c_str(), "/rmdir") == 0)
  {  
    return executeRmdirCommand(req, eInfo);
  }  
  else if(strcmp(req.cmd.c_str(), "/ls") == 0)
  {  
    return executeLsCommand(req, eInfo);
  }
  else
  {
    std::string response = "[ERROR] Unknown command received";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
}

//------------------------------------------------------------------------------
// FSctl
//------------------------------------------------------------------------------
int XrdProFilesystem::FSctl(const int cmd, XrdSfsFSctl &args, XrdOucErrInfo &eInfo, const XrdSecEntity *client)
{
  int checkResult = checkClient(client, eInfo);
  if(SFS_OK!=checkResult) {
    return checkResult;
  }
  if(cmd == SFS_FSCTL_PLUGIO)
  {
    return dispatchRequest(args, eInfo);
  }
  else
  {
    std::string response = "[ERROR] Unknown query request received";
    eInfo.setErrInfo(response.length(), response.c_str());
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
