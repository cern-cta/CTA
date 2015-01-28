#include "XrdProFilesystem.hpp"

#include "XrdOuc/XrdOucString.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdVersion.hh"

#include <iostream>
#include <pwd.h>
#include <sstream>
#include <sys/types.h>

XrdVERSIONINFO(XrdSfsGetFileSystem,XrdPro);

extern "C"
{
  XrdSfsFileSystem *XrdSfsGetFileSystem (XrdSfsFileSystem* native_fs, XrdSysLogger* lp, const char* configfn)
  {
    return new XrdProFilesystem;
  }
}

//------------------------------------------------------------------------------
// isDir
//------------------------------------------------------------------------------
bool XrdProFilesystem::isDir(const char *path) throw() {
  size_t length = strlen(path);
  if('/' == path[length-1]) return true;
  else return false;
}

//------------------------------------------------------------------------------
// checkClient
//------------------------------------------------------------------------------
int XrdProFilesystem::checkClient(const XrdSecEntity *client, XrdOucErrInfo &eInfo) {
  /*
  std::cout << "Calling FSctl with:\ncmd=" << cmd << "\narg=" << args.Arg1 << std::endl;
  std::cout << "Client info:\n"
          << "\nclient->host: " << client->host
          << "\nclient->name: " << client->name
          << "\nclient->prot: " << client->prot
          << "\nclient->sessvar: " << client->sessvar
          << "\nclient->tident: " << client->tident << std::endl;
   */
  if(!client || !client->host || strncmp(client->host, "localhost", 9))
  {
    eInfo.setErrInfo(EACCES, "[ERROR] operation possible only from localhost");
    return SFS_ERROR;
  }
  struct passwd pwd;
  struct passwd *result;
  char *buf;
  size_t bufsize;
  bufsize = sysconf(_SC_GETPW_R_SIZE_MAX);
  if (bufsize == -1)
  {
    bufsize = 16384;
  }
  buf = (char *)malloc(bufsize);
  if(buf == NULL)
  {
    eInfo.setErrInfo(ENOMEM, "[ERROR] malloc of the buffer failed");
    return SFS_ERROR;
  }
  int rc = getpwnam_r(client->name, &pwd, buf, bufsize, &result);
  if(result == NULL)
  {
    if (rc == 0)
    {
      XrdOucString errString = "[ERROR] User ";
      errString += client->name;
      errString += " not found";
      eInfo.setErrInfo(EACCES, errString.c_str());
      return SFS_ERROR;
    }
    else
    {
      eInfo.setErrInfo(EACCES, "[ERROR] getpwnam_r failed");
      return SFS_ERROR;
    }
  }
  //std::cout << "Logging request received from client. Username: " << client->name << " uid: " << pwd.pw_uid << " gid: " << pwd.pw_gid << std::endl;
  free(buf);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// parseArchiveRequest
//------------------------------------------------------------------------------
int XrdProFilesystem::parseArchiveRequest(const XrdSfsFSctl &args, ParsedArchiveCmdLine &cmdLine, XrdOucErrInfo &eInfo) {
  std::stringstream ss(args.Arg1);
  std::string s;
  getline(ss, s, '?');
  while (getline(ss, s, '+')) {
    cmdLine.srcFiles.push_back(s);
  }
  cmdLine.srcFiles.pop_back();
  cmdLine.dstPath = s;
  if(cmdLine.srcFiles.empty() || cmdLine.dstPath.empty()) {
    eInfo.setErrInfo(EINVAL, "[ERROR] Wrong arguments supplied");
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// executeArchiveCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeArchiveCommand(ParsedArchiveCmdLine &cmdLine, XrdOucErrInfo &eInfo) {
  std::cout << "archive request received:\n";
  for(std::list<std::string>::iterator it = cmdLine.srcFiles.begin(); it != cmdLine.srcFiles.end(); it++) {
    std::cout << "SRC: " << *it << std::endl;
  }  
  std::cout << "DST: " << cmdLine.dstPath << std::endl;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// parseCreateStorageClassRequest
//------------------------------------------------------------------------------
int XrdProFilesystem::parseCreateStorageClassRequest(const XrdSfsFSctl &args, ParsedCreateStorageClassCmdLine &cmdLine, XrdOucErrInfo &eInfo) {
  std::stringstream ss(args.Arg1);
  std::string s;
  getline(ss, s, '?');
  getline(ss, s, '+');
  cmdLine.storageClassName = s;
  getline(ss, s, '+');
  cmdLine.numberOfCopies = atoi(s.c_str());
  if(cmdLine.storageClassName.empty() || cmdLine.numberOfCopies==0) {
    eInfo.setErrInfo(EINVAL, "[ERROR] Wrong arguments supplied");
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// executeCreateStorageClassCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeCreateStorageClassCommand(ParsedCreateStorageClassCmdLine &cmdLine, XrdOucErrInfo &eInfo) {
  std::cout << "create-storage-class request received:\n";
  std::cout << "NAME: " << cmdLine.storageClassName << std::endl;
  std::cout << "Number of copies on tape: " << cmdLine.numberOfCopies << std::endl;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// parseChangeStorageClassRequest
//------------------------------------------------------------------------------
int XrdProFilesystem::parseChangeStorageClassRequest(const XrdSfsFSctl &args, ParsedChangeStorageClassCmdLine &cmdLine, XrdOucErrInfo &eInfo) {
  std::stringstream ss(args.Arg1);
  std::string s;
  getline(ss, s, '?');
  getline(ss, s, '+');
  cmdLine.dirName = s;
  getline(ss, s, '+');
  cmdLine.storageClassName = s;
  if(cmdLine.storageClassName.empty() || cmdLine.dirName.empty()) {
    eInfo.setErrInfo(EINVAL, "[ERROR] Wrong arguments supplied");
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// executeCreateStorageClassCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeChangeStorageClassCommand(ParsedChangeStorageClassCmdLine &cmdLine, XrdOucErrInfo &eInfo) {
  std::cout << "change-storage-class request received:\n";
  std::cout << "DIR: " << cmdLine.dirName << std::endl;
  std::cout << "NAME: " << cmdLine.storageClassName << std::endl;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// parseDeleteStorageClassRequest
//------------------------------------------------------------------------------
int XrdProFilesystem::parseDeleteStorageClassRequest(const XrdSfsFSctl &args, ParsedDeleteStorageClassCmdLine &cmdLine, XrdOucErrInfo &eInfo) {
  std::stringstream ss(args.Arg1);
  std::string s;
  getline(ss, s, '?');
  getline(ss, s, '+');
  cmdLine.storageClassName = s;
  if(cmdLine.storageClassName.empty()) {
    eInfo.setErrInfo(EINVAL, "[ERROR] Wrong arguments supplied");
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// executeDeleteStorageClassCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeDeleteStorageClassCommand(ParsedDeleteStorageClassCmdLine &cmdLine, XrdOucErrInfo &eInfo) {
  std::cout << "delete-storage-class request received:\n";
  std::cout << "NAME: " << cmdLine.storageClassName << std::endl;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// parseMkdirRequest
//------------------------------------------------------------------------------
int XrdProFilesystem::parseMkdirRequest(const XrdSfsFSctl &args, ParsedMkdirCmdLine &cmdLine, XrdOucErrInfo &eInfo) {
  std::stringstream ss(args.Arg1);
  std::string s;
  getline(ss, s, '?');
  getline(ss, s, '+');
  cmdLine.dirName = s;
  if(cmdLine.dirName.empty()) {
    eInfo.setErrInfo(EINVAL, "[ERROR] Wrong arguments supplied");
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// executeMkdirCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeMkdirCommand(ParsedMkdirCmdLine &cmdLine, XrdOucErrInfo &eInfo) {
  std::cout << "mkdir request received:\n";
  std::cout << "DIR: " << cmdLine.dirName << std::endl;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// parseRmdirRequest
//------------------------------------------------------------------------------
int XrdProFilesystem::parseRmdirRequest(const XrdSfsFSctl &args, ParsedRmdirCmdLine &cmdLine, XrdOucErrInfo &eInfo) {
  std::stringstream ss(args.Arg1);
  std::string s;
  getline(ss, s, '?');
  getline(ss, s, '+');
  cmdLine.dirName = s;
  if(cmdLine.dirName.empty()) {
    eInfo.setErrInfo(EINVAL, "[ERROR] Wrong arguments supplied");
    return SFS_ERROR;
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// executeRmdirCommand
//------------------------------------------------------------------------------
int XrdProFilesystem::executeRmdirCommand(ParsedRmdirCmdLine &cmdLine, XrdOucErrInfo &eInfo) {
  std::cout << "rmdir request received:\n";
  std::cout << "DIR: " << cmdLine.dirName << std::endl;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// dispatchRequest
//------------------------------------------------------------------------------
int XrdProFilesystem::dispatchRequest(XrdSfsFSctl &args, XrdOucErrInfo &eInfo) {
  if(strncmp(args.Arg1, "/archive?", strlen("/archive?")) == 0)
  {  
    ParsedArchiveCmdLine cmdLine;
    int checkParse = parseArchiveRequest(args, cmdLine, eInfo);
    if(SFS_OK!=checkParse) {
      return checkParse;
    }
    int checkExecute = executeArchiveCommand(cmdLine, eInfo);
    if(SFS_OK!=checkExecute) {
      return checkExecute;
    }
    return SFS_OK;
  }
  else if(strncmp(args.Arg1, "/create-storage-class?", strlen("/create-storage-class?")) == 0)
  {  
    ParsedCreateStorageClassCmdLine cmdLine;
    int checkParse = parseCreateStorageClassRequest(args, cmdLine, eInfo);
    if(SFS_OK!=checkParse) {
      return checkParse;
    }
    int checkExecute = executeCreateStorageClassCommand(cmdLine, eInfo);
    if(SFS_OK!=checkExecute) {
      return checkExecute;
    }
    return SFS_OK;
  }  
  else if(strncmp(args.Arg1, "/change-storage-class?", strlen("/change-storage-class?")) == 0)
  {  
    ParsedChangeStorageClassCmdLine cmdLine;
    int checkParse = parseChangeStorageClassRequest(args, cmdLine, eInfo);
    if(SFS_OK!=checkParse) {
      return checkParse;
    }
    int checkExecute = executeChangeStorageClassCommand(cmdLine, eInfo);
    if(SFS_OK!=checkExecute) {
      return checkExecute;
    }
    return SFS_OK;
  }
  else if(strncmp(args.Arg1, "/delete-storage-class?", strlen("/delete-storage-class?")) == 0)
  {  
    ParsedDeleteStorageClassCmdLine cmdLine;
    int checkParse = parseDeleteStorageClassRequest(args, cmdLine, eInfo);
    if(SFS_OK!=checkParse) {
      return checkParse;
    }
    int checkExecute = executeDeleteStorageClassCommand(cmdLine, eInfo);
    if(SFS_OK!=checkExecute) {
      return checkExecute;
    }
    return SFS_OK;
  }
  else if(strncmp(args.Arg1, "/mkdir?", strlen("/mkdir?")) == 0)
  {  
    ParsedMkdirCmdLine cmdLine;
    int checkParse = parseMkdirRequest(args, cmdLine, eInfo);
    if(SFS_OK!=checkParse) {
      return checkParse;
    }
    int checkExecute = executeMkdirCommand(cmdLine, eInfo);
    if(SFS_OK!=checkExecute) {
      return checkExecute;
    }
    return SFS_OK;
  }  
  else if(strncmp(args.Arg1, "/rmdir?", strlen("/rmdir?")) == 0)
  {  
    ParsedRmdirCmdLine cmdLine;
    int checkParse = parseRmdirRequest(args, cmdLine, eInfo);
    if(SFS_OK!=checkParse) {
      return checkParse;
    }
    int checkExecute = executeRmdirCommand(cmdLine, eInfo);
    if(SFS_OK!=checkExecute) {
      return checkExecute;
    }
    return SFS_OK;
  }
  else
  {
    eInfo.setErrInfo(EINVAL, "[ERROR] Unknown plugin request string received");
    return SFS_ERROR;
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
    int checkDispatch = dispatchRequest(args, eInfo);
    if(SFS_OK!=checkDispatch) {
      return checkDispatch;
    }
    std::string response = "Request logged!";
    eInfo.setErrInfo(response.length(), response.c_str());
    return SFS_DATA;
  }
  else
  {
    eInfo.setErrInfo(EINVAL, "[ERROR] Unknown query request received");
    return SFS_ERROR;
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
XrdProFilesystem::XrdProFilesystem() {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
XrdProFilesystem::~XrdProFilesystem() {}