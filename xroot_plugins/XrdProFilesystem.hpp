#pragma once

#include "XrdSfs/XrdSfsInterface.hh"

#include "ParsedArchiveCmdLine.hpp"
#include "ParsedCreateStorageClassCmdLine.hpp"
#include "ParsedDeleteStorageClassCmdLine.hpp"
#include "ParsedChangeStorageClassCmdLine.hpp"
#include "ParsedMkdirCmdLine.hpp"
#include "ParsedRmdirCmdLine.hpp"

class XrdProFilesystem : public XrdSfsFileSystem {
public:
  virtual XrdSfsDirectory *newDir(char *user=0, int MonID=0);
  virtual XrdSfsFile *newFile(char *user=0, int MonID=0);
  virtual int chksum(csFunc Func, const char *csName, const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0,const char *opaque = 0);
  virtual int chmod(const char *path, XrdSfsMode mode, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual void Disc(const XrdSecEntity *client = 0);
  virtual void EnvInfo(XrdOucEnv *envP);
  virtual int FSctl(const int cmd, XrdSfsFSctl &args, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0);
  virtual int fsctl(const int cmd, const char *args, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0);
  virtual int getStats(char *buff, int blen);
  virtual const char *getVersion();
  virtual int exists(const char *path, XrdSfsFileExistence &eFlag, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual int mkdir(const char *path, XrdSfsMode mode, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual int prepare(XrdSfsPrep &pargs, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0);
  virtual int rem(const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual int remdir(const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual int rename(const char *oPath, const char *nPath, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaqueO = 0, const char *opaqueN = 0);
  virtual int stat(const char *Name, struct stat *buf, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0,const char *opaque = 0);
  virtual int stat(const char *path, mode_t &mode, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual int truncate(const char *path, XrdSfsFileOffset fsize, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  XrdProFilesystem();
  ~XrdProFilesystem();
  
protected:
  
  /**
   * Function that checks whether a path is a directory or not
   * 
   * @param path to be checked
   * @return true if it's a directory false otherwise
   */
  bool isDir(const char *path) throw();
  
  /**
   * Checks whether client has correct permissions
   * 
   * @param client client information
   * @param eInfo  Error information
   * @return SFS_OK in case check is passed, SFS_ERROR otherwise
   */
  int checkClient(const XrdSecEntity *client, XrdOucErrInfo &eInfo);
  
  /**
   * Parses the archive request into the command line structure
   * 
   * @param args     the request string
   * @param cmdLine  the resulting command line structure
   * @param eInfo    Error information
   * @return SFS_OK in case parsing is done correctly, SFS_ERROR otherwise
   */
  int parseArchiveRequest(const XrdSfsFSctl &args, ParsedArchiveCmdLine &cmdLine, XrdOucErrInfo &eInfo);
  
  /**
   * Executes the command contained within the command line structure
   * 
   * @param cmdLine command to execute
   * @param eInfo   Error information
   * @return SFS_OK in case executed correctly, SFS_ERROR otherwise
   */
  int executeArchiveCommand(ParsedArchiveCmdLine &cmdLine, XrdOucErrInfo &eInfo);
  
  /**
   * Parses the create-storage-class request into the command line structure
   * 
   * @param args     the request string
   * @param cmdLine  the resulting command line structure
   * @param eInfo    Error information
   * @return SFS_OK in case parsing is done correctly, SFS_ERROR otherwise
   */
  int parseCreateStorageClassRequest(const XrdSfsFSctl &args, ParsedCreateStorageClassCmdLine &cmdLine, XrdOucErrInfo &eInfo);
  
  /**
   * Executes the command contained within the command line structure
   * 
   * @param cmdLine command to execute
   * @param eInfo   Error information
   * @return SFS_OK in case executed correctly, SFS_ERROR otherwise
   */
  int executeCreateStorageClassCommand(ParsedCreateStorageClassCmdLine &cmdLine, XrdOucErrInfo &eInfo);
  
  /**
   * Parses the change-storage-class request into the command line structure
   * 
   * @param args     the request string
   * @param cmdLine  the resulting command line structure
   * @param eInfo    Error information
   * @return SFS_OK in case parsing is done correctly, SFS_ERROR otherwise
   */
  int parseChangeStorageClassRequest(const XrdSfsFSctl &args, ParsedChangeStorageClassCmdLine &cmdLine, XrdOucErrInfo &eInfo);
  
  /**
   * Executes the command contained within the command line structure
   * 
   * @param cmdLine command to execute
   * @param eInfo   Error information
   * @return SFS_OK in case executed correctly, SFS_ERROR otherwise
   */
  int executeChangeStorageClassCommand(ParsedChangeStorageClassCmdLine &cmdLine, XrdOucErrInfo &eInfo);
  
  /**
   * Parses the delete-storage-class request into the command line structure
   * 
   * @param args     the request string
   * @param cmdLine  the resulting command line structure
   * @param eInfo    Error information
   * @return SFS_OK in case parsing is done correctly, SFS_ERROR otherwise
   */
  int parseDeleteStorageClassRequest(const XrdSfsFSctl &args, ParsedDeleteStorageClassCmdLine &cmdLine, XrdOucErrInfo &eInfo);
  
  /**
   * Executes the command contained within the command line structure
   * 
   * @param cmdLine command to execute
   * @param eInfo   Error information
   * @return SFS_OK in case executed correctly, SFS_ERROR otherwise
   */
  int executeDeleteStorageClassCommand(ParsedDeleteStorageClassCmdLine &cmdLine, XrdOucErrInfo &eInfo);
  
  /**
   * Parses the mkdir request into the command line structure
   * 
   * @param args     the request string
   * @param cmdLine  the resulting command line structure
   * @param eInfo    Error information
   * @return SFS_OK in case parsing is done correctly, SFS_ERROR otherwise
   */
  int parseMkdirRequest(const XrdSfsFSctl &args, ParsedMkdirCmdLine &cmdLine, XrdOucErrInfo &eInfo);
  
  /**
   * Executes the command contained within the command line structure
   * 
   * @param cmdLine command to execute
   * @param eInfo   Error information
   * @return SFS_OK in case executed correctly, SFS_ERROR otherwise
   */
  int executeMkdirCommand(ParsedMkdirCmdLine &cmdLine, XrdOucErrInfo &eInfo);
  
  /**
   * Parses the rmdir request into the command line structure
   * 
   * @param args     the request string
   * @param cmdLine  the resulting command line structure
   * @param eInfo    Error information
   * @return SFS_OK in case parsing is done correctly, SFS_ERROR otherwise
   */
  int parseRmdirRequest(const XrdSfsFSctl &args, ParsedRmdirCmdLine &cmdLine, XrdOucErrInfo &eInfo);
  
  /**
   * Executes the command contained within the command line structure
   * 
   * @param cmdLine command to execute
   * @param eInfo   Error information
   * @return SFS_OK in case executed correctly, SFS_ERROR otherwise
   */
  int executeRmdirCommand(ParsedRmdirCmdLine &cmdLine, XrdOucErrInfo &eInfo);
  
  /**
   * Dispatches the request based on the query
   * 
   * @param args     the archive request string
   * @param eInfo    Error information
   * @return SFS_OK in case dispatching is done correctly, SFS_ERROR otherwise
   */
  int dispatchRequest(XrdSfsFSctl &args, XrdOucErrInfo &eInfo);
};