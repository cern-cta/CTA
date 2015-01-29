#pragma once

#include "XrdSfs/XrdSfsInterface.hh"

#include "ParsedRequest.hpp"

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
   * Parses the query into the request structure
   * 
   * @param args     the query strings
   * @param req      resulting parsed request
   * @param eInfo    Error information
   * @return SFS_OK in case parsing is done correctly, SFS_DATA otherwise
   */
  int parseRequest(const XrdSfsFSctl &args, ParsedRequest &req, XrdOucErrInfo &eInfo);
  
  /**
   * Checks whether client has correct permissions
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @return SFS_OK in case check is passed, SFS_DATA otherwise
   */
  int checkClient(const XrdSecEntity *client, XrdOucErrInfo &eInfo);
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @return SFS_DATA
   */
  int executeArchiveCommand(ParsedRequest &req, XrdOucErrInfo &eInfo);
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @return SFS_DATA
   */
  int executeCreateStorageClassCommand(ParsedRequest &req, XrdOucErrInfo &eInfo);
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @return SFS_DATA
   */
  int executeChangeStorageClassCommand(ParsedRequest &req, XrdOucErrInfo &eInfo);
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @return SFS_DATA
   */
  int executeDeleteStorageClassCommand(ParsedRequest &req, XrdOucErrInfo &eInfo);
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @return SFS_DATA
   */
  int executeListStorageClassCommand(ParsedRequest &req, XrdOucErrInfo &eInfo);
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @return SFS_DATA
   */
  int executeMkdirCommand(ParsedRequest &req, XrdOucErrInfo &eInfo);
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @return SFS_DATA
   */
  int executeRmdirCommand(ParsedRequest &req, XrdOucErrInfo &eInfo);
  
  /**
   * Dispatches the request based on the query
   * 
   * @param args     the archive request string
   * @param eInfo    Error information
   * @return SFS_OK in case dispatching is done correctly, SFS_DATA otherwise
   */
  int dispatchRequest(XrdSfsFSctl &args, XrdOucErrInfo &eInfo);
};