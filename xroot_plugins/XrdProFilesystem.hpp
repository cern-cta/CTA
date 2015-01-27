#pragma once

#include "XrdSfs/XrdSfsInterface.hh"

#include "xroot_utils/ParsedArchiveCmdLine.hpp"

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
   * @param eInfo  Error information
   * @param client client information
   * @return SFS_OK in case check is passed, SFS_ERROR otherwise
   */
  int checkClient(XrdOucErrInfo &eInfo, const XrdSecEntity *client);
  
  /**
   * Parses the archive request into the command line structure
   * 
   * @param args     the archive request string
   * @param cmdLine  the resulting command line structure
   * @return SFS_OK in case parsing is done correctly, SFS_ERROR otherwise
   */
  int parseArchiveRequest(const XrdSfsFSctl &args, ParsedArchiveCmdLine &cmdLine);
  
  /**
   * Executes the command contained within the command line structure
   * 
   * @param cmdLine command to execute
   * @return SFS_OK in case executed correctly, SFS_ERROR otherwise
   */
  int executeArchiveCommand(ParsedArchiveCmdLine &cmdLine);
};