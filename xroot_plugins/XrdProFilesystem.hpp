#pragma once

#include "libs/client/MockClientAPI.hpp"
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
   * Pointer to the client API object
   */
  cta::ClientAPI *m_clientAPI;
    
  /**
   * Parses the query into the request structure
   * 
   * @param args     the query strings
   * @param req      resulting parsed request
   * @param eInfo    Error information
   * @return SFS_OK in case parsing is done correctly, SFS_DATA otherwise
   */
  int parseRequest(const XrdSfsFSctl &args, ParsedRequest &req, XrdOucErrInfo &eInfo) const;
  
  /**
   * Checks whether client has correct permissions and fills the UserIdentity structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The structure to be filled
   * @return SFS_OK in case check is passed, SFS_DATA otherwise
   */
  int checkClient(const XrdSecEntity *client, XrdOucErrInfo &eInfo, cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeArchiveCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeMkclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeChdirclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeCldirclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeGetdirclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeRmclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeLsclassCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeMkdirCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeRmdirCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeLsCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeMkpoolCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeRmpoolCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeLspoolCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeMkrouteCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeRmrouteCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeLsrouteCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeMkadminuserCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeRmadminuserCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeLsadminuserCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeMkadminhostCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeRmadminhostCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Executes the command contained within the request structure
   * 
   * @param req     parsed request
   * @param eInfo   Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_DATA
   */
  int executeLsadminhostCommand(const ParsedRequest &req, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
  
  /**
   * Dispatches the request based on the query
   * 
   * @param args     the archive request string
   * @param eInfo    Error information
   * @param requester The UserIdentity structure of the requester
   * @return SFS_OK in case dispatching is done correctly, SFS_DATA otherwise
   */
  int dispatchRequest(const XrdSfsFSctl &args, XrdOucErrInfo &eInfo, const cta::SecurityIdentity &requester) const;
};
