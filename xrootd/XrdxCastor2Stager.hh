//          $Id: XrdxCastor2Stager.hh,v 1.3 2009/07/06 08:27:11 apeters Exp $

#ifndef __XCASTOR2_STAGER_H__
#define __XCASTOR2_STAGER_H__

#include <XrdOuc/XrdOucHash.hh>
#include <XrdOuc/XrdOucErrInfo.hh>
#include <XrdOuc/XrdOucString.hh>

class XrdxCastor2Stager {
public:
  static XrdOucHash<XrdOucString> *prepare2getstore;
  static XrdOucHash<XrdOucString> *getstore;
  static XrdOucHash<XrdOucString> *putstore;
  static XrdOucHash<XrdOucString> *delaystore;

  static int GetDelayValue(const char* tag);
  
  XrdxCastor2Stager() {};


  ~XrdxCastor2Stager() {};

  static bool Prepare2Get(XrdOucErrInfo &error, uid_t uid, gid_t gid,const char* path, const char* stagehost, const char* serviceclass, XrdOucString &redirectionhost, XrdOucString &redirectionpfn, XrdOucString &stagestatus) ;
  static bool Get(XrdOucErrInfo &error, uid_t uid, gid_t gid,const char* path, const char* stagehost, const char* serviceclass, XrdOucString &redirectionhost, XrdOucString &redirectionpfn, XrdOucString &redirectionpfn2, XrdOucString &stagestatus); 
  static bool Put(XrdOucErrInfo & error, uid_t uid, gid_t gid,const char* path, const char* stagehost, const char* serviceclass, XrdOucString &redirectionhost, XrdOucString &redirectionpfn, XrdOucString &redirectionpfn2, XrdOucString &stagestatus); 

  static bool Rm(XrdOucErrInfo & error, uid_t uid, gid_t gid, const char* path, const char* stagehost, const char* serviceclass );

  static bool Update(XrdOucErrInfo & error, uid_t uid, gid_t gid,const char* path, const char* stagehost, const char* serviceclass, XrdOucString &redirectionhost, XrdOucString &redirectionpfn, XrdOucString &redirectionpfn2, XrdOucString &stagestatus); 

  static bool StagerQuery(XrdOucErrInfo &error, uid_t uid, gid_t gid, const char* path, const char* stagehost, const char* serviceclass, XrdOucString &stagestatus);
  
  //  static bool PrepareForMigration(XrdOucErrInfo &error, const char* path, const char* reqid, const char* fileid, const char* nameserver, const char* stagehost, const char* serviceclass, const char* filesize, const char* mtime);
  
  static bool UpdateDone(XrdOucErrInfo &error, const char* path, const char* reqid, const char* fileid, const char* nameserver, const char* stagehost, const char* serviceclass);

  static bool FirstWrite(XrdOucErrInfo &error, const char* path, const char* reqid, const char* fileid, const char* nameserver, const char* stagehost, const char* serviceclass );
  
  static bool PutFailed(XrdOucErrInfo &error, const char* path, const char* reqid, const char* fileid, const char* nameserver, const char* stagehost, const char* serviceclass );
};

#endif
