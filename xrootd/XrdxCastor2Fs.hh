//         $Id: XrdxCastor2Fs.hh,v 1.6 2009/07/06 08:27:11 apeters Exp $

#ifndef __XCASTOR2_FS_H__
#define __XCASTOR2_FS_H__

#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <memory.h>
#include <string.h>
#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <attr/xattr.h>
#include <utime.h>
#include <pwd.h>

#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTable.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOfs/XrdOfsEvr.hh"
#include "XrdCms/XrdCmsFinder.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdxCastor2FsConstants.hh"
#include "XrdxCastor2ServerAcc.hh"
#include "XrdxCastor2Proc.hh"
#include "XrdxCastor2ClientAdmin.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysTimer.hh"

#define RFIO_NOREDEFINE
#include "serrno.h"
#include "Cns_api.h"


extern "C" XrdAccAuthorize *XrdAccAuthorizeObject(XrdSysLogger *lp,
                                                  const char   *cfn,
                                                  const char   *parm);

class XrdSysError;
class XrdSysLogger;

/******************************************************************************/
/*            U n i x   F i l e   S y s t e m   I n t e r f a c e             */
/******************************************************************************/

class XrdxCastor2FsUFS
{
public:

static int Chmod(const char *fn, mode_t mode) {return Cns_chmod(fn, mode);}

static int Chown(const char *fn, uid_t owner, gid_t group) {return Cns_chown(fn, owner,group);}

static int Lchown(const char *fn, uid_t owner, gid_t group) {return Cns_lchown(fn, owner,group);}

static int Close(int fd) {return close(fd);}

static int Mkdir(const char *fn, mode_t mode) {return Cns_mkdir(fn, mode);}

static int Open(const char *path, int oflag, mode_t omode)
               {return open(path, oflag, omode);}

static int Creat(const char *path, mode_t omode)
               {return Cns_creat(path, omode);}

static int Rem(const char *fn) {return Cns_unlink(fn);}

static int Remdir(const char *fn) {return Cns_rmdir(fn);}

static int Rename(const char *ofn, const char *nfn) {return Cns_rename(ofn, nfn);}

static int SetFileSize(const char *fn, unsigned long long size, time_t mtime) { return Cns_setfsize(fn, NULL,size,mtime,0);}

static int Statfd(int fd, struct stat *buf) {return fstat(fd, buf);}

static int Statfn(const char *fn, struct Cns_filestatcs *buf) {return Cns_statcs(fn, buf);}

static int Lstatfn(const char *fn, struct Cns_filestat *buf) {return Cns_lstat(fn, buf);}

static int Readlink(const char *fn, char *buf, size_t bufsize) {return Cns_readlink(fn, buf,bufsize);}

static int SetId(uid_t uid, gid_t gid) { return Cns_setid(uid,gid); }
   
static int Symlink(const char *oldn, const char *newn) {return Cns_symlink(oldn, newn);}

static int Unlink(const char* fn) {return Cns_unlink(fn);}

static int Access(const char* fn, int mode) {return Cns_access(fn,mode);}

  static int Utimes(const char* /*fn*/, struct timeval* /*tvp*/) {return 0;/* return Cns_utimes(fn,tvp);*/}

static DIR* OpenDir(const char* dn) { return (DIR*)Cns_opendir(dn);}
static struct dirent*  ReadDir(DIR* dp) { return Cns_readdir((Cns_DIR*) dp); }
static int  CloseDir(DIR* dp) { return Cns_closedir((Cns_DIR*) dp); }

};

/******************************************************************************/
/*                 X r d C a t a l o g F s P e r m s                          */
/******************************************************************************/
class XrdxCastor2FsPerms {
protected:
public:

  XrdOucString name;
  uid_t owner_id;
  gid_t group_id;
  mode_t mode;

  XrdxCastor2FsPerms(const char* path, uid_t oid,  gid_t gid, mode_t m);

  bool       allows(const XrdSecEntity* client,const char* privs, XrdOucErrInfo &error);
  static int Lifetime;
  virtual ~XrdxCastor2FsPerms(){ };
private:
};

class XrdxCastor2FsGroupInfo {
public:
  XrdOucString DefaultGroup;
  XrdOucString AllGroups;
  struct passwd Passwd;
  int Lifetime;

  XrdxCastor2FsGroupInfo(const char* defgroup="", const char* allgrps="", struct passwd *pw=0, int lf=60) { DefaultGroup = defgroup; AllGroups = allgrps; if (pw) memcpy(&Passwd,pw,sizeof(struct passwd)); Lifetime=lf;}
  virtual ~XrdxCastor2FsGroupInfo() {}
};

class XrdxCastor2FsNotifier {
public:
  XrdOucString NotifyDirectory;
  
  bool Notify(const char* owner, const char* grouprole, const char* filename);

  XrdxCastor2FsNotifier(const char* notifydir) {
    NotifyDirectory = notifydir;
  }

private:
  FILE* NotifyFile;
  time_t NotifyTime;
  XrdOucString NotifyFileName;

  virtual ~XrdxCastor2FsNotifier() {}
};

/******************************************************************************/
/*                 X r d C a t a l o g F s D i r e c t o r y                  */
/******************************************************************************/
  
class XrdxCastor2FsDirectory : public XrdSfsDirectory
{
public:

        int         open(const char              *dirName,
                         const XrdSecClientName  *client = 0,
                         const char              *opaque = 0);

        const char *nextEntry();

        int         close();

const   char       *FName() {return (const char *)fname;}

                    XrdxCastor2FsDirectory(char *user=0, int MonID=0) : XrdSfsDirectory(user,MonID)
                                {ateof = 0; fname = 0;
                                 dh    = (DIR *)0;
                                 d_pnt = &dirent_full.d_entry;
                                }

                   ~XrdxCastor2FsDirectory() {if (dh) close();}
private:

DIR           *dh;  // Directory stream handle
char           ateof;
char          *fname;
XrdOucString   entry;

struct {struct dirent d_entry;
               char   pad[MAXNAMLEN];   // This is only required for Solaris!
       } dirent_full;

struct dirent *d_pnt;

};

/******************************************************************************/
/*                      X r d C a t a l o g F s S t a t s                       */
/******************************************************************************/


class XrdxCastor2StatULongLong {
private:
  unsigned long long cnt;
  
public:
  void Inc() {cnt++;}
  unsigned long long Get() {return cnt;}
  void Reset() {cnt=0;}
  XrdxCastor2StatULongLong() {Reset();};
  virtual ~XrdxCastor2StatULongLong() {};
};


class XrdxCastor2FsStats 
{
private:
 
  long long read300s[300];
  long long write300s[300];
  long long stat300s[300];
  long long readd300s[300];
  long long rm300s[300];
  long long cmd300s[300];

  double readrate1s;
  double readrate60s;
  double readrate300s;
  
  double writerate1s;
  double writerate60s;
  double writerate300s;

  double statrate1s;
  double statrate60s;
  double statrate300s;

  double readdrate1s;
  double readdrate60s;
  double readdrate300s;

  double rmrate1s;
  double rmrate60s;
  double rmrate300s;

  double cmdrate1s;
  double cmdrate60s;
  double cmdrate300s;

  XrdOucHash<XrdxCastor2StatULongLong> ServerRead;
  XrdOucHash<XrdxCastor2StatULongLong> ServerWrite;
  XrdOucTable<XrdOucString> *ServerTable;

  XrdOucHash<XrdxCastor2StatULongLong> UserRead;
  XrdOucHash<XrdxCastor2StatULongLong> UserWrite;
  XrdOucTable<XrdOucString> *UserTable;

  XrdSysMutex statmutex;
  XrdxCastor2Proc* Proc;

public:

  void IncRead() {
    time_t now = time(NULL);
    statmutex.Lock();
    read300s[(now+1)%300] = 0;
    read300s[now%300]++;
    IncCmd(false);
    statmutex.UnLock();
  }

  void IncWrite() {
    time_t now = time(NULL);
    statmutex.Lock();
    write300s[(now+1)%300] = 0;
    write300s[now%300]++;
    IncCmd(false);
    statmutex.UnLock();
  }

  void IncStat() {
    time_t now = time(NULL);
    statmutex.Lock();
    stat300s[(now+1)%300] = 0;
    stat300s[now%300]++;
    IncCmd(false);
    statmutex.UnLock();
  }

  void IncReadd() {
    time_t now = time(NULL);
    statmutex.Lock();
    readd300s[(now+1)%300] = 0;
    readd300s[now%300]++;
    IncCmd(false);
    statmutex.UnLock();
  }

  void IncRm() {
    time_t now = time(NULL);
    statmutex.Lock();
    rm300s[(now+1)%300] = 0;
    rm300s[now%300]++;
    IncCmd(false);
    statmutex.UnLock();
  }

  void IncCmd(bool lock=true) {
    time_t now = time(NULL);
    if (lock) statmutex.Lock();
    cmd300s[(now+1)%300] = 0;
    cmd300s[now%300]++;
    if (lock) statmutex.UnLock();
  }
  
  void IncServerRead(const char* server) {
    statmutex.Lock();
    XrdxCastor2StatULongLong* rc = ServerRead.Find(server);
    if (!rc) {
      rc = new XrdxCastor2StatULongLong();
      rc->Inc();
      ServerRead.Add(server,rc);
      if (!ServerTable->Find(server)) {
	ServerTable->Insert(new XrdOucString(server),server);
      }
    } else {
      rc->Inc();
    }
    statmutex.UnLock();
  }

  void IncServerWrite(const char* server) {
    statmutex.Lock();
    XrdxCastor2StatULongLong* rc = ServerWrite.Find(server);
    if (!rc) {
      rc = new XrdxCastor2StatULongLong();
      rc->Inc();
      ServerWrite.Add(server,rc);
      if (!ServerTable->Find(server)) {
	ServerTable->Insert(new XrdOucString(server),server);
      }
    } else {
      rc->Inc();
    }
    statmutex.UnLock();
  }

  void IncUserRead(const char* user) {
    statmutex.Lock();
    XrdxCastor2StatULongLong* rc = UserRead.Find(user);
    if (!rc) {
      rc = new XrdxCastor2StatULongLong();
      rc->Inc();
      UserRead.Add(user,rc);
      UserTable->Insert(new XrdOucString(user),user);
    } else {
      rc->Inc();
    }
    statmutex.UnLock();
  }

  void IncUserWrite(const char* user) {
    statmutex.Lock();
    XrdxCastor2StatULongLong* rc = UserWrite.Find(user);
    if (!rc) {
      rc = new XrdxCastor2StatULongLong();
      rc->Inc();
      UserWrite.Add(user,rc);
      UserTable->Insert(new XrdOucString(user),user);
    } else {
      rc->Inc();
    }
    statmutex.UnLock();
  }

  double ReadRate(int nbins) {
    if (!nbins)
      return 0;

    time_t now = time(NULL);
    double sum=0;
    for (int i=0 ;i < nbins; i++) {
      sum+= (read300s[(now-1-i)%300]);
    }
    sum /= nbins;
    return sum;
  }

  double WriteRate(int nbins) {
    if (!nbins)
      return 0;
    time_t now = time(NULL);
    double sum=0;
    for (int i=0 ;i < nbins; i++) {
      sum+= (write300s[(now-1-i)%300]);
    }
    sum /= nbins;
    return sum;
  }


  double StatRate(int nbins) {
    if (!nbins)
      return 0;
    time_t now = time(NULL);
    double sum=0;
    for (int i=0 ;i < nbins; i++) {
      sum+= (stat300s[(now-1-i)%300]);
    }
    sum /= nbins;
    return sum;
  }

  double ReaddRate(int nbins) {
    if (!nbins)
      return 0;
    time_t now = time(NULL);
    double sum=0;
    for (int i=0 ;i < nbins; i++) {
      sum+= (readd300s[(now-1-i)%300]);
    }
    sum /= nbins;
    return sum;
  }

  double RmRate(int nbins) {
    if (!nbins)
      return 0;
    time_t now = time(NULL);
    double sum=0;
    for (int i=0 ;i < nbins; i++) {
      sum+= (rm300s[(now-1-i)%300]);
    }
    sum /= nbins;
    return sum;
  }

  double CmdRate(int nbins) {
    if (!nbins)
      return 0;

    time_t now = time(NULL);
    double sum=0;
    for (int i=0 ;i < nbins; i++) {
      sum+= (cmd300s[(now-1-i)%300]);
    }
    sum /= nbins;
    return sum;
  }
  void Update(); 
    
  void UpdateLoop() {
    while (1) {
      XrdSysTimer::Wait(490);
      time_t now = time(NULL);
      read300s[(now+1)%300] = 0;
      write300s[(now+1)%300] = 0;
      stat300s[(now+1)%300] = 0;
      readd300s[(now+1)%300] = 0;
      rm300s[(now+1)%300] = 0;
      cmd300s[(now+1)%300] = 0;
      Update();
    }
  }

  void Lock() {
    statmutex.Lock();
  }

  void UnLock() {
    statmutex.UnLock();
  }

  XrdxCastor2FsStats(XrdxCastor2Proc* proc=NULL){
    Lock();
    readrate1s=readrate60s=readrate300s=0;
    writerate1s=writerate60s=writerate300s=0;
    statrate1s=statrate60s=statrate300s=0;
    readdrate1s=readdrate60s=readdrate300s=0;
    rmrate1s=rmrate60s=rmrate300s=0;
    cmdrate1s=cmdrate60s=cmdrate300s=0;

    memset(read300s,0,sizeof(read300s));
    memset(write300s,0,sizeof(write300s));
    memset(stat300s,0,sizeof(stat300s));
    memset(readd300s,0,sizeof(readd300s));
    memset(rm300s,0,sizeof(rm300s));
    memset(cmd300s,0,sizeof(cmd300s));

    ServerTable = new XrdOucTable<XrdOucString> (XCASTOR2FS_MAXFILESYSTEMS);
    UserTable   = new XrdOucTable<XrdOucString> (XCASTOR2FS_MAXDISTINCTUSERS);

    Proc = proc;
    UnLock();
  }

  void SetProc(XrdxCastor2Proc* proc) {
    Proc = proc;
  }

  virtual ~XrdxCastor2FsStats(){
    if (ServerTable) delete ServerTable;
    if (UserTable)   delete UserTable;
  };
};

extern void* XrdxCastor2FsStatsStart(void *pp) ;

/******************************************************************************/
/*                      X r d C a t a l o g F s F i l e                         */
/******************************************************************************/
  
class XrdSfsAio;

class XrdxCastor2FsFile : public XrdSfsFile
{
public:

        int            open(const char                *fileName,
                                  XrdSfsFileOpenMode   openMode,
                                  mode_t               createMode,
                            const XrdSecEntity        *client = 0,
                            const char                *opaque = 0);

        int            close();

        const char    *FName() {return fname;}


  int Fscmd( const char*         /*path*/,  
             const char*         /*path2*/, 
             const char*         /*orgipath*/, 
             const XrdSecEntity* /*client*/,  
             XrdOucErrInfo&      /*error*/, 
             const char*         /*info*/ ) { return SFS_OK;}

        int            getMmap(void **Addr, off_t &Size)
                              {if (Addr) Addr = 0; Size = 0; return SFS_OK;}

  int read(XrdSfsFileOffset /*fileOffset*/,
           XrdSfsXferSize /*preread_sz*/) {return SFS_OK;}

        XrdSfsXferSize read(XrdSfsFileOffset   fileOffset,
                            char              *buffer,
                            XrdSfsXferSize     buffer_size);

        int            read(XrdSfsAio *aioparm);

        XrdSfsXferSize write(XrdSfsFileOffset   fileOffset,
                             const char        *buffer,
                             XrdSfsXferSize     buffer_size);

        int            write(XrdSfsAio *aioparm);

        int            sync();

        int            sync(XrdSfsAio *aiop);

        int            stat(struct stat *buf);

        int            truncate(XrdSfsFileOffset   fileOffset);

  int getCXinfo(char* /*cxtype[4]*/, int &cxrsz) {return cxrsz = 0;}

        int            fctl(int, const char*, XrdOucErrInfo&) {return 0;}

        XrdxCastor2FsFile(char *user=0, int MonID=0) : XrdSfsFile(user,MonID)
                                          {oh = -1; fname = 0; ohp = NULL; ohperror = NULL; ohperror_cnt=0;}
                      ~XrdxCastor2FsFile() {if (oh) close(); if (ohp) pclose(ohp); if (ohperror) pclose(ohperror);}
private:

int   oh;
FILE* ohp;            // file pointer to fs command pipe
FILE* ohperror;       // file pointer to fs command stderr file 
int   ohperror_cnt;   // cnt to find stderr file 
XrdSysMutex     fscmdLock;  

char *fname;
};


/******************************************************************************/
/*                          X r d C a t a l o g F s                           */
/******************************************************************************/
  
class XrdxCastor2Fs : public XrdSfsFileSystem
{
  friend class XrdxCastor2FsFile;
  friend class XrdxCastor2FsDirectory;
  friend class XrdxCastor2FsStats;

public:

// Object Allocation Functions
//
        XrdSfsDirectory *newDir(char *user=0, int MonID=0)
                        {return (XrdSfsDirectory *)new XrdxCastor2FsDirectory(user,MonID);}

        XrdSfsFile      *newFile(char *user=0, int MonID=0)
                        {return      (XrdSfsFile *)new XrdxCastor2FsFile(user,MonID);}

// Other Functions
//
        int            chmod(const char             *Name,
                                   XrdSfsMode        Mode,
                                   XrdOucErrInfo    &out_error,
                             const XrdSecEntity *client = 0,
                             const char             *opaque = 0);

        int            exists(const char                *fileName,
                                    XrdSfsFileExistence &exists_flag,
                                    XrdOucErrInfo       &out_error,
                              const XrdSecEntity    *client = 0,
                              const char                *opaque = 0);

        int            _exists(const char                *fileName,
                                    XrdSfsFileExistence &exists_flag,
                                    XrdOucErrInfo       &out_error,
                              const XrdSecEntity    *client = 0,
                              const char                *opaque = 0);

  enum eFSCTL { kFsctlxCastor2FsOffset= 40000, kFsctlSetMeta=40001 };

        int            fsctl(const int               cmd,
                             const char             *args,
                                   XrdOucErrInfo    &out_error,
			     const XrdSecEntity *client = 0);

  int getStats(char* /*buff*/, int /*blen*/) {return 0;}

const   char          *getVersion();


        int            mkdir(const char             *dirName,
                                   XrdSfsMode        Mode,
                                   XrdOucErrInfo    &out_error,
                             const XrdSecClientName *client = 0,
                             const char             *opaque = 0);

        int            _mkdir(const char             *dirName,
			      XrdSfsMode        Mode,
                                   XrdOucErrInfo    &out_error,
                             const XrdSecClientName *client = 0,
                             const char             *opaque = 0);

        int       stageprepare(const char           *path, 
			       XrdOucErrInfo        &error, 
			       const XrdSecEntity   *client, 
			       const char* info);
  
        int            prepare(      XrdSfsPrep       &pargs,
                                     XrdOucErrInfo    &out_error,
			     const XrdSecEntity *client = 0);

        int            rem(const char             *path,
                                 XrdOucErrInfo    &out_error,
                           const XrdSecEntity *client = 0,
                           const char             *opaque = 0);

        int            _rem(const char             *path,
                                 XrdOucErrInfo    &out_error,
                           const XrdSecEntity *client = 0,
                           const char             *opaque = 0);


        int            remdir(const char             *dirName,
                                    XrdOucErrInfo    &out_error,
                              const XrdSecEntity *client = 0,
                              const char             *opaque = 0);

        int            _remdir(const char             *dirName,
                                    XrdOucErrInfo    &out_error,
                              const XrdSecEntity *client = 0,
                              const char             *opaque = 0);

        int            rename(const char             *oldFileName,
                              const char             *newFileName,
                                    XrdOucErrInfo    &out_error,
                              const XrdSecEntity *client = 0,
                              const char             *opaqueO = 0,
                              const char             *opaqueN = 0);

        int            stat(const char             *Name,
                                  struct stat      *buf,
                                  XrdOucErrInfo    &out_error,
                            const XrdSecEntity *client = 0,
                            const char             *opaque = 0);

        int            lstat(const char            *Name,
                                  struct stat      *buf,
                                  XrdOucErrInfo    &out_error,
                            const XrdSecEntity *client = 0,
                            const char             *opaque = 0);

        int            stat(const char             *Name,
                                  mode_t           &mode,
                                  XrdOucErrInfo    &out_error,
                            const XrdSecEntity     *client = 0,
                            const char             *opaque = 0)
                       {struct stat bfr;
                        int rc = stat(Name, &bfr, out_error, client,opaque);
                        if (!rc) mode = bfr.st_mode;
                        return rc;
                       }

        int            truncate(const char*, XrdSfsFileOffset, XrdOucErrInfo&, const XrdSecEntity*, const char*);


        int            symlink(const char*, const char*, XrdOucErrInfo&, const XrdSecEntity*, const char*);

        int            readlink(const char*, XrdOucString& ,  XrdOucErrInfo&, const XrdSecEntity*, const char*);

        int            access(const char*, int mode, XrdOucErrInfo&, const XrdSecEntity*, const char*);

        int            utimes(const char*, struct timeval *tvp, XrdOucErrInfo&, const XrdSecEntity*, const char*);

// Common functions
//
static  int            Mkpath(const char *path, mode_t mode, 
                              const char *info=0, XrdSecEntity* client = NULL, XrdOucErrInfo* error = NULL);

static  int            Emsg(const char *, XrdOucErrInfo&, int, const char *x,
                            const char *y="");

                       XrdxCastor2Fs(XrdSysError *lp);
virtual               ~XrdxCastor2Fs() {}

virtual int            Configure(XrdSysError &);
virtual bool           Init();
        int            Stall(XrdOucErrInfo &error, int stime, const char *msg);
        bool           GetStageVariables(const char* Path, const char* Opaque, XrdOucString &stagevariables, uid_t client_uid, gid_t client_gid, const char* tident); 
        int           SetStageVariables(const char* Path, const char* Opaque, XrdOucString stagevariables, XrdOucString &stagehost, XrdOucString &serviceclass, int n, const char* tident);


        char          *ConfigFN;       

static  bool           Map(XrdOucString inmap, XrdOucString &outmap);
        bool           MapCernCertificates;
        XrdOucString   GridMapFile;
        XrdOucString   VomsMapFile;
        XrdOucString   LocationCacheDir;
        void           ReloadGridMapFile();
        void           ReloadVomsMapFile();
        bool           VomsMapGroups(const XrdSecEntity* client, XrdSecEntity& mappedclient, XrdOucString& allgroups, XrdOucString& defaultgroup); 

// we must check if all this things need to be thread safed with a mutex ...
static  XrdOucHash<XrdOucString> *filesystemhosttable;
static  XrdSysMutex               filesystemhosttablelock;
static  XrdOucHash<XrdOucString> *nstable;
static  XrdOucHash<XrdOucString> *stagertable;
static  XrdOucHash<XrdOucString> *stagerpolicy;
static  XrdOucHash<XrdOucString> *roletable;
static  XrdOucHash<XrdOucString> *stringstore;
static  XrdOucHash<XrdSecEntity> *secentitystore; 
static  XrdOucHash<struct passwd> *passwdstore;
static  XrdOucHash<XrdOucString>  *gridmapstore;
static  XrdOucHash<XrdOucString>  *vomsmapstore;
static  XrdOucHash<XrdxCastor2FsPerms> *nsperms;
static  XrdOucHash<XrdxCastor2FsGroupInfo>  *groupinfocache;
static  XrdOucHash<XrdxCastor2ClientAdmin> *clientadmintable;


static  int TokenLockTime;  // -> specifies the grace period for client to show up on a disk server in seconds before the token expires

        XrdxCastor2ServerAcc* ServerAcc; // -> authorization module for token encryption/decryption
        XrdSysMutex     encodeLock;
        XrdSysMutex     locatorLock;
        XrdSysMutex     PermissionMutex;      // -> protecting the permission hash
        XrdSysMutex     StoreMutex;           // -> protecting the string+pwd store hash
        XrdSysMutex     MapMutex;             // -> protecting all ROLEMAP, GETID etc. functions
        XrdSysMutex     SecEntityMutex;       // -> protecting the sec entity hash
        XrdSysMutex     GridMapMutex;         // -> protecting the gridmap store hash
        XrdSysMutex     VomsMapMutex;         // -> protecting the vomsmap store hash
        XrdSysMutex     ClientMutex;
        XrdOucString     xCastor2FsName;       // -> mount point of the catalog fs
        XrdOucString     xCastor2FsTargetPort;  // -> xrootd port where redirections go on the OSTs -default is 1094
        XrdOucString     xCastor2FsLocatePolicy; // -> can be configured to read only from the primary FS, from the twin FS or balanced between both 

        long long        xCastor2FsDelayRead;  // if true, all reads get a default delay to come back later
        long long        xCastor2FsDelayWrite; // if true, all writes get a default dealy to come back later
        XrdOucString     Procfilesystem;     // -> location of the proc file system directory
        bool             ProcfilesystemSync; // -> sync every access on disk
        XrdOucString     DeletionDirectory;  // -> directory where deletion spool files are stored
        XrdOucString     zeroProc;           // -> path to a 0-byte file in the proc filesystem

        XrdOucString     AuthLib;            // -> path to a possible authorizationn library
        bool             authorize;          // -> determins if the autorization should be applied or not
        XrdAccAuthorize *Authorization;      // ->Authorization   Service

protected:
        char*            HostName;           // -> our hostname as derived in XrdOfs
        char*            HostPref;           // -> our hostname as derived in XrdOfs without domain
        XrdOucString     ManagerId;          // -> manager id in <host>:<port> format
        XrdOucString     ManagerLocation;    // -> [::<IP>]:<port>
        bool             IssueCapability;    // -> attach an opaque capability for verification on the disk server
        XrdxCastor2Proc*  Proc;              // -> proc handling object
        XrdxCastor2FsStats Stats;
        XrdOfsEvr        evrObject;          // Event receiver
        XrdCmsFinderTRG* Balancer;           // 


private:

static  XrdSysError *eDest;

        XrdxCastor2FsPerms *getpermission( const XrdSecEntity    *client,
					   const char            *path,
					   const Access_Operation oper,
					   const char            *opaque,
					   XrdOucErrInfo   &error);

};

#endif
