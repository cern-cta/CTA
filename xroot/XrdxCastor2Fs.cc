//          $Id: XrdxCastor2Fs.cc,v 1.14 2009/07/06 08:27:11 apeters Exp $
const char *XrdxCastor2FsCVSID = "$Id: XrdxCastor2Fs.cc,v 1.14 2009/07/06 08:27:11 apeters Exp $";

#include "XrdVersion.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucTList.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSec/XrdSecInterface.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdxCastor2Fs/XrdxCastor2Fs.hh"
#include "XrdxCastor2Fs/XrdxCastor2Trace.hh"
#include "XrdxCastor2Fs/XrdxCastor2FsConstants.hh"
#include "XrdxCastor2Fs/XrdxCastor2Timing.hh"
#include "XrdxCastor2Fs/XrdxCastor2Stager.hh"
#include "XrdxCastor2Fs/XrdxCastor2FsSecurity.hh"
#include "XrdClient/XrdClientEnv.hh"
#include <pwd.h>
#include <grp.h>

#define RFIO_NOREDEFINE
#include <shift.h>
#include "Cns_api.h"
 
#ifdef AIX
#include <sys/mode.h>
#endif

/******************************************************************************/
/*       O S   D i r e c t o r y   H a n d l i n g   I n t e r f a c e        */
/******************************************************************************/

#ifndef S_IAMB
#define S_IAMB  0x1FF
#endif

/******************************************************************************/
/*                        G l o b a l   O b j e c t s                         */
/******************************************************************************/

XrdSysError     xCastor2FsEroute(0);  
XrdSysError    *XrdxCastor2Fs::eDest;
XrdOucTrace      xCastor2FsTrace(&xCastor2FsEroute);
XrdOucHash<XrdOucString> *XrdxCastor2Fs::filesystemhosttable;
XrdOucHash<XrdOucString> *XrdxCastor2Fs::nstable;
XrdOucHash<XrdOucString> *XrdxCastor2Fs::stagertable;
XrdOucHash<XrdOucString> *XrdxCastor2Fs::stagerpolicy;
XrdOucHash<XrdOucString> *XrdxCastor2Stager::delaystore;
XrdOucHash<XrdOucString> *XrdxCastor2Fs::roletable;
XrdOucHash<XrdOucString> *XrdxCastor2Fs::stringstore;
XrdOucHash<XrdSecEntity> *XrdxCastor2Fs::secentitystore;
XrdOucHash<XrdOucString> *XrdxCastor2Fs::gridmapstore;
XrdOucHash<XrdOucString> *XrdxCastor2Fs::vomsmapstore;
XrdOucHash<struct passwd> *XrdxCastor2Fs::passwdstore;
XrdOucHash<XrdxCastor2FsGroupInfo>  *XrdxCastor2Fs::groupinfocache;
XrdOucHash<XrdxCastor2FsPerms> *XrdxCastor2Fs::nsperms;

XrdOucHash<XrdxCastor2ClientAdmin> *XrdxCastor2Fs::clientadmintable;

int XrdxCastor2FsPerms::Lifetime=5;
int XrdxCastor2Fs::TokenLockTime=5;

XrdxCastor2Fs* XrdxCastor2FS=0;


#define XCASTOR2FS_EXACT_MATCH 1000
/******************************************************************************/

/*                        C o n v i n i e n c e                               */
/******************************************************************************/

/*----------------------------------------------------------------------------*/
/* this helps to avoid memory leaks by strdup                                 */
/* we maintain a string hash to keep all used user ids/group ids etc.         */

char* 
STRINGSTORE(const char* __charptr__) {
  XrdOucString* yourstring;
  if (!__charptr__ ) return "";

  if (yourstring = XrdxCastor2FS->stringstore->Find(__charptr__)) {
    return (char*)yourstring->c_str();
  } else {
    XrdOucString* newstring = new XrdOucString(__charptr__);
    XrdxCastor2FS->StoreMutex.Lock();
    XrdxCastor2FS->stringstore->Add(__charptr__,newstring);
    XrdxCastor2FS->StoreMutex.UnLock();
    return (char*)newstring->c_str();
  } 
}

/******************************************************************************/
/*                        D e f i n e s                                       */
/******************************************************************************/

/*----------------------------------------------------------------------------*/
#define MAP(x) \
  XrdOucString _inmap = x; XrdOucString _outmap;		        \
  if (XrdxCastor2Fs::Map(_inmap,_outmap)) x = _outmap.c_str();		\
  else x = NULL;							


/*----------------------------------------------------------------------------*/
#define GETID(_x,_client, _uid, _gid)                                    \
  do {									 \
    XrdxCastor2FS->MapMutex.Lock();                                      \
    _uid=99;                                                             \
    _gid=99;                                                             \
    struct passwd* pw;                                                   \
    XrdxCastor2FsGroupInfo* ginfo=NULL;                                  \
    if (_client.name) {                                                  \
      if ((ginfo = XrdxCastor2FS->groupinfocache->Find(_client.name))) { \
        pw = &(ginfo->Passwd);                                           \
        if (pw) _uid=pw->pw_uid;                                         \
      } else {                                                           \
        if (!(pw = XrdxCastor2Fs::passwdstore->Find(_client.name))) {    \
          pw = getpwnam(_client.name);                                   \
          struct passwd* pwdcpy = (struct passwd*) malloc(sizeof(struct passwd));\
          memcpy(pwdcpy,pw,sizeof(struct passwd));                       \
          pw = pwdcpy;                                                   \
          XrdxCastor2Fs::passwdstore->Add(_client.name,pwdcpy,60);       \
        }                                                                \
        if (pw) _uid=pw->pw_uid;                                         \
      }                                                                  \
    }                                                                    \
    if (_client.role) {                                                  \
      struct group* gr = getgrnam(_client.role);                         \
      if (gr) _gid= gr->gr_gid;                                          \
    } else {                                                             \
      if (pw) _gid=pw->pw_gid;                                           \
    }                                                                    \
    if (_uid==0) {_gid=0;}                                               \
    if (GTRACE(authorize)) {						 \
      XrdOucString tracestring = "getgid ";				 \
      tracestring += (int)_uid;						 \
      tracestring += "/";						 \
      tracestring += (int)_gid;						 \
      XTRACE(authorize, _x, tracestring.c_str());			 \
    }									 \
    XrdxCastor2FS->MapMutex.UnLock();                                    \
  } while (0);


/*----------------------------------------------------------------------------*/
#define SETACL(_x,_client,_link) \
  do {                                                                  \
    XrdxCastor2FS->MapMutex.Lock();                                     \
    uid_t uid=99;                                                       \
    uid_t gid=0;                                                        \
    struct passwd* pw;                                                  \
    XrdxCastor2FsGroupInfo* ginfo=NULL;                                 \
    if (_client.name) {                                                 \
      if ((ginfo = XrdxCastor2FS->groupinfocache->Find(_client.name))) {\
        pw = &(ginfo->Passwd);                                          \
        if (pw) uid=pw->pw_uid;                                         \
      } else {                                                          \
        if (!(pw = XrdxCastor2Fs::passwdstore->Find(_client.name))) {   \
          pw = getpwnam(_client.name);                                  \
          struct passwd* pwdcpy = (struct passwd*) malloc(sizeof(struct passwd));\
          memcpy(pwdcpy,pw,sizeof(struct passwd));                      \
          pw = pwdcpy;                                                  \
          XrdxCastor2Fs::passwdstore->Add(_client.name,pwdcpy,60);      \
        }                                                               \
        if (pw) uid=pw->pw_uid;                                         \
      }                                                                 \
    }                                                                   \
    if (_client.role) { \
      struct group* gr = getgrnam(_client.role);                        \
      if (gr) gid= gr->gr_gid;                                          \
    } else {                                                            \
      if (pw) gid=pw->pw_gid;                                           \
    }                                                                   \
    if (GTRACE(authorize)) {                                            \
      XrdOucString tracestring = "setacl ";                             \
      tracestring += (int)uid;                                          \
      tracestring += "/";                                               \
      tracestring += (int)gid;                                          \
      XTRACE(authorize, _x, tracestring.c_str());                       \
    }                                                                   \
    if (_link) {                                                        \
      if (XrdxCastor2FsUFS::Lchown(_x , uid,gid)) {                     \
        XTRACE(authorize, _x,"error: chown failed");                    \
      }                                                                 \
    } else {                                                            \
      if (XrdxCastor2FsUFS::Chown(_x , uid,gid)) {                      \
        XTRACE(authorize, _x,"error: chown failed");                    \
      }                                                                 \
    }                                                                   \
    XrdxCastor2FS->MapMutex.UnLock();                                    \
  } while (0);


/*----------------------------------------------------------------------------*/
#define GETALLGROUPS(__name__, __allgroups__, __defaultgroup__)         \
  do {                                                                  \
  __allgroups__=":";                                                    \
  __defaultgroup__="";                                                  \
  XrdxCastor2FsGroupInfo* ginfo=NULL;                                   \
  if (ginfo = XrdxCastor2FS->groupinfocache->Find(__name__)) {          \
    __allgroups__ = ginfo->AllGroups;                                   \
    __defaultgroup__ = ginfo->DefaultGroup;                             \
    break;                                                              \
  }                                                                     \
  struct group* gr;                                                     \
  struct passwd* passwdinfo = NULL;                                     \
  if (!(passwdinfo = XrdxCastor2Fs::passwdstore->Find(__name__))) {     \
      passwdinfo = getpwnam(__name__);                                  \
      if (passwdinfo) {                                                 \
        struct passwd* pwdcpy = (struct passwd*) malloc(sizeof(struct passwd));\
        memcpy(pwdcpy,passwdinfo,sizeof(struct passwd));                \
        passwdinfo = pwdcpy;                                            \
        XrdxCastor2Fs::passwdstore->Add(__name__,pwdcpy,60);            \
      }                                                                 \
  }                                                                     \
  if (!passwdinfo)                                                      \
    continue;                                                           \
  setgrent();                                                           \
  while( (gr = getgrent() ) ) {                                         \
     int cnt;                                                           \
     cnt=0;                                                             \
     if (gr->gr_gid == passwdinfo->pw_gid) {                            \
       if (!__defaultgroup__.length()) __defaultgroup__+= gr->gr_name;  \
       __allgroups__+= gr->gr_name; __allgroups__+=":";                 \
     }                                                                  \
     while (gr->gr_mem[cnt]) {                                          \
       if (!strcmp(gr->gr_mem[cnt],__name__)) {                         \
          __allgroups__+= gr->gr_name; __allgroups__+=":";              \
       }                                                                \
       cnt++;                                                           \
     }                                                                  \
  }                                                                     \
  endgrent();                                                           \
  ginfo = new XrdxCastor2FsGroupInfo(__defaultgroup__.c_str(), __allgroups__.c_str(),passwdinfo);\
  XrdxCastor2FS->groupinfocache->Add(__name__,ginfo, ginfo->Lifetime);  \
  } while(0);                                                           \

/*----------------------------------------------------------------------------*/

void ROLEMAP(const XrdSecEntity* _client,const char* _env,XrdSecEntity &_mappedclient, const char* tident)		        
{
  EPNAME("rolemap");
  XrdSecEntity* entity = NULL;
  XrdOucEnv lenv(_env); const char* role = lenv.Get("role");		
  
  XrdxCastor2FS->SecEntityMutex.Lock();
  char clientid[1024];
  sprintf(clientid,"%s:%llu",tident, (unsigned long long) _client);

  if ((!role) && (entity = XrdxCastor2FS->secentitystore->Find(clientid))) {
    // find existing client rolemaps ....
    _mappedclient.name = entity->name;
    _mappedclient.role = entity->role;
    _mappedclient.host = entity->host;
    _mappedclient.vorg = entity->vorg;
    _mappedclient.grps = entity->grps;
    _mappedclient.endorsements = entity->endorsements;
    _mappedclient.tident = entity->endorsements;
     uid_t _client_uid;
     gid_t _client_gid;
     GETID("-",_mappedclient,_client_uid,_client_gid);                    
     XrdxCastor2FsUFS::SetId(_client_uid,_client_gid);                      
     XrdxCastor2FS->SecEntityMutex.UnLock();
     return;
  }
  
  // (re-)create client rolemaps
  do {
     XrdxCastor2FS->MapMutex.Lock();

    _mappedclient.name="__noauth__";					
    _mappedclient.role="__noauth__";					
    _mappedclient.host="";						
    _mappedclient.vorg="";						
    _mappedclient.role="";						
    _mappedclient.grps="__noauth__";					
    _mappedclient.endorsements="";					
    _mappedclient.tident="";						
    if (_client) {							
      if (_client->prot)						
	strcpy(_mappedclient.prot,_client->prot);			
      if (_client->name)_mappedclient.name = STRINGSTORE(_client->name);	
      if (_client->host)_mappedclient.host = STRINGSTORE(_client->host);	
      if (_client->vorg)_mappedclient.vorg = STRINGSTORE(_client->vorg);	
      if (_client->role)_mappedclient.role = STRINGSTORE(_client->role);	
      if (_client->grps)_mappedclient.grps = STRINGSTORE(_client->grps);	
      if (_client->endorsements)_mappedclient.endorsements = STRINGSTORE(_client->endorsements); 
      if (_client->tident)_mappedclient.tident = STRINGSTORE(_client->tident); 
      /* static user mapping */                                                
      XrdOucString* hisroles;                                                  
      if ((hisroles = XrdxCastor2FS->roletable->Find("*"))){                    
      XrdOucString allgroups; XrdOucString defaultgroup;                   
      XrdOucString FixedName;                                              
      if (hisroles->beginswith("static:")) {                               
	FixedName = hisroles->c_str()+7;                                   
      } else {                                                             
	FixedName = hisroles->c_str();                                     
      }                                                                    
      while( (FixedName.find(":"))!=STR_NPOS) {FixedName.replace(":","");}         
      _mappedclient.name = STRINGSTORE(FixedName.c_str());                 
      GETALLGROUPS(_mappedclient.name,allgroups,defaultgroup);             
      _mappedclient.grps=STRINGSTORE(defaultgroup.c_str());                
      _mappedclient.role=STRINGSTORE(defaultgroup.c_str());                        
      break;
      }
      if ((_client->prot) && ((!strcmp(_client->prot,"gsi")) || (!strcmp(_client->prot,"ssl")))) {
	XrdOucString certsubject = _client->name;
	if ( (XrdxCastor2FS->MapCernCertificates) && (certsubject.beginswith("/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN="))) {
	  certsubject.erasefromstart(strlen("/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN="));
	  int pos=certsubject.find('/');                               
	  if (pos != STR_NPOS)                                         
	    certsubject.erase(pos);			  	        
	  _mappedclient.name = STRINGSTORE(certsubject.c_str());       
	}                                                              
	certsubject.replace("/CN=proxy","");                           
	XrdOucString* gridmaprole;                                     
	XrdxCastor2FS->GridMapMutex.Lock();                             
	if (XrdxCastor2FS->GridMapFile.length()) XrdxCastor2FS->ReloadGridMapFile();
	if ((gridmaprole = XrdxCastor2FS->gridmapstore->Find(certsubject.c_str()))) { 
	  _mappedclient.name = STRINGSTORE(gridmaprole->c_str());      
	  _mappedclient.role = 0;                                      
	}                                                              
	XrdxCastor2FS->GridMapMutex.UnLock();                           
      }                                                                 
    }									
    if (_client && _mappedclient.name) {				
      XrdOucString defaultgroup="";                                     
      XrdOucString allgroups="";                                        
      if ((_client->grps==0)|| (!strlen(_client->grps))) {GETALLGROUPS(_mappedclient.name,allgroups,defaultgroup); _mappedclient.grps=STRINGSTORE(allgroups.c_str());} else {                                 
	if (XrdxCastor2FS->VomsMapGroups(_client,_mappedclient, allgroups,defaultgroup)) 
	  if (! role) role = _mappedclient.role;                       
      }                                                                 
      if ( ((! _mappedclient.role) || (!strlen(_mappedclient.role))) && (!role)) { role=STRINGSTORE(defaultgroup.c_str()); }
      XrdOucString* hisroles = XrdxCastor2FS->roletable->Find(_mappedclient.name); 
      XrdOucString match = ":";match+= role; match+=":";		
      if (0)printf("default %s all %s match %s role %sn",defaultgroup.c_str(),allgroups.c_str(),match.c_str(),role);
      if (hisroles)							
	if ((hisroles->find(match.c_str()))!=STR_NPOS) {		
	  _mappedclient.role = STRINGSTORE(role);			
	} else {							
	  if (hisroles->beginswith("static:")) {			
	    _mappedclient.role = STRINGSTORE(hisroles->c_str()+7);	
	  } else { 					                
	    if ( (allgroups.find(match.c_str())) != STR_NPOS )          
	      _mappedclient.role = STRINGSTORE(role);                   
	    else                                                        
	      _mappedclient.role = STRINGSTORE(defaultgroup.c_str());   
	  }                                                             
	}								
      else								
	{                                                               
	  if ((allgroups.find(match.c_str())) != STR_NPOS) {             
	    _mappedclient.role = STRINGSTORE(role);                      
	  } else {                                                       
	    _mappedclient.role = STRINGSTORE(defaultgroup.c_str());	
	  }                                                              
	}                                                               
      if ((!strlen(_mappedclient.role)) && (_client->grps))               
	_mappedclient.role = STRINGSTORE(_client->grps);                  
    }									
    {									
      /* try tident mapping */						
      XrdOucString reducedTident,user, stident;				
      reducedTident=""; user = ""; stident = tident;			
      int dotpos = stident.find(".");					
      reducedTident.assign(tident,0,dotpos-1);reducedTident += "@";	
      int adpos  = stident.find("@"); user.assign(tident,adpos+1); reducedTident += user; 
      XrdOucString* hisroles = XrdxCastor2FS->roletable->Find(reducedTident.c_str()); 
      XrdOucString match = ":";match+= role; match+=":";		
      if (hisroles)							
	if ((hisroles->find(match.c_str())) != STR_NPOS) {		
	  _mappedclient.role = STRINGSTORE(role);			
	  _mappedclient.name = STRINGSTORE(role);			
	} else {							
	  if (hisroles->beginswith("static:")) {			
	    if (_mappedclient.role) (_mappedclient.role);		
	    _mappedclient.role = STRINGSTORE(hisroles->c_str()+7);	
	    _mappedclient.name = STRINGSTORE(hisroles->c_str()+7);	
	  }								
	}								
    }									
    if (_mappedclient.role && (!strcmp(_mappedclient.role,"root")))     
      _mappedclient.name=STRINGSTORE("root");                           
    break;
  } while (0);

  XrdxCastor2FS->MapMutex.UnLock();
     
  XrdSecEntity* newentity = new XrdSecEntity();
  if (newentity) {
    newentity->name = _mappedclient.name;
    newentity->role = _mappedclient.role;
    newentity->host = _mappedclient.host;
    newentity->vorg = _mappedclient.vorg;
    newentity->grps = _mappedclient.grps;
    newentity->endorsements = _mappedclient.endorsements;
    newentity->tident = _mappedclient.tident;
    XrdxCastor2FS->secentitystore->Add(clientid, newentity, 60);
  }
  XrdxCastor2FS->SecEntityMutex.UnLock();

  uid_t _client_uid;
  gid_t _client_gid;

  GETID("-",_mappedclient,_client_uid,_client_gid);                    
  XrdxCastor2FsUFS::SetId(_client_uid,_client_gid);                      
  return;
}

/*----------------------------------------------------------------------------*/

void 
XrdxCastor2Fs::ReloadGridMapFile()
{
  EPNAME("ReloadGridMapFile");
  const char *tident = "pool-thread";

  static time_t         GridMapMtime=0;
  static time_t         GridMapCheckTime=0;
  int now = time(NULL);

  if ((!GridMapCheckTime) || ((now >GridMapCheckTime + XCASTOR2FS_GRIDMAPCHECKINTERVAL)) ) {
    // load it for the first time or again
    struct stat buf;
    if (!::stat(GridMapFile.c_str(),&buf)) {
      if (buf.st_mtime != GridMapMtime) {
	GridMapMutex.Lock();
	// store the last modification time
	GridMapMtime = buf.st_mtime;
	// store the current time of the check
	GridMapCheckTime = now;
	// dump the current table
	gridmapstore->Purge();
	// open the gridmap file
	FILE* mapin = fopen(GridMapFile.c_str(),"r");
	if (!mapin) {
	  // error no grid map possible
	  TRACES("Unable to open gridmapfile " << GridMapFile.c_str() << " - no mapping!");
	} else {
	  char userdnin[4096];
	  char usernameout[4096];
	  int nitems;
	  // parse it
	  while ( (nitems = fscanf(mapin,"%s %s", userdnin,usernameout)) == 2) {
	    XrdOucString dn = userdnin;
	    dn.replace("\"","");
	    if (!gridmapstore->Find(dn.c_str())) {
	      gridmapstore->Add(dn.c_str(), new XrdOucString(usernameout));
	      TRACES("GridMapFile Mapping Added: " << dn.c_str() << " |=> " << usernameout);
	    }
	  }
	  fclose(mapin);
	}
	GridMapMutex.UnLock();
      } else {
	// the file didn't change, we don't do anything
      }
    } else {
      //      printf("Gridmapfile %s\n",GridMapFile.c_str());
      TRACES("Unable to stat gridmapfile " << GridMapFile.c_str() << " - no mapping!");
    }
  }
}

/*----------------------------------------------------------------------------*/

void 
XrdxCastor2Fs::ReloadVomsMapFile()
{
  EPNAME("ReloadVomsMapFile");
  const char *tident = "pool-thread";

  static time_t         VomsMapMtime=0;
  static time_t         VomsMapCheckTime=0;
  int now = time(NULL);

  if ((!VomsMapCheckTime) || ((now >VomsMapCheckTime + XCASTOR2FS_VOMSMAPCHECKINTERVAL)) ) {
    // load it for the first time or again
    struct stat buf;
    if (!::stat(VomsMapFile.c_str(),&buf)) {
      if (buf.st_mtime != VomsMapMtime) {
	VomsMapMutex.Lock();
	// store the last modification time
	VomsMapMtime = buf.st_mtime;
	// store the current time of the check
	VomsMapCheckTime = now;
	// dump the current table
	vomsmapstore->Purge();
	// open the vomsmap file
	FILE* mapin = fopen(VomsMapFile.c_str(),"r");
	if (!mapin) {
	  // error no voms map possible
	  TRACES("Unable to open vomsmapfile " << VomsMapFile.c_str() << " - no mapping!");
	} else {
	  char userdnin[4096];
	  char usernameout[4096];
	  int nitems;
	  // parse it
	  while ( (nitems = fscanf(mapin,"%s %s", userdnin,usernameout)) == 2) {
	    XrdOucString dn = userdnin;
	    dn.replace("\"","");
	    if (!vomsmapstore->Find(dn.c_str())) {
	      vomsmapstore->Add(dn.c_str(), new XrdOucString(usernameout));
	      TRACES("VomsMapFile Mapping Added: " << dn.c_str() << " |=> " << usernameout);
	    }
	  }
	  fclose(mapin);
	}
	VomsMapMutex.UnLock();
      } else {
	// the file didn't change, we don't do anything
      }
    } else {
      //      printf("Vomsmapfile %s\n",VomsMapFile.c_str());
      TRACES("Unable to stat vomsmapfile " << VomsMapFile.c_str() << " - no mapping!");
    }
  }
}

/*----------------------------------------------------------------------------*/

bool
XrdxCastor2Fs::VomsMapGroups(const XrdSecEntity* client, XrdSecEntity& mappedclient, XrdOucString& allgroups, XrdOucString& defaultgroup) 
{
  if (client->grps) {
    if (XrdxCastor2FS->VomsMapFile.length()) 
      XrdxCastor2FS->ReloadVomsMapFile();
    else 
      return false;
    
    // loop over all VOMS groups and replace them according to the mapping
    XrdOucString vomsline = client->grps;
    allgroups = ":";
    defaultgroup = "";
    vomsline.replace(":","\n");
    XrdOucTokenizer vomsgroups((char*)vomsline.c_str());
    const char* stoken;
    int ntoken=0;
    XrdOucString* vomsmaprole;                                     
    while( (stoken = vomsgroups.GetLine())) {
      //      printf("Verifying |%s|\n",stoken);
      if ((vomsmaprole = XrdxCastor2FS->vomsmapstore->Find(stoken))) { 
	allgroups += vomsmaprole->c_str();
	allgroups += ":";
	if (ntoken == 0) {
	  defaultgroup = vomsmaprole->c_str();
	}
	ntoken++;
      }
    }
    mappedclient.role = STRINGSTORE(defaultgroup.c_str());                                       
    mappedclient.grps = STRINGSTORE(allgroups.c_str());
    return true;
  } else {
    return false;
  }
}

/*----------------------------------------------------------------------------*/

int
XrdxCastor2Fs::SetStageVariables(const char* Path, const char* Opaque, XrdOucString stagevariables, XrdOucString &stagehost, XrdOucString &serviceclass,  int n, const char* tident)
{
  EPNAME("SetStageVariables");
  XrdOucEnv Open_Env(Opaque);
  
  if (Opaque) {
    ZTRACE(open,"path=" << Path << " Opaque=" << Opaque << " Stagevariables=" << stagevariables.c_str());
  } else {
    ZTRACE(open,"path=" << Path << " Stagevariables=" << stagevariables.c_str());
  }
  // the tokenizer likes line feeds
  stagevariables.replace(',','\n');
  XrdOucTokenizer stagetokens((char*)stagevariables.c_str());
  XrdOucString desiredstagehost="";
  XrdOucString desiredserviceclass="";
  
  const char* sh=0;
  
  const char* stoken;
  int ntoken=0;
  while( (stoken = stagetokens.GetLine())) {
    if ((sh = Open_Env.Get("stagerHost"))) {
      desiredstagehost = sh;
    } else {
      desiredstagehost = "";
    }
    
    if ((sh = Open_Env.Get("svcClass"))) {
      desiredserviceclass=sh;
    } else {
      desiredserviceclass = "";
    }
  

    XrdOucString mhost = stoken;
    XrdOucString shost;
    int offset;
    if ((offset=mhost.find("::")) != STR_NPOS) {
      shost.assign(mhost.c_str(),0,offset-1);
      stagehost = shost.c_str();
      shost.assign(mhost.c_str(),offset+2);
      serviceclass = shost;
    } else {
      stagehost = mhost;
      serviceclass = "default";
    }

    ZTRACE(open,"0 path=" << Path << " dstagehost=" << desiredstagehost.c_str() << " dserviceclass=" << desiredserviceclass.c_str() << " stagehost=" << stagehost.c_str() << " serviceclass=" << serviceclass.c_str() << " n="<<n << " ntoken=" << ntoken);
    if (!desiredstagehost.length()) {
      if (stagehost == "*") {
	XrdOucString* dh=0;
	// use the default one
	if ( (dh = stagertable->Find("default"))) {
	  XrdOucString defhost = dh->c_str();
	  int spos=0;
	  // remove the service class if defined
	  if ((spos = defhost.find("::")) != STR_NPOS) {
	    defhost.assign(defhost.c_str(),0,spos-1);
	  }
	  // set the default one now!
	  desiredstagehost = defhost;
	} 
      } else {
	desiredstagehost = stagehost;
      }
    }


    if ((stagehost == "*"))  {
      if ( desiredstagehost.length()) {
	stagehost = desiredstagehost;
	if (!desiredserviceclass.length()) {
	  if (serviceclass != "*") {
	    desiredserviceclass = serviceclass;
	  }
	}
      }
    } 

    if ((serviceclass == "*") && desiredserviceclass.length()) {
      serviceclass = desiredserviceclass;
    }
    ZTRACE(open,"1 path=" << Path << " dstagehost=" << desiredstagehost.c_str() << " dserviceclass=" << desiredserviceclass.c_str() << " stagehost=" << stagehost.c_str() << " serviceclass=" << serviceclass.c_str() << " n="<<n << " ntoken=" << ntoken);
    // this handles the case, where  only the stager is set
    if ((desiredstagehost == stagehost) && (!desiredserviceclass.length())) {
      if (serviceclass != "*") {
	desiredserviceclass = serviceclass;
      }
    }

    ZTRACE(open,"2 path=" << Path << " dstagehost=" << desiredstagehost.c_str() << " dserviceclass=" << desiredserviceclass.c_str() << " stagehost=" << stagehost.c_str() << " serviceclass=" << serviceclass.c_str() << " n="<<n << " ntoken=" << ntoken);
    // we are successfull if we either return the appropriate token or if the token matches the user specified
    // stager/svc class pair
    // as a result: if the host/svc class pair is not defined, a user cannot request it!
    // this now also supports to allow any service class or host via *::default or stager::* to be user selected
    if (((desiredstagehost == stagehost) && (desiredserviceclass == serviceclass)) && ((n==ntoken)||(n==XCASTOR2FS_EXACT_MATCH))) {
      ZTRACE(open,"path=" << Path << " stagehost=" << stagehost.c_str() << " serviceclass=" << serviceclass.c_str());
      return 1;
    } else {
      if ( (n!=XCASTOR2FS_EXACT_MATCH) && (ntoken==n) )
	return 0;
    }
    ntoken++;
  }
  return -1;
}

bool 
XrdxCastor2Fs::GetStageVariables( const char* Path, const char* Opaque, XrdOucString &stagevariables, uid_t client_uid, gid_t client_gid, const char* tident) 
{
  EPNAME("GetStageVariables");
  if (Opaque) {
    ZTRACE(open,"path=" << Path << " Opaque=" << Opaque);
  } else {
    ZTRACE(open,"path=" << Path);
  }
  
  XrdOucEnv Open_Env(Opaque);
  const char* val;
  //  if ((val=Open_Env.Get("stagerHost"))){
  //    stagevariables = val;
  //    stagevariables += "::";
  //    if ((val=Open_Env.Get("svcClass"))) {
  //      stagevariables += val;
  //    } else {
  //      stagevariables += "default";
  //    }
  //  } else {
  if (1) {
    XrdOucString spath = Path;
    XrdOucString *mhost;
    // try the stagertable
    int pos=0;
    bool found=false;

    // mapping by path is done first
    while ( (pos = spath.find("/",pos) ) != STR_NPOS ) {
      XrdOucString subpath;
      subpath.assign(Path,0,pos);
      if ( (mhost = stagertable->Find(subpath.c_str()))) {
	stagevariables = mhost->c_str();
	found=true;
      }
      pos++;
    }

    // mapping by uid/gid overwrites path mapping
    XrdOucString uidmap="uid:";
    XrdOucString gidmap="gid:";
    uidmap += (int)client_uid;
    gidmap += (int)client_gid;
    
    if ( (mhost = stagertable->Find(uidmap.c_str()))) {
      stagevariables = mhost->c_str();
      found=true;
    }
    
    if ( (mhost = stagertable->Find(gidmap.c_str()))) {
      stagevariables = mhost->c_str();
      found=true;
    }

    // uid/gid + path mapping wins!
    // user mapping superseeds group mapping
    pos=0;
    while ( (pos = spath.find("/",pos) ) != STR_NPOS ) {
      XrdOucString subpath;
      subpath.assign(Path,0,pos);
      subpath += "::";
      subpath += "gid:";
      subpath += (int) client_gid;
      if ( (mhost = stagertable->Find(subpath.c_str()))) {
	stagevariables = mhost->c_str();
	found=true;
      }
      pos++;
    }
    pos=0;
    while ( (pos = spath.find("/",pos) ) != STR_NPOS ) {
      XrdOucString subpath;
      subpath.assign(Path,0,pos);
      subpath += "::";
      subpath += "uid:";
      subpath += (int) client_uid;
      if ( (mhost = stagertable->Find(subpath.c_str()))) {
	stagevariables = mhost->c_str();
	found=true;
      }
      pos++;
    }

    if (!found) {
      if (mhost = stagertable->Find("default")) {
	stagevariables = mhost->c_str();
      } else {
	return false;
      }
    }
  }   
  
  return true;
}

/*----------------------------------------------------------------------------*/
  
/******************************************************************************/
/*                           C o n s t r u c t o r                            */
/******************************************************************************/

XrdxCastor2Fs::XrdxCastor2Fs(XrdSysError *ep)
{
  eDest = ep;
  ConfigFN  = 0;  
}

/******************************************************************************/
/*                           I n i t i a l i z a t i o n                      */
/******************************************************************************/
bool
XrdxCastor2Fs::Init(XrdSysError &ep)
{
  int rc;
  pthread_t tid;
  
  filesystemhosttable = new XrdOucHash<XrdOucString> ();
  nstable   = new XrdOucHash<XrdOucString> ();
  stagertable   = new XrdOucHash<XrdOucString> ();
  stagerpolicy  = new XrdOucHash<XrdOucString> ();
  roletable = new XrdOucHash<XrdOucString> ();
  stringstore = new XrdOucHash<XrdOucString> ();
  secentitystore = new XrdOucHash<XrdSecEntity> ();
  gridmapstore = new XrdOucHash<XrdOucString> ();
  vomsmapstore = new XrdOucHash<XrdOucString> ();
  passwdstore = new XrdOucHash<struct passwd> ();
  nsperms   = new XrdOucHash<XrdxCastor2FsPerms> ();
  groupinfocache   = new XrdOucHash<XrdxCastor2FsGroupInfo> ();
  clientadmintable = new XrdOucHash<XrdxCastor2ClientAdmin> ();

  XrdxCastor2Stager::delaystore   = new XrdOucHash<XrdOucString> ();

  return true;
}

/******************************************************************************/
/*                         G e t F i l e S y s t e m                          */
/******************************************************************************/
  
XrdSfsFileSystem *XrdSfsGetFileSystem(XrdSfsFileSystem *native_fs, 
                                      XrdSysLogger     *lp,
				      const char       *configfn)
{
  xCastor2FsEroute.SetPrefix("xcastor2fs_");
  xCastor2FsEroute.logger(lp);
  
  static XrdxCastor2Fs myFS(&xCastor2FsEroute);

  XrdOucString vs="xCastor2Fs (xCastor2 File System) v ";
  vs += PACKAGE_VERSION;
  xCastor2FsEroute.Say("++++++ (c) 2008 CERN/IT-DM-SMD ",vs.c_str());
  
  // Initialize the subsystems
  //
  if (!myFS.Init(xCastor2FsEroute) ) return 0;

  myFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);
  if ( myFS.Configure(xCastor2FsEroute) ) return 0;

  XrdxCastor2FS = &myFS;

  // Initialize authorization module ServerAcc
  XrdxCastor2FS->ServerAcc = (XrdxCastor2ServerAcc*) XrdAccAuthorizeObject(lp, configfn, NULL);
  if (!XrdxCastor2FS->ServerAcc) {
    return 0;
  }
  return &myFS;
}

/******************************************************************************/
/*           S t a t s   T h r e a d S t a r t U p                            */
/******************************************************************************/
void* XrdxCastor2FsStatsStart(void *pp) 
{
  XrdxCastor2FsStats* stats = (XrdxCastor2FsStats*) pp;
  stats->UpdateLoop();
  return (void*) 0;
}



/******************************************************************************/
/*           D i r e c t o r y   O b j e c t   I n t e r f a c e s            */
/******************************************************************************/
/******************************************************************************/
/*                                  o p e n                                   */
/******************************************************************************/
  
int XrdxCastor2FsDirectory::open(const char              *dir_path, // In
                                const XrdSecEntity  *client,   // In
                                const char              *info)     // In
/*
  Function: Open the directory `path' and prepare for reading.

  Input:    path      - The fully qualified name of the directory to open.
            cred      - Authentication credentials, if any.
            info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success, otherwise SFS_ERROR.
*/
{
   static const char *epname = "opendir";
   const char *tident = error.getErrUser();
   const char* rpath;
   XrdSecEntity mappedclient;
   XrdOucEnv Open_Env(info);

   AUTHORIZE(client,&Open_Env,AOP_Readdir,"open directory",dir_path,error);

   MAP((dir_path));
   if (!dir_path) {
     return XrdxCastor2Fs::Emsg(epname, error, ENOMEDIUM, 
                             "map filename", dir_path);
   }
   ROLEMAP(client,info,mappedclient,tident);

   if (XrdxCastor2FS->Proc) {
     XrdxCastor2FS->Stats.IncCmd();
   }

// Verify that this object is not already associated with an open directory
//
     if (dh) return
        XrdxCastor2Fs::Emsg(epname, error, EADDRINUSE, 
                             "open directory", dir_path);

// Set up values for this directory object
//
   ateof = 0;
   fname = strdup(dir_path);

   
// Open the directory and get it's id
//

   if (!(dh = XrdxCastor2FsUFS::OpenDir(dir_path))) 
       return  XrdxCastor2Fs::Emsg(epname,error,serrno,"open directory",dir_path);

   //
   return SFS_OK;
}

/******************************************************************************/
/*                             n e x t E n t r y                              */
/******************************************************************************/

const char *XrdxCastor2FsDirectory::nextEntry()
/*
  Function: Read the next directory entry.

  Input:    None.

  Output:   Upon success, returns the contents of the next directory entry as
            a null terminated string. Returns a null pointer upon EOF or an
            error. To differentiate the two cases, getErrorInfo will return
            0 upon EOF and an actual error code (i.e., not 0) on error.
*/
{
    static const char *epname = "nextEntry";
    struct dirent *rp;
    int retc;

// Lock the direcrtory and do any required tracing
//
   if (!dh) 
      {XrdxCastor2Fs::Emsg(epname,error,EBADF,"read directory",fname);
       return (const char *)0;
      }

   // Check if we are at EOF (once there we stay there)
   //
   if (ateof) return (const char *)0;
   
   if (XrdxCastor2FS->Proc) {
     XrdxCastor2FS->Stats.IncReadd();
   }
   
   // Read the next directory entry
   //
   if (!(d_pnt = XrdxCastor2FsUFS::ReadDir(dh))) {
     if (serrno != 0) {
       XrdxCastor2Fs::Emsg(epname,error,serrno,"read directory",fname);
     }
     return (const char *)0;
   }
   
   entry = d_pnt->d_name;
   
   
   // Return the actual entry
   //
   return (const char *)(entry.c_str());
}

/******************************************************************************/
/*                                 c l o s e                                  */
/******************************************************************************/
  
int XrdxCastor2FsDirectory::close()
/*
  Function: Close the directory object.

  Input:    cred       - Authentication credentials, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
   static const char *epname = "closedir";

   if (XrdxCastor2FS->Proc) {
     XrdxCastor2FS->Stats.IncCmd();
   }

// Release the handle
//
    if (dh && XrdxCastor2FsUFS::CloseDir(dh))
       {XrdxCastor2Fs::Emsg(epname, error, serrno?serrno:EIO, "close directory", fname);
        return SFS_ERROR;
       }

// Do some clean-up
//
   if (fname) free(fname);
   dh = (DIR *)0; 
   return SFS_OK;
}

/******************************************************************************/
/*                F i l e   O b j e c t   I n t e r f a c e s                 */
/******************************************************************************/
/******************************************************************************/
/*                                  o p e n                                   */
/******************************************************************************/

int XrdxCastor2FsFile::open(const char          *path,      // In
                           XrdSfsFileOpenMode   open_mode, // In
                           mode_t               Mode,      // In
                     const XrdSecEntity        *client,    // In
                     const char                *info)      // In
/*
  Function: Open the file `path' in the mode indicated by `open_mode'.  

  Input:    path      - The fully qualified name of the file to open.
            open_mode - One of the following flag values:
                        SFS_O_RDONLY - Open file for reading.
                        SFS_O_WRONLY - Open file for writing.
                        SFS_O_RDWR   - Open file for update
                        SFS_O_CREAT  - Create the file open in RDWR mode
                        SFS_O_TRUNC  - Trunc  the file open in RDWR mode
            Mode      - The Posix access mode bits to be assigned to the file.
                        These bits correspond to the standard Unix permission
                        bits (e.g., 744 == "rwxr--r--"). Mode may also conatin
                        SFS_O_MKPTH is the full path is to be created. The
                        agument is ignored unless open_mode = SFS_O_CREAT.
            client    - Authentication credentials, if any.
            info      - Opaque information to be used as seen fit.

  Output:   Returns OOSS_OK upon success, otherwise SFS_ERROR is returned.
*/
{
   static const char *epname = "open";
   const char *tident = error.getErrUser();
   const char *origpath = path;

   XrdSecEntity mappedclient;
   XrdxCastor2Timing opentiming("fileopen");
   
   XTRACE(open, path,"");

   if (info && strstr(info,"tried")) {
     info = 0;
   }

   // if the clients sends tried info, we have to dump it
   if (info) 
     XTRACE(open, info,"");

   

   TIMING(xCastor2FsTrace,"START",&opentiming);

   MAP((path));
   ROLEMAP(client,info,mappedclient,tident);

   TIMING(xCastor2FsTrace,"MAPPING",&opentiming);

   if (!path) {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
   }

   const int AMode = S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH; // 775
   char *opname;
   int aop=0;

   mode_t acc_mode = Mode & S_IAMB;
   int retc, open_flag = 0;
   struct Cns_filestatcs buf;

   int isRW = 0;
   int isRewrite = 0;

   int crOpts = (Mode & SFS_O_MKPTH ? XRDOSS_mkpath : 0);

   XrdOssDF *fp;
   XrdOucEnv Open_Env(info);

   int rcode=SFS_ERROR;

   XrdOucString redirectionhost="";
   int ecode=0;

   ZTRACE(open, std::hex <<open_mode <<"-" <<std::oct <<Mode <<std::dec <<" fn = " <<path);

// Set the actual open mode and find mode
//

   if (open_mode & SFS_O_CREAT) open_mode = SFS_O_CREAT;
      else if (open_mode & SFS_O_TRUNC) open_mode = SFS_O_TRUNC;

   switch(open_mode & (SFS_O_RDONLY | SFS_O_WRONLY | SFS_O_RDWR |
                       SFS_O_CREAT  | SFS_O_TRUNC))
   {
   case SFS_O_CREAT:  open_flag   = O_RDWR     | O_CREAT  | O_EXCL;
                      crOpts     |= XRDOSS_new;
                      isRW = 1;
                      break;
   case SFS_O_TRUNC:  open_flag  |= O_RDWR     | O_CREAT     | O_TRUNC;
                      isRW = 1;
                      break;
   case SFS_O_RDONLY: open_flag = O_RDONLY; 
                      isRW = 0;
                      break;
   case SFS_O_WRONLY: open_flag = O_WRONLY; 
                      isRW = 1;
                      break;
   case SFS_O_RDWR:   open_flag = O_RDWR;   
                      isRW = 1;
                      break;
   default:           open_flag = O_RDONLY; 
                      isRW = 0;
                      break;
   }

   if (open_flag & O_CREAT) {
     AUTHORIZE(client,&Open_Env,AOP_Create,"create",path,error);
   } else {
     AUTHORIZE(client,&Open_Env,(isRW?AOP_Update:AOP_Read),"open",path,error);
   }
   
   // see if the cluster is in maintenance mode and has delays configured for write/read

   if (!isRW) {
     if (XrdxCastor2FS->xCastor2FsDelayRead) {
       // send a delay response to the client
       int delay=0;
       if (XrdxCastor2FS->xCastor2FsDelayRead <= 0) {
	 delay = 0;
       } else {
	 if (XrdxCastor2FS->xCastor2FsDelayRead > (4*3600)) {
	   // we don't delay for more than 4 hours
	   delay = (4*3600);
	 }
	 
	 XrdOucString adminmessage="";
	 if (XrdxCastor2FS->Proc) {
	   XrdxCastor2ProcFile* pf = XrdxCastor2FS->Proc->Handle("sysmsg"); if (pf) { pf->Read(adminmessage);}
	 }
	 
	 if (adminmessage == "") {
	   adminmessage="system is in maintenance mode for READ";
	 } 
	 TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	 opentiming.Print(xCastor2FsTrace);
	 return XrdxCastor2FS->Stall(error, XrdxCastor2FS->xCastor2FsDelayRead, adminmessage.c_str());
       }
     }
   } else {
     if (XrdxCastor2FS->xCastor2FsDelayWrite) {
       // send a delay response to the client
       int delay=0;
       if (XrdxCastor2FS->xCastor2FsDelayWrite <= 0) {
	 delay = 0;
       } else {
	 if (XrdxCastor2FS->xCastor2FsDelayWrite > (4*3600)) {
	   // we don't delay for more than 4 hours
	   delay = (4*3600);
	 }
	 XrdOucString adminmessage="";
	 if (XrdxCastor2FS->Proc) {
	   XrdxCastor2ProcFile* pf = XrdxCastor2FS->Proc->Handle("sysmsg"); if (pf) { pf->Read(adminmessage);}
	 }
	 
	 if (adminmessage == "") {
	   adminmessage="system is in maintenance mode for WRITE";
	 } 
	 opentiming.Print(xCastor2FsTrace);
	 TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	 return XrdxCastor2FS->Stall(error, XrdxCastor2FS->xCastor2FsDelayWrite, adminmessage.c_str());
       }
     }
   }
     
   TIMING(xCastor2FsTrace,"DELAYCHECK",&opentiming);
   

// Prepare to create or open the file, as needed
//

   if (isRW) {
     if (!(retc = XrdxCastor2FsUFS::Statfn(path, &buf))) {
       isRewrite=1;
     }
   } else {
     // this is the read/stage case
     if ((retc = XrdxCastor2FsUFS::Statfn(path, &buf))) {
       return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat file", path);
     }
     
     if (buf.filesize == 0) {
       //  todo:     return 0 file
     }
     
     if (!retc && !(buf.filemode & S_IFREG)) {
       if (buf.filemode &S_IFDIR) 
	 return XrdxCastor2Fs::Emsg(epname, error, EISDIR, "open directory as file", path);
       else 
	 return XrdxCastor2Fs::Emsg(epname, error, ENOTBLK, "open not regular file", path);
     }
   }

   TIMING(xCastor2FsTrace,"PREOPEN",&opentiming);
 
   uid_t client_uid;
   gid_t client_gid;
   XrdOucString sclient_uid="";
   XrdOucString sclient_gid="";

   GETID(path,mappedclient,client_uid,client_gid);

   sclient_uid += (int) client_uid;
   sclient_gid += (int) client_gid;

   int rc;
   XrdOucString redirectionpfn1="";
   XrdOucString redirectionpfn2="";

   XrdOucString stagehost="";
   XrdOucString serviceclass="default";
   XrdOucString stagestatus="";
   XrdOucString stagevariables="";

   const char* val;

   if (!XrdxCastor2FS->GetStageVariables(path,info,stagevariables,client_uid,client_gid,tident)) {
     return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "set the stager host - you didn't specify a valid one and I have no stager map for fn = ", path);	   
   }


   XrdOucString *policy=NULL;

   if (isRW) {
     const char* val;;
     // we try the user specified or the first possible setting
     if ((val = Open_Env.Get("stagerHost"))) {
       stagehost = val;
     }
     
     if ((val = Open_Env.Get("svcClass"))) {
       serviceclass=val;
     }
     
     if (stagehost.length() && serviceclass.length()) {
       // try the user specified pair
       if ((XrdxCastor2FS->SetStageVariables(path,info,stagevariables, stagehost, serviceclass, XCASTOR2FS_EXACT_MATCH, tident))==1) {
	 // select the policy
	 
	 XrdOucString *wildcardpolicy=NULL;
	 XrdOucString policytag = stagehost; policytag +="::"; policytag += serviceclass;
	 XrdOucString grouppolicytag = policytag;grouppolicytag+="::gid:";grouppolicytag += (int)client_gid;
	 XrdOucString userpolicytag  = policytag;userpolicytag +="::uid:"; userpolicytag += (int)client_uid;
	 
	 XrdOucString wildcardpolicytag = stagehost; wildcardpolicytag +="::"; wildcardpolicytag += "*"; 
	 
	 wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());
	 
	 if (! (policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str()))) 
	   if (! (policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str()))) 
	     policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());
	 
	 if ((!policy) && (wildcardpolicy)) {
	   policy = wildcardpolicy;
	 }
	 
	 if (policy && (strstr(policy->c_str(),"ronly")) && isRW ) {
	   return XrdxCastor2Fs::Emsg(epname, error, EPERM, "write - path is forced readonly for you : fn = ", path); 
	 }
       }else
	 return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "write - cannot find any valid stager/service class mapping for you for fn = ", path); 
     } else {
       int n = 0;
       // try the list and stop when we find a matching policy tag
       bool allowed= false;
       int hasmore=0;
       do {
	 if ((hasmore=XrdxCastor2FS->SetStageVariables(path,info,stagevariables, stagehost, serviceclass,n, tident))==1) {
	   // select the policy
	   policy=NULL;
	   XrdOucString *wildcardpolicy=NULL;
	   XrdOucString policytag = stagehost; policytag +="::"; policytag += serviceclass;
	   XrdOucString grouppolicytag = policytag;grouppolicytag+="::gid:";grouppolicytag += (int)client_gid;
	   XrdOucString userpolicytag  = policytag;userpolicytag +="::uid:"; userpolicytag += (int)client_uid;
	   
	   XrdOucString wildcardpolicytag = stagehost; wildcardpolicytag +="::"; wildcardpolicytag += "*"; 
	   
	   wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());
	   if (! (policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str()))) 
	     if (! (policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str()))) 
	       policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());
	   
	   if ((!policy) && (wildcardpolicy)) {
	     policy = wildcardpolicy;
	   }
	   printf("Policy is %d |%s|\n",policy, policytag.c_str());
	   if (policy && (!strstr(policy->c_str(),"ronly") ) ) {
	     allowed = true;
	     break;
	   }
	 }
	 n++;
       } while((n<999) && (hasmore>=0));
       if (!allowed) {
	 return XrdxCastor2Fs::Emsg(epname, error, EPERM, "write - you are not allowed to do write requests for fn = ", path);	   
       }
     }
   }

   // initialize rpfn2 for not scheduled access
   redirectionpfn2 = 0; 
   redirectionpfn2 += ":"; redirectionpfn2 += stagehost;
   redirectionpfn2 += ":"; redirectionpfn2 += serviceclass;
   redirectionpfn2 += ":0"; 
   redirectionpfn2 += ":0";

   bool nocachelookup=true;

   XrdOucString delaytag = tident;
   delaytag += "::"; delaytag += path;

   if (isRW) {
     if (isRewrite) {
       // the file existed already, first try an Update
     } else {
       // check if we have to execute mkpath
       if ((Mode & SFS_O_MKPTH) || (policy && (strstr(policy->c_str(),"mkpath") ) ) ) {
	 int retc=0;
	 XrdOucString newpath = path;
	 while(newpath.replace("//","/")) {}; 
	 int rpos=STR_NPOS;
	 while ((rpos = newpath.rfind("/",rpos))!=STR_NPOS) {
	   rpos--;
	   struct stat buf;
	   struct Cns_filestatcs cstat;
	   XrdOucString spath;
	   spath.assign(newpath,0,rpos);
	   if (XrdxCastor2FsUFS::Statfn(spath.c_str(),&cstat)) 
	     continue;
	   else {
	     // start to create from here and finish
	     int fpos= rpos+2;
	     while ( (fpos = newpath.find("/",fpos)) != STR_NPOS ) {
	       XrdOucString createpath;
	       createpath.assign(newpath,0,fpos);
	       ZTRACE(open,"Creating Path as uid:" << client_uid << " gid: " << client_gid);
	       mode_t cmask = 0;
	       Cns_umask (cmask);
	       if (XrdxCastor2FsUFS::Mkdir(createpath.c_str(),S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) && (serrno != EEXIST)) {
       		 return XrdxCastor2Fs::Emsg(epname, error, serrno , "create path need dir = ", createpath.c_str());
	       }	   
	       fpos++;
	     }
	     break;
	   }
	 }
       }
     }

     // for all writes
     TIMING(xCastor2FsTrace,"PUT",&opentiming);

     ZTRACE(open, "Doing Put [stagehost="<<stagehost.c_str()<<" serviceclass="<<serviceclass.c_str());
     if (!XrdxCastor2Stager::Put(error, (uid_t) client_uid, (gid_t) client_gid, path, stagehost.c_str(),serviceclass.c_str(),redirectionhost,redirectionpfn1,redirectionpfn2, stagestatus)) {
       // if the file is not yet closed from the last write delay the client
       if (error.getErrInfo() == EBUSY) {
	 TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	 opentiming.Print(xCastor2FsTrace);
	 return XrdxCastor2FS->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()) , "file is still busy");
       }
       TIMING(xCastor2FsTrace,"RETURN",&opentiming);
       opentiming.Print(xCastor2FsTrace);
       return SFS_ERROR;
     }
     
     
     if ( (stagestatus == "STAGEIN") ) {
       TIMING(xCastor2FsTrace,"RETURN",&opentiming);
       opentiming.Print(xCastor2FsTrace);
       return XrdxCastor2FS->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()) , "file is being staged in");
     }
     
     if ( (stagestatus == "STAGEOUT") ) {
       TIMING(xCastor2FsTrace,"RETURN",&opentiming);
       opentiming.Print(xCastor2FsTrace);
       return XrdxCastor2FS->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()), "file is being staged out");
     }
     
     if ( (stagestatus == "INVALID") ) {
       TIMING(xCastor2FsTrace,"RETURN",&opentiming);
       opentiming.Print(xCastor2FsTrace);
       return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "access file in stager (status=INVALID)  fn = ", path);	   
     }
     
     if ( (stagestatus == "PUT_FAILED") ) {
       TIMING(xCastor2FsTrace,"RETURN",&opentiming);
       opentiming.Print(xCastor2FsTrace);
       return XrdxCastor2Fs::Emsg(epname, error, EPERM, "do put in stager (status=PUT_FAILED)  fn = ", path);	   
     }
   } else {
     XrdOucString usedpolicytag="";

     if (nocachelookup) {
       int n = 0;
       bool possible=false;
       XrdOucString possiblestagehost="";
       XrdOucString possibleserviceclass="";

       int hasmore=0;
       // loop through the possible stager settings to find the file somewhere staged
       do {
	 if ((hasmore=XrdxCastor2FS->SetStageVariables(path,info,stagevariables, stagehost, serviceclass, n, tident))==1) {
	   // select the policy
	   policy=NULL;
	   XrdOucString *wildcardpolicy=NULL;
	   XrdOucString policytag = stagehost; policytag +="::"; policytag += serviceclass;
	   XrdOucString grouppolicytag = policytag;grouppolicytag+="::gid:";grouppolicytag += (int)client_gid;
	   XrdOucString userpolicytag  = policytag;userpolicytag +="::uid:"; userpolicytag += (int)client_uid;
	   
	   XrdOucString wildcardpolicytag = stagehost; wildcardpolicytag +="::"; wildcardpolicytag += "*"; 
	   
	   wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());
	   usedpolicytag = userpolicytag.c_str();
	   if (! (policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str()))) {
	     usedpolicytag = grouppolicytag.c_str();
	     if (! (policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str()))) {
	       usedpolicytag = policytag;
	       policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());
	     }
	   }
	   
	   if ((!policy) && (wildcardpolicy)) {
	     policy = wildcardpolicy;
	     usedpolicytag = wildcardpolicytag;
	   }
	   if (policy && (!strstr(policy->c_str(),"nostage") ) ) {
	     // if the policy allows we set this pair for an eventual stagin command
	     if (!possiblestagehost.length()) {
	       possiblestagehost = stagehost;
	       possibleserviceclass = serviceclass;
	     }
	   }
	   if ( policy && (strstr(policy->c_str(),"cache") ) ) {
	     // we try our cache
	     XrdOucString locationfile = XrdxCastor2FS->LocationCacheDir;
	     locationfile += "/"; locationfile+= stagehost; locationfile+="/"; locationfile += serviceclass; 
	     locationfile += path;
	     
	     TIMING(xCastor2FsTrace,"CACHELOOKUP",&opentiming);
	     ZTRACE(open,"Doing cache lookup: "<< locationfile.c_str());
	     
	     char linklookup[8192];
	     int lsize=0;
	     if ((lsize=::readlink(locationfile.c_str(),linklookup,sizeof(linklookup)))>0) {
	       linklookup[lsize]=0;
	       ZTRACE(open,"Cache location: " << linklookup);
	       XrdOucString slinklookup = linklookup;
	       XrdOucString slinkpath;
	       XrdOucString slinkhost;
	       while(slinklookup.replace("%","/")) {}; 
	       int pathposition;
	       if ((slinklookup.length() > 7) && ((pathposition=slinklookup.find("//",7))!=STR_NPOS)) {
		 slinkpath.assign(slinklookup,pathposition + 1);
		 slinkhost.assign(slinklookup,7,pathposition-1);
		 XrdOucString addport =":"; addport += XrdxCastor2FS->xCastor2FsTargetPort.c_str();
 		 slinklookup.insert(addport.c_str(),pathposition);

		 TIMING(xCastor2FsTrace,"CACHESTAT",&opentiming);
		 ZTRACE(open,"Doing file lookup on cached location: " << slinklookup.c_str() << " path: " << slinkpath);
		 
		 EnvPutInt(NAME_CONNECTTIMEOUT,1);
		 EnvPutInt(NAME_REQUESTTIMEOUT,10);
		 EnvPutInt(NAME_MAXREDIRECTCOUNT,1);
		 EnvPutInt(NAME_RECONNECTTIMEOUT,1);
		 EnvPutInt(NAME_FIRSTCONNECTMAXCNT,1);
		 EnvPutInt(NAME_DATASERVERCONN_TTL,60);
		 
		 // this has to be further investigated ... under load we have seen SEGVs during the XrdClientAdmin creation
		 XrdxCastor2FS->ClientMutex.Lock();
		 XrdClientAdmin* newadmin = new XrdClientAdmin(slinklookup.c_str());
		 if (!newadmin) {
		   XrdxCastor2FS->ClientMutex.UnLock();
		   return XrdxCastor2Fs::Emsg(epname, error, ENOMEM, "open file: cannot create client admin to check cached location ", path);	   
		 }
		 
		 newadmin->Connect();
		 XrdxCastor2FS->ClientMutex.UnLock();
		 
		 long id; long long size; long flags; long modtime;
		 if (newadmin->Stat(slinkpath.c_str(),id,size,flags,modtime)) {
		   redirectionhost = slinkhost;
		   redirectionpfn1 = slinkpath;
		   nocachelookup = false;
		 } else {
		   // stat failed, remove location from cache
		   ::unlink(locationfile.c_str());
		 } 
		 delete newadmin;
	       } else {
		 // illegal location
		 ::unlink(locationfile.c_str());
	       }
	     } else {
	       // no cache location, proceed as usual
	     }
	     TIMING(xCastor2FsTrace,"CACHEDONE",&opentiming);
	   }

	   if (!nocachelookup) {
	     possible=true;
	     break;
	   }

	   // query the staging status
	   TIMING(xCastor2FsTrace,"STAGERQUERY",&opentiming);
	   if (!XrdxCastor2Stager::StagerQuery(error, (uid_t) client_uid, (gid_t) client_gid, path, stagehost.c_str(),serviceclass.c_str(),stagestatus)) {
	     stagestatus = "NA";
	   } 
	   
	   if ( (stagestatus == "STAGED") || (stagestatus == "CANBEMIGR") || (stagestatus == "STAGEOUT" ) ) {
	   } else {
	     // check if we want transparent staging
	     if ( policy && ((strstr(policy->c_str(),"nohsm")))) {
	       opentiming.Print(xCastor2FsTrace);
	       n++;
	       continue;
	     }
	   }

	   TIMING(xCastor2FsTrace,"PREP2GET",&opentiming);
	   if (!XrdxCastor2Stager::Prepare2Get(error, (uid_t) client_uid, (gid_t) client_gid, path, stagehost.c_str(),serviceclass.c_str(),redirectionhost,redirectionpfn1,stagestatus)) {
	     TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	     opentiming.Print(xCastor2FsTrace);
	     n++;
	     continue;
	   }
	   
	   ZTRACE(open,"Got |" << redirectionpfn1.c_str() << "| " << stagestatus.c_str());
	   if ( redirectionpfn1.length() && (stagestatus=="READY")) {
	     // that is perfect, we can use the setting to read
	     
	     possible = true;
	     break;
	   } 
	   
	   XrdOucString delaytag = tident;
	   delaytag += "::"; delaytag += path;
	   // we select this setting and tell the client to wait for the staging in this pool/svc class
	   if ( (stagestatus == "READY") ) {
	     TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	     opentiming.Print(xCastor2FsTrace);
	     return XrdxCastor2FS->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()) , "file is being staged in");
	   } else {
	     TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	     opentiming.Print(xCastor2FsTrace);
	     n++;
	     continue;
	   }
	 }
	 n++;
       } while ((n<999) && (hasmore>=0));

       if (!possible) {
	 if (!possiblestagehost.length()) {
	   TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	   opentiming.Print(xCastor2FsTrace);
	   return XrdxCastor2Fs::Emsg(epname, error, ENODATA, "access file in stager (no online copy exists or your stager/svc settings are not allowed by the rules)  fn = ", path);	   
	 }

	 if ( policy && ((strstr(policy->c_str(),"nohsm")) || (strstr(policy->c_str(),"nostage"))) ) {
	   TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	   opentiming.Print(xCastor2FsTrace);
	   return XrdxCastor2Fs::Emsg(epname, error, EPERM, "access file in stager (file is not staged and you are not allowed to stage it anywhere)  fn = ", path);	   
	 }

	 // here we can try to stage the possible stagerhost/service class
	 stagehost    = possiblestagehost;
	 serviceclass = possibleserviceclass;

	 if (!XrdxCastor2Stager::Get(error, (uid_t) client_uid, (gid_t) client_gid, path, stagehost.c_str(),serviceclass.c_str(),redirectionhost,redirectionpfn1,redirectionpfn2, stagestatus)) {
	   TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	   opentiming.Print(xCastor2FsTrace);
	   return SFS_ERROR;
	 }
       
	 if ( (stagestatus == "STAGEIN") ) {
	   TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	   opentiming.Print(xCastor2FsTrace);
	   return XrdxCastor2FS->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()) , "file is being staged in");
	 }

	 if ( (stagestatus == "INVALID") || ( (stagestatus != "STAGED" ) &&
					      (stagestatus != "STAGEOUT" ) &&
					      (stagestatus != "CANBEMIGR") ) ) {
	   
	   TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	   opentiming.Print(xCastor2FsTrace);
	   return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "access file in stager (illegal status after GET)  fn = ", path);	   
	 }
       }

       // if there was no stager get we fill request id 0
       if (!redirectionpfn2.length()) {
	 redirectionpfn2 += ":"; redirectionpfn2 += stagehost;
	 redirectionpfn2 += ":"; redirectionpfn2 += serviceclass;
       }

       // CANBEMIGR or STAGED, STAGEOUT files can be read ;-)
       
       if (policy) {
	 ZTRACE(open,"Policy for "<< usedpolicytag.c_str() << " is " << policy->c_str());
       } else {
	 ZTRACE(open,"Policy for "<< usedpolicytag.c_str() << " is default [read not scheduled]");
       }

       if ( policy && ((strstr(policy->c_str(),"schedread")) || (strstr(policy->c_str(),"schedall"))) ) {
	 // 
	 TIMING(xCastor2FsTrace,"GET",&opentiming);

	 if (!XrdxCastor2Stager::Get(error, (uid_t) client_uid, (gid_t) client_gid, path, stagehost.c_str(),serviceclass.c_str(),redirectionhost,redirectionpfn1,redirectionpfn2, stagestatus)) {
	   TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	   opentiming.Print(xCastor2FsTrace);
	   return SFS_ERROR;
	 }
       
	 if ( (stagestatus == "STAGEIN") ) {
	   TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	   opentiming.Print(xCastor2FsTrace);
	   return XrdxCastor2FS->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()) , "file is being staged in");
	 }
	 
	 if ( (stagestatus == "STAGEOUT") ) {
	   TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	   opentiming.Print(xCastor2FsTrace);
	   return XrdxCastor2FS->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()), "file is being staged out");
	 }
	 
	 if ( (stagestatus == "INVALID") ) {
	   TIMING(xCastor2FsTrace,"RETURN",&opentiming);
	   opentiming.Print(xCastor2FsTrace);
	   return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "access file in stager (status=INVALID)  fn = ", path);	   
	 }
	 
	 // CANBEMIGR or STAGED files can be read ;-)
       }
     }
   }
   
   // read+ write
   printf("Cachellokup %d %d\n",nocachelookup,policy); 
   if ( nocachelookup && (policy && (strstr(policy->c_str(),"cache") ) ) ) {
     // update location in cache
     XrdOucString locationfile = XrdxCastor2FS->LocationCacheDir;
     locationfile += "/"; locationfile+= stagehost; locationfile+="/"; locationfile += serviceclass; 
     locationfile += path;
     
     TIMING(xCastor2FsTrace,"CACHESTORE",&opentiming);
     ZTRACE(open,"Doing cache store: "<< locationfile.c_str());
     
     char linklookup[8192];
     ::unlink(locationfile.c_str());
     
     XrdOucString linklocation = "root://";
     linklocation += redirectionhost; 
     linklocation += "/"; linklocation += redirectionpfn1;
     while(linklocation.replace("/","%")) {}; 
     int rpos=STR_NPOS;
     while ((rpos = locationfile.rfind("/",rpos))!=STR_NPOS) {
       rpos--;
       struct stat buf;
       XrdOucString spath;
       spath.assign(locationfile,0,rpos);
       if (::stat(spath.c_str(),&buf))
	 continue;
       else {
	 // start to create from here and finish
	 int fpos= rpos+1;
	 while ( (fpos = locationfile.find("/",fpos)) != STR_NPOS ) {
	   XrdOucString createpath;
	   createpath.assign(locationfile,0,fpos);
	   if ((::mkdir(createpath.c_str(),S_IRWXU))!=0) {
	     if (errno == ENOTDIR) {
	       // if this was previous a file, we have to remove it and try again
	       ::unlink(createpath.c_str());
	       ::mkdir(createpath.c_str(),S_IRWXU);
	     }
	   }
	   fpos++;
	 }
	 break;
       }
     }
     
     if (::symlink(linklocation.c_str(),locationfile.c_str())) {
       TRACES("warning: cannot create symbolic location link: " << locationfile.c_str() << " |=> " << linklocation.c_str());
     }
   }


    
   //redirectionhost="pcitsmd01.cern.ch";
   XrdOucString nodomainhost="";
   int pos;
   if ((pos = redirectionhost.find(".")) != STR_NPOS) {
     nodomainhost.assign(redirectionhost,0,pos-1);
     if (!XrdxCastor2FS->filesystemhosttable->Find(nodomainhost.c_str())) {
       XrdxCastor2FS->filesystemhosttable->Add(nodomainhost.c_str(),new XrdOucString(nodomainhost.c_str()));
     }
   }

   if (!XrdxCastor2FS->filesystemhosttable->Find(redirectionhost.c_str())) {
     XrdxCastor2FS->filesystemhosttable->Add(redirectionhost.c_str(),new XrdOucString(redirectionhost.c_str()));
   }



   //   return SFS_OK;

   TIMING(xCastor2FsTrace,"REDIRECTION",&opentiming);
   
   ZTRACE(open,"redirection to " << redirectionhost.c_str());

   if (XrdxCastor2FS->Proc) {
     XrdOucString stagersvcclient="";
     XrdOucString stagersvcserver="";
     stagersvcclient=stagehost; stagersvcclient+="::"; stagersvcclient+=serviceclass; stagersvcclient+="::"; stagersvcclient+=mappedclient.name;
     stagersvcserver=stagehost; stagersvcserver+="::"; stagersvcserver+=serviceclass; stagersvcserver+="::"; stagersvcserver+=redirectionhost.c_str();
     
     if (isRW) {
       XrdxCastor2FS->Stats.IncServerWrite(stagersvcserver.c_str());
       XrdxCastor2FS->Stats.IncUserWrite(stagersvcclient.c_str());
     } else {
       XrdxCastor2FS->Stats.IncServerRead(stagersvcserver.c_str());
       XrdxCastor2FS->Stats.IncUserRead(stagersvcclient.c_str());
     }
   }

   rcode = SFS_REDIRECT;

   if (rcode == SFS_REDIRECT) {
     // let's add the opaque authorization information for the server
     // for read & write
     XrdOucString accopaque="";
   
     XrdxCastor2ServerAcc::AuthzInfo authz;
     authz.sfn = (char*)origpath;
     authz.pfn1 = (char*) redirectionpfn1.c_str();
     authz.pfn2 = (char*) redirectionpfn2.c_str();
     //     authz.pfn2 = strdup("-");

     authz.id  = (char*)tident;

     authz.client_sec_uid = STRINGSTORE(sclient_uid.c_str());
     authz.client_sec_gid = STRINGSTORE(sclient_gid.c_str());

     authz.accessop = aop;
     time_t now = time(NULL);
     authz.exptime  = (now + XrdxCastor2FS->TokenLockTime);
     
     if (XrdxCastor2FS->IssueCapability) {
       authz.token = NULL;
       authz.signature= NULL;
     } else {
       authz.token = "";
       authz.signature="";
     }
     
     authz.manager=(char*)XrdxCastor2FS->ManagerId.c_str();
     
     XrdOucString authztoken;
     XrdOucString sb64;
     
     if (!XrdxCastor2ServerAcc::BuildToken(&authz,authztoken)) {
       XrdxCastor2Fs::Emsg(epname, error, retc, "build authorization token for sfn = ", path);	   
       return SFS_ERROR;
     }
     
     authz.token = (char*) authztoken.c_str();

     TIMING(xCastor2FsTrace,"BUILDTOKEN",&opentiming);

     if (XrdxCastor2FS->IssueCapability) {
       //       XrdxCastor2FS->encodeLock.Lock();
       int sig64len=0;
       if(!XrdxCastor2FS->ServerAcc->SignBase64((unsigned char*)authz.token,strlen(authz.token),sb64,sig64len,stagehost.c_str())) {
	 XrdxCastor2Fs::Emsg(epname, error, retc, "sign authorization token for sfn = ", path);	   
	 //	 XrdxCastor2FS->encodeLock.UnLock();
	 return SFS_ERROR;
       }					
       authz.signature = (char*)sb64.c_str();
     }
     
     TIMING(xCastor2FsTrace,"SIGNBASE64",&opentiming);

     if (!XrdxCastor2ServerAcc::BuildOpaque(&authz,accopaque)) {
       XrdxCastor2Fs::Emsg(epname, error, retc, "build authorization opaque string for sfn = ", path);	   
       //       XrdxCastor2FS->encodeLock.UnLock();
       return SFS_ERROR;
     }

     TIMING(xCastor2FsTrace,"BUILDOPAQUE",&opentiming);

     // add the internal token opaque tag
     redirectionhost+= "?";
     // add the external token opaque tag
     redirectionhost+=accopaque;

     if (!isRW) {
       // we always assume adler, but we should check the type here ...
       if (!strcmp(buf.csumtype,"AD")) {
	 redirectionhost+="adler32="; redirectionhost+=buf.csumvalue; redirectionhost+="&";
       }
     }

     //     if (XrdxCastor2FS->xCastor2FsTargetPort != "1094") {
       ecode = atoi(XrdxCastor2FS->xCastor2FsTargetPort.c_str());
       //     }

     error.setErrInfo(ecode,redirectionhost.c_str());
     if (XrdxCastor2FS->IssueCapability) {
       //       XrdxCastor2FS->encodeLock.UnLock();
     }
       
   }

   TIMING(xCastor2FsTrace,"AUTHZ",&opentiming);
   
   // do the proc statistics if a proc file system is specified
   if (XrdxCastor2FS->Proc) {
     if (!isRW) {
       XrdxCastor2FS->Stats.IncRead();
     } else {
       XrdxCastor2FS->Stats.IncWrite();
     }
   }

   // check if a mode was given
   if (Open_Env.Get("mode")) {
     mode_t mode = strtol(Open_Env.Get("mode"),NULL,8);
     if (XrdxCastor2FsUFS::Chmod(path, mode) )
       return XrdxCastor2Fs::Emsg(epname,error,serrno,"set mode on",path);
   }

   // All done.
   //
   TIMING(xCastor2FsTrace,"PROC/DONE",&opentiming);
   opentiming.Print(xCastor2FsTrace);
   ZTRACE(open,"redirection to " << redirectionhost.c_str());
   return rcode;
}

/******************************************************************************/
/*                                 c l o s e                                  */
/******************************************************************************/

int XrdxCastor2FsFile::close()
/*
  Function: Close the file object.

  Input:    None

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
   static const char *epname = "close";

   if (XrdxCastor2FS->Proc) {
     XrdxCastor2FS->Stats.IncCmd();
   }

// Release the handle and return
//
    if (oh >= 0  && XrdxCastor2FsUFS::Close(oh))
       return XrdxCastor2Fs::Emsg(epname, error, serrno, "close", fname);
    oh = -1;
    if (fname) {free(fname); fname = 0;}
    if (ohp != NULL){
      pclose(ohp);
      ohp = NULL;
    }

    if (ohperror_cnt != 0) {
      XrdOucString ohperrorfile = "/tmp/xrdfscmd.";
      ohperrorfile += (int)ohperror_cnt;
      ::unlink(ohperrorfile.c_str());
    }
    
    if (ohperror != NULL) {
      fclose(ohperror);
      ohperror = NULL;
    }
    return SFS_OK;
}

/******************************************************************************/
/*                                  r e a d                                   */
/******************************************************************************/

XrdSfsXferSize XrdxCastor2FsFile::read(XrdSfsFileOffset  offset,    // In
                                      char             *buff,      // Out
                                      XrdSfsXferSize    blen)      // In
/*
  Function: Read `blen' bytes at `offset' into 'buff' and return the actual
            number of bytes read.

  Input:    offset    - The absolute byte offset at which to start the read.
            buff      - Address of the buffer in which to place the data.
            blen      - The size of the buffer. This is the maximum number
                        of bytes that will be read from 'fd'.

  Output:   Returns the number of bytes read upon success and SFS_ERROR o/w.
*/
{
   static const char *epname = "read";
   XrdSfsXferSize nbytes;

   if (XrdxCastor2FS->Proc) {
     XrdxCastor2FS->Stats.IncCmd();
   }

// Make sure the offset is not too large
//
#if _FILE_OFFSET_BITS!=64
   if (offset >  0x000000007fffffff)
      return XrdxCastor2Fs::Emsg(epname, error, EFBIG, "read", fname);
#endif

   // pipe reads
   if (ohp) {
     char linestorage[4096];
     char *lineptr = linestorage;
       
     char* bufptr = buff;
     size_t n = 4096;
     int pn;
     nbytes = 0;
     while( ( pn =  getline(&lineptr, &n, ohp) ) > 0 ) {
       XrdOucString line = lineptr;

       XrdOucString rep1 = "/"; rep1 += XrdxCastor2FS->xCastor2FsName; rep1 += "/";
       if (line.beginswith(rep1))
	 line.replace(rep1,"/");
       

       memcpy(bufptr, line.c_str(),line.length());
       bufptr[line.length()] = 0;
       nbytes += line.length();
       bufptr += line.length();
       if (nbytes > (blen-1024))
	 break;

       // read something from stderr if, existing
       if (ohperror)
       {
	 while ( ( pn = getline(&lineptr, &n, ohperror) ) > 0) {
	   XrdOucString line = "__STDERR__>";
	   line += lineptr;

	   XrdOucString rep1 = "/"; rep1 += XrdxCastor2FS->xCastor2FsName; rep1 += "/";
	   line.replace(rep1,"/");
       
	   memcpy(bufptr, line.c_str(),line.length());
	   bufptr[line.length()] = 0;
	   nbytes += line.length();
	   bufptr += line.length();
	   if (nbytes > (blen-1024))
	     break;
	 }
       }
     }

     // read something from stderr if, existing
     if (ohperror)
     while ( ( pn = getline(&lineptr, &n, ohperror) ) > 0) {
       XrdOucString line = "__STDERR__>";
       line += lineptr;

       XrdOucString rep1 = "/"; rep1 += XrdxCastor2FS->xCastor2FsName; rep1 += "/";
       line.replace(rep1,"/");
       
       memcpy(bufptr, line.c_str(),line.length());
       bufptr[line.length()] = 0;
       nbytes += line.length();
       bufptr += line.length();
       if (nbytes > (blen-1024))
	 break;
     }
     
     return nbytes;
   }

// Read the actual number of bytes
//
   do { nbytes = pread(oh, (void *)buff, (size_t)blen, (off_t)offset); }
        while(nbytes < 0 && errno == EINTR);

   if (nbytes  < 0) {
     return XrdxCastor2Fs::Emsg(epname, error, errno, "read", fname);
   }

// Return number of bytes read
//
   return nbytes;
}
  
/******************************************************************************/
/*                              r e a d   A I O                               */
/******************************************************************************/
  
int XrdxCastor2FsFile::read(XrdSfsAio *aiop)
{

   if (XrdxCastor2FS->Proc) {
     XrdxCastor2FS->Stats.IncCmd();
   }

// Execute this request in a synchronous fashion
//

   aiop->Result = this->read((XrdSfsFileOffset)aiop->sfsAio.aio_offset,
                                       (char *)aiop->sfsAio.aio_buf,
                               (XrdSfsXferSize)aiop->sfsAio.aio_nbytes);
   aiop->doneRead();
   return 0;
}

/******************************************************************************/
/*                                 w r i t e                                  */
/******************************************************************************/

XrdSfsXferSize XrdxCastor2FsFile::write(XrdSfsFileOffset   offset,    // In
                                       const char        *buff,      // In
                                       XrdSfsXferSize     blen)      // In
/*
  Function: Write `blen' bytes at `offset' from 'buff' and return the actual
            number of bytes written.

  Input:    offset    - The absolute byte offset at which to start the write.
            buff      - Address of the buffer from which to get the data.
            blen      - The size of the buffer. This is the maximum number
                        of bytes that will be written to 'fd'.

  Output:   Returns the number of bytes written upon success and SFS_ERROR o/w.

  Notes:    An error return may be delayed until the next write(), close(), or
            sync() call.
*/
{
   static const char *epname = "write";
   XrdSfsXferSize nbytes;

   if (XrdxCastor2FS->Proc) {
     XrdxCastor2FS->Stats.IncCmd();
   }

// Make sure the offset is not too large
//
#if _FILE_OFFSET_BITS!=64
   if (offset >  0x000000007fffffff)
      return XrdxCastor2Fs::Emsg(epname, error, EFBIG, "write", fname);
#endif

// Write the requested bytes
//
   do { nbytes = pwrite(oh, (void *)buff, (size_t)blen, (off_t)offset); }
        while(nbytes < 0 && errno == EINTR);

   if (nbytes  < 0)
      return XrdxCastor2Fs::Emsg(epname, error, errno, "write", fname);

// Return number of bytes written
//
   return nbytes;
}

/******************************************************************************/
/*                             w r i t e   A I O                              */
/******************************************************************************/
  
int XrdxCastor2FsFile::write(XrdSfsAio *aiop)
{

// Execute this request in a synchronous fashion
//

   aiop->Result = this->write((XrdSfsFileOffset)aiop->sfsAio.aio_offset,
                                        (char *)aiop->sfsAio.aio_buf,
                                (XrdSfsXferSize)aiop->sfsAio.aio_nbytes);
   aiop->doneWrite();
   return 0;
}
  
/******************************************************************************/
/*                                  s t a t                                   */
/******************************************************************************/

int XrdxCastor2FsFile::stat(struct stat     *buf)         // Out
/*
  Function: Return file status information

  Input:    buf         - The stat structiure to hold the results

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
   static const char *epname = "stat";

   // fake for pipe reads
   if (ohp) {
     memset(buf, sizeof(struct stat), 0);
     buf->st_size = 1024*1024*1024;
     return SFS_OK;
   }

// Execute the function
//
   struct Cns_filestatcs cstat;

   if (XrdxCastor2FsUFS::Statfn(fname, &cstat)) {
     return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", fname);
   }

// All went well
//
   
   memset(buf, sizeof(struct stat),0);
   
   buf->st_dev     = 0xcaff;      
   buf->st_ino     = cstat.fileid;
   buf->st_mode    = cstat.filemode;
   buf->st_nlink   = cstat.nlink; 
   buf->st_uid     = cstat.uid; 
   buf->st_gid     = cstat.gid;
   buf->st_rdev    = 0;     /* device type (if inode device) */
   buf->st_size    = cstat.filesize;
   buf->st_blksize = 4096;
   buf->st_blocks  = cstat.filesize/ 4096;
   buf->st_atime   = cstat.atime;
   buf->st_mtime   = cstat.mtime;
   buf->st_ctime   = cstat.ctime;

   return SFS_OK;
}

/******************************************************************************/
/*                                  s y n c                                   */
/******************************************************************************/

int XrdxCastor2FsFile::sync()
/*
  Function: Commit all unwritten bytes to physical media.

  Input:    None

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
   static const char *epname = "sync";

   if (XrdxCastor2FS->Proc) {
     XrdxCastor2FS->Stats.IncCmd();
   }

// Perform the function
//
   if (fsync(oh))
      return XrdxCastor2Fs::Emsg(epname,error,errno,"synchronize",fname);

// All done
//
   return SFS_OK;
}

/******************************************************************************/
/*                              s y n c   A I O                               */
/******************************************************************************/
  
int XrdxCastor2FsFile::sync(XrdSfsAio *aiop)
{

// Execute this request in a synchronous fashion
//

   aiop->Result = this->sync();
   aiop->doneWrite();
   return 0;
}

/******************************************************************************/
/*                              t r u n c a t e                               */
/******************************************************************************/

int XrdxCastor2FsFile::truncate(XrdSfsFileOffset  flen)  // In
/*
  Function: Set the length of the file object to 'flen' bytes.

  Input:    flen      - The new size of the file.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.

  Notes:    If 'flen' is smaller than the current size of the file, the file
            is made smaller and the data past 'flen' is discarded. If 'flen'
            is larger than the current size of the file, a hole is created
            (i.e., the file is logically extended by filling the extra bytes 
            with zeroes).
*/
{
   static const char *epname = "trunc";

   if (XrdxCastor2FS->Proc) {
     XrdxCastor2FS->Stats.IncCmd();
   }

// Make sure the offset is not too larg
//
#if _FILE_OFFSET_BITS!=64
   if (flen >  0x000000007fffffff)
      return XrdxCastor2Fs::Emsg(epname, error, EFBIG, "truncate", fname);
#endif

// Perform the function
//
   if (ftruncate(oh, flen))
      return XrdxCastor2Fs::Emsg(epname, error, errno, "truncate", fname);

// All done
//
   return SFS_OK;
}

/******************************************************************************/
/*                       F s c m d  I n t e r f a c e                         */
/******************************************************************************/

// int
// XrdxCastor2FsFile::Fscmd(const char* path,  const char* path2, const char* origpath, const XrdSecEntity *client,  XrdOucErrInfo &error, const char* info) {
//   const char* epname = "Fscmd";
//   const char* tident = error.getErrUser();
//   static int fscmd_cnt=0;

//   fscmdLock.Lock();
//   fscmd_cnt++;
//   fscmdLock.UnLock();
//   ohperror_cnt = fscmd_cnt;

//   if (XrdxCastor2FS->Proc) {
//     XrdxCastor2FS->Stats.IncCmd();
//   }

//   XrdOucString ohperrorfile = "/tmp/xrdfscmd.";
//   ohperrorfile += (int)ohperror_cnt;

//   XrdOucEnv Env(info);
  
//   XrdOucString Cmd = Env.Get("fscmd");
//   XrdOucString RCmd;

//   Cmd.replace("*","\\*");
//   if (Cmd.beginswith("ls ") || (Cmd == "ls") ) {
//     RCmd = "sudo -u ";
//     RCmd += client->name;
//     RCmd += " "; RCmd += Cmd; RCmd += " ";
//     RCmd += path;
//     RCmd += " 2> ";
//     RCmd += ohperrorfile;
//     RCmd += "|| echo __RETC__=$?";

//     XTRACE(open, path,RCmd.c_str());
//     ohp = popen(RCmd.c_str(),"r");
//     if (!ohp) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "list file/directory", path);
//       return SFS_ERROR;
//     }

//     // we need a short delay to wait for the stderr file to be visible in the FS
//     usleep(5000);
//     ohperror = fopen(ohperrorfile.c_str(),"r");
//     if (!ohperror) {
//       // try to wait a little bit and open it again
//       usleep(500000);
//       ohperror = fopen(ohperrorfile.c_str(),"r");
//     }

//     if (!ohperror) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "open stderr for ls", path);
//       if (ohp) {
// 	pclose(ohp);
// 	ohp = NULL;
//       }
//       return SFS_ERROR;
//     }
//   }
  
//   if (Cmd.beginswith("stat ") || (Cmd == "stat")) {
//     RCmd = "sudo -u ";
//     RCmd += client->name;
//     RCmd += " "; RCmd += Cmd; RCmd += " ";
//     RCmd += path;

//     RCmd += " 2> ";
//     RCmd += ohperrorfile;
//     RCmd += "|| echo __RETC__=$?";

//     XTRACE(open, path,RCmd.c_str());
//     ohp = popen(RCmd.c_str(),"r");
//     if (!ohp) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "stat file/directory", path);
//       return SFS_ERROR;
//     }

//     // we need a short delay to wait for the stderr file to be visible in the FS
//     usleep(5000);
//     ohperror = fopen(ohperrorfile.c_str(),"r");
//     if (!ohperror) {
//       // try to wait a little bit and open it again
//       usleep(500000);
//       ohperror = fopen(ohperrorfile.c_str(),"r");
//     }
//     if (!ohperror) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "open stderr for stat", path);
//       if (ohp) {
// 	pclose(ohp);
// 	ohp = NULL;
//       }
//       return SFS_ERROR;
//     }
//   }

//   if (Cmd.beginswith("mkdir ") || (Cmd == "mkdir")) {
//     RCmd = "sudo -u ";
//     RCmd += client->name;
//     RCmd += " "; RCmd += Cmd; RCmd += " ";
//     RCmd += path;
//     if (path2) {
//       RCmd += " "; RCmd += path2;
//     }

//     RCmd += " 2> ";
//     RCmd += ohperrorfile;
//     RCmd += "|| echo __RETC__=$?";
//     XTRACE(open, path,RCmd.c_str());
//     ohp = popen(RCmd.c_str(),"r");
//     if (!ohp) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "mkdir file/directory", path);
//       return SFS_ERROR;
//     }
//     // we need a short delay to wait for the stderr file to be visible in the FS
//     usleep(5000);
//     ohperror = fopen(ohperrorfile.c_str(),"r");
//     if (!ohperror) {
//       // try to wait a little bit and open it again
//       usleep(500000);
//       ohperror = fopen(ohperrorfile.c_str(),"r");
//     }
//     if (!ohperror) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "open stderr for mkdir", path);
//       if (ohp) {
// 	pclose(ohp);
// 	ohp = NULL;
//       }
//       return SFS_ERROR;
//     }
//   }

//   if (Cmd.beginswith("rmdir ") || (Cmd == "rmdir")) {
//     RCmd = "sudo -u ";
//     RCmd += client->name;
//     RCmd += " "; RCmd += Cmd; RCmd += " ";
//     RCmd += path;
//     if (path2) {
//       RCmd += " "; RCmd += path2;
//     }

//     RCmd += " 2> ";
//     RCmd += ohperrorfile;
//     RCmd += "|| echo __RETC__=$?";

//     XTRACE(open, path,RCmd.c_str());
//     ohp = popen(RCmd.c_str(),"r");
//     if (!ohp) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "rmdir file/directory", path);
//       return SFS_ERROR;
//     }

//     // we need a short delay to wait for the stderr file to be visible in the FS
//     usleep(5000);
//     ohperror = fopen(ohperrorfile.c_str(),"r");
//     if (!ohperror) {
//       // try to wait a little bit and open it again
//       usleep(500000);
//       ohperror = fopen(ohperrorfile.c_str(),"r");
//     }
//     if (!ohperror) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "open stderr for rmdir", path);
//       if (ohp) {
// 	pclose(ohp);
// 	ohp = NULL;
//       }
//       return SFS_ERROR;
//     }
//   }

//   if (Cmd.beginswith("rm ") || (Cmd == "rm")) {
//     RCmd = "sudo -u ";
//     RCmd += client->name;
//     RCmd += " "; RCmd += Cmd; RCmd += " ";
//     RCmd += path;
//     if (path2) {
//       RCmd += " "; RCmd += path2;
//     }

//     RCmd += " 2> ";
//     RCmd += ohperrorfile;
//     RCmd += "|| echo __RETC__=$?";

//     XTRACE(open, path,RCmd.c_str());
//     ohp = popen(RCmd.c_str(),"r");
//     if (!ohp) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "rm file/directory", path);
//       return SFS_ERROR;
//     }

//     // we need a short delay to wait for the stderr file to be visible in the FS
//     usleep(5000);
//     ohperror = fopen(ohperrorfile.c_str(),"r");
//     if (!ohperror) {
//       // try to wait a little bit and open it again
//       usleep(500000);
//       ohperror = fopen(ohperrorfile.c_str(),"r");
//     }
//     if (!ohperror) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "open stderr for find rm", path);
//       if (ohp) {
// 	pclose(ohp);
// 	ohp = NULL;
//       }
//       return SFS_ERROR;
//     }
//   }

//   if (Cmd.beginswith("chown ") || (Cmd == "chown")) {
//     RCmd = "sudo -u ";
//     RCmd += client->name;
//     RCmd += " "; RCmd += Cmd; RCmd += " ";
//     RCmd += path;
//     if (path2) {
//       RCmd += " "; RCmd += path2;
//     }

//     RCmd += " 2> ";
//     RCmd += ohperrorfile;
//     RCmd += "|| echo __RETC__=$?";

//     XTRACE(open, path,RCmd.c_str());
//     ohp = popen(RCmd.c_str(),"r");
//     if (!ohp) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "chown file/directory", path);
//       return SFS_ERROR;
//     }

//     // we need a short delay to wait for the stderr file to be visible in the FS
//     usleep(5000);
//     ohperror = fopen(ohperrorfile.c_str(),"r");
//     if (!ohperror) {
//       // try to wait a little bit and open it again
//       usleep(500000);
//       ohperror = fopen(ohperrorfile.c_str(),"r");
//     }
//     if (!ohperror) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "open stderr for chown", path);
//       if (ohp) {
// 	pclose(ohp);
// 	ohp = NULL;
//       }
//       return SFS_ERROR;
//     }
//   }

//   if (Cmd.beginswith("chmod ") || (Cmd == "chmod")) {
//     RCmd = "sudo -u ";
//     RCmd += client->name;
//     RCmd += " "; RCmd += Cmd; RCmd += " ";
//     RCmd += path;
//     if (path2) {
//       RCmd += " "; RCmd += path2;
//     }

//     RCmd += " 2> ";
//     RCmd += ohperrorfile;
//     RCmd += "|| echo __RETC__=$?";

//     XTRACE(open, path,RCmd.c_str());
//     ohp = popen(RCmd.c_str(),"r");
//     if (!ohp) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "chmod file/directory", path);
//       return SFS_ERROR;
//     }

//     // we need a short delay to wait for the stderr file to be visible in the FS
//     usleep(5000);
//     ohperror = fopen(ohperrorfile.c_str(),"r");
//     if (!ohperror) {
//       // try to wait a little bit and open it again
//       usleep(500000);
//       ohperror = fopen(ohperrorfile.c_str(),"r");
//     }
//     if (!ohperror) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "open stderr for chmod", path);
//       if (ohp) {
// 	pclose(ohp);
// 	ohp = NULL;
//       }
//       return SFS_ERROR;
//     }
//   }

//   if (Cmd.beginswith("getfacl ") || (Cmd == "getfacl") ) {
//     RCmd = "sudo -u ";
//     RCmd += client->name;
//     RCmd += " "; RCmd += Cmd; RCmd += " ";
//     RCmd += path;

//     RCmd += " 2> ";
//     RCmd += ohperrorfile;
//     RCmd += "|| echo __RETC__=$?";

//     XTRACE(open, path,RCmd.c_str());
//     ohp = popen(RCmd.c_str(),"r");
//     if (!ohp) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "getfacl file/directory", path);
//       return SFS_ERROR;
//     }

//     // we need a short delay to wait for the stderr file to be visible in the FS
//     usleep(5000);
//     ohperror = fopen(ohperrorfile.c_str(),"r");
//     if (!ohperror) {
//       // try to wait a little bit and open it again
//       usleep(500000);
//       ohperror = fopen(ohperrorfile.c_str(),"r");
//     }
//     if (!ohperror) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "open stderr for getfacl", path);
//       if (ohp) {
// 	pclose(ohp);
// 	ohp = NULL;
//       }
//       return SFS_ERROR;
//     }
//   }

//   if (Cmd.beginswith("setfacl ") || (Cmd == "setfacl") ) {
//     RCmd = "sudo -u ";
//     RCmd += client->name;
//     RCmd += " "; RCmd += Cmd; RCmd += " ";
//     RCmd += path;
//     if (path2) {
//       RCmd += " "; RCmd += path2;
//     }

//     RCmd += " 2> ";
//     RCmd += ohperrorfile;
//     RCmd += "|| echo __RETC__=$?";

//     XTRACE(open, path,RCmd.c_str());
//     ohp = popen(RCmd.c_str(),"r");
//     if (!ohp) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "setfacl file/directory", path);
//       return SFS_ERROR;
//     }

//     // we need a short delay to wait for the stderr file to be visible in the FS
//     usleep(5000);
//     ohperror = fopen(ohperrorfile.c_str(),"r");
//     if (!ohperror) {
//       // try to wait a little bit and open it again
//       usleep(500000);
//       ohperror = fopen(ohperrorfile.c_str(),"r");
//     }
//     if (!ohperror) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "open stderr for setfacl", path);
//       if (ohp) {
// 	pclose(ohp);
// 	ohp = NULL;
//       }
//       return SFS_ERROR;
//     }
//   }

//   if (Cmd.beginswith("find ") || (Cmd == "find")) {

//     RCmd = "sudo -u ";
//     RCmd += client->name;
//     RCmd += " find ";
//     Cmd.erasefromstart(5);
//     RCmd += path;
//     RCmd += " ";
//     RCmd += Cmd;
//     RCmd += " 2> ";
//     RCmd += ohperrorfile;
//     RCmd += "|| echo __RETC__=$?";
    
//     XTRACE(open, path,RCmd.c_str());
//     ohp = popen(RCmd.c_str(),"r");
//     if (!ohp) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "find file/directory", path);
//       return SFS_ERROR;
//     }
//     // we need a short delay to wait for the stderr file to be visible in the FS
//     usleep(5000);
//     ohperror = fopen(ohperrorfile.c_str(),"r");
//     if (!ohperror) {
//       // try to wait a little bit and open it again
//       usleep(500000);
//       ohperror = fopen(ohperrorfile.c_str(),"r");
//     }
//     if (!ohperror) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "open stderr for find", path);
//       if (ohp) {
// 	pclose(ohp);
// 	ohp = NULL;
//       }
//       return SFS_ERROR;
//     }
//   }

//   if (Cmd.beginswith("whoami ") || (Cmd == "whoami") ) {
//     RCmd = "echo ";
//     RCmd += client->name;
//     RCmd += "/"; 
//     RCmd += client->role;
//     RCmd += " [";
//     RCmd += client->grps;
//     RCmd += "]";
//     RCmd += " 2> ";
//     RCmd += ohperrorfile;
//     RCmd += "|| echo __RETC__=$?";

//     XTRACE(open, path,RCmd.c_str());
//     ohp = popen(RCmd.c_str(),"r");
//     if (!ohp) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "whoami", "");
//       return SFS_ERROR;
//     }

//     // we need a short delay to wait for the stderr file to be visible in the FS
//     usleep(5000);
//     ohperror = fopen(ohperrorfile.c_str(),"r");
//     if (!ohperror) {
//       // try to wait a little bit and open it again
//       usleep(500000);
//       ohperror = fopen(ohperrorfile.c_str(),"r");
//     }
//     if (!ohperror) {
//       XrdxCastor2Fs::Emsg(epname, error, errno, "open stderr for find file/directory", path);
//       if (ohp) {
// 	pclose(ohp);
// 	ohp = NULL;
//       }
//       return SFS_ERROR;
//     }
//   }

//   return SFS_OK;
// }


/******************************************************************************/
/*         F i l e   S y s t e m   O b j e c t   I n t e r f a c e s          */
/******************************************************************************/
/******************************************************************************/
/*                                 c h m o d                                  */
/******************************************************************************/

int XrdxCastor2Fs::chmod(const char             *path,    // In
                              XrdSfsMode        Mode,    // In
                              XrdOucErrInfo    &error,   // Out
                        const XrdSecEntity *client,  // In
                        const char             *info)    // In
/*
  Function: Change the mode on a file or directory.

  Input:    path      - Is the fully qualified name of the file to be removed.
            einfo     - Error information object to hold error details.
            client    - Authentication credentials, if any.
            info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "chmod";
  const char *tident = error.getErrUser(); 
  mode_t acc_mode = Mode & S_IAMB;
  XrdOucEnv chmod_Env(info);

  XrdSecEntity mappedclient;

  XTRACE(chmod, path,"");

  AUTHORIZE(client,&chmod_Env,AOP_Chmod,"chmod",path,error);

  MAP((path));
  if (!path) {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }
  
  ROLEMAP(client,info,mappedclient,tident);
  
  if (XrdxCastor2FS->Proc) {
    XrdxCastor2FS->Stats.IncCmd();
  }

  // Perform the actual chmod
  //
  if (XrdxCastor2FsUFS::Chmod(path, acc_mode) )
    return XrdxCastor2Fs::Emsg(epname,error,serrno,"change mode on",path);
  
  // All done
  //
  return SFS_OK;
}
  
/******************************************************************************/
/*                                e x i s t s                                 */
/******************************************************************************/
int XrdxCastor2Fs::exists(const char                *path,        // In
                               XrdSfsFileExistence &file_exists, // Out
                               XrdOucErrInfo       &error,       // Out
                         const XrdSecEntity    *client,          // In
                         const char                *info)        // In

{
  static const char *epname = "exists";
  const char *tident = error.getErrUser();
  XrdOucEnv exists_Env(info);

  XrdSecEntity mappedclient;

  XTRACE(exists, path,"");

  AUTHORIZE(client,&exists_Env,AOP_Stat,"execute exists",path,error);

  MAP((path));
  ROLEMAP(client,info,mappedclient,tident);
  if (!path) {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }


  if (XrdxCastor2FS->Proc) {
    XrdxCastor2FS->Stats.IncCmd();
  }
  
  return _exists(path,file_exists,error,&mappedclient,info);
}

int XrdxCastor2Fs::_exists(const char                *path,        // In
                               XrdSfsFileExistence &file_exists, // Out
                               XrdOucErrInfo       &error,       // Out
                         const XrdSecEntity    *client,          // In
                         const char                *info)        // In
/*
  Function: Determine if file 'path' actually exists.

  Input:    path        - Is the fully qualified name of the file to be tested.
            file_exists - Is the address of the variable to hold the status of
                          'path' when success is returned. The values may be:
                          XrdSfsFileExistsIsDirectory - file not found but path is valid.
                          XrdSfsFileExistsIsFile      - file found.
                          XrdSfsFileExistsIsNo        - neither file nor directory.
            einfo       - Error information object holding the details.
            client      - Authentication credentials, if any.
            info        - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.

  Notes:    When failure occurs, 'file_exists' is not modified.
*/
{
   static const char *epname = "exists";
   struct Cns_filestatcs fstat;

// Now try to find the file or directory
//
   if (!XrdxCastor2FsUFS::Statfn(path, &fstat) )
      {     if (S_ISDIR(fstat.filemode)) file_exists=XrdSfsFileExistIsDirectory;
       else if (S_ISREG(fstat.filemode)) file_exists=XrdSfsFileExistIsFile;
       else                             file_exists=XrdSfsFileExistNo;
       return SFS_OK;
      }
   if (serrno == ENOENT)
      {file_exists=XrdSfsFileExistNo;
       return SFS_OK;
      }

// An error occured, return the error info
//
   return XrdxCastor2Fs::Emsg(epname, error, serrno, "locate", path);
}

  
/******************************************************************************/
/*                            g e t V e r s i o n                             */
/******************************************************************************/

const char *XrdxCastor2Fs::getVersion() {return XrdVERSION;}

/******************************************************************************/
/*                                 m k d i r                                  */
/******************************************************************************/
int XrdxCastor2Fs::mkdir(const char             *path,    // In
                              XrdSfsMode        Mode,    // In
                              XrdOucErrInfo    &error,   // Out
                        const XrdSecEntity     *client,  // In
                        const char             *info)    // In
{
   static const char *epname = "mkdir";
   const char *tident = error.getErrUser();
   XrdOucEnv mkdir_Env(info);
   
   XrdSecEntity mappedclient;

   XTRACE(mkdir, path,"");

   
   
   MAP((path));
   ROLEMAP(client,info,mappedclient,tident);
   if (!path) {
     error.setErrInfo(ENOMEDIUM, "No mapping for file name");
     return SFS_ERROR;
   }

   if (XrdxCastor2FS->Proc) {
     XrdxCastor2FS->Stats.IncCmd();
   }

   int r1 = _mkdir(path,Mode,error,&mappedclient,info);
   int r2 = SFS_OK;

   return (r1 | r2);
}


/******************************************************************************/

int XrdxCastor2Fs::_mkdir(const char            *path,    // In
                              XrdSfsMode        Mode,    // In
                              XrdOucErrInfo    &error,   // Out
                        const XrdSecEntity     *client,  // In
                        const char             *info)    // In
/*
  Function: Create a directory entry.

  Input:    path      - Is the fully qualified name of the file to be removed.
            Mode      - Is the POSIX mode setting for the directory. If the
                        mode contains SFS_O_MKPTH, the full path is created.
            einfo     - Error information object to hold error details.
            client    - Authentication credentials, if any.
            info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
   static const char *epname = "mkdir";
   mode_t acc_mode = (Mode & S_IAMB) | S_IFDIR;
   const char *tident = error.getErrUser();

// Create the path if it does not already exist
//
   if (Mode & SFS_O_MKPTH) {
     char actual_path[4096], *local_path, *next_path;
     unsigned int plen;
     struct Cns_filestatcs buf;
     // Extract out the path we should make
     //
     if (!(plen = strlen(path))) return -ENOENT;
     if (plen >= sizeof(actual_path)) return -ENAMETOOLONG;
     strcpy(actual_path, path);
     if (actual_path[plen-1] == '/') actual_path[plen-1] = '\0';
     
     // Typically, the path exist. So, do a quick check before launching into it
     //
     if (!(local_path = rindex(actual_path, (int)'/'))
	 ||  local_path == actual_path) return 0;
     *local_path = '\0';
     if (XrdxCastor2FsUFS::Statfn(actual_path, &buf)) {
       *local_path = '/';
       // Start creating directories starting with the root. Notice that we will not
       // do anything with the last component. The caller is responsible for that.
       //
       local_path = actual_path+1;
       while((next_path = index(local_path, int('/'))))
	 { int rc;
	   *next_path = '\0';
	   if ((rc=XrdxCastor2FsUFS::Mkdir(actual_path,S_IRWXU)) && serrno != EEXIST)
	     return -serrno;
	   // Set acl on directory
	   if (!rc) {
	     if (client) SETACL(actual_path,(*client),0);
	   }
	   
	   *next_path = '/';
	   local_path = next_path+1;
	 }
     }
   }

// Perform the actual creation
//
   if (XrdxCastor2FsUFS::Mkdir(path, acc_mode) && (serrno != EEXIST))
      return XrdxCastor2Fs::Emsg(epname,error,serrno,"create directory",path);
// Set acl on directory
   if (client)SETACL(path,(*client),0); 

// All done
//
    return SFS_OK;
}


// TODO: caution! this function does not set any ownership attributes if no client & error is given !

/******************************************************************************/
/*                                M k p a t h                                 */
/******************************************************************************/
/*
  Function: Create a directory path

  Input:    path        - Is the fully qualified name of the new path.
            mode        - The new mode that each new directory is to have.
            info        - Opaque information, of any.

  Output:   Returns 0 upon success and -errno upon failure.
*/

int XrdxCastor2Fs::Mkpath(const char *path, mode_t mode, const char *info,XrdSecEntity* client, XrdOucErrInfo* error )
{
    char actual_path[4096], *local_path, *next_path;
    unsigned int plen;
    struct Cns_filestatcs buf;
    static const char *epname = "Mkpath";

// Extract out the path we should make
//
   if (!(plen = strlen(path))) return -ENOENT;
   if (plen >= sizeof(actual_path)) return -ENAMETOOLONG;
   strcpy(actual_path, path);
   if (actual_path[plen-1] == '/') actual_path[plen-1] = '\0';

// Typically, the path exist. So, do a quick check before launching into it
//
   if (!(local_path = rindex(actual_path, (int)'/'))
   ||  local_path == actual_path) return 0;
   *local_path = '\0';

   if (!XrdxCastor2FsUFS::Statfn(actual_path, &buf)) return 0;
   *local_path = '/';

// Start creating directories starting with the root. Notice that we will not
// do anything with the last component. The caller is responsible for that.
//
   local_path = actual_path+1;
   while((next_path = index(local_path, int('/')))) {
     *next_path = '\0';
     if (XrdxCastor2FsUFS::Statfn(actual_path, &buf)) {
       if (client && error) {
	 const char *tident = (*error).getErrUser();
	 if ( (XrdxCastor2FS->_mkdir(actual_path,mode,(*error),client,info)) )
	   return -serrno;
       } else {
	    if (XrdxCastor2FsUFS::Mkdir(actual_path,mode) && (serrno != EEXIST))
	      return -serrno;
       }
     }
     // Set acl on directory
     *next_path = '/';
     local_path = next_path+1;
   }

// All done
//
   return 0;
}


/******************************************************************************/
/*            ( s t a g e )     p r e p a r e                                */
/******************************************************************************/
int XrdxCastor2Fs::stageprepare( const char* path, XrdOucErrInfo &error, const XrdSecEntity *client, const char* info) {
  static const char *epname = "stageprepare";
  const char *tident = error.getErrUser();
  XrdSecEntity mappedclient;

  // this is somehow a hack, I don't know why this happens
  if (strchr(info,'?')) {
    *(strchr(info,'?')) = 0;
  }

  ZTRACE(stager, "path=" << path << " info=" << info);

  XrdOucEnv Open_Env(info);
  
  XrdxCastor2Timing preparetiming("stageprepare");

  MAP((path));
  if (!path) {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }
  
  ROLEMAP(client,info,mappedclient,tident);
  
  struct Cns_filestatcs cstat;
  
  if (XrdxCastor2FsUFS::Statfn(path, &cstat) ) {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", path);
  }
  
  uid_t client_uid;
  gid_t client_gid;
  
  GETID(path,mappedclient,client_uid,client_gid);
    
  int rc;
  XrdOucString stagehost="";
  XrdOucString serviceclass="default";
  XrdOucString stagestatus="";
  XrdOucString stagevariables="";

  if (!XrdxCastor2FS->GetStageVariables(path,info,stagevariables,client_uid,client_gid,tident)) {
    return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "set the stager host - you didn't specify it and I have no stager map for fn = ", path);	   
  }
  
  int n=0; 
  const char* val;
  // try with the stagehost from the environment
  if ((val = Open_Env.Get("stagerHost"))) {
    stagehost = val;
  }
  
  if ((val = Open_Env.Get("svcClass"))) {
    serviceclass=val;
  }

  if (stagehost.length() && serviceclass.length()) {
    // try the user specified pair
    if ((XrdxCastor2FS->SetStageVariables(path,info,stagevariables, stagehost, serviceclass, XCASTOR2FS_EXACT_MATCH, tident))==1) {
      // select the policy
      XrdOucString *policy=NULL;
      XrdOucString *wildcardpolicy=NULL;
      XrdOucString policytag = stagehost; policytag +="::"; policytag += serviceclass;
      XrdOucString grouppolicytag = policytag;grouppolicytag+="::gid:";grouppolicytag += (int)client_gid;
      XrdOucString userpolicytag  = policytag;userpolicytag +="::uid:"; userpolicytag += (int)client_uid;

      XrdOucString wildcardpolicytag = stagehost; wildcardpolicytag +="::"; wildcardpolicytag += "*"; 
      
      wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());
      if (! (policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str()))) 
	if (! (policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str()))) 
	  policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());
      
      if ((!policy) && (wildcardpolicy)) {
	policy = wildcardpolicy;
      }
      if (policy && (strstr(policy->c_str(),"nostage") ) ) {
	return XrdxCastor2Fs::Emsg(epname, error, EPERM, "do prepare request - you are not allowed to do stage requests fn = ", path);	   
      }
    } else {
      return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "write - cannot find any valid stager/service class mapping for you for fn = ", path); 
    }
  } else {
    // try the list and stop when we find a matching policy tag
    bool allowed= false;
    int hasmore=0;

    do {
      if ((hasmore=XrdxCastor2FS->SetStageVariables(path,info,stagevariables, stagehost, serviceclass,n, tident))==1) {
	// select the policy
	XrdOucString *policy=NULL;
	XrdOucString *wildcardpolicy=NULL;
	XrdOucString policytag = stagehost; policytag +="::"; policytag += serviceclass;
	XrdOucString grouppolicytag = policytag;grouppolicytag+="::gid:";grouppolicytag += (int)client_gid;
	XrdOucString userpolicytag  = policytag;userpolicytag +="::uid:"; userpolicytag += (int)client_uid;
	
	XrdOucString wildcardpolicytag = stagehost; wildcardpolicytag +="::"; wildcardpolicytag += "*"; 
	
	wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());
	if (! (policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str()))) 
	  if (! (policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str()))) 
	    policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());
	
	if ((!policy) && (wildcardpolicy)) {
	  policy = wildcardpolicy;
	}
	if (policy && (!strstr(policy->c_str(),"nostage") ) ) {
	  allowed = true;
	  break;
	}
      }
      n++;
    } while((n<999) && (hasmore>=0));
    if (!allowed) {
      return XrdxCastor2Fs::Emsg(epname, error, EPERM, "do prepare request - you are not allowed to do stage requests fn = ", path);	   
    }
  }

  // here we have the allowed stager/service class setting to issue the prepare request

  TIMING(xCastor2FsTrace,"STAGERQUERY",&preparetiming);  
  XrdOucString dummy1;
  XrdOucString dummy2;

  if (!XrdxCastor2Stager::Prepare2Get(error, (uid_t) client_uid, (gid_t) client_gid, path, stagehost.c_str(),serviceclass.c_str(),dummy1, dummy2, stagestatus)) {
    TIMING(xCastor2FsTrace,"END",&preparetiming);  
    preparetiming.Print(xCastor2FsTrace);
    return SFS_ERROR;
  } 
  TIMING(xCastor2FsTrace,"END",&preparetiming);  
  preparetiming.Print(xCastor2FsTrace);
  return SFS_OK;
} 

int XrdxCastor2Fs::prepare( XrdSfsPrep       &pargs,
			   XrdOucErrInfo    &error,
			   const XrdSecEntity *client)
{
  static const char *epname = "prepare";
  const char *tident = error.getErrUser();  

  XrdxCastor2Timing preparetiming("prepare");

  TIMING(xCastor2FsTrace,"START",&preparetiming);  

  // the security mechanism here is to allow only hosts which are defined by fsdisk
  XrdOucString reducedTident,hostname, stident;
  reducedTident=""; hostname = ""; stident = tident;
  int dotpos = stident.find(".");
  reducedTident.assign(tident,0,dotpos-1);reducedTident += "@";
  int adpos  = stident.find("@"); hostname.assign(tident,adpos+1); reducedTident += hostname;
  
  bool allowed = false;

  if (XrdxCastor2FS->Proc) {
    XrdxCastor2FS->Stats.IncCmd();
  }

  ZTRACE(prepare,"checking tident" << tident << " hostname " << hostname);
  
  if (!filesystemhosttable->Find(hostname.c_str()))  {
    // this is not a trusted disk server, assume the 'traditional' xrootd prepare
    XrdOucTList *tp = pargs.paths;
    XrdOucTList *op = pargs.oinfo; 
    // Run through the paths to make sure client can read each one
    //
    while(tp) {
      int retc;
      if ( (retc=stageprepare(tp->text,error, client, op?op->text:NULL))!=SFS_OK) {
	return retc;
      }
      if (tp)
	tp= tp->next;
      else
	break;
      if (op)
	op = op->next;

    }
    return SFS_OK;
  }

  if (! pargs.paths->text || !pargs.oinfo->text) {
    ZTRACE(prepare,"ignoring prepare request");
    return SFS_OK;
  }
  
  ZTRACE(prepare,"updating opaque: "<< pargs.oinfo->text);

  XrdOucEnv oenv(pargs.oinfo->text);
  
  const char* path = pargs.paths->text;
  MAP((path));

  TIMING(xCastor2FsTrace,"CNSID",&preparetiming);  

  // this is currently not needed anymore
  if (oenv.Get("prepareformigration")) {
    char cds[4096];
    char nds[4096];
    char* acpath;

    TIMING(xCastor2FsTrace,"CNSSELSERV",&preparetiming);  
    Cns_selectsrvr(path, cds, nds,&acpath);
    ZTRACE(prepare,"nameserver is "<< nds);

    struct Cns_filestatcs cstat;
    
    TIMING(xCastor2FsTrace,"CNSSTAT",&preparetiming);  
    if (XrdxCastor2FsUFS::Statfn(path, &cstat) ) {
      return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", path);
    }
    
    char fileid[4096];
    sprintf(fileid,"%ld",cstat.fileid);
    TIMING(xCastor2FsTrace,"PREP4MIG",&preparetiming);  
    if (!XrdxCastor2Stager::PrepareForMigration(error, path,oenv.Get("reqid"), fileid, nds, oenv.Get("stagehost"), oenv.Get("serviceclass"), oenv.Get("size"), oenv.Get("mtime"))) {
      TIMING(xCastor2FsTrace,"END",&preparetiming);  
      preparetiming.Print(xCastor2FsTrace);
      return SFS_ERROR;
    }
  }

  // this is currently needed 
  if (oenv.Get("firstbytewritten")) {
    char cds[4096];
    char nds[4096];
    char* acpath;

    TIMING(xCastor2FsTrace,"CNSSELSERV",&preparetiming);  
    Cns_selectsrvr(path, cds, nds,&acpath);
    ZTRACE(prepare,"nameserver is "<< nds);

    struct Cns_filestatcs cstat;
    
    TIMING(xCastor2FsTrace,"CNSSTAT",&preparetiming);  
    if (XrdxCastor2FsUFS::Statfn(path, &cstat) ) {
      return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", path);
    }
    
    char fileid[4096];
    sprintf(fileid,"%ld",cstat.fileid);
    TIMING(xCastor2FsTrace,"1STBYTEW",&preparetiming);  
    if (!XrdxCastor2Stager::FirstWrite(error, path,oenv.Get("reqid"), oenv.Get("stagehost"), oenv.Get("serviceclass"),fileid, nds)) {
      TIMING(xCastor2FsTrace,"END",&preparetiming);  
      preparetiming.Print(xCastor2FsTrace);
      return SFS_ERROR;
    }
  }

  if (oenv.Get("filesize") && oenv.Get("mtime")) {
    off_t filesize = strtoll(oenv.Get("filesize"),NULL,0);
    time_t mtime   = (time_t) strtol(oenv.Get("mtime"),NULL,0);
    XrdxCastor2FsUFS::SetId(0,0);
    if (XrdxCastor2FsUFS::SetFileSize(path,filesize,mtime)) {
      return XrdxCastor2Fs::Emsg(epname, error, serrno, "setfilesize",path);
    }
  }


  // this is currently not needed anymore
  if (oenv.Get("getupdatedone")) {
    char cds[4096];
    char nds[4096];
    char* acpath;

    TIMING(xCastor2FsTrace,"CNSSELSERV",&preparetiming);  
    Cns_selectsrvr(path, cds, nds,&acpath);
    ZTRACE(prepare,"nameserver is "<< nds);

    struct Cns_filestatcs cstat;
    
    TIMING(xCastor2FsTrace,"CNSSTAT",&preparetiming);  
    if (XrdxCastor2FsUFS::Statfn(path, &cstat) ) {
      return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", path);
    }
    
    char fileid[4096];
    sprintf(fileid,"%ld",cstat.fileid);
    TIMING(xCastor2FsTrace,"GETUPDDONE",&preparetiming);  
    if (!XrdxCastor2Stager::UpdateDone(error, path,oenv.Get("reqid"), oenv.Get("stagehost"), oenv.Get("serviceclass"),fileid, nds)) {
      TIMING(xCastor2FsTrace,"END",&preparetiming);  
      preparetiming.Print(xCastor2FsTrace);
      return SFS_ERROR;
    }
  }

  TIMING(xCastor2FsTrace,"END",&preparetiming);  
  preparetiming.Print(xCastor2FsTrace);
  return SFS_OK;
}


/******************************************************************************/
/*                                   r e m                                    */
/******************************************************************************/
  
int XrdxCastor2Fs::rem(const char             *path,    // In
                            XrdOucErrInfo    &error,   // Out
                      const XrdSecEntity *client,  // In
                      const char             *info)    // In
/*
  Function: Delete a file from the namespace.

  Input:    path      - Is the fully qualified name of the file to be removed.
            einfo     - Error information object to hold error details.
            client    - Authentication credentials, if any.
            info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
   static const char *epname = "rem";
   const char *tident = error.getErrUser();
   XrdSecEntity mappedclient;

   int nkept = 0;

   XrdxCastor2Timing rmtiming("fileremove");

   TIMING(xCastor2FsTrace,"START",&rmtiming);

   XTRACE(remove, path,"");

   XrdOucEnv env(info);
   
   AUTHORIZE(client,&env,AOP_Delete,"remove",path,error);

   MAP((path));
   XTRACE(remove, path,"");

   ROLEMAP(client,info,mappedclient,tident);

   if (!path) {
     error.setErrInfo(ENOMEDIUM, "No mapping for file name");
     return SFS_ERROR;
   }

   uid_t client_uid;
   gid_t client_gid;

   GETID(path,mappedclient,client_uid,client_gid);

   if (XrdxCastor2FS->Proc) {
     XrdxCastor2FS->Stats.IncRm();
   }
   
   TIMING(xCastor2FsTrace,"Authorize",&rmtiming);

   if (env.Get("stagerm")) {
     XrdOucString stagevariables="";
     XrdOucString stagehost="";
     XrdOucString serviceclass="";
     if (!XrdxCastor2FS->GetStageVariables(path,info,stagevariables,client_uid,client_gid,tident)) {
       return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "set the stager host - add remove opaque tag stagerm=<..> or specify a valid one because I have no stager map for fn = ", path);	   
     }
     
     int n=0;
     char* val = 0;
     
     // try with the stagehost from the environment
     if ((val = env.Get("stagerHost"))) {
       stagehost = val;
     }
     
     if ((val = env.Get("svcClass"))) {
       serviceclass=val;
     }
     
     if (stagehost.length() && serviceclass.length()) {
       // try the user specified pair
       if ((XrdxCastor2FS->SetStageVariables(path,info,stagevariables, stagehost, serviceclass, XCASTOR2FS_EXACT_MATCH, tident))==1) {
	 // select the policy
	 XrdOucString *policy=NULL;
	 XrdOucString *wildcardpolicy=NULL;
	 XrdOucString policytag = stagehost; policytag +="::"; policytag += serviceclass;
	 XrdOucString grouppolicytag = policytag;grouppolicytag+="::gid:";grouppolicytag += (int)client_gid;
	 XrdOucString userpolicytag  = policytag;userpolicytag +="::uid:"; userpolicytag += (int)client_uid;
	 
	 XrdOucString wildcardpolicytag = stagehost; wildcardpolicytag +="::"; wildcardpolicytag += "*"; 
	 
	 wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());
	 if (! (policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str()))) 
	   if (! (policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str()))) 
	     policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());
	 
	 if ((!policy) && (wildcardpolicy)) {
	   policy = wildcardpolicy;
	 }
	 if (policy && (strstr(policy->c_str(),"nodelete") ) ) {
	   return XrdxCastor2Fs::Emsg(epname, error, EPERM, "do stage_rm request - you are not allowed to do stage_rm requests fn = ", path);	   
	 }
       } else {
	 return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "write - cannot find any valid stager/service class mapping for you for fn = ", path); 
       }
     } else {
       // try the list and stop when we find a matching policy tag
       bool allowed= false;
       int hasmore=0;
       
       do {
	 if ((hasmore=XrdxCastor2FS->SetStageVariables(path,info,stagevariables, stagehost, serviceclass,n, tident))==1) {
	   // select the policy
	   XrdOucString *policy=NULL;
	   XrdOucString *wildcardpolicy=NULL;
	   XrdOucString policytag = stagehost; policytag +="::"; policytag += serviceclass;
	   XrdOucString grouppolicytag = policytag;grouppolicytag+="::gid:";grouppolicytag += (int)client_gid;
	   XrdOucString userpolicytag  = policytag;userpolicytag +="::uid:"; userpolicytag += (int)client_uid;
	   
	   XrdOucString wildcardpolicytag = stagehost; wildcardpolicytag +="::"; wildcardpolicytag += "*"; 
	   
	   wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());
	   if (! (policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str()))) 
	     if (! (policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str()))) 
	       policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());
	   
	   if ((!policy) && (wildcardpolicy)) {
	     policy = wildcardpolicy;
	   }
	   if (policy && (!strstr(policy->c_str(),"nodelete") ) ) {
	     allowed = true;
	     break;
	   }
	 }
	 n++;
       } while((n<999) && (hasmore>=0));
       if (!allowed) {
	 return XrdxCastor2Fs::Emsg(epname, error, EPERM, "do stage_rm request - you are not allowed to do stage_rm requests for fn = ", path);	   
       }
     }

     // here we have the allowed stager/service class setting to issue the stage_rm request
     
     TIMING(xCastor2FsTrace,"STAGERRM",&rmtiming);  

     if (!XrdxCastor2Stager::Rm(error, (uid_t) client_uid, (gid_t) client_gid, path, stagehost.c_str(),serviceclass.c_str())) {
       TIMING(xCastor2FsTrace,"END",&rmtiming);  
       rmtiming.Print(xCastor2FsTrace);
       return SFS_ERROR;
     } 
     // the stage_rm worked, let's proceed with the namespace
   }
   
   int retc = 0;
   retc = _rem(path,error,&mappedclient,info);
   
   TIMING(xCastor2FsTrace,"RemoveNamespace",&rmtiming);
   rmtiming.Print(xCastor2FsTrace);
   return retc;
}


int XrdxCastor2Fs::_rem(   const char             *path,    // In
                               XrdOucErrInfo    &error,   // Out
                         const XrdSecEntity *client,  // In
                         const char             *info)    // In
/*
  Function: Delete a file from the namespace.

  Input:    path      - Is the fully qualified name of the dir to be removed.
            einfo     - Error information object to hold error details.
            client    - Authentication credentials, if any.
            info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
   static const char *epname = "rem";
   // Perform the actual deletion
   //
   const char *tident = error.getErrUser();

   XTRACE(remove, path,"");

   if (XrdxCastor2FsUFS::Rem(path) )
       return XrdxCastor2Fs::Emsg(epname, error, serrno, "remove", path);

// All done
//
    return SFS_OK;

}

/******************************************************************************/
/*                                r e m d i r                                 */
/******************************************************************************/

int XrdxCastor2Fs::remdir(const char             *path,    // In
                               XrdOucErrInfo    &error,   // Out
                         const XrdSecEntity *client,  // In
                         const char             *info)    // In
/*
  Function: Delete a directory from the namespace.

  Input:    path      - Is the fully qualified name of the dir to be removed.
            einfo     - Error information object to hold error details.
            client    - Authentication credentials, if any.
            info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
   static const char *epname = "remdir";
   const char *tident = error.getErrUser();
   XrdOucEnv remdir_Env(info);

   XrdSecEntity mappedclient;

   XTRACE(remove, path,"");

   AUTHORIZE(client,&remdir_Env,AOP_Delete,"remove",path, error);

   MAP((path));
   ROLEMAP(client,info,mappedclient,tident);
   if (!path) {
     error.setErrInfo(ENOMEDIUM, "No mapping for file name");
     return SFS_ERROR;
   }

   if (XrdxCastor2FS->Proc) {
     XrdxCastor2FS->Stats.IncCmd();
   }

   return _remdir(path,error,&mappedclient,info);
}



int XrdxCastor2Fs::_remdir(const char             *path,    // In
                               XrdOucErrInfo    &error,   // Out
                         const XrdSecEntity *client,  // In
                         const char             *info)    // In
/*
  Function: Delete a directory from the namespace.

  Input:    path      - Is the fully qualified name of the dir to be removed.
            einfo     - Error information object to hold error details.
            client    - Authentication credentials, if any.
            info      - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
   static const char *epname = "remdir";
   // Perform the actual deletion
   //
    if (XrdxCastor2FsUFS::Remdir(path) )
       return XrdxCastor2Fs::Emsg(epname, error, serrno, "remove", path);

// All done
//
    return SFS_OK;

}


/******************************************************************************/
/*                                r e n a m e                                 */
/******************************************************************************/

int XrdxCastor2Fs::rename(const char             *old_name,  // In
                         const char             *new_name,  // In
                               XrdOucErrInfo    &error,     //Out
                         const XrdSecEntity *client,    // In
                         const char             *infoO,     // In
                         const char             *infoN)     // In
/*
  Function: Renames a file/directory with name 'old_name' to 'new_name'.

  Input:    old_name  - Is the fully qualified name of the file to be renamed.
            new_name  - Is the fully qualified name that the file is to have.
            error     - Error information structure, if an error occurs.
            client    - Authentication credentials, if any.
            info      - old_name opaque information, if any.
            info      - new_name opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "rename";
  const char *tident = error.getErrUser();

  XrdOucString source, destination;
  XrdOucString oldn,newn;
  XrdSecEntity mappedclient;
  XrdOucEnv renameo_Env(infoO);
  XrdOucEnv renamen_Env(infoN);
  
  AUTHORIZE(client,&renameo_Env,AOP_Update,"rename",old_name, error);
  AUTHORIZE(client,&renamen_Env,AOP_Update,"rename",new_name, error);

  {
    MAP((old_name)); oldn = old_name;
    if (!old_name) {
      error.setErrInfo(ENOMEDIUM, "No mapping for file name");
      return SFS_ERROR;
    }
  }

  {
    MAP((new_name)); newn = new_name;
    if (!new_name) {
      error.setErrInfo(ENOMEDIUM, "No mapping for file name");
      return SFS_ERROR;
    }
  }

  ROLEMAP(client,infoO,mappedclient,tident);

  if (XrdxCastor2FS->Proc) {
    XrdxCastor2FS->Stats.IncCmd();
  }

  int r1,r2;
  
  r1=r2=SFS_OK;


  // check if dest is existing
  XrdSfsFileExistence file_exists;

  if (!_exists(newn.c_str(),file_exists,error,&mappedclient,infoN)) {
    // it exists
    if (file_exists == XrdSfsFileExistIsDirectory) {
      // we have to path the destination name since the target is a directory
      XrdOucString sourcebase = oldn.c_str();
      int npos = oldn.rfind("/");
      if (npos == STR_NPOS) {
	return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "rename", oldn.c_str());
      }
      sourcebase.assign(oldn, npos);
      newn+= "/";
      newn+= sourcebase;
      while (newn.replace("//","/")) {};
    }
    if (file_exists == XrdSfsFileExistIsFile) {
      // remove the target file first!
      int remrc = _rem(newn.c_str(),error,&mappedclient,infoN);
      if (remrc) {
	return remrc;
      }
    }
  }

  r1 = XrdxCastor2FsUFS::Rename(oldn.c_str(), newn.c_str());

  if (r1) 
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "rename", oldn.c_str());

  ZTRACE(rename,"namespace rename done: " << oldn.c_str() << " => " << newn.c_str());

// All done
//
   return SFS_OK;
}
  
/******************************************************************************/
/*                                  s t a t                                   */
/******************************************************************************/

int XrdxCastor2Fs::stat(const char              *path,        // In
                             struct stat       *buf,         // Out
                             XrdOucErrInfo     &error,       // Out
                       const XrdSecEntity  *client,          // In
                       const char              *info)        // In
/*
  Function: Get info on 'path'.

  Input:    path        - Is the fully qualified name of the file to be tested.
            buf         - The stat structiure to hold the results
            error       - Error information object holding the details.
            client      - Authentication credentials, if any.
            info        - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "stat";
  const char *tident = error.getErrUser(); 
  XrdSecEntity mappedclient;

  XrdxCastor2Timing stattiming("filestat");

  XrdOucEnv Open_Env(info);
  
  TIMING(xCastor2FsTrace,"START",&stattiming);

  XTRACE(stat, path,"");

  AUTHORIZE(client,&Open_Env,AOP_Stat,"stat",path,error);

  MAP((path));

  if (!path) {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }
   
  ROLEMAP(client,info,mappedclient,tident);

  TIMING(xCastor2FsTrace,"CNSSTAT",&stattiming);


  struct Cns_filestatcs cstat;

  if (XrdxCastor2FsUFS::Statfn(path, &cstat) ) {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", path);
  }
  
  uid_t client_uid;
  uid_t client_gid;
  
  GETID(path,mappedclient,client_uid,client_gid);

  int rc;
  XrdOucString stagehost="";
  XrdOucString serviceclass="default";
  XrdOucString stagestatus="";
  XrdOucString stagevariables="";
  if (!(Open_Env.Get("nostagerquery"))) {
    if (!S_ISDIR(cstat.filemode)) {
      if (!XrdxCastor2FS->GetStageVariables(path,info,stagevariables,client_uid,client_gid,tident)) {
	return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "set the stager host - you didn't specify it and I have no stager map for fn = ", path);	   
      }
      
      // now loop over all possible stager/svc combinations and to find an online on
      int n = 0;
      // try the list and stop when we find a matching policy tag
      bool allowed= false;
      int hasmore=0;
	  
      do {
	stagestatus = "NA";
	if ((hasmore=XrdxCastor2FS->SetStageVariables(path,info,stagevariables, stagehost, serviceclass,n, tident))==1) {
	  // we don't care about policies for stat's 
	  XrdOucString qrytag="STAGERQUERY-";qrytag+= n;
	  TIMING(xCastor2FsTrace,qrytag.c_str(),&stattiming);  
	  if (!XrdxCastor2Stager::StagerQuery(error, (uid_t) client_uid, (gid_t) client_gid, path, stagehost.c_str(),serviceclass.c_str(),stagestatus)) {
	    stagestatus = "NA";
	  } 
	  
	  if ( (stagestatus == "STAGED") || (stagestatus == "CANBEMIGR") || (stagestatus == "STAGEOUT" ) ) {
	    break;
	  }
	  n++;
	} 
      } while ((n<999)&&(hasmore>=0));  
    }
  }
  if (XrdxCastor2FS->Proc) {
    XrdxCastor2FS->Stats.IncStat();
  }
  
  memset(buf, sizeof(struct stat),0);
  
  buf->st_dev     = 0xcaff;      
  buf->st_ino     = cstat.fileid;
  buf->st_mode    = cstat.filemode;
  buf->st_nlink   = cstat.nlink; 
  buf->st_uid     = cstat.uid; 
  buf->st_gid     = cstat.gid;
  buf->st_rdev    = 0;     /* device type (if inode device) */
  buf->st_size    = cstat.filesize;
  buf->st_blksize = 4096;
  buf->st_blocks  = cstat.filesize/ 4096;
  buf->st_atime   = cstat.atime;
  buf->st_mtime   = cstat.mtime;
  buf->st_ctime   = cstat.ctime;

  if (!S_ISDIR(cstat.filemode)) {
    if ( (stagestatus != "STAGED") && (stagestatus != "CANBEMIGR") && (stagestatus != "STAGEOUT" ) ) {
      // this file is offline
      buf->st_mode = (mode_t) -1;
      //      buf->st_ino   = 0;
      buf->st_dev   = 0;
    }
    XTRACE(stat, path,stagestatus.c_str());
  } 

  TIMING(xCastor2FsTrace,"END",&stattiming);

  stattiming.Print(xCastor2FsTrace);

  // All went well
  //
  return SFS_OK;
}

/******************************************************************************/
/*                                l s t a t                                   */
/******************************************************************************/

int XrdxCastor2Fs::lstat(const char              *path,        // In
			      struct stat       *buf,         // Out
                              XrdOucErrInfo     &error,       // Out
                            const XrdSecEntity  *client,      // In
                        const char              *info)        // In
/*
  Function: Get info on 'path'.

  Input:    path        - Is the fully qualified name of the file to be tested.
            buf         - The stat structiure to hold the results
            error       - Error information object holding the details.
            client      - Authentication credentials, if any.
            info        - Opaque information, if any.

  Output:   Returns SFS_OK upon success and SFS_ERROR upon failure.
*/
{
  static const char *epname = "lstat";
  const char *tident = error.getErrUser(); 
  XrdSecEntity mappedclient;
  XrdOucEnv lstat_Env(info);

  XrdxCastor2Timing stattiming("filelstat");
 
  TIMING(xCastor2FsTrace,"START",&stattiming);

  XTRACE(stat, path,"");

  AUTHORIZE(client,&lstat_Env,AOP_Stat,"lstat",path,error);

  MAP((path));
  if (!path) {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }
   
  ROLEMAP(client,info,mappedclient,tident);
 
  if (!strcmp(path,"/")) {
    buf->st_dev     = 0xcaff;      
    buf->st_ino     = 0;
    buf->st_mode    = S_IFDIR |S_IRUSR | S_IRGRP | S_IROTH|S_IXUSR|S_IXGRP|S_IXOTH ;
    buf->st_nlink   = 1;
    buf->st_uid     = 0;
    buf->st_gid     = 0;
    buf->st_rdev    = 0;     /* device type (if inode device) */
    buf->st_size    = 0;
    buf->st_blksize = 4096;
    buf->st_blocks  = 0;
    buf->st_atime   = 0;
    buf->st_mtime   = 0;
    buf->st_ctime   = 0;
    return SFS_OK;
  }    
  struct Cns_filestat cstat;

  TIMING(xCastor2FsTrace,"CNSLSTAT",&stattiming);

  if (XrdxCastor2FsUFS::Lstatfn(path, &cstat) ) {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "lstat", path);
  }

  if (XrdxCastor2FS->Proc) {
    XrdxCastor2FS->Stats.IncStat();
  }

  memset(buf, sizeof(struct stat),0);
  
  buf->st_dev     = 0xcaff;      
  buf->st_ino     = cstat.fileid;
  buf->st_mode    = cstat.filemode;
  buf->st_nlink   = cstat.nlink; 
  buf->st_uid     = cstat.uid; 
  buf->st_gid     = cstat.gid;
  buf->st_rdev    = 0;     /* device type (if inode device) */
  buf->st_size    = cstat.filesize;
  buf->st_blksize = 4096;
  buf->st_blocks  = cstat.filesize/ 4096;
  buf->st_atime   = cstat.atime;
  buf->st_mtime   = cstat.mtime;
  buf->st_ctime   = cstat.ctime;


  TIMING(xCastor2FsTrace,"END",&stattiming);
  stattiming.Print(xCastor2FsTrace);

  // All went well
  //
  return SFS_OK;
}


int XrdxCastor2Fs::truncate(const char*, 
			   XrdSfsFileOffset, 
			   XrdOucErrInfo&, 
			   const XrdSecEntity*, 
			   const char*)
{
  return SFS_ERROR;
}

/******************************************************************************/
/*                           r e a d l i n k                                  */
/******************************************************************************/

int XrdxCastor2Fs::readlink(const char          *path,        // In
			   XrdOucString        &linkpath,    // Out
			   XrdOucErrInfo       &error,       // Out
			   const XrdSecEntity  *client,       // In
			   const char          *info)        // In
{
  static const char *epname = "readlink";
  const char *tident = error.getErrUser(); 
  XrdSecEntity mappedclient;
  XrdOucEnv rl_Env(info);

  XTRACE(fsctl, path,"");

  AUTHORIZE(client,&rl_Env,AOP_Stat,"readlink",path,error);
  
  MAP((path));
  if (!path) {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }
  
  ROLEMAP(client,info,mappedclient,tident);

  char lp[4097];
  int nlen;


  if ((nlen=XrdxCastor2FsUFS::Readlink(path,lp,4096))==-1) {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "readlink", path);
  }

  lp[nlen]=0;

  linkpath = lp;

  // All went well
  //
  return SFS_OK;
}

/******************************************************************************/
/*                             s y m l i n k                                  */
/******************************************************************************/

int XrdxCastor2Fs::symlink(const char           *path,        // In
			  const char           *linkpath,    // In
			  XrdOucErrInfo        &error,       // Out
			  const XrdSecEntity   *client,      // In
			  const char           *info)        // In
{
  static const char *epname = "symlink";
  const char *tident = error.getErrUser(); 
  XrdSecEntity mappedclient;
  XrdOucEnv sl_Env(info);

  XTRACE(fsctl, path,"");

  XrdOucString source, destination;

  AUTHORIZE(client,&sl_Env,AOP_Create,"symlink",linkpath,error);
  
  // we only need to map absolut links
  source = path;

  if (path[0] == '/'){
    MAP((path)); source = path;
    if (!path) {
      error.setErrInfo(ENOMEDIUM, "No mapping for file name");
      return SFS_ERROR;
    }
  }
  
  {
    MAP((linkpath)); destination = linkpath;
    if (!linkpath) {
      error.setErrInfo(ENOMEDIUM, "No mapping for file name");
      return SFS_ERROR;
    }
  }
  
  ROLEMAP(client,info,mappedclient,tident);

  char lp[4096];

  if (XrdxCastor2FsUFS::Symlink(source.c_str(),destination.c_str())) {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "symlink", source.c_str());
  }


  SETACL(destination.c_str(),mappedclient,1);

  linkpath = lp;

  // All went well
  //
  return SFS_OK;
}

/******************************************************************************/
/*                             a c c e s s                                    */
/******************************************************************************/
int XrdxCastor2Fs::access( const char           *path,        // In
                          int                   mode,        // In
			  XrdOucErrInfo        &error,       // Out
			  const XrdSecEntity   *client,      // In
			  const char           *info)        // In
{
  static const char *epname = "access";
  const char *tident = error.getErrUser(); 
  XrdSecEntity mappedclient;
  XrdOucEnv access_Env(info);

  XTRACE(fsctl, path,"");

  AUTHORIZE(client,&access_Env,AOP_Stat,"access",path,error);
  
  MAP((path));
  if (!path) {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }
  
  ROLEMAP(client,info,mappedclient,tident);

  return SFS_OK;
}

/******************************************************************************/
/*                             u t i m e s                                    */
/******************************************************************************/

int XrdxCastor2Fs::utimes(  const char          *path,        // In
			   struct timeval      *tvp,         // In
			   XrdOucErrInfo       &error,       // Out
			   const XrdSecEntity  *client,       // In
			   const char          *info)        // In
{
  static const char *epname = "utimes";
  const char *tident = error.getErrUser(); 
  XrdSecEntity mappedclient;
  XrdOucEnv utimes_Env(info);

  XTRACE(fsctl, path,"");

  AUTHORIZE(client,&utimes_Env,AOP_Update,"set utimes",path,error);
  
  MAP((path));
  if (!path) {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }
  
  ROLEMAP(client,info,mappedclient,tident);

  if ((XrdxCastor2FsUFS::Utimes(path,tvp))) {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "utimes", path);
  }

  // All went well
  //
  return SFS_OK;
}


/******************************************************************************/
/*                                  E m s g                                   */
/******************************************************************************/

int XrdxCastor2Fs::Emsg(const char    *pfx,    // Message prefix value
                       XrdOucErrInfo &einfo,  // Place to put text & error code
                       int            ecode,  // The error code
                       const char    *op,     // Operation being performed
                       const char    *target) // The target (e.g., fname)
{
    char *etext, buffer[4096], unkbuff[64];

// Get the reason for the error
//
   if (ecode < 0) ecode = -ecode;
   if (!(etext = strerror(ecode)))
      {sprintf(unkbuff, "reason unknown (%d)", ecode); etext = unkbuff;}

// Format the error message
//
    snprintf(buffer,sizeof(buffer),"Unable to %s %s; %s", op, target, etext);

// Print it out if debugging is enabled
//
#ifndef NODEBUG
    eDest->Emsg(pfx, buffer);
#endif

// Place the error message in the error object and return
//
    einfo.setErrInfo(ecode, buffer);

    return SFS_ERROR;
}


/******************************************************************************/
/*                                 S t a l l                                  */
/******************************************************************************/

int XrdxCastor2Fs::Stall(XrdOucErrInfo   &error, // Error text & code
                  int              stime, // Seconds to stall
                  const char      *msg)   // Message to give
{
  XrdOucString smessage = msg;
  smessage += "; come back in ";
  smessage += stime;
  smessage += " seconds!";
  
  EPNAME("Stall");
  const char *tident = error.getErrUser();
  
  ZTRACE(delay, "Stall " <<stime <<": " << smessage.c_str());

  // Place the error message in the error object and return
  //
  error.setErrInfo(0, smessage.c_str());
  
  // All done
  //
  return stime;
}

/******************************************************************************/
/*                      X r d C a t a l o g F s S t a t s                     */
/******************************************************************************/

void
XrdxCastor2FsStats::Update() 
{
  if (Proc) {
    {XrdxCastor2ProcFile* pf = Proc->Handle("read1"); pf && pf->Write(ReadRate(1),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("read60"); pf && pf->Write(ReadRate(60),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("read300"); pf && pf->Write(ReadRate(298),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("write1"); pf && pf->Write(WriteRate(1),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("write60"); pf && pf->Write(WriteRate(60),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("write300"); pf && pf->Write(WriteRate(298),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("stat1"); pf && pf->Write(StatRate(1),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("stat60"); pf && pf->Write(StatRate(60),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("stat300"); pf && pf->Write(StatRate(298),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("readd1"); pf && pf->Write(ReaddRate(1),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("readd60"); pf && pf->Write(ReaddRate(60),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("readd300"); pf && pf->Write(ReaddRate(298),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("rm1"); pf && pf->Write(RmRate(1),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("rm60"); pf && pf->Write(RmRate(60),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("rm300"); pf && pf->Write(RmRate(298),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("cmd1"); pf && pf->Write(CmdRate(1),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("cmd60"); pf && pf->Write(CmdRate(60),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("cmd300"); pf && pf->Write(CmdRate(298),1);}
    {XrdxCastor2ProcFile* pf = Proc->Handle("delayread");  if (pf) {XrdxCastor2FS->xCastor2FsDelayRead  = pf->Read();}}
    {XrdxCastor2ProcFile* pf = Proc->Handle("delaywrite"); if (pf) {XrdxCastor2FS->xCastor2FsDelayWrite = pf->Read();}}
    {XrdxCastor2ProcFile* pf = Proc->Handle("trace"); if (pf) { XrdOucString newtrace; pf->Read(newtrace);xCastor2FsTrace.What = strtol(newtrace.c_str(),NULL,0);}}
    
    {XrdxCastor2ProcFile* pf = Proc->Handle("serverread"); if (pf) {
      // loop over the ServerTable and write keyval pairs for each server
      int cursor=0;
      bool first=true;
      do {
	cursor = XrdxCastor2FS->Stats.ServerTable->Next(cursor);
	if (cursor>=0) {
	  XrdOucString* name = XrdxCastor2FS->Stats.ServerTable->Item(cursor);
	  if (!name) {
	    cursor++;
	    continue;
	  }

	  XrdxCastor2StatULongLong* sval;
	  if ( (sval = XrdxCastor2FS->Stats.ServerRead.Find(name->c_str()) ) ) {
	    // if we don't write in this time bin, we just stop the loop - or if there is an error writing
	    if (!pf->WriteKeyVal(name->c_str(), sval->Get(),2, first))
	      break;
	    first = false;
	  }
	  cursor++;
	}
      } while(cursor >= 0);
    }}

    {XrdxCastor2ProcFile* pf = Proc->Handle("serverwrite"); if (pf) {
      // loop over the ServerTable and write keyval pairs for each server
      int cursor=0;
      bool first=true;
      do {
	cursor = XrdxCastor2FS->Stats.ServerTable->Next(cursor);
	if (cursor>=0) {
	  XrdOucString* name = XrdxCastor2FS->Stats.ServerTable->Item(cursor);
	  if (!name) {
	    cursor++;
	    continue;
	  }

	  XrdxCastor2StatULongLong* sval;
	  if ( (sval = XrdxCastor2FS->Stats.ServerWrite.Find(name->c_str()) ) ) {
	    // if we don't write in this time bin, we just stop the loop - or if there is an error writing
	    if (!pf->WriteKeyVal(name->c_str(), sval->Get(),2, first))
	      break;
	    first = false;
	  }
	  cursor++;
	}
      } while(cursor >= 0);
    }}

    {XrdxCastor2ProcFile* pf = Proc->Handle("userread"); if (pf) {
      // loop over the UserTable and write keyval pairs for each server
      int cursor=0;
      bool first=true;
      do {
	cursor = XrdxCastor2FS->Stats.UserTable->Next(cursor);
	if (cursor>=0) {
	  XrdOucString* name = XrdxCastor2FS->Stats.UserTable->Item(cursor);
	  if (!name) {
	    cursor++;
	    continue;
	  }

	  XrdxCastor2StatULongLong* sval;
	  if ( (sval = XrdxCastor2FS->Stats.UserRead.Find(name->c_str()) ) ) {
	    // if we don't write in this time bin, we just stop the loop - or if there is an error writing
	    if (!pf->WriteKeyVal(name->c_str(), sval->Get(),2, first))
	      break;
	    first = false;
	  }
	  cursor++;
	}
      } while(cursor >= 0);
    }}

    {XrdxCastor2ProcFile* pf = Proc->Handle("userwrite"); if (pf) {
      // loop over the UserTable and write keyval pairs for each server
      int cursor=0;
      bool first=true;
      do {
	cursor = XrdxCastor2FS->Stats.UserTable->Next(cursor);
	if (cursor>=0) {
	  XrdOucString* name = XrdxCastor2FS->Stats.UserTable->Item(cursor);
	  if (!name) {
	    cursor++;
	    continue;
	  }

	  XrdxCastor2StatULongLong* sval;
	  if ( (sval = XrdxCastor2FS->Stats.UserWrite.Find(name->c_str()) ) ) {
	    // if we don't write in this time bin, we just stop the loop - or if there is an error writing
	    if (!pf->WriteKeyVal(name->c_str(), sval->Get(),2, first))
	      break;
	    first = false;
	  }
	  cursor++;
	}
      } while(cursor >= 0);
    }}
  }
}

int            
XrdxCastor2Fs::fsctl(const int               cmd,
		    const char             *args,
		    XrdOucErrInfo    &error,
		    const XrdSecEntity *client)
  
{  
  static const char *epname = "fsctl";
  const char *tident = error.getErrUser(); 

  XrdOucString path = args;
  XrdOucString opaque;
  opaque.assign(path,path.find("?")+1);
  path.erase(path.find("?"));

  XrdOucEnv env(opaque.c_str());
  const char* scmd;

  ZTRACE(fsctl,args);
  if ((cmd == SFS_FSCTL_LOCATE) || ( cmd == 16777217 ) ) {
    // check if this file exists
    XrdSfsFileExistence file_exists;
    if ((_exists(path.c_str(),file_exists,error,client,0)) || (file_exists!=XrdSfsFileExistIsFile)) {
      return SFS_ERROR;
    }
    
    char locResp[4096];
    int  locRlen = 4096;
    struct stat fstat;
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';//(fstat.st_mode & S_IFBLK == S_IFBLK ? 's' : 'S');
    // we don't want to manage writes via global redirection - therefore we mark the files as 'r'
    rType[1] = 'r';//(fstat.st_mode & S_IWUSR            ? 'w' : 'r');
    rType[2] = '\0';
    sprintf(locResp,"[::%s] ",(char*)XrdxCastor2FS->ManagerId.c_str());
    error.setErrInfo(strlen(locResp)+3, (const char **)Resp, 2);
    ZTRACE(fsctl,"located at headnode: " << locResp);
    return SFS_DATA;
  }
  

  if (cmd!=SFS_FSCTL_PLUGIN) {
    return SFS_ERROR;
  }


  if ((scmd = env.Get("pcmd"))) {
    XrdOucString execmd = scmd;

    if (execmd == "stat") {
      struct stat buf;

      int retc = lstat(path.c_str(),
		      &buf,  
		      error, 
		      client,
		      0);
      
      if (retc == SFS_OK) {
	char statinfo[16384];
	// convert into a char stream
	sprintf(statinfo,"stat: %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %u %u %u\n",
		(unsigned long long) buf.st_dev,
		(unsigned long long) buf.st_ino,
		(unsigned long long) buf.st_mode,
		(unsigned long long) buf.st_nlink,
		(unsigned long long) buf.st_uid,
		(unsigned long long) buf.st_gid,
		(unsigned long long) buf.st_rdev,
		(unsigned long long) buf.st_size,
		(unsigned long long) buf.st_blksize,
		(unsigned long long) buf.st_blocks,
		(unsigned long long) buf.st_atime,
		(unsigned long long) buf.st_mtime,
		(unsigned long long) buf.st_ctime);
	error.setErrInfo(strlen(statinfo)+1,statinfo);
	return SFS_DATA;
      }
    }

    if (execmd == "chmod") {
      char* smode;
      if ((smode = env.Get("mode"))) {
	XrdSfsMode newmode = atoi(smode);
	int retc = chmod(path.c_str(),
			newmode,  
			error, 
			client,
			0);
	XrdOucString response="chmod: retc=";
	response += retc;
	error.setErrInfo(response.length()+1,response.c_str());
	return SFS_DATA;
      } else {
	XrdOucString response="chmod: retc=";
	response += EINVAL;
	error.setErrInfo(response.length()+1,response.c_str());
	return SFS_DATA;
      }
    }

    if (execmd == "symlink") {
      char* destination;
      char* source;
      if ((destination = env.Get("linkdest"))) {
	if ((source = env.Get("linksource"))) {
	  int retc = symlink(source,destination,error,client,0);
	  XrdOucString response="sysmlink: retc=";
	  response += retc;
	  error.setErrInfo(response.length()+1,response.c_str());
	  return SFS_DATA;
	}
      }
      XrdOucString response="symlink: retc=";
      response += EINVAL;
      error.setErrInfo(response.length()+1,response.c_str());
      return SFS_DATA;
    }

    if (execmd == "readlink") {
      XrdOucString linkpath="";
      int retc=readlink(path.c_str(),linkpath,error,client,0);
      XrdOucString response="readlink: retc=";
      response += retc;
      response += " link=";
      response += linkpath;
      error.setErrInfo(response.length()+1,response.c_str());
      return SFS_DATA;
    }

    if (execmd == "access") {
      char* smode;
      if ((smode = env.Get("mode"))) {
	int newmode = atoi(smode);
	int retc = access(path.c_str(),newmode, error,client,0);
	XrdOucString response="access: retc=";
	response += retc;
	error.setErrInfo(response.length()+1,response.c_str());
	return SFS_DATA;
      } else {
	XrdOucString response="access: retc=";
	response += EINVAL;
	error.setErrInfo(response.length()+1,response.c_str());
	return SFS_DATA;
      }
    }

    if (execmd == "utimes") {
      char* tv1_sec;
      char* tv1_usec;
      char* tv2_sec;
      char* tv2_usec;

      tv1_sec  = env.Get("tv1_sec");
      tv1_usec = env.Get("tv1_usec");
      tv2_sec  = env.Get("tv2_sec");
      tv2_usec = env.Get("tv2_usec");

      struct timeval tvp[2];
      if (tv1_sec && tv1_usec && tv2_sec && tv2_usec) {
	tvp[0].tv_sec  = strtol(tv1_sec,0,10);
	tvp[0].tv_usec = strtol(tv1_usec,0,10);
	tvp[1].tv_sec  = strtol(tv2_sec,0,10);
	tvp[1].tv_usec = strtol(tv2_usec,0,10);

	int retc = utimes(path.c_str(), 
			  tvp,
			  error, 
			  client,
			  0);

	XrdOucString response="utimes: retc=";
	response += retc;
	error.setErrInfo(response.length()+1,response.c_str());
	return SFS_DATA;
      } else {
	XrdOucString response="utimes: retc=";
	response += EINVAL;
	error.setErrInfo(response.length()+1,response.c_str());
	return SFS_DATA;
      }
    }

  }
  
  return SFS_ERROR;
}
