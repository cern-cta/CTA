//          $Id: XrdxCastor2Config.cc,v 1.3 2009/07/06 08:27:11 apeters Exp $


#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <dirent.h>
#include <string.h>

#include "XrdxCastor2Fs.hh"
#include "XrdxCastor2Trace.hh"
#include "XrdSys/XrdSysDNS.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysPlugin.hh"

extern XrdOucTrace xCastor2FsTrace;

/******************************************************************************/
/*                      L o c a l   V a r i a b l e s                         */
/******************************************************************************/

int XrdxCastor2Fs::Configure(XrdSysError &Eroute) 
{
  char *var;
  const char *val;
  int  cfgFD, retc, NoGo = 0;
  XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"));
  XrdOucString role="server";
  bool xcastor2fsdefined=false;
  bool SubscribeCms=false;
  bool authorize=false;
  AuthLib = "";
  Authorization = 0;

  IssueCapability = false;
  Procfilesystem="";
  ProcfilesystemSync= false;
  xCastor2FsTargetPort = "1094";
  xCastor2FsLocatePolicy = "balanced";
  MapCernCertificates = false;
  xCastor2FsDelayRead = 0;
  xCastor2FsDelayWrite = 0;
  GridMapFile="";
  VomsMapFile="";
  LocationCacheDir="";
  long myPort=0;

  if (getenv("XRDDEBUG")) xCastor2FsTrace.What = TRACE_MOST | TRACE_debug;

  {
    // borrowed from XrdOfs 
    unsigned int myIPaddr = 0;

    char buff[256], *bp;
    int i;
    //char* locResp;
    
    // Obtain port number we will be using
    //
    myPort = (bp = getenv("XRDPORT")) ? strtol(bp, (char **)NULL, 10) : 0;
    
    // Establish our hostname and IPV4 address
    //
    HostName      = XrdSysDNS::getHostName();
    
    if (!XrdSysDNS::Host2IP(HostName, &myIPaddr)) myIPaddr = 0x7f000001;
    strcpy(buff, "[::"); bp = buff+3;
    bp += XrdSysDNS::IP2String(myIPaddr, 0, bp, 128);
    *bp++ = ']'; *bp++ = ':';
    sprintf(bp, "%li", myPort);
    for (i = 0; HostName[i] && HostName[i] != '.'; i++);
    HostName[i] = '\0';
    HostPref = strdup(HostName);
    HostName[i] = '.';
    Eroute.Say("=====> xcastor2.hostname: ", HostName,"");
    Eroute.Say("=====> xcastor2.hostpref: ", HostPref,"");
    ManagerLocation=buff;
    ManagerId=HostName;
    ManagerId+=":";
    ManagerId+=(int)myPort;
    Eroute.Say("=====> xcastor2.managerid: ",ManagerId.c_str(),"");
  }

  if( !ConfigFN || !*ConfigFN) {
    Eroute.Emsg("Config", "Configuration file not specified.");
  } else {
    // Try to open the configuration file.
    //
    if ( (cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
      return Eroute.Emsg("Config", errno, "open config file", ConfigFN);
    Config.Attach(cfgFD);
    // Now start reading records until eof.
    //
    XrdOucString nsin;
    XrdOucString nsout;

    while((var = Config.GetMyFirstWord())) {
      if (!strncmp(var, "all.",4)) {
	var += 4;
	if (!strcmp("role",var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument for all.role missing.");NoGo=1;
	  } else {
	    XrdOucString lrole = val;

	    if ((val = Config.GetWord())) {
	      if (!strcmp(val,"if")) {
		if ((val = Config.GetWord())) {
		  if (!strcmp(val, HostName)) {
		    role = lrole;
		  }
		  if (!strcmp(val, HostPref)) {
		    role = lrole;
		  }
		}
	      } else {
		role = lrole;
	      }
	    } else {
		role = lrole;
	    }
	  }
	}
      }
      if (!strncmp(var, "xcastor2.", 9)) {
	var += 9;
	if (!strcmp("fs",var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument for fs invalid.");NoGo=1;
	  } else {
	    Eroute.Say("=====> xcastor2.fs: ", val,"");
	    xcastor2fsdefined=true;
	    xCastor2FsName = val;
	  }
	}
	if (!strcmp("targetport",var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument for fs invalid.");NoGo=1;
	  } else {
	    Eroute.Say("=====> xcastor2.targetport: ", val,"");
	    xCastor2FsTargetPort = val;
	  }
	}
	if (!strcmp("nsmap",var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument for nsmap invalid.");NoGo=1;
	  } else {
	    nsin = val;
	  }
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument 2 for nsmap missing.");NoGo=1;
	  } else {
	    nsout = val;
	  }
	  XrdOucString sayit;
	  if (!nsin.endswith('/'))
	    nsin += "/";
	  if (!nsout.endswith('/')) 
	    nsout += "/";
	  sayit = nsin;
	  sayit += " -> ";
	  sayit += nsout;
	  Eroute.Say("=====> xcastor2.nsmap: ", sayit.c_str(),"");
	  XrdOucString* mnsout = new XrdOucString(nsout.c_str());
	  nstable->Add(nsin.c_str(),mnsout);
	}

	if (!strcmp("proc",var)) 
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument for proc invalid.");NoGo=1;
	  } else {
	    Procfilesystem = val;
	    Procfilesystem += "/proc";
	    while (Procfilesystem.replace("//","/")) {};

	    if ((val = Config.GetWord())) {
	      if ((!strcmp(val, "sync")) || (!strcmp(val,"async"))) {
		if (!strcmp(val,"sync")) ProcfilesystemSync=true;
		if (!strcmp(val,"async")) ProcfilesystemSync=false;
	      } else {
		Eroute.Emsg("Config","argument for proc invalid. Specify xcastor2.proc [sync|async]");
		NoGo=1;
	      }
	    }
	    if (ProcfilesystemSync) 
	      Eroute.Say("=====> xcastor2.proc: ", Procfilesystem.c_str()," sync");
	    else 
	      Eroute.Say("=====> xcastor2.proc: ", Procfilesystem.c_str()," async");
	  }

	
	if (!strcmp("role",var)) {
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument for role invalid.");NoGo=1;
	  } else {
	    nsin = val;
	  }
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument 2 for role missing.");NoGo=1;
	  } else {
	    nsout = val;
	  } 

	  XrdOucString sayit;
	  sayit = nsin;
	  sayit += " -> ";
	  sayit += nsout;
	  Eroute.Say("=====> xcastor2.role : ", sayit.c_str(),"");
	  XrdOucString* mnsout = new XrdOucString(nsout.c_str());
	  roletable->Add(nsin.c_str(),mnsout);
	}
      }
      if (!strcmp("capability",var)) {
	if (!(val = Config.GetWord())) {
	  Eroute.Emsg("Config","argument 2 for capbility missing. Can be true/lazy/1 or false/0"); NoGo=1;
	} else {
	  if ( (!(strcmp(val,"true"))) || (!(strcmp(val,"1"))) || (!(strcmp(val,"lazy")))) {
	    IssueCapability = true;
	  } else {
	    if ( (!(strcmp(val,"false"))) || (!(strcmp(val,"0")))) {
	      IssueCapability = false;
	    } else {
	      Eroute.Emsg("Config","argument 2 for capbility invalid. Can be <true>/1 or <false>/0"); NoGo=1;
	    }
	  }
	}
      }
      if (!strcmp("tokenlocktime",var)) {
	if (!(val = Config.GetWord())) {
	  Eroute.Emsg("Config","argument 2 for writelocktime missing. Specifies the grace period for a client to show up with a write on a disk server in seconds."); NoGo=1;
	} else {
	  TokenLockTime = atoi(val);
	  if (TokenLockTime == 0) {
	    Eroute.Emsg("Config","argument 2 for writelocktime is 0/illegal. Specify the grace period for a client to show up with a write on a disk server in seconds."); NoGo=1;
	  }
	}
      }
      if (!strcmp("locatepolicy",var)) {
	if ((!(val = Config.GetWord())) || (strcmp("fs",val) && strcmp("twin",val) && strcmp("balanced",val))) {
	  Eroute.Emsg("Config","argument 2 for locatepolicy missing. Can be <fs>, <twin> or <balanced> "); NoGo=1;
	} else {
	  xCastor2FsLocatePolicy = val;
	}
      }
      
      if (!strcmp("mapcerncertificates",var)) {
	if ((!(val = Config.GetWord())) || (strcmp("true",val) && strcmp("false",val) && strcmp("1",val) && strcmp("0",val))) {
	  Eroute.Emsg("Config","argument 2 for mapcerncertificates illegal or missing. Must be <true>,<false>,<1> or <0>!"); NoGo=1;} else {
	    if ((!strcmp("true",val) || (!strcmp("1",val)))) {
	      MapCernCertificates=true;
	    }
	  }
      }


      if (!strcmp("gridmapfile",var)) {
	if ((!(val = Config.GetWord())) || (::access(val,R_OK))) {
	  Eroute.Emsg("Config","I cannot access your specified gridmap file!"); NoGo=1;} else {
	    GridMapFile=val;
	  }
	XrdOucString sayit;
	sayit = nsin;
	sayit += " -> ";
	sayit += nsout;
	Eroute.Say("=====> xcastor2.gridmapfile : ", val);
      }

      if (!strcmp("stagermap",var)) {
	if ((!(val = Config.GetWord()))) {
	  Eroute.Emsg("Config","you have to give a path + the stagerhost to assign"); NoGo=1;
	} else {
	  XrdOucString stagepath = val;
	  if ((!(val = Config.GetWord()))) {
	    Eroute.Emsg("Config","you have to give a path + the stagerhost to assign"); NoGo=1;
	  } else {
	    if (!stagertable->Find(stagepath.c_str())) {
	      stagertable->Add(stagepath.c_str(),new XrdOucString(val));
	      XrdOucString sayit;
	      sayit = stagepath;
	      sayit += " -> ";
	      sayit += val;
	      Eroute.Say("=====> xcastor2.stagermap : ", sayit.c_str());
	    }
	  }
	}
      }

      if (!strcmp("stagerpolicy",var)) {
	if ((!(val = Config.GetWord()))) {
	  Eroute.Emsg("Config","you have to give a stagerhost::svcclass tag and a policy ( schedall|schedread|schedwrite| )"); NoGo=1;
	} else {
	  XrdOucString stagepair = val;
	  if ((!(val = Config.GetWord())) || ((!strstr(val,"schedall")) && (!strstr(val,"schedread")) && (!strstr(val,"schedwrite")))) {
	    Eroute.Emsg("Config","you have to give a stagerhost::svcclass tag and a policy ( schedall|schedread|schedwrite| )"); NoGo=1;
	  } else {
	    if (!stagertable->Find(stagepair.c_str())) {
	      stagerpolicy->Add(stagepair.c_str(),new XrdOucString(val));
	      XrdOucString sayit;
	      sayit = stagepair;
	      sayit += " -> ";
	      sayit += val;
	      Eroute.Say("=====> xcastor2.stagerpolicy : ", sayit.c_str());
	    }
	  }
	}
      }

      if (!strcmp("vomsmapfile",var)) {

	if ((!(val = Config.GetWord())) || (::access(val,R_OK))) {
	  Eroute.Emsg("Config","I cannot access your specified vomsmap file!"); NoGo=1;} else {
	    VomsMapFile=val;
	  }
	Eroute.Say("=====> xcastor2.vomsmapfile : ", val);
      }

      if (!strcmp("locationcache",var)) {

	if ((!(val = Config.GetWord())) || (::access(val,W_OK))) {
	  Eroute.Emsg("Config","I cannot access your location cache directory!"); NoGo=1;} else {
	    LocationCacheDir=val;
	  }
	Eroute.Say("=====> xcastor2.locationcache : ", val);
      }

      if (!strcmp("subscribe",var)) {	
	if ((!(val = Config.GetWord())) || (strcmp("true",val) && strcmp("false",val) && strcmp("1",val) && strcmp("0",val))) {
	  Eroute.Emsg("Config","argument 2 for subscribe illegal or missing. Must be <true>,<false>,<1> or <0>!"); NoGo=1;} else {
	    if ((!strcmp("true",val) || (!strcmp("1",val)))) {
	      SubscribeCms=true;
	    }
	  }
	if (SubscribeCms)
	  Eroute.Say("=====> xcastor2.subscribe : true");
	else
	  Eroute.Say("=====> xcastor2.subscribe : false");
		   
      }  

      if (!strcmp("authlib",var)) {
	if ((!(val = Config.GetWord())) || (::access(val,R_OK))) {
	  Eroute.Emsg("Config","I cannot acccess you authorization library!"); NoGo=1;} else {
	    AuthLib=val;
	  }
	Eroute.Say("=====> xcastor2.authlib : ", AuthLib.c_str());
      }

      if (!strcmp("authorize",var)) {
	if ((!(val = Config.GetWord())) || (strcmp("true",val) && strcmp("false",val) && strcmp("1",val) && strcmp("0",val))) {
	  Eroute.Emsg("Config","argument 2 for authorize ilegal or missing. Must be <true>,<false>,<1> or <0>!"); NoGo=1;} else {
	    if ((!strcmp("true",val) || (!strcmp("1",val)))) {
	      authorize = true;
	    }
	  }
	if (authorize)
	  Eroute.Say("=====> xcastor2.authorize : true");
	else
	  Eroute.Say("=====> xcastor2.authorize : false");
      }

      if (!strcmp("trace",var)) {
	static struct traceopts {const char *opname; int opval;} tropts[] =
								   {
								     {"aio",      TRACE_aio},
								     {"all",      TRACE_ALL},
								     {"chmod",    TRACE_chmod},
								     {"close",    TRACE_close},
								     {"closedir", TRACE_closedir},
								     {"debug",    TRACE_debug},
								     {"delay",    TRACE_delay},
								     {"dir",      TRACE_dir},
								     {"exists",   TRACE_exists},
								     {"getstats", TRACE_getstats},
								     {"fsctl",    TRACE_fsctl},
								     {"io",       TRACE_IO},
								     {"mkdir",    TRACE_mkdir},
								     {"most",     TRACE_MOST},
								     {"open",     TRACE_open},
								     {"opendir",  TRACE_opendir},
								     {"qscan",    TRACE_qscan},
								     {"read",     TRACE_read},
								     {"readdir",  TRACE_readdir},
								     {"redirect", TRACE_redirect},
								     {"remove",   TRACE_remove},
								     {"rename",   TRACE_rename},
								     {"sync",     TRACE_sync},
								     {"truncate", TRACE_truncate},
								     {"write",    TRACE_write},
								     {"authorize",TRACE_authorize},
								     {"map",      TRACE_map},
								     {"role",     TRACE_role},
								     {"access",   TRACE_access},
								     {"attributes",TRACE_attributes},
								     {"allows",   TRACE_allows}
								   };
	int i, neg, trval = 0, numopts = sizeof(tropts)/sizeof(struct traceopts);

	if (!(val = Config.GetWord())) {
	  Eroute.Emsg("Config", "trace option not specified"); return 1;
	}

	while (val) {
	  Eroute.Say("=====> xcastor2.trace: ", val,"");
	  if (!strcmp(val, "off")) trval = 0;
	  else {if ((neg = (val[0] == '-' && val[1]))) val++;
	    for (i = 0; i < numopts; i++)
	      {if (!strcmp(val, tropts[i].opname))
		  {if (neg) trval &= ~tropts[i].opval;
		    else  trval |=  tropts[i].opval;
		    break;
		  }
	      }
	    if (i >= numopts)
	      Eroute.Say("Config warning: ignoring invalid trace option '",val,"'.");
	  }
          val = Config.GetWord();
	}

	xCastor2FsTrace.What = trval;
      }
    }
  }

  // check if xcastor2fs has been set

  if (!xcastor2fsdefined) {
    Eroute.Say("Config error: no xcastor2 fs has been defined (xcastor2.fs /...)","","");
  } else {
    Eroute.Say("=====> xcastor2.fs: ",xCastor2FsName.c_str(),"");
  } 

  // we need to specify this if the server was not started with the explicit manager option ... e.g. see XrdOfs
  // the variable is needed in t XrdxCastor2ServerAcc to find out which keys are required to be loaded 
  
  Eroute.Say("=====> all.role: ", role.c_str(),"");
  XrdOucString sTokenLockTime="";
  sTokenLockTime += TokenLockTime;

  Eroute.Say("=====> xcastor2.tokenlocktime: ", sTokenLockTime.c_str(),"");

  Eroute.Say("=====> xcastor2.locatepolicy : ", xCastor2FsLocatePolicy.c_str(),"");  

  if (MapCernCertificates)
    Eroute.Say("=====> xcastor2.mapcerncertificates : true","","");  

  if (role == "manager") {
    putenv((char *)"XRDREDIRECT=R");
  }

  if (( AuthLib != "") && (authorize) ) {
    // load the authorization plugin
    XrdSysPlugin    *myLib;
    XrdAccAuthorize *(*ep)(XrdSysLogger *, const char *, const char *);
    
    // Authorization comes from the library or we use the default
    //
    Authorization = XrdAccAuthorizeObject(Eroute.logger(),ConfigFN,NULL);

    if (!(myLib = new XrdSysPlugin(&Eroute, AuthLib.c_str()))) {
      Eroute.Emsg("Config","Failed to load authorization library!"); NoGo=1;
    } else {      
      ep = (XrdAccAuthorize *(*)(XrdSysLogger *, const char *, const char *))
	(myLib->getPlugin("XrdAccAuthorizeObject"));
      if (!ep) {
	Eroute.Emsg("Config","Failed to get authorization library plugin!"); NoGo=1;
      } else {
	Authorization = ep(Eroute.logger(), ConfigFN,NULL);
      }
    }
  }

  if (Procfilesystem !="") {
    // create the proc directories
    XrdOucString makeProc;
    makeProc="mkdir -p ";
    makeProc+=Procfilesystem;
#ifdef XFS_SUPPORT
    makeProc+="/xfs";
#endif
    system(makeProc.c_str());

    // create empty file in proc directory
    zeroProc="";
    zeroProc="rm -rf ";
    zeroProc+=Procfilesystem;
    zeroProc+="/zero";
    zeroProc+="; touch ";
    zeroProc+=Procfilesystem; 
    zeroProc+="/zero";
    system(zeroProc.c_str());
    // store the filename in zeroProc
    zeroProc = Procfilesystem;
    zeroProc += "/zero";
  }

  // create the proc object, if a proc fs was specified
  if (Procfilesystem !="") {
    Proc = new XrdxCastor2Proc(Procfilesystem.c_str(),ProcfilesystemSync);
    if (Proc) {
      if (Proc->Open()) {
	time_t now = time(NULL);
	pthread_t tid;
	int rc;
	// store version
	XrdxCastor2ProcFile* pf;
	XrdOucString ps = "1.1";
        // TODO: PACKAGE_STRING; 
        ps+="\n";
	pf = Proc->Handle("version"); pf && pf->Write(ps.c_str());
	pf = Proc->Handle("start"); pf && pf->Write(ctime(&now));

	// we have to give the proc pointer to the Stats class
	Stats.SetProc(Proc);
	if ((rc = XrdSysThread::Run(&tid, XrdxCastor2FsStatsStart, static_cast<void *>(&(this->Stats)),0,"Stats Updater"))) {
	  Eroute.Emsg("Config", "cannot run stats update thread");NoGo=1;
	}
      } else {
      Eroute.Emsg("Config", "cannot create proc fs");
      }
    } else {
      Eroute.Emsg("Config", "cannot get proc fs");
    }
  } else {
    Eroute.Say("=====> xcastor2.proc: ", "unspecified","");
  }  

  if ((retc = Config.LastError())) 
    NoGo = Eroute.Emsg("Config", -retc,"read config file",ConfigFN);
  Config.Close();
  
  return NoGo;
}
