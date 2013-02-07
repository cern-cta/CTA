//////////////////////////////////////////////////////////////////////////
//                                                                      //
// XrdStagerQry                                                          //
//                                                                      //
// Author: Andreas-Jochim Peters                                        // 
//                                                                      //
//                                                                      //
//////////////////////////////////////////////////////////////////////////

//       $Id: XrdStagerQry.cc,v 1.2 2009/03/06 13:45:04 apeters Exp $

#include "XrdClient/XrdClientUrlInfo.hh"
#include "XrdClient/XrdClient.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdClient/XrdClientDebug.hh"
#include "XrdClient/XrdClientEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include <string.h>

#include <stdio.h>
#include <iostream>
#include <unistd.h>
#include <stdarg.h>
#include <sstream>

#ifdef HAVE_READLINE
#include <readline/readline.h>
#include <readline/history.h>
#include <curses.h>
#include <term.h>
#endif


/////////////////////////////////////////////////////////////////////
// function + macro to allow formatted print via cout,cerr
/////////////////////////////////////////////////////////////////////
extern "C" {

   void cout_print(const char *format, ...) {
      char cout_buff[4096];
      va_list args;
      va_start(args, format);
      vsprintf(cout_buff, format,  args);
      va_end(args);
      cout << cout_buff;
   }

   void cerr_print(const char *format, ...) {
      char cerr_buff[4096];
      va_list args;
      va_start(args, format);
      vsprintf(cerr_buff, format,  args);
      va_end(args);
      cerr << cerr_buff;
   }

#define COUT(s) do {				\
      cout_print s;				\
   } while (0)

#define CERR(s) do {				\
      cerr_print s;				\
   } while (0)

}

//////////////////////////////////////////////////////////////////////


#define XRDSTAGETOOL_VERSION            "(C) 2004 SLAC INFN $Revision: 1.2 $ - Xrootd version: "XrdVSTRING


///////////////////////////////////////////////////////////////////////
// Coming from parameters on the cmd line

XrdOucString opaqueinfo;

// Default open flags for opening a file (xrd)
kXR_unt16 xrd_open_flags = kXR_retstat;

XrdOucString srcurl;
bool useprepare = false;
int maxopens = 2;
int dbglvl = 0;
int verboselvl = 0;

///////////////////////




///////////////////////////////////////////////////////////////////////
// Generic instances used throughout the code

XrdClient *genclient;
XrdClientAdmin *genadmin = 0;
XrdClientVector<XrdClientUrlInfo> urls;

struct OpenInfo {
  XrdClient *cli;
  XrdClientUrlInfo *origurl;
};

XrdClientVector<struct OpenInfo> opening;

///////////////////////

void PrintUsage() {
   cerr << 
     "usage1: xrdstager_qry [-d dbglevel] [-s] [-O info] xrootd_url1 ... xrootd_urlN " << endl <<
     " Requests xrootd MSS status for the listed complete xrootd URLs" << endl <<
     "  in the form root://host//absolute_path_of_a_file" << endl <<
     " Parameters:" << endl <<
     "  -d dbglevel      : set the XrdClient debug level (0..4)" << endl <<
     "  -O info          : add some opaque info to the issued requests" << endl;
}


// Main program
int main(int argc, char**argv) {

  dbglvl = -1;

   // We want this tool to be able to connect everywhere
   // Note that the side effect of these calls here is to initialize the
   // XrdClient environment.
   // This is crucial if we want to later override its default values
   EnvPutString( NAME_REDIRDOMAINALLOW_RE, "*" );
   EnvPutString( NAME_CONNECTDOMAINALLOW_RE, "*" );
   EnvPutString( NAME_REDIRDOMAINDENY_RE, "" );
   EnvPutString( NAME_CONNECTDOMAINDENY_RE, "" );

   if (argc <= 1) {
     PrintUsage();
     exit(0);
   }

   for (int i=1; i < argc; i++) {

     
     if ( (strstr(argv[i], "-O") == argv[i]) && (argc >= i+2)) {
	 opaqueinfo=argv[i+1];
	 ++i;
	 continue;
      }

      if ( (strstr(argv[i], "-h") == argv[i])) {
	 PrintUsage();
	 exit(0);
      }

      if ( (strstr(argv[i], "-v") == argv[i])) {
	 // Increase verbosity level
 	 verboselvl++;
	 cout << "Verbosity level is now " << verboselvl << endl;
	 continue;
      }

      if ( (strstr(argv[i], "-d") == argv[i])) {
	 // Debug level
	 dbglvl = atoi(argv[i+1]);
	 i++;
	 continue;
      }

      if ( (strstr(argv[i], "-S") == argv[i])) {
	 // The url to fetch the file from
	 srcurl = argv[i+1];
	 i++;
	 continue;
      }

      // Any other par is considered as an url and queued
      if ( (strstr(argv[i], "-") != argv[i]) && (strlen(argv[i]) > 1) ) {
	// Enqueue
	if (verboselvl > 0)
	  cout << "Enqueueing file " << argv[i] << endl;
	XrdClientUrlInfo u(argv[i]);
	urls.Push_back(u);

	if (verboselvl > 1)
	  cout << "Enqueued URL " << u.GetUrl() << endl;

	continue;
      }


   }

   EnvPutInt(NAME_DEBUG, dbglvl);

   // Fire all the isonline requests at max speed

   vecString vecurl;
   vecString vecpath;
   vecBool   vecbool;

   for (int i = 0; i < urls.GetSize(); i++) {
     XrdClientUrlInfo u;
     u.TakeUrl(urls[i].GetUrl().c_str());
     
     if (opaqueinfo.length() > 0) {
       // Take care of the heading "?"
       if (opaqueinfo[0] != '?') {
	 u.File += "?";
       }
       
       u.File += opaqueinfo;
     }
     XrdOucString newfile = u.File.c_str();
     vecpath.Push_back(newfile);
     newfile = u.GetUrl().c_str();
     vecurl.Push_back(newfile);
   }
   
   XrdClientAdmin adm(vecurl[0].c_str());
   
   if (verboselvl > 1)
     cout << "Connecting to: " << vecurl[0].c_str() << endl;
   
   if (!adm.Connect()) {
     cout << "Unable to connect to " << vecurl[0].c_str() << endl;
     exit(-1);
   }

   if (verboselvl > 0) {
     for (int i = 0; i < urls.GetSize(); i++) {   
       cout << "Requesting isfileonline for: " << vecurl[i].c_str() << endl;
     }
   }
   
   bool generror=false;
   int exitcode=0;

   if (!adm.IsFileOnline(vecpath, vecbool)) {
     generror = true;
     cout << "Unable to execute IsFileOnline request for " << vecurl[0].c_str() << endl;
     
     switch (adm.LastServerResp()->status) {
     case kXR_ok:
       break;
     case kXR_error:
       exitcode = adm.LastServerError()->errnum;
       cout << "Error " << adm.LastServerError()->errnum <<
	 ": " << adm.LastServerError()->errmsg << endl << endl;
       break;
     default:
       cout << "Server response: " << adm.LastServerResp()->status << endl;
     }
   }
   
   for (int i = 0; i < urls.GetSize(); i++) {
     if (generror) {
       cout << "[  QUERY  ]: path=" << vecurl[i].c_str() << " staged=1 status=UNKNOWN" << endl;
       continue;
     }
     if (vecbool[i]) {
       cout << "[  QUERY  ]: path=" << vecurl[i].c_str() << " staged=1 status=ONLINE" << endl;
     } else {
       cout << "[  QUERY  ]: path=" << vecurl[i].c_str() << " staged=0 status=OFFLINE" << endl;
     }
   }
   exit(exitcode);
}
