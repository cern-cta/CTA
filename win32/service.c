/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/IP
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)service.c	1.1 03/26/99 CERN IT-PDP/IP Aneta Baran";
#endif /* not lint */

#if defined(_WIN32)
#include <stdio.h>
#include <windows.h>
#include "syslog.h"
#include "log.h"

HANDLE 	exit_event = NULL;
SERVICE_STATUS_HANDLE rfiod_stat_handle;
int 	rfiod_paused = 0;
int	rfiod_running = 0;
HANDLE 	rfiod_handle = 0;
 
char *argv0;

int      standalone = 0;   /* standalone flag                      */
char     logfile[128];   /* log file name buffer                 */

extern int  debug = 0;      	/* Debug flag           */
extern int  port = 0;           /* Non-standard port    */
extern int  logging = 0;        /* Default not to log   */
extern int  singlethread = 0;   /* Single threaded      */
extern int  lfname = 0;         /* log file given       */

extern int rfiod( void );

int rfiod_main(int argc, char *argv[]);

int rfiod_init()
{
   DWORD id;
   
   rfiod_handle = CreateThread(0, 0, (LPTHREAD_START_ROUTINE)rfiod, 0, 0, &id);
   if( rfiod_handle == NULL ) return -1;
   else  {
      rfiod_running = 1;
      return 0;
   }
}

void rfiod_resume()
{
   rfiod_paused = 0;
   ResumeThread(rfiod_handle);
}

void rfiod_pause()
{
   rfiod_paused = 1;
   SuspendThread(rfiod_handle);
}

void rfiod_stop()
{
   rfiod_running = 0;
   SetEvent(exit_event);
}

int send_status_to_SCM(curr_stat, exit_code, rfiod_exit_code, check_point, wait_hint)
DWORD curr_stat, exit_code, rfiod_exit_code, check_point, wait_hint;
{
   SERVICE_STATUS rfiod_stat;
   int rcode;

   rfiod_stat.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
   rfiod_stat.dwCurrentState = curr_stat;

   if( curr_stat == SERVICE_START_PENDING )
      rfiod_stat.dwControlsAccepted = 0;
   else
      rfiod_stat.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_PAUSE_CONTINUE
	 | SERVICE_ACCEPT_SHUTDOWN;
   if ( rfiod_exit_code == 0 )
      rfiod_stat.dwWin32ExitCode = rfiod_exit_code;
   else
      rfiod_stat.dwWin32ExitCode = ERROR_SERVICE_SPECIFIC_ERROR;
   rfiod_stat.dwServiceSpecificExitCode = rfiod_exit_code;
   rcode = SetServiceStatus(rfiod_stat_handle, &rfiod_stat);
   if( rcode == 0 )
      rfiod_stop();

   return rcode; 
}	

void rfiod_ctrl_handler(code)
DWORD code;
{
   DWORD curr_stat = 0;
   int rcode;

   switch(code)  {  
    case SERVICE_CONTROL_STOP:
       curr_stat = SERVICE_STOP_PENDING;
       rcode = send_status_to_SCM(SERVICE_STOP_PENDING, NO_ERROR, 0, 1, 5000);
       rfiod_stop();
       return;
    case SERVICE_CONTROL_PAUSE:
       if( rfiod_running && !rfiod_paused)  {
	  rcode = send_status_to_SCM(SERVICE_PAUSE_PENDING, NO_ERROR, 0, 1, 1000);
       rfiod_pause();
       curr_stat = SERVICE_PAUSED;
       }
       break;
    case SERVICE_CONTROL_CONTINUE:
       if(rfiod_running && rfiod_paused)  {
	  rcode = send_status_to_SCM(SERVICE_CONTINUE_PENDING, NO_ERROR, 0, 1, 1000);
       rfiod_resume();
       curr_stat =  SERVICE_RUNNING;
       }
       break;
    case SERVICE_CONTROL_INTERROGATE:
       break;
    case SERVICE_CONTROL_SHUTDOWN:
       break;
    default:
       break;
   }	/* end switch(code) */
   
   send_status_to_SCM(curr_stat, NO_ERROR, 0, 0, 0);
}

void terminate(error)
DWORD error;
{
   if(exit_event) CloseHandle(exit_event);
   if(rfiod_stat_handle) send_status_to_SCM(SERVICE_STOPPED, error, 0, 0, 0);
   if(rfiod_handle) CloseHandle(rfiod_handle);
}



main() {
   
   int rcode;
   SERVICE_TABLE_ENTRY rfiod_table[] = {
      { "rfiod", (LPSERVICE_MAIN_FUNCTION)rfiod_main },
      { NULL, NULL }
   };

/*    openlog("rfiod", LOG_INFO, LOG_DAEMON);      
   (void) initlog("rfiod", LOG_INFO, "syslog");*/
   
   rcode = StartServiceCtrlDispatcher(rfiod_table);
   if( rcode == 0 )      {
      perror("StartServiceCtrlDispatcher");
      exit(1);
   }
}	


rfiod_main(argc, argv)
int argc;
char *argv[];
{
   
   extern int      opterr, optind;         /* required by getopt(3)*/
   extern char     *optarg;                /* required by getopt(3)*/
   register int    option;

   int rcode;

   /*
    * immediately call registration function
    */
   rfiod_stat_handle = RegisterServiceCtrlHandler("rfiod", (LPHANDLER_FUNCTION)rfiod_ctrl_handler);
   if( rfiod_stat_handle == 0 )  {
      terminate(GetLastError());
      return 1;
   }
   rcode = send_status_to_SCM(SERVICE_START_PENDING, NO_ERROR, 0, 1, 5000);
   if(rcode == 0) {
      terminate(GetLastError());
      return 1;
   }

   openlog("rfiod", LOG_INFO, LOG_DAEMON);      
   (void) initlog("rfiod", LOG_INFO, "syslog");

   rcode = send_status_to_SCM(SERVICE_START_PENDING, NO_ERROR, 0, 1, 5000);
   if(rcode == 0) {
      terminate(GetLastError());
      return 1;
   } 

   exit_event = CreateEvent(0, TRUE, FALSE, 0);
   if( exit_event == NULL ) {
      terminate(GetLastError());
      syslog(LOG_ERR, "CreateEvent: errno=%d",GetLastError());
      return 1;
   }

   rcode = send_status_to_SCM(SERVICE_START_PENDING, NO_ERROR, 0, 1, 5000);
   if(rcode == 0) {
      terminate(GetLastError());
      return 1;
   }

   while ((option = getopt(argc,argv,"sdltf:p:")) != EOF)        {
      switch (option) {
       case 'd': debug++;
	  break;
       case 's': standalone++;
	  break;
       case 'f':
	  lfname++;
	  strcpy(logfile,optarg);
	  break;
       case 'l':
	  logging++;
	  break;
       case 't':
	  singlethread++;
	  break;
       case 'p':
	  port=atoi(optarg);
	  break;
      }
   }

   rcode = send_status_to_SCM(SERVICE_START_PENDING, NO_ERROR, 0, 1, 5000);
   if(rcode == 0) {
      terminate(GetLastError());
      return 1;
   }

   argv0 = (char*)malloc(strlen(argv[0])+1);
   if( argv0 == NULL )  {
      terminate(GetLastError());
      return errno;
   }

   rcode = send_status_to_SCM(SERVICE_START_PENDING, NO_ERROR, 0, 1, 5000);
   if(rcode == 0) {
      terminate(GetLastError());
      return 1;
   }

   strcpy(argv0, argv[0]);

   rcode = rfiod_init();
   if(rcode) {
      terminate(GetLastError());
      return 1;
   }
   
   /* service is now running... */
   rcode = send_status_to_SCM(SERVICE_RUNNING, NO_ERROR, 0, 0, 0);
   if(rcode == 0) {
      terminate(GetLastError());
      return 1;
   }  
   WaitForSingleObject(exit_event, INFINITE);

   terminate(0);
}

#endif 	/* WIN32 */
