/*
 * $Id: Cinitservice.c,v 1.2 2000/08/25 12:53:58 baud Exp $
 */

/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cinitservice.c,v $ $Revision: 1.2 $ $Date: 2000/08/25 12:53:58 $ CERN/IT/PDP/DM Aneta Baran/Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <winsock2.h>
#include "Cinit.h"

static HANDLE 	exit_event = NULL;
static struct main_args main_args;
static char	service_name[12];
static int 	service_paused = 0;
static int	service_running = 0;
static SERVICE_STATUS_HANDLE service_stat_handle;
static LPTHREAD_START_ROUTINE start_address;
static HANDLE 	thread_handle = 0;
 
void
service_main(int argc, char *argv[]);

int service_init()
{
   unsigned id;
   
   thread_handle = (HANDLE)_beginthreadex(NULL, 0, start_address, &main_args, 0, &id);
   if( thread_handle == NULL ) return -1;
   else  {
      service_running = 1;
      return 0;
   }
}

void service_resume()
{
   service_paused = 0;
   ResumeThread(thread_handle);
}

void service_pause()
{
   service_paused = 1;
   SuspendThread(thread_handle);
}

void service_stop()
{
   service_running = 0;
   SetEvent(exit_event);
}

int send_status_to_SCM(curr_stat, exit_code, service_exit_code, check_point, wait_hint)
DWORD curr_stat, exit_code, service_exit_code, check_point, wait_hint;
{
   SERVICE_STATUS service_stat;
   int rcode;

   service_stat.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
   service_stat.dwCurrentState = curr_stat;

   if( curr_stat == SERVICE_START_PENDING )
      service_stat.dwControlsAccepted = 0;
   else
      service_stat.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_PAUSE_CONTINUE
	 | SERVICE_ACCEPT_SHUTDOWN;
   if ( service_exit_code == 0 )
      service_stat.dwWin32ExitCode = exit_code;
   else
      service_stat.dwWin32ExitCode = ERROR_SERVICE_SPECIFIC_ERROR;
   service_stat.dwServiceSpecificExitCode = service_exit_code;
   service_stat.dwCheckPoint = check_point;
   service_stat.dwWaitHint = wait_hint;
   rcode = SetServiceStatus(service_stat_handle, &service_stat);
   if( rcode == 0 )
      service_stop();

   return rcode; 
}	

void service_ctrl_handler(code)
DWORD code;
{
   DWORD curr_stat = 0;
   int rcode;

   switch(code)  {  
    case SERVICE_CONTROL_STOP:
       curr_stat = SERVICE_STOP_PENDING;
       rcode = send_status_to_SCM(SERVICE_STOP_PENDING, NO_ERROR, 0, 1, 5000);
       service_stop();
       return;
    case SERVICE_CONTROL_PAUSE:
       if( service_running && !service_paused)  {
	  rcode = send_status_to_SCM(SERVICE_PAUSE_PENDING, NO_ERROR, 0, 1, 1000);
       service_pause();
       curr_stat = SERVICE_PAUSED;
       }
       break;
    case SERVICE_CONTROL_CONTINUE:
       if(service_running && service_paused)  {
	  rcode = send_status_to_SCM(SERVICE_CONTINUE_PENDING, NO_ERROR, 0, 1, 1000);
       service_resume();
       curr_stat =  SERVICE_RUNNING;
       }
       break;
    case SERVICE_CONTROL_INTERROGATE:
       break;
    case SERVICE_CONTROL_SHUTDOWN:
       return;
    default:
       break;
   }	/* end switch(code) */
   
   send_status_to_SCM(curr_stat, NO_ERROR, 0, 0, 0);
}

void terminate(error)
DWORD error;
{
   if(exit_event) CloseHandle(exit_event);
   if(service_stat_handle) send_status_to_SCM(SERVICE_STOPPED, error, 0, 0, 0);
   if(thread_handle) CloseHandle(thread_handle);
}



Cinitservice(name, startroutine)
char *name;
int (*startroutine)(void *);
{
	SERVICE_TABLE_ENTRY service_table[] = {
		{ service_name, (LPSERVICE_MAIN_FUNCTION)service_main },
		{ NULL, NULL }
	};

	strcpy (service_name, name);
	start_address = (LPTHREAD_START_ROUTINE)startroutine;

	if (StartServiceCtrlDispatcher (service_table) == 0) {
		perror("StartServiceCtrlDispatcher");
		return (-1);
	}
	return (0);
}	


void
service_main(argc, argv)
int argc;
char *argv[];
{
   int rcode;
   WSADATA wsadata;

	/*
	 * immediately call registration function
	 */
	if ((service_stat_handle = RegisterServiceCtrlHandler (service_name,
			(LPHANDLER_FUNCTION)service_ctrl_handler)) == 0) {
		terminate (GetLastError());
		return;
	}
   rcode = send_status_to_SCM(SERVICE_START_PENDING, NO_ERROR, 0, 1, 5000);
   if(rcode == 0) {
      terminate(GetLastError());
      return;
   }

   exit_event = CreateEvent(0, TRUE, FALSE, 0);
   if( exit_event == NULL ) {
      terminate(GetLastError());
      return;
   }

   rcode = send_status_to_SCM(SERVICE_START_PENDING, NO_ERROR, 0, 2, 1000);
   if(rcode == 0) {
      terminate(GetLastError());
      return;
   }

   if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
      terminate(GetLastError());
      return;
   }

   rcode = send_status_to_SCM(SERVICE_START_PENDING, NO_ERROR, 0, 3, 1000);
   if(rcode == 0) {
      terminate(GetLastError());
      WSACleanup();
      return;
   }

   main_args.argc = argc;
   main_args.argv = argv;
   rcode = service_init();
   if(rcode) {
      terminate(GetLastError());
      WSACleanup();
      return;
   }
   
   /* service is now running... */
   rcode = send_status_to_SCM(SERVICE_RUNNING, NO_ERROR, 0, 0, 0);
   if(rcode == 0) {
      terminate(GetLastError());
      WSACleanup();
      return;
   }  
   WaitForSingleObject(exit_event, INFINITE);
   WSACleanup();
   terminate(0);
}
