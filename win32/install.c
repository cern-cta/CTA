/* 
 * Copyright (C) 1990-1999 by CERN/IT/PDP/IP
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)install.c	1.2 03/26/99 CERN IT-PDP/IP Aneta Baran";
#endif /* not lint */


#include <stdio.h>
#include <windows.h>

SC_HANDLE 	service, scm;

void 	usage(void);
int 	install_service(char*, char*, char*);
int 	remove_service(char*);

void main(int argc, char *argv[])
{
   int rcode;

   if( (argc != 5) && (argc != 3) ) {
      usage();
      return;
   }
   switch(argv[1][1]) {
    case 'i':
       if( argc == 5 )  {
	  printf("Installing service %s\n", argv[2]);
	  rcode = install_service(argv[2], argv[3], argv[4]);
	  if( rcode == 0 )  printf("Service %s installed successfully\n", argv[2]);
	  else printf("Error installing service %s\n", argv[2]);
       }  else usage();
       break;
    case 'u':
       if( argc == 3 )  {
       printf("Removing service %s\n",  argv[2]);
       rcode = remove_service(argv[2]);
       if( rcode )
	  printf("Error removing service %s\n", argv[2]);
       } else usage();
       break;
    default:
       usage();
       break;
   }
   return;
}

void usage(void) {
      printf("Usage:  install -i service_name service_label executable\n");
      printf("\tinstall -u service_name\n");
      printf("\t(service_name is the name used internally by SCM,\n");
      printf("\tservice_label is the name that appears in the Services applet,\n");
      printf("\texecutable is the full path to the .exe file)\n");
      return;
}

int install_service(service_name, service_label, path)
char *service_name, *service_label, *path;
{
   /*
    * Opening a connection to the SCM
    */
   scm = OpenSCManager(0, 0, SC_MANAGER_CREATE_SERVICE);
   if( scm == NULL )  {
      errno = GetLastError();
      printf("OpenSCManager: errno=%d", errno);
      return (errno);
   }
   service = CreateService(scm, service_name, service_label, SERVICE_ALL_ACCESS, SERVICE_WIN32_OWN_PROCESS,
			    SERVICE_DEMAND_START, SERVICE_ERROR_NORMAL, path, 0, 0, 0, 0, 0);
   if( service == NULL ) {
      errno = GetLastError();
      printf("CreateService: errno=%d", errno);
      return(errno);
   }
   CloseServiceHandle(service);
   CloseServiceHandle(scm);  
   return(0);
}

int remove_service(service_name)
char 	*service_name;
{
   int rcode;
   SERVICE_STATUS status;

   scm = OpenSCManager(0, 0, SC_MANAGER_CREATE_SERVICE);
   if( scm == NULL )  {
      errno = GetLastError();
      printf("OpenSCManager: errno=%d\n", errno);
      return(errno);
   }
   service = OpenService(scm, service_name, SERVICE_ALL_ACCESS | DELETE);
   if( service == NULL )  {
      errno = GetLastError();
      printf("OpenService: errno=%d\n", errno);
      return(errno);
   }
   rcode = QueryServiceStatus(service, &status);
   if( rcode == 0 ) {
      errno = GetLastError();
      printf("QueryServiceStatus: errno=%d\n", errno);
      return(errno);
   }
   if( status.dwCurrentState != SERVICE_STOPPED )  {
      printf("Stopping service %s\n", service_name);
      rcode = ControlService(service, SERVICE_CONTROL_STOP, &status);
      if( rcode == 0 ) {
	 errno = GetLastError();
	 printf("ControlService: errno=%d\n", errno);
	 return(errno);
      }
      Sleep(500);
   }
   rcode = DeleteService(service);
   if( rcode )  printf("Service %s removed successfully\n", service_name);
   else {
      errno = GetLastError();
      printf("DeleteService: errno=%d", errno);
      return(errno);
   }
   CloseServiceHandle(service);
   CloseServiceHandle(scm);
   return(0);
}

