/*
 * Copyright (C) 1997-1998 by CERN/IT/PDP/IP
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)syslog.c	1.5 10/21/98 CERN IT-PDP/IP Christoph von Praun";
#endif /* not lint */
 
/*
 *  05/11/97	(CVP)
 *
 * Mods:
 *  08/02/98	Added conditional main program at the end of this module
 *            Added the file slogmsg.h to the libsyslg package (CVP)
 *            Changed openlog()
 *     
 */

#ifdef _UNICODE
#error syslog does not support the UNICODE character set
#endif

/* 
 * c-headers 
 */
#include <sys/types.h>
#include <string.h>
#include <stdarg.h>   /* ANSI C*/
#include <time.h>
#include <stdio.h>
#include <process.h>
#include <stdlib.h>

/* 
 * windows-headers 
 */
#include <windows.h>
#include <wtypes.h>
#include <winbase.h>
#include <winerror.h>

/* 
 * local-headers 
 */
#include "syslog.h"
#include "slogmsg.h"

/* 
 * defines
 */

#define SLG_APP_NAME "syslog"
#define SLG_APP_FILE_PATH "%SystemRoot%\\system32\\slogmsg.dll"
#define SLG_CAT_FILE_PATH "%SystemRoot%\\system32\\slogmsg.dll"
#define SLG_CAT_COUNT 1
#define SLG_APP_REG_PATH "SYSTEM\\CurrentControlSet\\Services\\EventLog\\Application\\syslog"

#ifdef _DEBUG
#define SLG_PANIC() c = getc(stdin)
#define SLG_DEBUG(COMMAND) COMMAND
#else 
#define SLG_PANIC() ; 
#define SLG_DEBUG(COMMAND) ;
#endif

#define SLG_DEBUG_OUT stderr
#define SLG_BUFSIZE 2048

/* 
 * error messages
 */
#define SLG00 "SLG00 - failed to initialise SID\n"
#define SLG01 "SLG01 - failed to initialise username due to error %d\n"
#define SLG02 "SLG02 - could not report event; maybe log was not opened before?\n"
#define SLG03 "SLG03 - could not create nor open registry key\n"
#define SLG04 "SLG04 - could not set event message file\n"
#define SLG05 "SLG05 - could not set category message file\n"
#define SLG06 "SLG06 - could not set category count file\n"
#define SLG07 "SLG07 - could not set supported types\n"
#define SLG08 "SLG08 - could not register event source\n"
#define SLG09 "SLG09 - DeregisterEventSource failed due to error %d\n"

/* 
 * global variables 
 */
static HANDLE h = NULL;
static int c = 0;
static char username[SLG_BUFSIZE];

/* 
 * procedures 
 */
void syslog(int pri, const char *fmt, ...)
/* 
 * Purpose:
 *		Wrapper function, called by the user, 
 *    The actual work is done by vsyslog()
 *
 * Precondition: 
 *		ftm != NULL, varag list matches the number 
 *    of substitusion strings in fmt
 *    pri is a valid priority
 *
 * Postcondition:
 *		The indicated message is logged into the 
 *    NT - Application log at the given priority
 *
 * Mods:
 *		05/11/97	(CVP)
 */
{
	va_list ap;
	va_start(ap, fmt);
	vsyslog(pri, fmt, ap);
	va_end(ap);
}
	
void vsyslog(int pri, const char *fmt, va_list ap)
/* 
 * Purpose:
 *		Does the actual logging to the NT event log.
 *		This is actually never called directly by the user
 *		but always through syslog()
 *
 * Precondition: 
 *		dto. syslog()
 *
 * Postcondition:
 *		dto. syslog()
 *
 * Mods:
 *		05/11/97	(CVP)
 */
{

	char *aszMsg[1], *p;
	WORD nt_prio = 0;
	
	BYTE sidBuffer[SLG_BUFSIZE];
	PSID psid = (PSID) &sidBuffer;
	PSID PNtSid_ = NULL;
	DWORD sidBufferSize = SLG_BUFSIZE * sizeof(BYTE);
	TCHAR domainBuffer[SLG_BUFSIZE];
	DWORD domainBufferSize = SLG_BUFSIZE * sizeof(TCHAR);
	SID_NAME_USE snu;
	UCHAR SubAuthorityCount = 0;
	BOOL retBOOL = TRUE;
	DWORD SubAuthIndex = 0;

	int saved_errno = 0;
	int nt_pid = _getpid();
	char tbuf[SLG_BUFSIZE];
	char fmt_cpy[SLG_BUFSIZE];
	DWORD bufsize = SLG_BUFSIZE;
	BOOL resBOOL = 0;
	char ch, *t1, *t2;
	DWORD retDWORD = 0;

	saved_errno = errno;
	
	/* 
	 * Username retrieval 
	 */
	if (*username == 0) {
		retBOOL = GetUserName(username, &bufsize); 
		if (retBOOL == 0) {
			SLG_DEBUG(fprintf(SLG_DEBUG_OUT, SLG01, GetLastError()));
			SLG_PANIC();
		}
	}

	ZeroMemory(sidBuffer, SLG_BUFSIZE);
	LookupAccountName(
			NULL, 
			username, 
			sidBuffer,
			&sidBufferSize,
			domainBuffer, 
			&domainBufferSize,
			&snu);

	SubAuthorityCount = *GetSidSubAuthorityCount(psid);

	retBOOL = AllocateAndInitializeSid (
		GetSidIdentifierAuthority(psid),
		SubAuthorityCount,
		0,0,0,0,0,0,0,0,
		&PNtSid_
		);
			
	if (retBOOL != 0) {
		for( ; SubAuthIndex < SubAuthorityCount ; SubAuthIndex++) {
                *GetSidSubAuthority(PNtSid_, SubAuthIndex) =
                *GetSidSubAuthority(psid, SubAuthIndex);
		}
	}
/*
 * in case of error, PNtSid_ will remain NULL, 
 * which is OK anyway
 */

	(void)sprintf(tbuf, "[pid %d / user %s]  ", nt_pid, username);
	for (p = tbuf; *p; ++p);

/*
 * build the message
 * substitute error message for %m 
 */

	for (t1 = fmt_cpy; ch = *fmt; ++fmt) {
		if (ch == '%' && fmt[1] == 'm') {
			++fmt;
			for (t2 = strerror(saved_errno); *t1 = *t2++; ++t1);
		}
		else
			*t1++ = ch;
	}
	*t1 = '\0';

	vsprintf(p, fmt_cpy, ap);
	aszMsg[0] = tbuf;

/*
 * consider the event types:
 *		EVENTLOG_ERROR_TYPE 
 *		EVENTLOG_WARNING_TYPE 
 *		EVENTLOG_INFORMATION_TYPE
 *
 */

	switch(pri) {
	case LOG_EMERG:												/* system is unusable */
	case LOG_ALERT:												/* action must be taken immediately */
	case LOG_CRIT:												/* critical conditions */
	case LOG_ERR:													/* error conditions */
		nt_prio = EVENTLOG_ERROR_TYPE;
		break;
	case LOG_WARNING:											/* warning conditions */
	case LOG_NOTICE:											/* normal but significant condition */
		nt_prio = EVENTLOG_WARNING_TYPE;
		break;
	case LOG_INFO:												/* informational */
	case LOG_DEBUG:												/* debug-level messages */
		nt_prio = EVENTLOG_INFORMATION_TYPE;
		break;
	}

 
	if (!ReportEvent(
				h,										/* event log handle            */ 
        nt_prio,							/* event type                  */ 
        SLG_ONE,							/* category identifier         */ 
        SLG_01,								/* event identifier            */ 
        PNtSid_,							/* user security identifier    */ 
        1,                    /* one substitution string     */ 
        0,                    /* no data                     */ 
        (LPTSTR *) aszMsg,    /* address of string array     */ 
        NULL)                 /* address of data             */ 
	) {
			SLG_DEBUG(fprintf(SLG_DEBUG_OUT, SLG02));
			SLG_PANIC(); 
	}
}



void openlog(const char *ident, int logstat, int logfac)
/* 
 * Purpose:
 *		Initialises the handle to the systemlog that is needed
 *    to put messages in it. Before it checks the registry and
 *    looks if the syslog is already registered as an event 
 *    source
 *
 * Precondition: 
 *    ident, logstat and logfac are ignored, they are there to  
 *    provide the Unix interface
 *
 * Postcondition:
 *		handle released if it was opened before
 *
 * Mods:
 *		05/11/97	(CVP)
 *    12/12/97  (CVP) check if handle == NULL to handle repeated 
 *                    calls to this procedure
 *    09/02/97  (CVP) add check if the key HKEY_LOCAL_MACHINE\SLG_APP_REG_PATH
 *                    does already exist in the registry. If so, all entires to 
 *                    the registry are skipped and a simple RegisterEventSource 
 *                    is sufficient.
 *    10/10/98	(AB)  use of ident parameter, which indicates source of the log
 *                    entries. This parameter was previously ignored. 
 */
{
	HKEY hk; 
	DWORD dwData; 
	CHAR szBuf[SLG_BUFSIZE];
	DWORD catCount;
	
	/* 
	 * repeated calls to openlog should have no effect
	 */ 

	if (h == NULL) {
		ZeroMemory(username, SLG_BUFSIZE * sizeof(char));
		
		/* 
		 * Check if the key is already there 
		 * HKEY_LOCAL_MACHINE\SLG_APP_REG_PATH
		 */
		if (RegOpenKeyEx(HKEY_LOCAL_MACHINE, SLG_APP_REG_PATH, 0,
			KEY_READ, &hk) != ERROR_SUCCESS) {
		
			/* 
			 * Add your source name as a subkey under the Application 
			 * key in the EventLog service portion of the registry. 
			 */ 

			if (RegCreateKey(HKEY_LOCAL_MACHINE, SLG_APP_REG_PATH, &hk)) {
					SLG_DEBUG(fprintf(SLG_DEBUG_OUT, SLG03));
					SLG_PANIC();
			}
 
			/* 
			 * Set the Event ID message-file name. 
			 */ 
 
			strcpy(szBuf, SLG_APP_FILE_PATH); 
 
			/* 
			 * Add the Event ID message-file name to the subkey. 
			 */ 
			if (RegSetValueEx(hk,             /* subkey handle         */ 
							"EventMessageFile",       /* value name            */ 
							0,                        /* must be zero          */ 
							REG_EXPAND_SZ,            /* value type            */ 
							(LPBYTE) szBuf,           /* address of value data */ 
							strlen(szBuf) + 1)				/* length of value data  */ 
					) {      
					SLG_DEBUG(printf(SLG04));
					SLG_PANIC();
			}

			/* 
			 * Add the Category ID category-file name to the subkey. 
			 */ 
			strcpy(szBuf, SLG_CAT_FILE_PATH); 
			if (RegSetValueEx(hk,             /* subkey handle         */ 
							"CategoryMessageFile",    /* value name            */ 
							0,                        /* must be zero          */ 
							REG_EXPAND_SZ,            /* value type            */ 
							(LPBYTE) szBuf,           /* address of value data */ 
							strlen(szBuf) + 1)				/* length of value data  */ 
					) {      
					SLG_DEBUG(fprintf(SLG_DEBUG_OUT, SLG05));
					SLG_PANIC();
			}

			/* 
			 * Set the category Counter 
			 */
			catCount = SLG_CAT_COUNT;
			if (RegSetValueEx(hk,             /* subkey handle         */ 
							"CategoryCount",          /* value name            */ 
							0,                        /* must be zero          */ 
							REG_DWORD,                /* value type            */ 
							(LPBYTE) &catCount,       /* address of value data */ 
							sizeof(DWORD))				    /* length of value data  */ 
					) {      
					SLG_DEBUG(fprintf(SLG_DEBUG_OUT, SLG06));
					SLG_PANIC();
			}

			/* 
			 * Set the supported types flags. 
			 */ 
			dwData = EVENTLOG_ERROR_TYPE | EVENTLOG_WARNING_TYPE | EVENTLOG_INFORMATION_TYPE; 
 			if (RegSetValueEx(hk,      /* subkey handle                */ 
							"TypesSupported",  /* value name                   */ 
							0,                 /* must be zero                 */ 
							REG_DWORD,         /* value type                   */ 
							(LPBYTE) &dwData,  /* address of value data        */ 
							sizeof(DWORD))	   /* length of value data         */ 
					) {
				SLG_DEBUG(fprintf(SLG_DEBUG_OUT, SLG07));
				SLG_PANIC(); 
			}
		} /* make appropriate regentries for the event source syslog */
		
		RegCloseKey(hk);

		h = RegisterEventSource(
			NULL,						/* uses local computer         */ 
			(ident==NULL) ? SLG_APP_NAME:ident); 	/* source name - syslog by default */

		if (h == NULL) {	
			SLG_DEBUG(fprintf(SLG_DEBUG_OUT, SLG08));
			SLG_PANIC();
		}
	}
}

void closelog() 
/* 
 * Purpose:
 *		Close the handle to the systemlog 
 *
 * Precondition: 
 *    None
 *
 * Postcondition:
 *		handle released if it was opened before
 *
 * Mods:
 *		05/11/97	(CVP)
 *    12/12/97  (CVP) handle will ne set to NULL as soon as it is closed
 */
{
	BOOL retBOOL = 0;

	if (h != NULL) {
		retBOOL = DeregisterEventSource(h); 
		if (retBOOL == 0) {
			SLG_DEBUG(fprintf(SLG_DEBUG_OUT, SLG09, GetLastError()));
			SLG_PANIC();
		}
	}
	h = NULL;
}



#ifdef SLG_TEST
void main() {
	char c;
	openlog(NULL, 0, 0);
	syslog(LOG_INFO, "event %m %d ", 10);
	closelog();
	c = getc(stdin);
}
#endif
