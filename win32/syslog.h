/*
 * $Id: syslog.h,v 1.2 1999/12/09 13:47:56 jdurand Exp $
 */

/*
 * Copyright (C) 1997 by CERN/IT/PDP/IP
 * All rights reserved
 */
 
/*
 * @(#)syslog.h	1.2 11/14/97 CERN IT-PDP/IP Christoph von Praun
 */

#ifndef SysLog_h
#define SysLog_h 1

/*
 * priorities/facilities are encoded into a single 32-bit quantity, where the
 * bottom 3 bits are the priority (0-7) and the top 28 bits are the facility
 * (0-big number).  Both the priorities and the facilities map roughly
 * one-to-one to strings in the syslogd(8) source code.  This mapping is
 * included in this file.
 *
 * priorities (these are ordered)
 */

#define	LOG_EMERG	0		/* system is unusable */
#define	LOG_ALERT	1		/* action must be taken immediately */
#define	LOG_CRIT	2		/* critical conditions */
#define	LOG_ERR		3		/* error conditions */
#define	LOG_WARNING	4	/* warning conditions */
#define	LOG_NOTICE	5	/* normal but significant condition */
#define	LOG_INFO	6		/* informational */
#define	LOG_DEBUG	7		/* debug-level messages */

/*
 * Option flags for openlog.
 *
 */
#define	LOG_PID		0x01	  /* log the pid with each message */
#define	LOG_CONS	0x02	  /* log on the console if errors in sending */
#define	LOG_ODELAY	0x04	/* delay open until first syslog() (default) */
#define	LOG_NDELAY	0x08	/* don't delay open */
#define	LOG_NOWAIT	0x10	/* don't wait for console forks: DEPRECATED */
#define	LOG_PERROR	0x20	/* log to stderr as well */

/* 
 * Facility codes 
 */	

#define	LOG_KERN	(0<<3)	    /* kernel messages */
#define	LOG_USER	(1<<3)	    /* random user-level messages */
#define	LOG_MAIL	(2<<3)	    /* mail system */
#define	LOG_DAEMON	(3<<3)	  /* system daemons */
#define	LOG_AUTH	(4<<3)	    /* security/authorization messages */
#define	LOG_SYSLOG	(5<<3)	  /* messages generated internally by syslogd */
#define	LOG_LPR		(6<<3)	    /* line printer subsystem */
#define	LOG_NEWS	(7<<3)	    /* network news subsystem */
#define	LOG_UUCP	(8<<3)	    /* UUCP subsystem */
#define	LOG_CRON	(9<<3)	    /* clock daemon */
#define	LOG_AUTHPRIV	(10<<3)	/* security/authorization messages (private) */


#include <stdarg.h>

#ifdef  __cplusplus
extern "C" {
#endif

void	closelog(void);

/* 
 * program arguments are not used 
 * for openlog
 */
void	openlog(const char *, int, int);

/* 
 * not implemented
 * int	setlogmask(int); 
 */
void	syslog(int, const char *, ...);
void	vsyslog(int, const char *, va_list);

#ifdef  __cplusplus
}
#endif

#endif
