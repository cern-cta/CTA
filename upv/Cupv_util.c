/*
 * $Id: Cupv_util.c,v 1.5 2002/09/10 12:54:56 bcouturi Exp $
 */

#include <sys/types.h>
#include <stdlib.h>
#include <limits.h>                     /* For INT_MIN and INT_MAX */
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <ctype.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include <winsock2.h>
#endif
#include "osdep.h"
#include "u64subr.h"
#include "serrno.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "Cupv_util.h"
#include "Cupv_constants.h"

#ifdef __STDC__
#define NAMEOFVAR(x) #x
#else
#define NAMEOFVAR(x) "x"
#endif

#ifdef SIXMONTHS
#undef SIXMONTHS
#endif
#define SIXMONTHS (6*30*24*60*60)

#if defined(_WIN32)
static char strftime_format_sixmonthsold[] = "%b %d %Y";
static char strftime_format[] = "%b %d %H:%M:%S";
#else /* _WIN32 */
static char strftime_format_sixmonthsold[] = "%b %e %Y";
static char strftime_format[] = "%b %e %H:%M:%S";
#endif /* _WIN32 */

#define DUMP_VAL(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20d\n", NAMEOFVAR(member) , (int) st->member)
#define PRINT_VAL(st,member) fprintf(stdout, "%-23s : %20d\n", NAMEOFVAR(member) , (int) st->member)
#define DUMP_VALHEX(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20lx (hex)\n", NAMEOFVAR(member) , (unsigned long) st->member)
#define PRINT_VALHEX(st,member) fprintf(stdout, "%-23s : %20lx (hex)\n", NAMEOFVAR(member) , (unsigned long) st->member)
#define DUMP_VALSTATUS(rpfd,st,member) { \
	char statusstring[1024]; \
	funcrep(rpfd, MSG_OUT, "%-23s : %20lx (hex) == %s\n", NAMEOFVAR(member) , (unsigned long) st->member, Cupv_util_status2string((char *) statusstring, (int) 1024, (int) st->member) == 0 ? statusstring : "<?>"); \
}
#define PRINT_VALSTATUS(st,member) { \
	char statusstring[1024]; \
	fprintf(stdout, "%-23s : %20lx (hex) == %s\n", NAMEOFVAR(member) , (unsigned long) st->member, Cupv_util_status2string((char *) statusstring, (int) 1024, (int) st->member) == 0 ? statusstring : "<?>"); \
}
#define DUMP_TIME(rpfd,st,member) {                                         \
    char tmpbuf[21];                                                        \
	char timestr[64] ;                                                      \
    Cupv_util_time((time_t) st->member, timestr);                          \
	funcrep(rpfd, MSG_OUT, "%-23s : %20s (%s)\n", NAMEOFVAR(member) ,	    \
				 u64tostr((u_signed64) st->member, tmpbuf,0), timestr);	    \
}
#define PRINT_TIME(st,member) {                                             \
    char tmpbuf[21];                                                        \
	char timestr[64] ;                                                      \
    Cupv_util_time((time_t) st->member, timestr);                          \
	fprintf(stdout, "%-23s : %20s (%s)\n", NAMEOFVAR(member) ,	            \
				 u64tostr((u_signed64) st->member, tmpbuf,0), timestr);	    \
}
#define DUMP_U64(rpfd,st,member) {                                          \
    char tmpbuf[21];                                                        \
	funcrep(rpfd, MSG_OUT, "%-23s : %20s\n", NAMEOFVAR(member) ,	        \
				 u64tostr((u_signed64) st->member, tmpbuf,0));	            \
}
#define DUMP_U64_WITH_COMMENT(rpfd,st,member,comment) {                     \
    char tmpbuf[21];                                                        \
	funcrep(rpfd, MSG_OUT, "%-23s : %20s (%s)\n", NAMEOFVAR(member) ,	        \
				 u64tostr((u_signed64) st->member, tmpbuf,0), (comment != NULL) ? comment : ""); \
}
#define PRINT_U64(st,member) {                                              \
    char tmpbuf[21];                                                        \
	fprintf(stdout, "%-23s : %20s\n", NAMEOFVAR(member) ,	                \
				 u64tostr((u_signed64) st->member, tmpbuf,0));	            \
}
#define PRINT_U64_WITH_COMMENT(st,member,comment) {                         \
    char tmpbuf[21];                                                        \
	fprintf(stdout, "%-23s : %20s (%s)\n", NAMEOFVAR(member) ,	            \
				 u64tostr((u_signed64) st->member, tmpbuf,0), (comment != NULL) ? comment : ""); \
}

#define DUMP_CHAR(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20c\n", NAMEOFVAR(member) , st->member != '\0' ? st->member : ' ')

#define PRINT_CHAR(st,member) fprintf(stdout, "%-23s : %20c\n", NAMEOFVAR(member) , st->member != '\0' ? st->member : ' ')

#define DUMP_STRING(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20s\n", NAMEOFVAR(member) , st->member)
#define PRINT_STRING(st,member) fprintf(stdout, "%-23s : %20s\n", NAMEOFVAR(member) , st->member)

extern char *getenv();         /* To get environment variables */
extern char *getconfent();     /* To get configuration entries */

char *Cupv_forced_endptr_error = "Z";

struct flag2name {
	u_signed64 flag;
	char *name;
};





/* Idem than strtol() but returns 0 if OK, -1 if error, result in &ouput */
int DLL_DECL Cupv_strtoi(output,nptr,endptr,base)
	int *output;
	char *nptr;
	char **endptr;
	int base;
{
	long thislong;
	int rc = 0;

	errno = 0;
	thislong = strtol (nptr, endptr, base);
	if ((**endptr != '\0') || (((thislong == LONG_MIN) || (thislong == LONG_MAX)) && (errno == ERANGE))) {
		if (thislong <= INT_MIN) {
			*output = INT_MIN;
			serrno = errno = ERANGE;
		} else if (thislong >= INT_MAX) {
			*output = INT_MAX;
			serrno = errno = ERANGE;
		} else {
			*output = (int) thislong;
			serrno = errno = EINVAL;
		}
		if (**endptr == '\0') {
			/* We force the caller to have an error anyway, just checking **endptr */
			*endptr = Cupv_forced_endptr_error;
		}
		rc = -1;
	} else {
		if ((thislong < INT_MIN) || (thislong > INT_MAX)) {
			if (thislong < INT_MIN) {
				*output = INT_MIN;
			} else if (thislong > INT_MAX) {
				*output = INT_MAX;
			} else {
				*output = (int) thislong;
            }
			if (**endptr == '\0') {
				/* We force the caller to have an error anyway, just checking **endptr */
				*endptr = Cupv_forced_endptr_error;
			}
			serrno = errno = ERANGE;
			rc = -1;
		} else {
			*output = (int) thislong;
		}
	}
	return(rc);
}

void DLL_DECL Cupv_util_time(this,timestr)
     time_t this;
     char *timestr;
{
  time_t this_time = time(NULL);
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
  struct tm tmstruc;
#endif /* _REENTRANT || _THREAD_SAFE */
  struct tm *tp;

#if ((defined(_REENTRANT) || defined(_THREAD_SAFE)) && !defined(_WIN32))
  localtime_r(&(this),&tmstruc);
  tp = &tmstruc;
#else
  tp = localtime(&(this));
#endif /* _REENTRANT || _THREAD_SAFE */
  if ((this - this_time) > SIXMONTHS) {
    strftime(timestr,64,strftime_format_sixmonthsold,tp);
  } else {
    strftime(timestr,64,strftime_format,tp);
  }
}

/* Parses the privilege string to return the int value */
int DLL_DECL Cupv_parse_privstring(char *privstr) {

  char *p;
  char buf[MAXPRIVSTRLEN + 1];
  int priv = 0;

  memset(buf, 0, sizeof(buf));

  if (strlen (privstr) >= sizeof (buf)) {
    return(-1);
  }
  
  strcpy (buf, privstr);
  p = strtok (buf, STR_SEP);
  while (p) {
    if (strcmp (p, STR_NONE) == 0)
      priv |= P_NONE;
    else if (strcmp (p, STR_OPERATOR) == 0)
      priv |= P_OPERATOR;
    else if (strcmp (p, STR_TAPE_OPERATOR) == 0)
      priv |= P_TAPE_OPERATOR;
    else if (strcmp (p, STR_EXPT_ADMIN) == 0)
      priv |= P_EXPT_ADMIN;
    else if (strcmp (p, STR_ADMIN) == 0)
      priv |= P_ADMIN;
    else if (strcmp (p, STR_UPV_ADMIN) == 0)
      priv |= P_UPV_ADMIN;
    else if (strcmp (p, STR_TAPE_SYSTEM) == 0)
      priv |= P_TAPE_SYSTEM;
    else if (strcmp (p, STR_STAGE_SYSTEM) == 0)
      priv |= P_STAGE_SYSTEM;
    else {
      return(-1);
    }
    p = strtok (NULL, STR_SEP);
  }
  return(priv);
}

/* Builds the privilege string  */
void DLL_DECL Cupv_build_privstring(int priv, char *privstring) {

  char buf[MAXPRIVSTRLEN +1];
  char *p;
  int firstentry = 1;

  if (priv <= 0) {
    strcpy(buf, STR_NONE);
    return;
  } else {
  
    p = buf;
    
    if (priv & P_OPERATOR) {
      WRITE_STR(p, STR_OPERATOR, firstentry);
    } 
    if (priv & P_TAPE_OPERATOR) {
      WRITE_STR(p, STR_TAPE_OPERATOR, firstentry);
    } 
    if (priv & P_TAPE_SYSTEM) {
      WRITE_STR(p, STR_TAPE_SYSTEM, firstentry);
    } 
    if (priv & P_STAGE_SYSTEM) {
      WRITE_STR(p, STR_STAGE_SYSTEM, firstentry);
    } 
    if (priv & P_EXPT_ADMIN) {
      WRITE_STR(p, STR_EXPT_ADMIN, firstentry);
    }
    if (priv & P_UPV_ADMIN) {
      WRITE_STR(p, STR_UPV_ADMIN, firstentry);
    }
    if (priv & P_ADMIN) {
      WRITE_STR(p, STR_ADMIN, firstentry);
    } 
    if (firstentry == 1) {
      strcpy(buf, STR_NONE);
    } 
  }

  *p = '\0';

  strcpy(privstring, buf);
  return;
}


/* Gets the uid given the name */
int Cupv_getuid(const char *name) {
  
  struct passwd *pwd;

  pwd = Cgetpwnam(name);
  if (pwd == NULL) {
    serrno = SEUSERUNKN;
    return(-1);
  }
  return(pwd->pw_uid);
}

/* Gets the gid given the name */
int Cupv_getgid(const char *name) {
  
  struct group *gr;

  gr = Cgetgrnam(name);
  if (gr == NULL) {
    serrno = SEGROUPUNKN;
    return(-1);
  }
  return(gr->gr_gid);
}



char *Cupv_getuname(uid_t uid) {
  
  struct passwd *pwd;

  pwd = Cgetpwuid(uid);
  if (pwd == NULL) {
    serrno = SEUSERUNKN;
    return(NULL);
  }
  return(pwd->pw_name);
}


char *Cupv_getgname(gid_t gid) {
  
  struct group *gr;

  gr = Cgetgrgid(gid);
  if (gr == NULL) {
    serrno = SEGROUPUNKN;
    return(NULL);
  }
  return(gr->gr_name);
}




