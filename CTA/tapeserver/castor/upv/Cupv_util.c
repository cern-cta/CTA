/*
 * $Id: Cupv_util.c,v 1.7 2009/03/20 10:38:15 waldron Exp $
 */

#define _GNU_SOURCE
#include <string.h>
#include <sys/types.h>
#include <stdlib.h>
#include <limits.h>                     /* For INT_MIN and INT_MAX */
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <ctype.h>
#include <unistd.h>
#include "osdep.h"
#include "u64subr.h"
#include "serrno.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "Cupv_util.h"
#include "Cupv_constants.h"

#define NAMEOFVAR(x) #x

static char strftime_format_sixmonthsold[] = "%b %e %Y";
static char strftime_format[] = "%b %e %H:%M:%S";

#define DUMP_VAL(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20d\n", NAMEOFVAR(member) , (int) st->member)
#define PRINT_VAL(st,member) fprintf(stdout, "%-23s : %20d\n", NAMEOFVAR(member) , (int) st->member)
#define DUMP_VALHEX(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20lx (hex)\n", NAMEOFVAR(member) , (unsigned long) st->member)
#define PRINT_VALHEX(st,member) fprintf(stdout, "%-23s : %20lx (hex)\n", NAMEOFVAR(member) , (unsigned long) st->member)
#define DUMP_VALSTATUS(rpfd,st,member) {				\
    char statusstring[1024];						\
    funcrep(rpfd, MSG_OUT, "%-23s : %20lx (hex) == %s\n", NAMEOFVAR(member) , (unsigned long) st->member, Cupv_util_status2string((char *) statusstring, (int) 1024, (int) st->member) == 0 ? statusstring : "<?>"); \
  }
#define PRINT_VALSTATUS(st,member) {					\
    char statusstring[1024];						\
    fprintf(stdout, "%-23s : %20lx (hex) == %s\n", NAMEOFVAR(member) , (unsigned long) st->member, Cupv_util_status2string((char *) statusstring, (int) 1024, (int) st->member) == 0 ? statusstring : "<?>"); \
  }
#define DUMP_TIME(rpfd,st,member) {					\
    char tmpbuf[21];							\
    char timestr[64] ;							\
    Cupv_util_time((time_t) st->member, timestr);			\
    funcrep(rpfd, MSG_OUT, "%-23s : %20s (%s)\n", NAMEOFVAR(member) ,	\
	    u64tostr((u_signed64) st->member, tmpbuf,0), timestr);	\
  }
#define PRINT_TIME(st,member) {						\
    char tmpbuf[21];							\
    char timestr[64] ;							\
    Cupv_util_time((time_t) st->member, timestr);			\
    fprintf(stdout, "%-23s : %20s (%s)\n", NAMEOFVAR(member) ,		\
	    u64tostr((u_signed64) st->member, tmpbuf,0), timestr);	\
  }
#define DUMP_U64(rpfd,st,member) {					\
    char tmpbuf[21];							\
    funcrep(rpfd, MSG_OUT, "%-23s : %20s\n", NAMEOFVAR(member) ,	\
	    u64tostr((u_signed64) st->member, tmpbuf,0));		\
  }
#define DUMP_U64_WITH_COMMENT(rpfd,st,member,comment) {			\
    char tmpbuf[21];							\
    funcrep(rpfd, MSG_OUT, "%-23s : %20s (%s)\n", NAMEOFVAR(member) ,	\
	    u64tostr((u_signed64) st->member, tmpbuf,0), (comment != NULL) ? comment : ""); \
  }
#define PRINT_U64(st,member) {					\
    char tmpbuf[21];						\
    fprintf(stdout, "%-23s : %20s\n", NAMEOFVAR(member) ,	\
	    u64tostr((u_signed64) st->member, tmpbuf,0));	\
  }
#define PRINT_U64_WITH_COMMENT(st,member,comment) {			\
    char tmpbuf[21];							\
    fprintf(stdout, "%-23s : %20s (%s)\n", NAMEOFVAR(member) ,		\
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
int Cupv_strtoi(int *output,
                char *nptr,
                char **endptr,
                int base)
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
      } else {
	*output = INT_MAX;
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

void Cupv_util_time(time_t this,
                    char *timestr)
{
  time_t this_time = time(NULL);
  struct tm tmstruc;
  struct tm *tp;

  localtime_r(&(this),&tmstruc);
  tp = &tmstruc;
  if ((this - this_time) > 6*30*24*60*60) {  // six months
    strftime(timestr,64,strftime_format_sixmonthsold,tp);
  } else {
    strftime(timestr,64,strftime_format,tp);
  }
}

/* Parses the privilege string to return the int value */
int Cupv_parse_privstring(char *privstr) {

  char *p;
  int priv = 0;

  char *buf = strndup(privstr, strlen(privstr)+1);
  if (NULL == buf) {
    return(-1);
  }

  p = strtok (buf, STR_SEP);
  while (p) {
    if (strcmp (p, STR_NONE) == 0)
      priv |= P_NONE;
    else if (strcmp (p, STR_OPERATOR) == 0)
      priv |= P_OPERATOR;
    else if (strcmp (p, STR_TAPE_OPERATOR) == 0)
      priv |= P_TAPE_OPERATOR;
    else if (strcmp (p, STR_GRP_ADMIN) == 0)
      priv |= P_GRP_ADMIN;
    else if (strcmp (p, STR_ADMIN) == 0)
      priv |= P_ADMIN;
    else if (strcmp (p, STR_UPV_ADMIN) == 0)
      priv |= P_UPV_ADMIN;
    else if (strcmp (p, STR_TAPE_SYSTEM) == 0)
      priv |= P_TAPE_SYSTEM;
    else if (strcmp (p, STR_STAGE_SYSTEM) == 0)
      priv |= P_STAGE_SYSTEM;
    else {
      free(buf);
      return(-1);
    }
    p = strtok (NULL, STR_SEP);
  }
  free (buf);
  return(priv);
}

/* safely concatenate 2 strings, reallocating destination if needed */
char* strsafecat(char* dest, int *destLen, char* src) {
  int needed = strlen(dest) + strlen(src) + 1;
  while (*destLen < needed) {
    *destLen *= 2;
    dest = realloc(dest, *destLen);
  }
  return strncat(dest, src, strlen(src));
}

/* concatenate 2 strings with '|' as separator (if first entry),
   reallocating the destination if needed. */
char* write_str(char* dest, int *destlen, char* src, int *firstentry) {
  char* ret = 0;
  if (!(*firstentry)) {
    strsafecat(dest, destlen, STR_SEP);
  }
  ret = strsafecat(dest, destlen, src);
  *firstentry = 0;
  return ret;
}


/* Builds the privilege string
 * note that the string is allocated here and that
 * the caller is responsible for its deallocation
 */
char* Cupv_build_privstring(int priv) {
  int firstentry = 1;
  int buflen = 64;
  char *buf = calloc(1, buflen);
  if (priv <= 0) {
    buf = strsafecat(buf, &buflen, STR_NONE);
  } else {
    if (priv & P_OPERATOR) {
      buf = write_str(buf, &buflen, STR_OPERATOR, &firstentry);
    }
    if (priv & P_TAPE_OPERATOR) {
      buf = write_str(buf, &buflen, STR_TAPE_OPERATOR, &firstentry);
    }
    if (priv & P_TAPE_SYSTEM) {
      buf = write_str(buf, &buflen, STR_TAPE_SYSTEM, &firstentry);
    }
    if (priv & P_STAGE_SYSTEM) {
      buf = write_str(buf, &buflen, STR_STAGE_SYSTEM, &firstentry);
    }
    if (priv & P_GRP_ADMIN) {
      buf = write_str(buf, &buflen, STR_GRP_ADMIN, &firstentry);
    }
    if (priv & P_UPV_ADMIN) {
      buf = write_str(buf, &buflen, STR_UPV_ADMIN, &firstentry);
    }
    if (priv & P_ADMIN) {
      buf = write_str(buf, &buflen, STR_ADMIN, &firstentry);
    }
    if (firstentry == 1) {
      buf = strsafecat(buf, &buflen, STR_NONE);
    }
  }
  return buf;
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




