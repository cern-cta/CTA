/*
 * Copyright (C) 1993-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include "Cnetdb.h"
#include "Cns.h"
#include "Cns_api.h"
#include "Cns_constants.h"
#include "Csec_api.h"
#include "marshall.h"
#include "net.h"
#include "serrno.h"

/* send2nsd - send a request to the name server and wait for the reply */


/* This wrapper tries to contact the secure nameserver first and if
 * authentication fails it falls back on the unsecure port.
 * Failures are memorized so that daemons (e.g. stagerd) can go straight
 * to the unsecure port after the first failure.
 * NOTE1: normal errors do NOT trigger a retry.
 * NOTE2: in order to know the kind of problem encountered, we define
 *        these return codes for send2nsdx:
 *          -1 -> generic error
 *          -2 -> security error
 */
int send2nsdx_wrapper(int *socketp,
                      char *host,
                      char *reqp,
                      int reql,
                      char *user_repbuf,
                      int user_repbuf_len,
                      void **repbuf2,
                      int *nbstruct)
{
  static int useSecureNS = 1; /* Special variable for daemons:
                               * try to use security the first time
                               * but remember if that didn't work */

  /* Default is to go in the unsecure branch */
  int returnValue = -2;

  if (useSecureNS) {
    /* Try to go secure */
    returnValue = send2nsdx (socketp, host, reqp, reql, user_repbuf,
                             user_repbuf_len, repbuf2, nbstruct, 1);
  }

  if (returnValue == -2) {
    /* Ok, we failed, avoid retrying security the next time.
     * NOTE: only useful for daemons (e.g. stagerd) */
    useSecureNS = 0;
    /* Now retry unsecured */
    serrno = 0; /* Reset to ignore security errors */
    returnValue = send2nsdx (socketp, host, reqp, reql, user_repbuf,
                             user_repbuf_len, repbuf2, nbstruct, 0);
  }
  return returnValue;
}


int send2nsdx(int *socketp,
              char *host,
              char *reqp,
              int reql,
              char *user_repbuf,
              int user_repbuf_len,
              void **repbuf2,
              int *nbstruct,
              int securityOpt)
{
  int actual_replen = 0;
  int alloced = 0;
  int c;
  char Cnshost[CA_MAXHOSTNAMELEN+1];
  Csec_context_t ctx;
  int errflag = 0;
  char func[16];
  char *getconfent();
  char *getenv();
  struct hostent *hp;
  struct Cns_linkinfo *li;
  int magic;
  int n;
  int nbretry;
  char *p,*sec_p;
  char prtbuf[PRTBUFSZ];
  char *q;
  int rep_type;
  char repbuf[REPBUFSZ];
  int repbuf2sz;
  int retrycnt = 0;
  int retryint;
  int s;
  struct sockaddr_in sin; /* internet socket */
  struct servent *sp,*sec_sp;
  struct Cns_api_thread_info *thip = NULL;
  int timeout;
  int errorRetVal;
  int needsLogging = !securityOpt; /* This controls the logging:
                                      log only if authenticated or
                                      running unsecured */

  /* Set a different return error value for security to allow for
   * specialized behavior */
  if (securityOpt) errorRetVal = -2;
  else errorRetVal = -1;

  strncpy (func, "send2nsd", 16);
  if (Cns_apiinit (&thip))
    return errorRetVal;
  if (*thip->defserver)
    host = thip->defserver;
  if (socketp && *socketp >= 0) { /* connection opened by Cns_list... */
    s = *socketp;
  } else if (socketp == NULL && thip->fd >= 0) {
    s = thip->fd;  /* connection opened by Cns_starttrans */
  } else {   /* connection not yet opened */
    sin.sin_family = AF_INET;
    sin.sin_port = 0;
    if (host && *host)
      strcpy (Cnshost, host);
    else if ((p = getenv (CNS_HOST_ENV)) || (p = getconfent (CNS_SCE, "HOST", 0)))
      strcpy (Cnshost, p);
    else {
#if defined(CNS_HOST)
      strcpy (Cnshost, CNS_HOST);
#else
      serrno = SENOSHOST;
      return errorRetVal;
#endif
    }
    /* If the user has decided to enable the security mode then the port can not be set
     * via host:port but with the specific SECURITY_PORT options
     */
    if ((p = strchr (Cnshost, ':'))) {
      *p = '\0';
      sin.sin_port = htons (atoi (p + 1));
    }
    if ((hp = Cgethostbyname (Cnshost)) == NULL) {
      /* Log only if in non-secure mode */
      if (needsLogging) Cns_errmsg (func, NS009, "Host unknown:", Cnshost);
      serrno = SENOSHOST;
      return errorRetVal;
    }
    sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;
    if (! sin.sin_port) {
      /* If the security option is set it will try ONLY in that port, if it fails to
       * connect it won't retry in an unsecure port
       */
      if (securityOpt) {
        if ((sec_p = getenv (CNS_SPORT_ENV)) ||
           ((sec_p = getconfent (CNS_SCE, "SEC_PORT", 0)))) {
          sin.sin_port = htons ((unsigned short)atoi (sec_p));
        } else if ((sec_sp = getservbyname (CNS_SEC_SVC, "tcp"))) {
          sin.sin_port = sec_sp->s_port;
        } else {
          sin.sin_port = htons ((unsigned short)CNS_SEC_PORT);
        }
      } else {
        if ((p = getenv (CNS_PORT_ENV)) || (p = getconfent (CNS_SCE, "PORT", 0))) {
          sin.sin_port = htons ((unsigned short)atoi (p));
        } else if ((sp = Cgetservbyname (CNS_SVC, "tcp"))) {
          sin.sin_port = sp->s_port;
          serrno = 0;
        } else {
          sin.sin_port = htons ((unsigned short)CNS_PORT);
          serrno = 0;
        }
      }
    }
    /* get timeout environment variables */
    if ((p = getenv (CNS_CONNTIMEOUT_ENV)) == NULL) {
      timeout = DEFAULT_CONNTIMEOUT;
    } else {
      timeout = atoi (p);
    }
    if (securityOpt) {
        /* TRANSITIONAL: Don't loop and go straight to unsecure if needed */
        retrycnt = 0;
        nbretry = 0;
    } else {
      /* get retry environment variables */
      nbretry = DEFAULT_RETRYCNT;
      if ((p = getenv (CNS_CONRETRY_ENV)) ||
          (p = getconfent (CNS_SCE, "CONRETRY", 0))) {
        nbretry = atoi(p);
      }

      retryint = RETRYI;
      if ((p = getenv (CNS_CONRETRYINT_ENV)) ||
          (p = getconfent (CNS_SCE, "CONRETRYINT", 0))) {
        retryint = atoi(p);
      }
    }

    /* retry as much as the user wishes */
    while (retrycnt <= nbretry) {

      if ((s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
        if (needsLogging) Cns_errmsg (func, NS002, "socket", neterror());
        serrno = SECOMERR;
        return errorRetVal;
      }

      if (netconnect_timeout (s, (struct sockaddr *)&sin,
                              sizeof(struct sockaddr_in), timeout) < 0) {

        if (serrno == SETIMEDOUT) {
          if (retrycnt == nbretry) {
            if (needsLogging) Cns_errmsg (func, NS002, "connect", neterror());
            (void) close(s);
            return errorRetVal;
          }
        } else {

          if (serrno == ECONNREFUSED) {
            if (retrycnt == nbretry) {
              if (needsLogging) Cns_errmsg (func, NS000, Cnshost);
              (void) close (s);
              serrno = ENSNACT;
              return errorRetVal;
            }
          } else {
            if (retrycnt == nbretry) {
              if (needsLogging) Cns_errmsg (func, NS002, "connect", neterror());
              (void) close (s);
              serrno = SECOMERR;
              return errorRetVal;
            }
          }
        }
        (void) close (s);
      } else {
        if (securityOpt) {
          Csec_client_initContext (&ctx, CSEC_SERVICE_TYPE_HOST, NULL);
          if (Csec_client_establishContext (&ctx, s) != 0) {
            switch (serrno) {
            case SECOMERR:
              if (needsLogging) Cns_errmsg (func, NS002, "send", "Communication error");
              (void) close (s);
              Csec_clearContext (&ctx);
              return errorRetVal;
            case SETIMEDOUT:
              if (retrycnt == nbretry) {
                if (needsLogging) Cns_errmsg (func, NS002, "send", "Operation timed out");
                (void) close (s);
                Csec_clearContext (&ctx);
                return errorRetVal;
              }
              break;
            default:
              if (needsLogging) Cns_errmsg (func, NS002, "send", "No valid credential found");
              (void) close (s);
              Csec_clearContext (&ctx);
              return errorRetVal;
            }

            (void) close (s);
            Csec_clearContext (&ctx);
          } else {
            /* This is the exit point of a successful loop w/security enabled */
            needsLogging = 1; /* turn on logging now */
            break;
          }
        } else {
          break;
        }
      }
      sleep (retryint);
      retrycnt++;
    }

    if (securityOpt)
      Csec_clearContext (&ctx);
    if (socketp)
      *socketp = s;
  }

  /* From now on we don't need to split the error codes anymore */

  /* send request to name server */
  serrno = 0;
  if ((n = netwrite (s, reqp, reql)) <= 0) {
    if (needsLogging) {
      if (n == 0) Cns_errmsg (func, NS002, "send", sys_serrlist[SERRNO]);
      else Cns_errmsg (func, NS002, "send", neterror());
    }
    (void) close (s);
    serrno = SECOMERR;
    return (-1);
  }

  /* get reply */

  while (1) {
    if ((n = netread (s, repbuf, 3 * LONGSIZE)) <= 0) {
      if (needsLogging) {
        if (n == 0) Cns_errmsg (func, NS002, "recv", sys_serrlist[SERRNO]);
        else Cns_errmsg (func, NS002, "recv", neterror());
      }
      (void) close (s);
      serrno = SECOMERR;
      return (-1);
    }
    p = repbuf;
    unmarshall_LONG (p, magic) ;
    unmarshall_LONG (p, rep_type) ;
    unmarshall_LONG (p, c) ;
    if (rep_type == CNS_RC) {
      (void) close (s);
      if (s == thip->fd)
        thip->fd = -1;
    }
    if (rep_type == CNS_RC || rep_type == CNS_IRC) {
      if (c) {
        serrno = c;
        c = -1;
      }
      break;
    }
    if (c > REPBUFSZ) {
      if (needsLogging) Cns_errmsg (func, "reply too large\n");
      (void) close (s);
      serrno = SEINTERNAL;
      return (-1);
    }
    if ((n = netread (s, repbuf, c)) <= 0) {
      if (needsLogging) {
        if (n == 0) Cns_errmsg (func, NS002, "recv", sys_serrlist[SERRNO]);
        else Cns_errmsg (func, NS002, "recv", neterror());
      }
      (void) close (s);
      serrno = SECOMERR;
      return (-1);
    }
    p = repbuf;
    if (rep_type == MSG_ERR) {
      unmarshall_STRING (p, prtbuf);
      if (needsLogging) Cns_errmsg (NULL, "%s", prtbuf);
    } else if (rep_type == MSG_LINKS) {
      if (errflag) continue;
      if (alloced == 0) {
        repbuf2sz = 4096;
        if ((*repbuf2 = malloc (repbuf2sz)) == NULL) {
          errflag++;
          continue;
        }
        alloced = 1;
        *nbstruct = 0;
        li = (struct Cns_linkinfo *) *repbuf2;
      }
      while (p < repbuf + c) {
        if ((int)((char *)li - (char *)(*repbuf2) +
            sizeof(struct Cns_linkinfo)) > repbuf2sz) {
          repbuf2sz += 4096;
          if ((q = realloc (*repbuf2, repbuf2sz)) == NULL) {
            errflag++;
            break;
          }
          *repbuf2 = q;
          li = ((struct Cns_linkinfo *) *repbuf2) + *nbstruct;
        }
        unmarshall_STRING (p, li->path);
        (*nbstruct)++;
        li++;
      }
    } else if (user_repbuf) {
      if (actual_replen + c <= user_repbuf_len)
        n = c;
      else
        n = user_repbuf_len - actual_replen;
      if (n) {
        memcpy (user_repbuf + actual_replen, repbuf, n);
        actual_replen += n;
      }
    }
  }
  return (c);
}

int send2nsd(int *socketp,
             char *host,
             char *reqp,
             int reql,
             char *user_repbuf,
             int user_repbuf_len)
{
  return (send2nsdx_wrapper (socketp, host, reqp, reql, user_repbuf, user_repbuf_len,
                     NULL, NULL));
}
