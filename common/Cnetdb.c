
/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */


#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cnetdb.c,v $ $Revision: 1.11 $ $Date: 2003/09/27 09:02:54 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * Cnetdb.c - CASTOR MT-safe wrappers on netdb routines.
 */ 

#include <sys/types.h>
#include <stddef.h>
#if defined(_WIN32)
#include <winsock2.h>
#else /* _WIN32 */
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#endif /* _WIN32 */

#include <Cglobals.h>
#include <Cnetdb.h>
#ifdef ADNS
#include <errno.h>
#include <Cthread_api.h>
#include <serrno.h>
#include <adns.h>
#ifndef CA_MAXIPNAMELEN
#define CA_MAXIPNAMELEN 15
#endif
#endif /* ADNS */

#if (defined(IRIX5) || defined(IRIX6) || defined(IRIX64))
EXTERN_C struct hostent *gethostbyname_r(const char *, struct hostent *, char *, int, int *);
EXTERN_C struct hostent *gethostbyaddr_r(const void *, size_t, int, struct hostent *, char *, int, int *);
EXTERN_C struct servent   *getservbyname_r(const char *, const char *,
                                         struct servent *, char *, int);
#endif
#ifdef ADNS
int _Cnetdb_ipaddr2domain _PROTO((char *, char *, char **, char **));
int _Cnetdb_ipaddr2str _PROTO((size_t, char *, unsigned long));
char *_Cgethostbyname_adns _PROTO((char *));
char *_Cgethostbyaddr_adns _PROTO((void *));
static int hostdata_key = -1;
/*
 * adns_if_noenv=        0x0001 :  do not look at environment
 * adns_if_noerrprint=   0x0002  : never print output to stderr (_debug overrides)
 * adns_if_noserverwarn= 0x0004  : do not warn to stderr about duff nameservers etc
 * adns_if_debug=        0x0008  : enable all output to stderr plus debug msgs
 * adns_if_logpid=       0x0080  : include pid in diagnostic output
 * adns_if_noautosys=    0x0010  : do not make syscalls at every opportunity
 * adns_if_eintr=        0x0020  : allow _wait and _synchronous to return EINTR
 * adns_if_nosigpipe=    0x0040  : applic has SIGPIPE set to SIG_IGN, do not protect
 * adns_if_checkc_entex= 0x0100  : do consistency checks on entry/exit to adns funcs
 * adns_if_checkc_freq=  0x0300  : do consistency checks very frequently (slow!)
 */
#define ADNS_INIT_FLAGS ( adns_if_noerrprint | adns_if_noserverwarn | adns_if_eintr )
/*
 * adns_qf_search=          0x00000001   : use the searchlist
 * adns_qf_usevc=           0x00000002   : use a virtual circuit (TCP connection)
 * adns_qf_owner=           0x00000004   : fill in the owner field in the answer
 * adns_qf_quoteok_query=   0x00000010   : allow special chars in query domain
 * adns_qf_quoteok_cname=   0x00000000   : allow ... in CNAME we go via - now default
 * adns_qf_quoteok_anshost= 0x00000040   : allow ... in things supposed to be hostnames
 * adns_qf_quotefail_cname= 0x00000080   : refuse if quote-req chars in CNAME we go via
 * adns_qf_cname_loose=     0x00000100   : allow refs to CNAMEs - without, get _s_cname
 * adns_qf_cname_forbid=    0x00000200   : don't follow CNAMEs, instead give _s_cname
 * adns__qf_internalmask=   0x0ff00000
 */
#define ADNS_QUERY_FLAGS ( adns_qf_search | adns_qf_quoteok_cname | adns_qf_cname_loose )
#endif /* ADNS */

#ifdef ADNS
/*
 * Parse the IP address and convert to a reverse domain name.
 */
int _Cnetdb_ipaddr2domain(buf, start, addr, rest)
     char *buf;
     char *start;
     char **addr;
     char **rest;
{
  char *ptrs[5];
  int i;
  
  ptrs[0]= start;
retry:
  while (!isdigit(*ptrs[0]))
    if (!*ptrs[0]++) {
      *addr= *rest= NULL;
      serrno = EINVAL;
      return(-1);
    }
  for (i= 1; i < 5; i++) {
    ptrs[i]= ptrs[i-1];
    while (isdigit(*ptrs[i]++));
    if (i == 4 && *(ptrs[i]-1) == '\0')
      break;
    if ((i == 4 && !isspace(ptrs[i][-1])) ||
        (i != 4 && ptrs[i][-1] != '.') ||
        (ptrs[i]-ptrs[i-1] > 4)) {
      ptrs[0]= ptrs[i]-1;
      goto retry;
    }
  }
  sprintf(buf, "%.*s.%.*s.%.*s.%.*s.in-addr.arpa.",
	  ptrs[4]-ptrs[3]-1, ptrs[3],
	  ptrs[3]-ptrs[2]-1, ptrs[2],
	  ptrs[2]-ptrs[1]-1, ptrs[1],
	  ptrs[1]-ptrs[0]-1, ptrs[0]);
  *addr= ptrs[0];
  *rest= ptrs[4]-1;
  return(0);
}
/*
 * _Cnetdb_ipaddr2str - Convert an unsigned long number to a string %d.%d.%d.%d
 */
int DLL_DECL _Cnetdb_ipaddr2str(maxsize,output,ipaddr)
     size_t maxsize;
     char *output;
     unsigned long ipaddr;
{
  unsigned char *stringified = (unsigned char *) &ipaddr;
  
  if (maxsize < strlen("255.255.255.255")) {
    serrno = SEINTERNAL;
    return(-1);
  }
  sprintf(output,"%d.%d.%d.%d",
          stringified[0] & 0xFF,
          stringified[1] & 0xFF,
          stringified[2] & 0xFF,
          stringified[3] & 0xFF);
  return (0);
}

char *_Cgethostbyname_adns(name)
     char *name;
{
  adns_state adns;
  adns_query query;
  adns_answer *answer;
  char *buffer = NULL;
  int bufsize = 1024;
  int status;

  /* Makes sure that we will return a thread-safe value */
  Cglobals_get(&hostdata_key,(void **)&buffer,bufsize);
  if (buffer == NULL) {
    h_errno = NO_RECOVERY;
    serrno = SEINTERNAL;
    return(NULL);
  }

  if (adns_init(&adns, ADNS_INIT_FLAGS, NULL) != 0) {
    serrno = SEADNSINIT;
    return(NULL);
  }

  if (adns_submit(adns, (char *) name, adns_r_addr, ADNS_QUERY_FLAGS, NULL, &query) != 0) {
    serrno = SEADNSSUBMIT;
    return(NULL);
  }

  if ((status = adns_check(adns, &query, &answer, NULL)) == EAGAIN) {
    while (1) {
      if ((status = adns_wait(adns, &query, &answer, NULL)) != EAGAIN) {
        break;
      }
      if (status == 0) {
        break;
      }
    }
  }
    
  status = answer->status;

  if (status == adns_s_ok) {
    if (answer->nrrs != 1) {
      status = SEADNSTOOMANY;
    }
  } else {
    status = SEADNS;
  }

  if (status == adns_s_ok) {
    CONST char *typename = NULL;
    char *datastr = NULL;
    
    if ((status = adns_rr_info(answer->type, &typename, NULL, NULL, answer->rrs.untyped, &datastr)) != adns_s_nomemory) {
      char *datastrok;
      /* This routine is creating the string "INET ...", and I want to get rid of "INET" */
      if ((datastrok = strrchr(datastr,' ')) != NULL) {
        strcpy(buffer,++datastrok);
      } else {
        strcpy(buffer,datastr);
      }
      free(datastr);
    }
  } else {
    serrno = status;
  }

  free(answer);
  adns_finish(adns);
  return(status == adns_s_ok ? buffer : NULL);
}

char *_Cgethostbyaddr_adns(addr)
     void *addr;
{
  char *line_addr, *line_rest;
  char str[1024];
  adns_state adns;
  adns_query query;
  adns_answer *answer;
  char *buffer = NULL;
  int bufsize = 1024;
  int status;

  /* Makes sure that we will return a thread-safe value */
  Cglobals_get(&hostdata_key,(void **)&buffer,bufsize);
  if (buffer == NULL) {
    h_errno = NO_RECOVERY;
    serrno = SEINTERNAL;
    return(NULL);
  }

  if (adns_init(&adns, ADNS_INIT_FLAGS, NULL) != 0) {
    serrno = SEADNSINIT;
    return(NULL);
  }

  if (_Cnetdb_ipaddr2domain(str, addr, &line_addr, &line_rest) != 0) {
    return(NULL);
  }

  if (adns_submit(adns, str, adns_r_ptr, ADNS_QUERY_FLAGS, NULL, &query) != 0) {
    serrno = SEADNSSUBMIT;
    return(NULL);
  }

  if ((status = adns_check(adns, &query, &answer, NULL)) == EAGAIN) {
    while (1) {
      if ((status = adns_wait(adns, &query, &answer, NULL)) != EAGAIN) {
        break;
      }
      if (status == 0) {
        break;
      }
    }
  }
    
  status = answer->status;

  if (status == adns_s_ok) {
    strcpy(buffer,*answer->rrs.str);
  } else {
    serrno = status;
  }

  free(answer);
  adns_finish(adns);
  return(status == adns_s_ok ? buffer : NULL);
}

#endif /* ADNS */

struct hostent DLL_DECL *Cgethostbyname(name)
CONST char *name;
{
#ifdef ADNS
  /*
   * We will always use a thread or process specific
   * hostent structure
   */
    static int hostent_key = -1;
    static int inaddr_key = -1;
    static int haddrlist_key = -1;
    struct hostent *result = (struct hostent *)NULL;
    struct in_addr *inaddr = (struct in_addr *)NULL;
    char **haddrlist = (char **)NULL;

    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&inaddr_key,(void **)&inaddr,sizeof(struct in_addr));
    Cglobals_get(&haddrlist_key,(void **)&haddrlist,sizeof(char **));
    if ( result == (struct hostent *)NULL || inaddr == (struct in_addr *)NULL || haddrlist == (char **)NULL ) {
        h_errno = NO_RECOVERY;
        return(NULL);
    }
    if ((result->h_name = _Cgethostbyname_adns((char *) name)) == NULL) {
        /*
         * One retry on EINTR
         */
        if ( errno == EINTR ) {
            if ((result->h_name = _Cgethostbyname_adns((char *) name)) == NULL) 
                return(NULL);
        } else return(NULL);
    }

    /* We convert the address into in_addr structure. This will fail if addr */
    /* is in a wrong format. */
    if (inet_aton((char *) result->h_name, inaddr) == 0) {
        h_errno = NO_RECOVERY;
        return(NULL);
    }
    /* Resolve succeeded. We store backward the inaddr result */
    haddrlist[0] = (char *) inaddr;
    result->h_addr_list = haddrlist;

    return(result);
#else /* ADNS */
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE)) || defined(__osf__) && defined(__alpha) || defined(_WIN32) || defined(HPUX11)
    /*
     * If single-thread compilation we don't do anything.
     * Also: Windows and Digital UNIX v4 and higher already
     *       provide thread safe version.
     */
    return(gethostbyname(name));
#elif defined(SOLARIS) || defined(IRIX6)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    struct hostent *hp;
    struct hostent *result = (struct hostent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;
    int h_errnoop = 0;
    
    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&hostdata_key,(void **)&buffer,bufsize);

    if ( result == (struct hostent *)NULL || buffer == (char *)NULL ) {
        h_errno = NO_RECOVERY;
        return(NULL);
    }
    hp = gethostbyname_r(name,result,buffer,bufsize,&h_errnoop);
    h_errno = h_errnoop;
    return(hp);
#elif defined(AIX42) || defined(hpux) || defined(HPUX10)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    int rc;
    struct hostent *result = (struct hostent *)NULL;
    struct hostent_data *ht_data = (struct hostent_data *)NULL;

    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&hostdata_key,(void **)&ht_data,sizeof(struct hostent_data));

    if ( result == (struct hostent *)NULL ||
        ht_data == (struct hostent_data *)NULL ) {
         h_errno = NO_RECOVERY;
         return(NULL);
    }

    rc = gethostbyname_r(name,result,ht_data);
    if (rc == -1) return(NULL);
    else return(result);
#elif defined(linux)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    int rc;
    struct hostent *hp;
    struct hostent *result = (struct hostent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;
    int h_errnoop = 0;

    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&hostdata_key,(void **)&buffer,bufsize);

    if ( result == (struct hostent *)NULL || buffer == (char *)NULL ) {
        h_errno = NO_RECOVERY;
        return(NULL);
    }
    rc = gethostbyname_r(name,result,buffer,bufsize,&hp,&h_errnoop);
    h_errno = h_errnoop;
    if ( rc == -1 ) hp = NULL;
    return(hp);
#else
    /*
     * Not supported
     */
    return(NULL);
#endif
#endif /* ADNS */
}

struct hostent DLL_DECL *Cgethostbyaddr(addr,len,type)
CONST void *addr;
size_t len;
int type;
{
#ifdef ADNS
  /*
   * We will always use a thread or process specific
   * hostent structure
   */
    static int hostent_key = -1;
    static int inaddr_key = -1;
    static int haddrlist_key = -1;
    struct hostent *result = (struct hostent *)NULL;
    struct in_addr *inaddr = (struct in_addr *)NULL;
    char **haddrlist = (char **)NULL;
    char ipaddr2str[CA_MAXIPNAMELEN+1]; /* IP address as a string */

    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&inaddr_key,(void **)&inaddr,sizeof(struct in_addr));
    Cglobals_get(&haddrlist_key,(void **)&haddrlist,sizeof(char **));
    if ( result == (struct hostent *)NULL || inaddr == (struct in_addr *)NULL || haddrlist == (char **)NULL ) {
        h_errno = NO_RECOVERY;
        return(NULL);
    }
    /* We convert the address into in_addr structure. This will fail if addr */
    /* is in a wrong format. */
    if (_Cnetdb_ipaddr2str(CA_MAXIPNAMELEN,ipaddr2str,((struct in_addr *) addr)->s_addr) < 0) {
        h_errno = NO_RECOVERY;
        return(NULL);
    }
    if ((result->h_name = _Cgethostbyaddr_adns(ipaddr2str)) == NULL) {
        /*
         * One retry on EINTR
         */
        if ( errno == EINTR ) {
            if ((result->h_name = _Cgethostbyaddr_adns(ipaddr2str)) == NULL)
                return(NULL);
        } else return(NULL);
    }
    /* We convert the address into in_addr structure. This will fail if addr */
    /* is in a wrong format. */
    if (inet_aton((char *) ipaddr2str, inaddr) == 0) {
        h_errno = NO_RECOVERY;
        return(NULL);
    }
    /* Resolve succeeded. We store backward the inaddr result */
    haddrlist[0] = (char *) inaddr;
    result->h_addr_list = haddrlist;

    return(result);
#else /* ADNS */
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE)) || defined(__osf__) && defined(__alpha) || defined(_WIN32) || defined(HPUX11)
    /*
     * If single-thread compilation we don't do anything.
     * Also: Windows and Digital UNIX v4 and higher already
     *       provide thread safe version.
     */
    return(gethostbyaddr(addr,len,type));
#elif defined(SOLARIS) || defined(IRIX6)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    struct hostent *hp;
    struct hostent *result = (struct hostent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;
    int h_errnoop = 0;
    
    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&hostdata_key,(void **)&buffer,bufsize);

    if ( result == (struct hostent *)NULL || buffer == (char *)NULL ) {
        h_errno = NO_RECOVERY;
        return(NULL);
    }
    hp = gethostbyaddr_r(addr,len,type,result,buffer,bufsize,&h_errnoop);
    h_errno = h_errnoop;
    return(hp);
#elif defined(AIX42) || defined(hpux) || defined(HPUX10)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    int rc;
    struct hostent *result = (struct hostent *)NULL;
    struct hostent_data *ht_data = (struct hostent_data *)NULL;

    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&hostdata_key,(void **)&ht_data,sizeof(struct hostent_data));

    if ( result == (struct hostent *)NULL ||
        ht_data == (struct hostent_data *)NULL ) {
         h_errno = NO_RECOVERY;
         return(NULL);
    }

    rc = gethostbyaddr_r(addr,len,type,result,ht_data);
    if (rc == -1) return(NULL);
    else return(result);
#elif defined(linux)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    int rc;
    struct hostent *hp;
    struct hostent *result = (struct hostent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;
    int h_errnoop = 0;

    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&hostdata_key,(void **)&buffer,bufsize);

    if ( result == (struct hostent *)NULL || buffer == (char *)NULL ) {
        h_errno = NO_RECOVERY;
        return(NULL);
    }
    rc = gethostbyaddr_r(addr,len,type,result,buffer,bufsize,&hp,&h_errnoop);
    h_errno = h_errnoop;
    if ( rc == -1 ) hp = NULL;
    return(hp);
#else
    /*
     * Not supported
     */
    return(NULL);
#endif
#endif /* ADNS */
}

struct servent DLL_DECL *Cgetservbyname(name,proto)
CONST char *name;
CONST char *proto;
{
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE)) || defined(__osf__) && defined(__alpha) || defined(_WIN32) || defined(HPUX11)
    /*
     * If single-thread compilation we don't do anything.
     * Also: Windows and Digital UNIX v4 and higher already
     *       provide thread safe version.
     */
    return(getservbyname(name,proto));
#elif defined(SOLARIS) || defined(IRIX6)
    static int servent_key = -1;
    static int servdata_key = -1;
    struct servent *sp;
    struct servent *result = (struct servent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;
   
    Cglobals_get(&servent_key,(void **)&result,sizeof(struct servent));
    Cglobals_get(&servdata_key,(void **)&buffer,bufsize);

    if ( result == (struct servent *)NULL || buffer == (char *)NULL ) {
        return(NULL);
    }
    sp = getservbyname_r(name,proto,result,buffer,bufsize);
    return(sp);
#elif defined(AIX42) || defined(hpux) || defined(HPUX10)
    static int servent_key = -1;
    static int servdata_key = -1;
    int rc;
    struct servent *result = (struct servent *)NULL;
    struct servent_data *st_data = (struct servent_data *)NULL;

    Cglobals_get(&servent_key,(void **)&result,sizeof(struct servent));
    Cglobals_get(&servdata_key,(void **)&st_data,sizeof(struct servent_data));

    if ( result == (struct servent *)NULL ||
        st_data == (struct servent_data *)NULL ) {
         return(NULL);
    }

    rc = getservbyname_r(name,proto,result,st_data);
    if (rc == -1) return(NULL);
    else return(result);
#elif defined(linux)
    static int servent_key = -1;
    static int servdata_key = -1;
    int rc;
    struct servent *sp;
    struct servent *result = (struct servent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;

    Cglobals_get(&servent_key,(void **)&result,sizeof(struct servent));
    Cglobals_get(&servdata_key,(void **)&buffer,bufsize);

    if ( result == (struct servent *)NULL || buffer == (char *)NULL ) {
        return(NULL);
    }
    rc = getservbyname_r(name,proto,result,buffer,bufsize,&sp);
    if ( rc == -1 ) sp = NULL;
    return(sp);
#else
    /*
     * Not supported
     */
    return(NULL);
#endif
}

