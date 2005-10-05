/*
 *
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlfserver.c,v $ $Revision: 1.14 $ $Date: 2005/10/05 12:42:42 $  CERN IT $Author: kotlyar $ ";
#endif /* not lint */

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <sys/time.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#endif
#include "Cinit.h"
#include "Cnetdb.h"
#include "Cpool_api.h"
#include "marshall.h"
#include "net.h"
#include "serrno.h"
#include "dlf.h"
#include "dlf_server.h"

#define MAXSTRLENGTH 33
int num_THR = DLF_NBTHREADS;
int being_shutdown;
char db_pwd[MAXSTRLENGTH];
char db_srvr[MAXSTRLENGTH];
char db_user[MAXSTRLENGTH];
char func[16];
int jid;
char localhost[CA_MAXHOSTNAMELEN+1];
int maxfds;
struct dlf_srv_thread_info *dlf_srv_thread_info;

unsigned int DLFBUFMESSAGES=DLFBUFMESSAGES_DEFAULT;
unsigned int DLFBUFPARAM=DLFBUFPARAM_DEFAULT;
unsigned int DLFFLUSHTIME=DLFFLUSHTIME_DEFUALT;

unsigned long dlf_allocate_buffers(struct dlf_srv_thread_info*);
void dlf_free_buffers(struct dlf_srv_thread_info*);



void* proc_flush (dummy)
 void* dummy;
 {
   int i;
   while(1)
   {
   sleep(DLFFLUSHTIME); /* seconds */
   for (i = 0; i < num_THR; i++) 
      {
      dlf_srv_thread_info[i].param_tag=1;
      if(dlf_srv_thread_info[i].dbfd.tr_started==0 && dlf_srv_thread_info[i].db_open_done==1 && dlf_srv_thread_info[i].s<0)
         {
         dlf_start_tr(dlf_srv_thread_info[i].s,&dlf_srv_thread_info[i].dbfd);
         dlf_flush_buffers(&dlf_srv_thread_info[i]);  
         dlf_end_tr(&dlf_srv_thread_info[i].dbfd);
         }
       }
   }
 }

   
dlf_main(main_args)
     struct main_args *main_args;
{
  int c;
  FILE *fopen(), *cf;
  char cfbuf[80];
  struct dlf_dbfd dbfd;
  void *doit(void *);
  char domainname[CA_MAXHOSTNAMELEN+1];
  struct sockaddr_in from;
  int fromlen = sizeof(from);
  char *getconfent();
  int i;
  int ipool;
  int on = 1; /* for REUSEADDR */
  char *p;
  char *p_p, *p_s, *p_u;
  fd_set readfd, readmask;
  int rqfd;
  int s;
  struct sockaddr_in sin;
  struct servent *sp;
  int thread_index;
  struct timeval timeval;
  char dlfconfigfile[CA_MAXPATHLEN+1];
  int errflg;
  char *endptr;
  int msgs_set = 0;
  unsigned long bufmem=0;
  unsigned long buftmp=0;

  char prtbuf[256];
  
  struct sigaction sact;
  sigset_t sset;
  
  jid = getpid();
  strcpy (func, "dlfserver");
  dlflogit (func, "started\n");
  
  errflg = 0;
/*
  if (main_args->argc > 3) {
        dlflogit (func, "Parameter error\n");
  	exit (USERR);
  }
*/

 
  optind = 1;
  opterr = 0;
  /* usage:  [-t number of thread] [-m messages in buffer per thread] [-p parrameters in buffer per thread] [-f flush buffer time in sec]*/
  while ((c = getopt (main_args->argc, main_args->argv, "t:m:p:f:")) != EOF) {
    switch (c) {
    case 't':
       if ((num_THR = atoi(optarg)) <= 0) 
          {
	  dlflogit(func, "-t option value is <= 0\n");
	  errflg++;
          }
       break;
    case 'm':
       if((i=atoi(optarg))<10 )
          {
          dlflogit(func, "-m option value is incorrect\n");
	  errflg++;
          }
       DLFBUFMESSAGES=i;   
       break;
    case 'p':
       if((i=atoi(optarg))<10)
          {
          dlflogit(func, "-p option value is incorrect\n");
	  errflg++;
          }
       DLFBUFPARAM=i;   
       break;
    case 'f':
       if((i=atoi(optarg))<1)
          {
          dlflogit(func, "-f option value is incorrect\n");
	  errflg++;
          }
       DLFFLUSHTIME=i;   
       break;
    default:
      errflg++;
      break;
    }
  }

  if (errflg) {
    dlflogit (func, "Parameter error\n");
    exit (USERR);
  }


 dlflogit(func, "initialize with %d threads\n", num_THR);
 dlflogit(func, "initialize with %d messages buffer per thread\n", DLFBUFMESSAGES);
 dlflogit(func, "initialize with %d parameters buffer per thread\n", DLFBUFPARAM);
 dlflogit(func, "initialize with %d sec flush buffers time\n", DLFFLUSHTIME);
 
 if ((dlf_srv_thread_info = calloc(num_THR,sizeof(struct dlf_srv_thread_info))) == NULL) {
   dlflogit(func, "calloc error (%s)\n", strerror(errno));
   exit(USERR);
 }

 /* allocate memory for the buffers at each thread */
 for(i=0;i<num_THR;i++)
    {
    if((buftmp=dlf_allocate_buffers(&dlf_srv_thread_info[i]))==0)
       {
       dlflogit(func, "calloc error for buffers (%s)\n", strerror(errno));
       exit(USERR);
       }
    bufmem+=buftmp;   
    }
    
  bufmem/=1024;
  dlflogit (func, "Allocated memory for buffers %uKB\n",bufmem);
 
  gethostname (localhost, CA_MAXHOSTNAMELEN+1);
  if (strchr (localhost, '.') == NULL) {
    if (Cdomainname (domainname, sizeof(domainname)) < 0) {
      dlflogit (func, "Unable to get domainname\n");
      exit (SYERR);
    }
    strcat (localhost, ".");
    strcat (localhost, domainname);
  }
  /* get login info from the dlf config file */

  if (strncmp (DLFCONFIG, "%SystemRoot%\\", 13) == 0 &&
      (p = getenv ("SystemRoot")))
    sprintf (dlfconfigfile, "%s%s", p, strchr (DLFCONFIG, '\\'));
  else
    strcpy (dlfconfigfile, DLFCONFIG);
  if ((cf = fopen (dlfconfigfile, "r")) == NULL) {
    dlflogit (func, "Can't open configuration file: %s\n", dlfconfigfile);
    return (CONFERR);
  }
  if (fgets (cfbuf, sizeof(cfbuf), cf) &&
      strlen (cfbuf) >= 5 && (p_u = strtok (cfbuf, "/\n"))) {
    strncpy (db_user, p_u, MAXSTRLENGTH-1);
    db_user[MAXSTRLENGTH-1] = 0;
    /* get the password, provided that it may be empty */
    if (cfbuf[strlen(p_u)+1] == '@') {
      /*  empty password */
      *db_pwd = 0;
    } else {
      if ((p_p = strtok (NULL, "@\n")) != NULL) {
        strncpy (db_pwd, p_p, MAXSTRLENGTH-1);
        db_user[MAXSTRLENGTH-1] = 0;
      } else {
        dlflogit (func, "Configuration file %s incorrect\n",
                  dlfconfigfile);
        return (CONFERR);
      }
    }
    if ((p_s = strtok (cfbuf+strlen(db_user)+strlen(db_pwd)+2, "\n")) != NULL) {
      strncpy (db_srvr, p_s, MAXSTRLENGTH-1);
      db_srvr[MAXSTRLENGTH-1] = 0;
    } else {
      dlflogit (func, "Configuration file %s incorrect : no server\n",
                dlfconfigfile);
      return (CONFERR);
    }
  } else {
    dlflogit (func, "Configuration file %s incorrect : no user name.\n",
              dlfconfigfile);
    return (CONFERR);
  }
  (void) fclose (cf);

  if (dlf_init_dbpkg () != 0) {
    return(SYERR);
  }

  /* create a pool of threads */

  if ((ipool = Cpool_create (num_THR, NULL)) < 0) {
    dlflogit (func, DLF02, "Cpool_create", sstrerror(serrno));
    return (SYERR);
  }
  for (i = 0; i < num_THR; i++) {
    dlf_srv_thread_info[i].s = -1;
    dlf_srv_thread_info[i].dbfd.idx = i;
  }

  FD_ZERO (&readmask);
  FD_ZERO (&readfd);
#if ! defined(_WIN32)
  signal (SIGPIPE,SIG_IGN);
  signal (SIGXFSZ,SIG_IGN);
#endif

  /* open request socket */

  if ((s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    dlflogit (func, DLF02, "socket", neterror());
    return (CONFERR);
  }
  memset ((char *)&sin, 0, sizeof(struct sockaddr_in)) ;
  sin.sin_family = AF_INET ;
  if ((p = getenv ("DLF_PORT")) || (p = getconfent ("DLF", "PORT", 0))) {
    sin.sin_port = htons ((unsigned short)atoi (p));
  } else if (sp = getservbyname ("dlf", "tcp")) {
    sin.sin_port = sp->s_port;
  } else {
    sin.sin_port = htons ((unsigned short)DLF_PORT);
  }

  sprintf(prtbuf, "port: %d\n", ntohs(sin.sin_port));
  dlflogit(func, prtbuf);

  sin.sin_addr.s_addr = htonl(INADDR_ANY);
  if (setsockopt (s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    dlflogit (func,  "setsockopt error (%s)\n", strerror(errno));
  if (bind (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
    dlflogit (func, DLF02, "bind", neterror());
    return (CONFERR);
  }
  if (listen (s, 5) != 0) {
    dlflogit (func, "listen error (%s)\n", strerror(errno));
    return (CONFERR);
  }

  FD_SET (s, &readmask);
  /* Creating flushing thread */ 
  if(Cthread_create(&proc_flush,NULL)<0) 
     {
     dlflogit (func, "Cthread_create error", sstrerror(serrno));
     return (SYERR);
     }
  
  /* main loop */
  while (1) {
    if (being_shutdown) {
      int nb_active_threads = 0;
      for (i = 0; i < num_THR; i++) {
        if (dlf_srv_thread_info[i].s >= 0) {
          nb_active_threads++;
          continue;
        }
        if (dlf_srv_thread_info[i].db_open_done)
           {
           dlf_start_tr(dlf_srv_thread_info[i].s,&dlf_srv_thread_info[i].dbfd);
           dlf_flush_buffers(&dlf_srv_thread_info[i]);  
           dlf_end_tr(&dlf_srv_thread_info[i].dbfd);
           (void) dlf_closedb (&dlf_srv_thread_info[i].dbfd);
           }
      }
      if (nb_active_threads == 0)
         {
         for (i = 0; i < num_THR; i++) dlf_free_buffers(&dlf_srv_thread_info[i]);
         return (0);
         }
    }
    if (FD_ISSET (s, &readfd)) {
      FD_CLR (s, &readfd);
      rqfd = accept (s, (struct sockaddr *) &from, &fromlen);
#if (defined(SOL_SOCKET) && defined(SO_KEEPALIVE))
      {
	int on = 1;
	/* Set socket option */
	setsockopt(rqfd,SOL_SOCKET,SO_KEEPALIVE,(char *) &on,sizeof(on));
      }
#endif
      if ((thread_index = Cpool_next_index (ipool)) < 0) {
        dlflogit (func, DLF02, "Cpool_next_index", sstrerror(serrno));
        if (serrno == SEWOULDBLOCK) {
          sendrep (rqfd, DLF_RC, serrno);
          continue;
        } else
          return (SYERR);
      }
      dlf_srv_thread_info[thread_index].s = rqfd;
      if (Cpool_assign (ipool, &doit,
                        &dlf_srv_thread_info[thread_index], 1) < 0) {
        dlf_srv_thread_info[thread_index].s = -1;
        dlflogit (func, DLF02, "Cpool_assign", sstrerror(serrno));
        return (SYERR);
      }
    }
    memcpy (&readfd, &readmask, sizeof(readmask));
    timeval.tv_sec = CHECKI;
    timeval.tv_usec = 0;
    if (select (maxfds, &readfd, (fd_set *)0, (fd_set *)0, &timeval) < 0) {
      FD_ZERO (&readfd);
    }
  }
}

/* returns 0 if errors or allocated memory in other case */
unsigned long dlf_allocate_buffers(thread_info)
struct dlf_srv_thread_info *thread_info;
       { 
        unsigned long bufmem=0;   
        if((thread_info->arr_msgseq_no=calloc(DLFBUFMESSAGES,sizeof(unsigned long))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFMESSAGES*sizeof(unsigned long);   
        if((thread_info->arr_time_usec=calloc(DLFBUFMESSAGES,sizeof(int))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFMESSAGES*sizeof(int);
        if((thread_info->arr_host_id=calloc(DLFBUFMESSAGES,sizeof(int))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }           
        bufmem+=DLFBUFMESSAGES*sizeof(int);   
        if((thread_info->arr_ns_host_id=calloc(DLFBUFMESSAGES,sizeof(int))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFMESSAGES*sizeof(int);   
        if((thread_info->arr_facility=calloc(DLFBUFMESSAGES,sizeof(int))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFMESSAGES*sizeof(int);   
        if((thread_info->arr_severity=calloc(DLFBUFMESSAGES,sizeof(int))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFMESSAGES*sizeof(int);   
        if((thread_info->arr_msg_no=calloc(DLFBUFMESSAGES,sizeof(int))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFMESSAGES*sizeof(int);    
        if((thread_info->arr_pid=calloc(DLFBUFMESSAGES,sizeof(int))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFMESSAGES*sizeof(int);   
        if((thread_info->arr_thread_id=calloc(DLFBUFMESSAGES,sizeof(int))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFMESSAGES*sizeof(int);   
        if((thread_info->arr_time=calloc(DLFBUFMESSAGES,sizeof(chartime))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFMESSAGES*sizeof(chartime);   
        if((thread_info->arr_req_id=calloc(DLFBUFMESSAGES,sizeof(charid))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFMESSAGES*sizeof(charid);           
        if((thread_info->arr_ns_file_id=calloc(DLFBUFMESSAGES,sizeof(charid))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFMESSAGES*sizeof(charid);           

        if((thread_info->arr_param_str_par_name=calloc(DLFBUFPARAM,sizeof(charparname))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFPARAM*sizeof(charparname);           
        if((thread_info->arr_param_str_val=calloc(DLFBUFPARAM,sizeof(charval))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFPARAM*sizeof(charparname);    
        if((thread_info->arr_msg_seq_num_str_param=calloc(DLFBUFPARAM,sizeof(unsigned long))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }   
        bufmem+=DLFBUFPARAM*sizeof(unsigned long);   
        if((thread_info->arr_msg_seq_num_tpvid=calloc(DLFBUFPARAM,sizeof(unsigned long))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFPARAM*sizeof(unsigned long);   
        if((thread_info->arr_tpvid_val=calloc(DLFBUFPARAM,sizeof(charval))) == NULL)
           {
           dlflogit (func, "calloc error (%s)\n", strerror(errno));
           return(0);
           }
        bufmem+=DLFBUFPARAM*sizeof(charval);  
        if((thread_info->arr_msg_seq_num_param_int64=calloc(DLFBUFPARAM,sizeof(unsigned long))) == NULL)
          {
          dlflogit (func, "calloc error (%s)\n", strerror(errno));
          return(0);
          }
        bufmem+=DLFBUFPARAM*sizeof(unsigned long);
        if((thread_info->arr_param_int64_par_name=calloc(DLFBUFPARAM,sizeof(charparname))) == NULL)
          {
          dlflogit (func, "calloc error (%s)\n", strerror(errno));
          return(0);
          }
        bufmem+=DLFBUFPARAM*sizeof(charparname);
        if((thread_info->arr_param_int64_par_val=calloc(DLFBUFPARAM,sizeof(charval2))) == NULL)
          {
          dlflogit (func, "calloc error (%s)\n", strerror(errno));
          return(0);
          }
        bufmem+=DLFBUFPARAM*sizeof(charval2);
           
       if((thread_info->arr_msg_seq_num_param_double=calloc(DLFBUFPARAM,sizeof(unsigned long))) == NULL)
          {
          dlflogit (func, "calloc error (%s)\n", strerror(errno));
          return(0);
          }
        bufmem+=DLFBUFPARAM*sizeof(unsigned long);
        
        if((thread_info->arr_param_duble_par_name=calloc(DLFBUFPARAM,sizeof(charval))) == NULL)
          {
          dlflogit (func, "calloc error (%s)\n", strerror(errno));
          return(0);
          }
        bufmem+=DLFBUFPARAM*sizeof(charval);
        
        if((thread_info->arr_param_double_val=calloc(DLFBUFPARAM,sizeof(charval2))) == NULL)
          {
          dlflogit (func, "calloc error (%s)\n", strerror(errno));
          return(0);
          }
        bufmem+=DLFBUFPARAM*sizeof(charval2);
        
        if((thread_info->arr_msg_seq_num_rq_ids_map=calloc(DLFBUFPARAM,sizeof(unsigned long))) == NULL)
          {
          dlflogit (func, "calloc error (%s)\n", strerror(errno));
          return(0);
          }
        bufmem+=DLFBUFPARAM*sizeof(unsigned long);
        
        if((thread_info->arr_rq_id_str=calloc(DLFBUFPARAM,sizeof(charid))) == NULL)
          {
          dlflogit (func, "calloc error (%s)\n", strerror(errno));
          return(0);
          }
        bufmem+=DLFBUFPARAM*sizeof(charid);
        if((thread_info->arr_subrq_id_str=calloc(DLFBUFPARAM,sizeof(charid))) == NULL)
          {
          dlflogit (func, "calloc error (%s)\n", strerror(errno));
          return(0);
          }
       bufmem+=DLFBUFPARAM*sizeof(charid);
       
        thread_info->nummsg=0;
        thread_info->numparamstr=0;
        thread_info->numtpvid=0;
        thread_info->paramint64=0;
        thread_info->paramdouble=0;
        thread_info->rqids=0;
        thread_info->param_tag=0;
        thread_info->seq_pos=10000;
        thread_info->seq_num=0;

       return (bufmem);
       }
void dlf_free_buffers(dlf_srv_thread_info)
      struct dlf_srv_thread_info *dlf_srv_thread_info;
        {
        free(dlf_srv_thread_info->arr_subrq_id_str);
        free(dlf_srv_thread_info->arr_rq_id_str);
        free(dlf_srv_thread_info->arr_msg_seq_num_rq_ids_map);
        free(dlf_srv_thread_info->arr_param_double_val);
        free(dlf_srv_thread_info->arr_param_duble_par_name);
        free(dlf_srv_thread_info->arr_msg_seq_num_param_double);
        free(dlf_srv_thread_info->arr_param_int64_par_val);
        free(dlf_srv_thread_info->arr_param_int64_par_name);
        free(dlf_srv_thread_info->arr_msg_seq_num_param_int64);
        free(dlf_srv_thread_info->arr_tpvid_val);
        free(dlf_srv_thread_info->arr_msg_seq_num_tpvid);
        free(dlf_srv_thread_info->arr_msg_seq_num_str_param);
        free(dlf_srv_thread_info->arr_param_str_val);
        free(dlf_srv_thread_info->arr_param_str_par_name);
        
        free(dlf_srv_thread_info->arr_ns_file_id);
        free(dlf_srv_thread_info->arr_req_id);
        free(dlf_srv_thread_info->arr_time);
        free(dlf_srv_thread_info->arr_thread_id);
        free(dlf_srv_thread_info->arr_pid);
        free(dlf_srv_thread_info->arr_msg_no);
        free(dlf_srv_thread_info->arr_severity);
        free(dlf_srv_thread_info->arr_facility);
        free(dlf_srv_thread_info->arr_ns_host_id);
        free(dlf_srv_thread_info->arr_host_id);
        free(dlf_srv_thread_info->arr_time_usec);
        free(dlf_srv_thread_info->arr_msgseq_no);
        }
int main(argc,argv)
     int argc;
     char **argv;
{
  struct main_args main_args;
  main_args.argc = argc;
  main_args.argv = argv;
#if ! defined(_WIN32)
  if ((maxfds = Cinitdaemon ("dlfserver", NULL)) < 0)
    exit (SYERR);
  exit (dlf_main (&main_args));
#else
  if (Cinitservice ("dlf", &dlf_main))
    exit (SYERR);
#endif
}

void *
doit(arg)
     void *arg;
{
  int c;
  char *clienthost;
  int magic;
  char* req_data;
  int req_type = 0;
  int data_len;
  struct dlf_srv_thread_info *thip = (struct dlf_srv_thread_info *) arg;

  if ((c = getreq (thip->s, &magic, &req_type,
                   &req_data, &data_len, &clienthost)) == 0)
    procreq (magic, req_type, req_data, data_len, clienthost, thip);
  else if (c > 0)
    sendrep (thip->s, DLF_RC, c);
  else
    netclose (thip->s);
  thip->s = -1;
  return (NULL);
}

getreq(s, magic, req_type, req_data, data_len, clienthost)
     int s;
     int *magic;
     int *req_type;
     char **req_data;
     int *data_len;
     char **clienthost;
{
  struct sockaddr_in from;
  int fromlen = sizeof(from);
  struct hostent *hp;
  int l;
  int msglen;
  int n;
  char *rbp;
  char req_hdr[3*LONGSIZE];

  l = netread_timeout (s, req_hdr, sizeof(req_hdr), DLF_TIMEOUT);
  if (l == sizeof(req_hdr)) 
    {
    rbp = req_hdr;
    unmarshall_LONG (rbp, n);
    *magic = n;
    unmarshall_LONG (rbp, n);
    *req_type = n;
    unmarshall_LONG (rbp, msglen);

    l = msglen - sizeof(req_hdr);
    *data_len = l;
    *req_data = (char*)malloc(l);
    if (*req_data == NULL)
       {
       dlflogit (func, "memory allocation failure (%s)\n", strerror(errno));
       return (ENOMEM);
       }
    n = netread_timeout (s, *req_data, l, DLF_TIMEOUT);
    if (being_shutdown) 
       {
       free(*req_data);
       return (EDLFNACT);
       }
    if (getpeername (s, (struct sockaddr *) &from, &fromlen) < 0) 
       {
       dlflogit (func, DLF02, "getpeername", neterror());
       free(*req_data);
       return (SEINTERNAL);
       }
    /* for what? decrease performance , Cupv_check must use then only ip address!? 
    hp = Cgethostbyaddr ((char *)(&from.sin_addr), sizeof(struct in_addr), from.sin_family);
    if (hp == NULL) *clienthost = inet_ntoa (from.sin_addr);
    else    *clienthost = hp->h_name ;
    */
    *clienthost = inet_ntoa (from.sin_addr); 
    return (0);
    } 
  else
    {
    if (l > 0)  dlflogit (func, DLF04, l);
    else if (l < 0)
       {
       dlflogit (func, DLF02, "netread", strerror(errno));
       }
    return (SEINTERNAL);
   }
}


procreq(magic, req_type, req_data, data_len, clienthost, thip)
     int magic;
     int req_type;
     char *req_data;
     int data_len;
     char *clienthost;
     struct dlf_srv_thread_info *thip;
{
  int c;

  /* connect to the database if not done yet */

  if (! thip->db_open_done) {

    if (req_type != DLF_SHUTDOWN) {
      if (dlf_opendb (db_srvr, db_user, db_pwd, &thip->dbfd) < 0) {
        c = serrno;
        sendrep (thip->s, MSG_ERR, "db open error: %d\n", c);
        sendrep (thip->s, DLF_RC, c);
        free (req_data);
        return;
      }
      thip->db_open_done = 1;
    }
  }
  
  switch (req_type) {
  case DLF_SHUTDOWN:
    c = dlf_srv_shutdown (magic, req_data, clienthost, thip);
    break;
  case DLF_STORE:
    c = proc_entermsgreq (magic, req_type, req_data, data_len,
                          clienthost, thip);
    break;
  case DLF_GETMSGTEXTS:
    c = dlf_srv_getmsgtexts (magic, req_data, clienthost, thip);
    break;
  case DLF_ENTERFAC:
    c = dlf_srv_enterfacility (magic, req_data, clienthost, thip);
    break;
  case DLF_GETFAC:
    c = dlf_srv_getfacilites(magic, req_data, clienthost, thip);
    break;
  case DLF_ENTERTXT:
    c = dlf_srv_entertext (magic, req_data, clienthost, thip);
    break;
  case DLF_MODFAC:
    c = dlf_srv_modfacility (magic, req_data, clienthost, thip);
    break;
  case DLF_DELFAC:
    c = dlf_srv_delfacility (magic, req_data, clienthost, thip);
    break;
  case DLF_MODTXT:
    c = dlf_srv_modtext (magic, req_data, clienthost, thip);
    break;
  case DLF_DELTXT:
    c = dlf_srv_deltext (magic, req_data, clienthost, thip);
    break;

  default:
    sendrep (thip->s, MSG_ERR, DLF03, req_type);
    c = SEINTERNAL;
    break;
  }
  if(c>0 && serrno==EDLFNOTCONNECTED) thip->db_open_done=0;
  if (req_type != DLF_STORE) {
    /*
      Free memory allocated by getreq.
      When request is DLF_STORE memory is freed by proc_entermsgreq()
    */
    free (req_data);
  }
  sendrep (thip->s, DLF_RC, c);
}

proc_entermsgreq(magic, req_type, req_data, data_len, clienthost, thip)
     int magic;
     int req_type;
     char *req_data;
     int data_len;
     char *clienthost;
     struct dlf_srv_thread_info *thip;
{
  int c;
  int last_message;
  int new_req_type = -1;
  int rc = 0;
  fd_set readfd, readmask;
  struct timeval timeval;

  int mag;
  char *r_data;
  int d_len;
  char *c_host;

  /* wait for requests and process them */
  mag = magic;
  r_data = req_data;
  d_len = data_len;
  c_host = clienthost;

  FD_ZERO (&readmask);
  FD_SET (thip->s, &readmask);
  while (1) {
    if (req_type == DLF_STORE) {
      if (c = dlf_srv_entermessage (mag, r_data, d_len, c_host,
                                    thip, &last_message)){
        return(c);
      }
    }
    else {
      return (EINVAL);
    }
    free (r_data);
    if (last_message) break;

    sendrep (thip->s, DLF_IRC, 0);
    memcpy (&readfd, &readmask, sizeof(readmask));
    timeval.tv_sec = DLF_READRQTIMEOUT;
    timeval.tv_usec = 0;
    if (select (thip->s+1, &readfd, (fd_set *)0, (fd_set *)0, &timeval) <= 0) {
      break;
    }
    if (rc = getreq (thip->s, &mag, &new_req_type, &r_data,
                     &d_len, &c_host) != 0) {
      break;
    }
    if (new_req_type != req_type)
      break;
  }
  return (rc);
}
