/*
 * 
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlflogstore.c,v $ $Revision: 1.7 $ $Date: 2004/10/20 11:22:56 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <ctype.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#include <netdb.h>
#endif
#include "net.h"
#include "marshall.h"
#include "serrno.h"
#include "dlf.h"
#include "dlf_api.h"
#include "Cnetdb.h"
#include <ctype.h>
#if !defined(_WIN32)
#include <unistd.h>
#endif

#define	  DLF_KEY_DATE   1
#define	  DLF_KEY_HOST   2
#define   DLF_KEY_LVL    3
#define   DLF_KEY_PRGN   4
#define   DLF_KEY_PROG   5
#define   DLF_KEY_PID    6
#define   DLF_KEY_TID    7
#define	  DLF_KEY_MSGN   8
#define	  DLF_KEY_MSG    9
#define	  DLF_KEY_RQID  10
#define	  DLF_KEY_SRQID 11
#define	  DLF_KEY_TPVID 12
#define   DLF_KEY_CNS   13
#define   DLF_KEY_FID   14

int DLL_DECL dlf_get_pair _PROTO((FILE*, char*, char*));
int DLL_DECL dlf_iskeyword _PROTO((const char*));
int DLL_DECL dlf_send_log_message _PROTO((dlf_log_dst_t*, dlf_log_message_t*, int));

char *getconfent();
char *getenv();

struct _dlf_keyword {
	  int key_id;
	  char *key_name;
};

static struct _dlf_keyword keywords[] = {
	  { DLF_KEY_DATE, "DATE" },
	  { DLF_KEY_HOST, "HOST" },
	  { DLF_KEY_LVL,  "LVL" },
	  { DLF_KEY_PRGN, "PRGN" },
	  { DLF_KEY_PROG, "PROG" },
	  { DLF_KEY_PID,  "PID" },
	  { DLF_KEY_TID,  "TID" },
	  { DLF_KEY_MSGN, "MSGN" },
	  { DLF_KEY_MSG,  "MSG" },
	  { DLF_KEY_RQID, "RQID" },
	  { DLF_KEY_CNS,  "CNS" },
          { DLF_KEY_FID,  "FID" },
	  { DLF_KEY_SRQID,"DLF.SRQID*" },
	  { DLF_KEY_TPVID,"DLF.TPVID*" },
	  {             0,"" } /* End of list */
};

extern struct _dlf_level g_dlf_levels[];

extern	char	*optarg;
extern	int	optind;

int main(argc, argv)
     int argc;
     char **argv;
{
	int errflg;
	char c;
	char file_name[CA_MAXPATHLEN + 1];
	uid_t uid;
	gid_t gid;
	dlf_log_message_t log_message;
	FILE *in_file;
	int rv;
	int gp_rv;
	char par_name[DLF_MAXPARNAMELEN + 1];
	char par_str_val[DLF_MAXSTRVALLEN + 1];
	char *p;
	int n;
	int key;
	int i;
	Cuuid_t sub_request_id;
	char *end_ptr;
	HYPER i64val;
	int i32val;
	double dval;
	dlf_log_dst_t dst_host;
	int file_given = 0;
	int host_given = 0;
	int store_severity = -1;
	int port_number = 0;
	int new_line = 0;
	int last_message;
	struct servent *sp;
	int line_no = 1;
#if defined (_WIN32)
	WSADATA wsadata;
#endif

	uid = geteuid();
	gid = getegid();

#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		fprintf (stderr, DLF53);
		exit (USERR);
	}
#endif
	errflg = 0;
        while ((c = getopt (argc, argv, "f:s:h:p:?")) != EOF) {
                switch (c) {
		case 'f':
			if (strlen (optarg) <= CA_MAXPATHLEN) {
				strcpy (file_name, optarg);
				file_given++;
			}
			else {
				fprintf (stderr, "Illegal file name: %s\n", strerror(EINVAL));
				errflg++;
			}
                        break;
		case 's':
#if defined(_WIN32)
			for (i = 0; g_dlf_levels[i].lvl_id != 0 && stricmp(optarg, g_dlf_levels[i].lvl_name) != 0 ; i++);
#else
			for (i = 0; g_dlf_levels[i].lvl_id != 0 && strcasecmp(optarg, g_dlf_levels[i].lvl_name) != 0 ; i++);
#endif
			store_severity = g_dlf_levels[i].lvl_id;
			if (store_severity == 0) {
			        fprintf (stderr, "Unrecognized severity level: %s\n", optarg);
				errflg++;
			}
			break;
		case 'h':
			if (strlen (optarg) <= CA_MAXHOSTNAMELEN) {
				strcpy (dst_host.name, optarg);
				host_given++;
			}
			else {
				fprintf (stderr, "Illegal host name: %s\n", strerror(EINVAL));
				errflg++;
			}
                        break;
		case 'p':
		        if (dlf_isinteger(optarg)) {
 				port_number = atoi (optarg);
			}
			else {
				fprintf (stderr, "Illegal port: %s\n", strerror(EINVAL));
				errflg++;
			}
                        break;
		case '?':
                        errflg++;
                        break;
                default:
		        errflg++;
                        break;
		}
	}
        if (optind < argc) {
                errflg++;
	}
	if (!(file_given && host_given)) {
	        fprintf (stderr, "Not all required parameters are given\n");
	        errflg++;
	}
        if (errflg) {
                fprintf (stderr, "usage: %s %s\n", argv[0],
		    "-f file_name -h host_name [-p port_number] [-s severity_level]");
                exit (USERR);
	}
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, DLF52);
		exit (SYERR);
	}
#endif
	/* Set port */
	if (port_number == 0) {
	  if ((p = getenv ("DLF_PORT")) || (p = getconfent ("DLF", "PORT", 0))) {
	        port_number = atoi (p);
	  }
	  else if ((sp = Cgetservbyname ("dlf", "tcp")) != NULL) {
		port_number = ntohs(sp->s_port);
	  }
	  else {
	        port_number = DLF_PORT;
	  }
	}


	/* Initialize dst_host structure */
	dst_host.dst_type = DLF_DST_TCPHOST;
	dst_host.descr = -1; /* Not connected yet */
	dst_host.port = port_number;

	log_message.param_list.head = NULL;
	log_message.param_list.tail = NULL;
	log_message.next_tapevid_idx = 1;
	log_message.next_subreq_idx = 1;

	/* Open the file for reading */
	in_file = fopen(file_name, "r");
	if (in_file == NULL) {
	  fprintf (stderr, "Error opening log file %s: %s\n", file_name, sstrerror(errno));
	  exit (SYERR);
	}
	while (1) {
	  /* Parse line */
	  if (!new_line) {
	    rv = dlf_get_pair (in_file, par_name, par_str_val);
	  }
	  else {
	    new_line = 0;
	    rv = gp_rv;
	  }
	  if (rv <= 0) {
	    if (rv < 0) {
	      fprintf (stderr, "Bad log file format. Log file is %s\n", file_name);
	      exit (SYERR);
	    } else if (rv == 0) { /* End of file reached */
	      fclose (in_file);
	      fprintf (stdout, "%d messages have been transferred.\n", line_no - 1);
	      /* Connection to the server should be already closed */
	      exit(0);
	      /************************* PROGRAM END ******************************************/
	    }
	  } else if (rv == 1) { /* New line reached */
	      line_no++;  
	      new_line = 1;
	      /* Try to get new pair */
	      gp_rv =  dlf_get_pair (in_file, par_name, par_str_val);
	      if (gp_rv <= 0) last_message = 1; else last_message = 0;
	      rv = 0;
	      if (store_severity < 0 || log_message.severity == store_severity) {
		rv = dlf_send_log_message(&dst_host, &log_message, last_message);
	      }
	      dlf_delete_param_list (&log_message.param_list);
	      dlf_log_message_init(&log_message);
	      if (rv < 0) {
		fprintf (stderr, "Send message ERROR.\n");
		break;
	      }
	      else
		continue;
	  }
     
	  /* Search whether par_name is a keyword */
	  key = dlf_iskeyword(par_name);
	  rv = 0;
	  switch (key) {
	  case DLF_KEY_DATE:
	    /* Copy the date portion until '.' */
	    p = strchr (par_str_val, '.');
	    if (p == NULL) { /* We don't have microseconds */
	      n = strlen (par_str_val);
	      log_message.time_usec = 0;
	    }
	    else {
	      n = p - par_str_val;
	      if (dlf_isinteger(++p))
		log_message.time_usec = atoi (p);
	      else {
		rv = -1;
		break;
	      }
	    }
	    if (n != DLF_TIMESTRLEN) { /* Error in time value */
	      rv = -1;
	      serrno = EDLFLOGFORMAT;
	      break;
	    }
	    strncpy (log_message.time, par_str_val, n);
	    log_message.time[n] = '\0'; /* Terminate string */
	    break;
	  case DLF_KEY_HOST:
	    if (strlen(par_str_val) > CA_MAXHOSTNAMELEN) {
	      rv = -1;
	      serrno = EDLFLOGFORMAT;
	      break;
	    }
	    strcpy (log_message.hostname, par_str_val);
	    break;
	  case DLF_KEY_CNS:
	    if (strlen(par_str_val) > CA_MAXHOSTNAMELEN) {
	      rv = -1;
	      serrno = EDLFLOGFORMAT;
	      break;
	    }
	    strcpy (log_message.ns_fileid.server, par_str_val);
	    break;
	  case DLF_KEY_LVL:
	    for (i = 0; g_dlf_levels[i].lvl_id != 0 && strcmp(par_str_val, g_dlf_levels[i].lvl_name) != 0 ; i++);
	    log_message.severity = g_dlf_levels[i].lvl_id;
	    break;
	  case DLF_KEY_PRGN:
	    if (!dlf_isinteger( par_str_val )) {
	      rv = -1;
	      serrno = EDLFLOGFORMAT;
	      break;
	    }
	    log_message.facility_no = atoi(par_str_val);
	    break;
	  case DLF_KEY_PROG:
	    if (strlen(par_str_val) > DLF_MAXFACNAMELEN) {
	      rv = -1;
	      serrno = EDLFLOGFORMAT;
	      break;
	    }
	    break;
	  case DLF_KEY_PID:
	    if (!dlf_isinteger( par_str_val )) {
	      rv = -1;
	      serrno = EDLFLOGFORMAT;
	      break;
	    }
	    log_message.pid = atoi(par_str_val);
	    break;
	  case DLF_KEY_TID:
	    if (!dlf_isinteger( par_str_val )) {
	      rv = -1;
	      serrno = EDLFLOGFORMAT;
	      break;
	    }
	    log_message.cid = atoi(par_str_val);
	    break;
	  case DLF_KEY_MSGN:
	    if (!dlf_isinteger( par_str_val )) {
	      serrno = EDLFLOGFORMAT;
	      rv = -1;
	      break;
	    }
	    log_message.message_no = atoi(par_str_val);
	    break;
	  case DLF_KEY_MSG:
	    break;
	  case DLF_KEY_RQID:
	    rv = dlf_hex2uuid( par_str_val, log_message.request_id );
	    break;
	  case DLF_KEY_FID:
	    rv = dlf_hex2u64 ( par_str_val, &log_message.ns_fileid.fileid );
	    break;
	  case DLF_KEY_SRQID:
	    rv = dlf_hex2uuid( par_str_val, sub_request_id );
	    if (rv < 0)
	      break;
	    rv = dlf_add_subreq_id ( &log_message, sub_request_id );
	    break;
	  case DLF_KEY_TPVID:
	    rv = dlf_add_tpvid_parameter (&log_message, par_str_val);
	    break;
	  default:
	    /* Here we may get string, integer or floating point */
	    if (dlf_isinteger(par_str_val)) {
	      errno = 0; /* Clear error */
		  /* Try to store it as 32-bit value */
		  i32val = strtol(par_str_val, &end_ptr, 10);
	      if (*end_ptr == '\0' && errno != ERANGE) {
			i64val = i32val;
			rv = dlf_add_int_parameter(&log_message, par_name, i64val);
	      }
#if !defined (_WIN32)
	      else { /* Try to store it as 64-bit value */
		      i64val = strtoll(par_str_val, &end_ptr, 10);
			  if (*end_ptr == '\0' && errno != ERANGE) {
				rv = dlf_add_int_parameter(&log_message, par_name, i64val);
			  }
#endif
			  else { /* Store it as a string */
				rv = dlf_add_str_parameter (&log_message, par_name, par_str_val);
#if !defined (_WIN32)
			  }
#endif
		  }
	    }
	    else if (dlf_isfloat(par_str_val)) {
	      errno = 0; /* Clear error */
	      dval = strtod(par_str_val, &end_ptr);
	      if (*end_ptr == '\0' && errno != ERANGE) {
		rv = dlf_add_double_parameter(&log_message, par_name, dval);
	      }
	      else { /* Store it as a string */
		rv = dlf_add_str_parameter (&log_message, par_name, par_str_val);	      
	      }
	    }
	    else { /* It is a string */
	      rv = dlf_add_str_parameter (&log_message, par_name, par_str_val);	      
	    }
	    break;
	  }
	  if (rv < 0)
	    break;
	}
	fclose(in_file);
	fprintf (stderr, "Error occurred during processing of the file %s:-\n-Line number: %d  serror: %s\n",
		 file_name, line_no, sstrerror(serrno));
	exit (SYERR);
}

int DLL_DECL dlf_get_pair (in_file, par_name, par_str_val)
FILE *in_file;
char *par_name;
char *par_str_val;
{
  int c, d;
  char *p;
  int i;

  /* Skip spaces */
  while ((c = fgetc (in_file)) == ' ' || c == '\t');
  if ( c == '\n' ) return (1); /* End of line reached */
  if ( c == EOF ) return (0); /* End of file or read error */
  /* Get parameter name */
  ungetc (c, in_file);
  for (i = 0, p = par_name; (c = fgetc (in_file)) != '=' && c != '\n' && c != EOF && i < DLF_MAXPARNAMELEN; i++) p[i] = c;
  p[i] = '\0'; /* Terminate string */
  if (c != '=') return (-1); /* Parse error */
  /* Get parameter string */
  /* String may be delimited by \" or \' */
  /* Try to obtain delimiter (if any) */
  c = fgetc (in_file);
  if (c == EOF || c == '\n') return (-1); /* Parse error: unexpected end of line/file */
  if ( c == '"' || c == '\'') d = c; else {
    d = ' ';
    ungetc (c, in_file);
  }
  for (i = 0, p = par_str_val; (c = fgetc (in_file)) != d && c != '\n' && c != EOF && i < DLF_MAXSTRVALLEN; i++) p[i] = c;
  p[i] = '\0'; /* Terminate string */
  if (d == ' ') {
    ungetc (c, in_file);
  }
  return (2);
}

int DLL_DECL dlf_iskeyword(par_name)
const char *par_name;
{
  int i;
  int n;
  char *p;

  for (i = 0; keywords[i].key_id !=0; i++) {
    n = strlen(keywords[i].key_name) - 1; 
    if (keywords[i].key_name[n] == '*') {
      if (strncmp(keywords[i].key_name, par_name, n) == 0) {
	for (p = (char *)par_name + n; *p != '\0' && isdigit(*p); p++);
	if (p == par_name + strlen(par_name)) 
	  break;
	else
	  continue;
      }
    }
    else {
      if ( strcmp(keywords[i].key_name, par_name) == 0 ) break;
    }
  }
  return (keywords[i].key_id);
}

int DLL_DECL dlf_send_log_message(dst, log_message, last_message)
dlf_log_dst_t *dst;
dlf_log_message_t *log_message;
int last_message;
{
  char *buf;
  int buf_len;
  int rep_type;

  if (dlf_send_to_host(dst, log_message, 1, last_message) < 0)
    return (-1);
  /* Get reply */
  if (getrep (dst->descr, &buf, &buf_len, &rep_type) < 0) {
    return (-1);
  }
  if (!last_message) {
    if (rep_type != DLF_IRC) { /* Protocol error */
      /* Close connection */
      (void)netclose(dst->descr);
      return (-1);
    }
  }
  return (0);
}
