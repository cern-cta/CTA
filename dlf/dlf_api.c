/*
*
* Copyright (C) 2003 by CERN/IT/ADC
* All rights reserved
*
*/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlf_api.c,v $ $Revision: 1.22 $ $Date: 2005/05/10 16:34:51 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */


/*********************************************************************

  API interface to the CASTOR Distributed Logging Facility
  
 ----------------------------------------------------------------------
    
 int dlf_init (const char* facility_name)
	  
 Initializes DLF global structure.
 Reads the configuration file and stores log destinations.
 Get log messages form the DLF server and stores them in memory. 
		
 ----------------------------------------------------------------------
		  
 int dlf_write (Cuuid_t request_id, int severity, int message_no,
               struct Cns_fileid *ns_invariant, int numparams, ...)
			
 Writes log message to the destination according to the severity.
 Message parameters should be in trios: parameter name, parameter type,
 parameter value.
			  
***********************************************************************/

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#if !defined(_WIN32)
#include <sys/file.h>
#include <unistd.h>
#else
#include <io.h>
#include <process.h>
#include <pwd.h>				/* For getegid(), geteuid() */
#endif
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <Cglobals.h>
#include <errno.h>
#include <serrno.h>
#include <dlf.h>
#include <dlf_api.h>
#include <marshall.h>
#if !defined(_WIN32)
#include <netinet/in.h>
#include <netdb.h>
#else
#include <winsock2.h>
#endif
#include <Cnetdb.h>
#include <Csnprintf.h>
#ifndef _WIN32
#include <sys/time.h>          /* For time_t */
#else
#include <time.h>              /* For time_t */
#endif
#include <ctype.h>

dlf_facility_info_t g_dlf_fac_info;

struct _dlf_level g_dlf_levels[] = {
	{ DLF_LVL_EMERGENCY, "Emergency" },
	{ DLF_LVL_ALERT,     "Alert" },
	{ DLF_LVL_ERROR,     "Error" },
	{ DLF_LVL_WARNING,   "Warning" },
	{ DLF_LVL_AUTH,      "Auth" },
	{ DLF_LVL_SECURITY,  "Security" },
	{ DLF_LVL_USAGE,     "Usage" },
	{ DLF_LVL_SYSTEM,    "System" },
	{ DLF_LVL_IMPORTANT, "Important" },
	{ DLF_LVL_DEBUG,     "Debug" },
	{ 0,                 "" } /* End of list */
};

char *getconfent();
char *getenv();

int DLL_DECL dlf_init(fac_name)
const char *fac_name;
{
	unsigned int fac_no;
	char *p;
	int standalone;
	int port;
	int n;
	dlf_log_dst_t *dst;
	char dlfhost[CA_MAXHOSTNAMELEN + 1];
	char dlfmsgfile[CA_MAXPATHLEN + 1];
	char dlfupname[DLF_MAXFACNAMELEN + 1];
	char dlfenvname[DLF_MAXFACNAMELEN + 2 + 16];
	int dlfnamelen;
	struct servent *sp;
    
	/* Initialize global structure */
    
	if (strlen(fac_name) > DLF_MAXFACNAMELEN) {
		serrno = EINVAL;
		return(-1);
	}
	strcpy(g_dlf_fac_info.fac_name, fac_name);
    
	g_dlf_fac_info.dest_list.head = NULL;
	g_dlf_fac_info.dest_list.tail = NULL;
    
	g_dlf_fac_info.text_list.head = NULL;
	g_dlf_fac_info.text_list.head = NULL;

	dlfnamelen = strlen(fac_name);
	if (dlfnamelen > DLF_MAXFACNAMELEN) return (-1);

	strcpy (dlfupname, fac_name);
	for (p = dlfupname; *p != '\0'; p++) *p = toupper(*p);

	standalone = 0;

	strcpy (dlfenvname, dlfupname);
	strcat (dlfenvname, "_LOGMODE");
	if ((p = getenv (dlfenvname)) || (p = getconfent (fac_name, "LOGMODE", 0))) {
		if (strcmp (p, "STANDALONE") == 0)
		       standalone = 1;
	}

	/* We don't need the server when in standalone mode */

	if (!standalone) {
    
	  /* Get the default server */
    
	  if ((p = getenv ("DLF_PORT")) || (p = getconfent ("DLF", "PORT", 0))) {
		port = atoi (p);
	  }
	  else if ((sp = Cgetservbyname ("dlf", "tcp")) != NULL) {
		port = ntohs(sp->s_port);
	  }
	  else {
		port = DLF_PORT;
	  }
	  if ((p = getenv ("DLF_HOST")) || (p = getconfent ("DLF", "HOST", 0)))
		strncpy (dlfhost, p, CA_MAXHOSTNAMELEN);
	  else
#if defined(DLF_HOST)
		strncpy (dlfhost, DLF_HOST, CA_MAXHOSTNAMELEN);
#else
	  gethostname (dlfhost, sizeof(dlfhost));
#endif
	
	  g_dlf_fac_info.default_port = port;
	  if (dlf_add_to_log_dst(DLF_DST_TCPHOST, port, dlfhost, DLF_LVL_SERVICE_ONLY,
		&g_dlf_fac_info.dest_list) < 0)
		return (-1);
	}
	/* Check if it's the DLF control application */
	/* It can not run in standalone mode         */
    
	if (strcmp(fac_name, "DLF-CONTROL") == 0) {
	  if (standalone) {
	    serrno = EINVAL;
	    return (-1);
	  }
	  else
	    return (0);
	}
    
	/* Try to get log destinations from environment variables or from the configuration file */
    
	dlfenvname[dlfnamelen] = '\0';           /* Set termination just after the name */
	strcat (dlfenvname, "_LOGALL");
	if ((p = getenv(dlfenvname)) || (p = getconfent(fac_name, "LOGALL", 1)))
		if (dlf_store_log_destinations
                        (p, DLF_LVL_ALL, &g_dlf_fac_info.dest_list) < 0)
			return (-1);

	dlfenvname[dlfnamelen] = '\0';           /* Set termination just after the name */
	strcat (dlfenvname, "_LOGEMERGENCY");
	if ((p = getenv(dlfenvname)) || (p = getconfent(fac_name, "LOGEMERGENCY", 1)))
		if (dlf_store_log_destinations
			(p, DLF_LVL_EMERGENCY, &g_dlf_fac_info.dest_list) < 0)
			return (-1);

	dlfenvname[dlfnamelen] = '\0';           /* Set termination just after the name */
	strcat (dlfenvname, "_LOGALERT");
	if ((p = getenv(dlfenvname)) || (p = getconfent(fac_name, "LOGALERT", 1)))
		if (dlf_store_log_destinations
			(p, DLF_LVL_ALERT, &g_dlf_fac_info.dest_list) < 0)
			return (-1);

	dlfenvname[dlfnamelen] = '\0';           /* Set termination just after the name */
	strcat (dlfenvname, "_LOGERROR");
	if ((p = getenv(dlfenvname)) || (p = getconfent(fac_name, "LOGERROR", 1)))
		if (dlf_store_log_destinations
			(p, DLF_LVL_ERROR, &g_dlf_fac_info.dest_list) < 0)
			return (-1);

	dlfenvname[dlfnamelen] = '\0';           /* Set termination just after the name */
	strcat (dlfenvname, "_LOGWARNING");
	if ((p = getenv(dlfenvname)) || (p = getconfent(fac_name, "LOGWARNING", 1)))
		if (dlf_store_log_destinations
			(p, DLF_LVL_WARNING, &g_dlf_fac_info.dest_list) < 0)
			return (-1);

	dlfenvname[dlfnamelen] = '\0';           /* Set termination just after the name */
	strcat (dlfenvname, "_LOGAUTH");
	if ((p = getenv(dlfenvname)) || (p = getconfent(fac_name, "LOGAUTH", 1)))
		if (dlf_store_log_destinations
			(p, DLF_LVL_AUTH, &g_dlf_fac_info.dest_list) < 0)
			return (-1);

	dlfenvname[dlfnamelen] = '\0';           /* Set termination just after the name */
	strcat (dlfenvname, "_LOGSECURITY");
	if ((p = getenv(dlfenvname)) || (p = getconfent(fac_name, "LOGSECURITY", 1)))
		if (dlf_store_log_destinations
			(p, DLF_LVL_SECURITY, &g_dlf_fac_info.dest_list) < 0)
			return (-1);

	dlfenvname[dlfnamelen] = '\0';           /* Set termination just after the name */
	strcat (dlfenvname, "_LOGUSAGE");
	if ((p = getenv(dlfenvname)) || (p = getconfent(fac_name, "LOGUSAGE", 1)))
		if (dlf_store_log_destinations
			(p, DLF_LVL_USAGE, &g_dlf_fac_info.dest_list) < 0)
			return (-1);

	dlfenvname[dlfnamelen] = '\0';           /* Set termination just after the name */
	strcat (dlfenvname, "_LOGSYSTEM");
	if ((p = getenv(dlfenvname)) || (p = getconfent(fac_name, "LOGSYSTEM", 1)))
		if (dlf_store_log_destinations
		(p, DLF_LVL_SYSTEM, &g_dlf_fac_info.dest_list) < 0)
			return (-1);

	dlfenvname[dlfnamelen] = '\0';           /* Set termination just after the name */
	strcat (dlfenvname, "_LOGIMPORTANT");
	if ((p = getenv(dlfenvname)) || (p = getconfent(fac_name, "LOGIMPORTANT", 1)))
		if (dlf_store_log_destinations
		(p, DLF_LVL_IMPORTANT, &g_dlf_fac_info.dest_list) < 0)
			return (-1);

	dlfenvname[dlfnamelen] = '\0';           /* Set termination just after the name */
	strcat (dlfenvname, "_LOGDEBUG");
	if ((p = getenv(dlfenvname)) || (p = getconfent(fac_name, "LOGDEBUG", 1)))
		if (dlf_store_log_destinations
		(p, DLF_LVL_DEBUG,&g_dlf_fac_info.dest_list) < 0)
		        return (-1);
	
	if (standalone) {
	  /* Read message texts from the supplied file.               */
	  /* The file structure:                                      */
	  /* The first line:                                          */
	  /* <facility_name><spaces><facility_number>                 */
          /* Other lines:                                             */
	  /* <message_number><spaces><message text until end of line> */

	  dlfenvname[dlfnamelen] = '\0';           /* Set termination just after the name */
	  strcat (dlfenvname, "_TXTFILE");
	  if ((p = getenv (dlfenvname)) || (p = getconfent (fac_name, "TXTFILE", 0)))
		strncpy (dlfmsgfile, p, CA_MAXPATHLEN);
	  else
	        return (-1); /* No file given */

	  if (dlf_gettexts_from_file(dlfmsgfile, &fac_no, &g_dlf_fac_info.text_list) < 0)
		return (-1);	
	  

	}
	else {

	  /* Read message texts from the DLF server and store them in memory */
		
	  if (dlf_gettexts(fac_name, &fac_no, &g_dlf_fac_info.text_list) < 0)
		return (-1);	
	}	
	g_dlf_fac_info.fac_no = fac_no;

	n = 0;
	for (dst = g_dlf_fac_info.dest_list.head; dst != NULL; dst = dst->next) {
	        if (dst->severity_mask != 0) n++;
	}
	if (n == 0) return (1); /* No log destinations have been specified at all */
	else return(0);
}

int DLL_DECL dlf_reinit(fac_name)
const char *fac_name;
{
	
	dlf_log_dst_t *p;
	dlf_log_dst_t *p1;
	dlf_msg_text_t *p3;
	dlf_msg_text_t *p4;
	
	/* Remove all destinations */
	
	for (p = g_dlf_fac_info.dest_list.head; p != NULL; ) {
		p1 = p;
		p = p->next;
		free (p1);
	}
	
	/* Remove all texts */
	
	for (p3 = g_dlf_fac_info.text_list.head; p3 != NULL; ) {
		p4 = p3;
		p3 = p3->next;
		free (p4->msg_text);
		free (p4);
	}
	return (dlf_init(fac_name));
}

char DLL_DECL * dlf_get_token (ptr, delim, del, buf, buf_size)
const char *ptr;
const char *delim;
char *del;
char *buf;
int buf_size;
{
	char *p;
	char *p1;
	char *p2;
	char *buf_end;




	
	*del = '\0';
	buf_end = buf + buf_size;
	/* Skip spaces */
	for (p = (char*)ptr; *p == ' ' || *p == '\t'; p++);
	if (*p == '\0' || *p == '\n') 
		return (NULL); /* No more tokens */
	for (p1 = buf; *p != '\0' && p1 < buf_end; p++, p1++) {
		for (p2 = (char*)delim; *p2 != '\0' && *p != *p2; p2++);
		if (*p == *p2) {
			*del = *p2;
			break;
		}
		*p1 = *p;
	}
	if (p1 >= buf_end)
		return (NULL);
	/* Terminate string */
	*p1 = '\0';
	if (*p == '\0') 
		return (p);
	else
		return (++p); /* Pointer to the next character after the delimiter */
}

char DLL_DECL * dlf_parse_dst (ptr, type, port, name)
const char *ptr;
int *type;
int *port;
char *name;
{
	char *p;
	char buf[CA_MAXPATHLEN + 1];
	char del;
	
	*buf = '\0';
	p = (char*) ptr;
	p = dlf_get_token(p, ":", &del, buf, sizeof(buf));
	if (p == NULL) return (NULL);
	
	if (strcmp(buf, "file") == 0)
		*type = DLF_DST_FILE;
	else if (strcmp(buf, "x-dlf") == 0)
		*type =  DLF_DST_TCPHOST;
	else
		return (NULL); /* Parse error */
	
	/* Next should be 2 slashes */
	if (strlen(p) < 2) return (NULL);
	if (*p == '/') p++; else return(NULL);
	if (*p == '/') p++; else return(NULL);
	
	/* Next should be file name or host name */
	switch(*type) {
	case DLF_DST_FILE:
		p = dlf_get_token(p, " ", &del, buf, sizeof(buf));
		if (p == NULL) return (NULL);
		*port = 0;
		/* buf has the same size as name */
		strcpy(name, buf);
		break;
	case DLF_DST_TCPHOST:
		*port = g_dlf_fac_info.default_port; /* default port number */
		p = dlf_get_token(p, ":/", &del, buf, sizeof(buf));
		if (p == NULL) return (NULL);
		/* buf has the same size as name */
		strcpy(name, buf);
		if (del == ':') {
			p = dlf_get_token(p, "/", &del, buf, sizeof(buf));
			if (p == NULL) return (NULL);
			*port = atoi(buf);
		}
		break;
	default:
		break;
	}
	return (p);
}

char DLL_DECL * dlf_get_severity_name(sev_no)
int sev_no;
{
	int i;
	
	for (i = 0; g_dlf_levels[i].lvl_id != 0; i++) {
		if (sev_no == g_dlf_levels[i].lvl_id)
			break;
	}
	if (g_dlf_levels[i].lvl_id != 0)
		return (g_dlf_levels[i].lvl_name);
	else
		return (NULL);
}

int DLL_DECL dlf_store_log_destinations(dst_str, severity, list)
const char *dst_str;
int severity;
dlf_log_dst_slist_t *list;
{
	int type;
	int port;
	char name[CA_MAXPATHLEN + 1];
	char *p;
	
	int i = 0;
	
	p = (char*)dst_str;
	
	while ((p = dlf_parse_dst(p, &type, &port, name)) != NULL) {
		if (dlf_add_to_log_dst(type, port, name, severity, list) < 0)
			return (-1);
		else
			i++;
	}
	if (i > 0) /* We have at least one destination */
		return (0);
	else
		return (-1);
}

dlf_log_dst_t DLL_DECL * dlf_find_dst( type, port, name, list )
int type;
int port;
const char *name;
dlf_log_dst_slist_t *list;
{
	dlf_log_dst_t *p;
	
	for (p = list->head; p != NULL; p = p->next) {
		if (p->dst_type != type) continue;
		if (strcmp(p->name, name) != 0) continue;
		if (type == DLF_DST_FILE)
			break;
		else {
			if (p->port == port)
				break;
		}
	}
	return (p);
}

int DLL_DECL dlf_add_to_log_dst(type, port, name, severity, list)
int type;
int port;
const char *name;
int severity;
dlf_log_dst_slist_t *list;
{
	dlf_log_dst_t *dst;
	int sev_mask;
	
	/* Try to find the destination in the list */
	/* If found update severity mask only */
	
	if ((dst = dlf_find_dst (type, port, name, list)) != NULL) {
		if (severity == DLF_LVL_SERVICE_ONLY)
			return(0);
		if (severity != DLF_LVL_ALL)
			sev_mask = 1 << (severity - 1);
		else
			sev_mask = -1;
		dst->severity_mask |= sev_mask;
		return (0);
	}
	
	/* Not found - create a new structure */
	
	if ((dst = (dlf_log_dst_t*)malloc(sizeof(dlf_log_dst_t))) == NULL) {
		serrno = ENOMEM;
		return (-1);
	}
	
	dst->dst_type = type;
	dst->port = port;
	
	if (severity == DLF_LVL_SERVICE_ONLY)
		sev_mask = 0;
	else if (severity != DLF_LVL_ALL)
		sev_mask = 1 << (severity - 1);
	else
		sev_mask = -1;
	
	dst->severity_mask = sev_mask;
	if (strlen(name) > CA_MAXPATHLEN) {
		serrno = EINVAL;
		return (-1);
	}
	strcpy (dst->name, name);
	
	dst->next = NULL;
	if (list->head == NULL)
		list->head = dst;
	if (list->tail != NULL)
		list->tail->next = dst;
	list->tail = dst;
	return (0);
}

dlf_log_dst_t DLL_DECL * dlf_find_server(severity)
int severity;
{
	dlf_log_dst_t *p;
	for (p = g_dlf_fac_info.dest_list.head; p != NULL; p = p->next) {
		if (p->dst_type != DLF_DST_TCPHOST) continue;
		if (severity == DLF_LVL_SERVICE_ONLY)
			break;
		else {
			if (p->severity_mask & (1 << (severity - 1)))
				break;
		}
	}
	return (p);
}

char DLL_DECL * dlf_get_msg_text(msg_no)
int msg_no;
{
	dlf_msg_text_t* p;
	
	for (p = g_dlf_fac_info.text_list.head; p != NULL; p = p->next) {
		if (p->msg_no == msg_no) break;
	}
	if (p != NULL)
		return (p->msg_text);
	else
		return (NULL);
}

int DLL_DECL dlf_gettexts(fac_name, fac_no, txt_list)
const char *fac_name;
unsigned int *fac_no;
dlf_msg_text_slist_t *txt_list;
{
	U_BYTE fnum;
	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[DLF_REQBUFSZ];
	uid_t uid;
	int status;
	char *reply;
	int rep_size;
	int rep_type;
	char *rep_buf_end;
	char *rbp;
	int msg_no;
	char msg_text[DLF_MAXSTRVALLEN + 1];
	int socket;
	dlf_log_dst_t *dst;
	int end_rc;
	
	uid = geteuid();
	gid = getegid();
	
	
	/* Build request header */
	
	sbp = sendbuf;
	marshall_LONG (sbp, DLF_MAGIC);
	marshall_LONG (sbp, DLF_GETMSGTEXTS);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
	
	/* Build request body */
	
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, fac_name);
	
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */
	
	
	if ((dst = dlf_find_server(DLF_LVL_SERVICE_ONLY)) == NULL)
		return (-1);
	
	socket = -1;
	
	if (send2dlf (&socket, dst, sendbuf, msglen) < 0) {
		return(-1);
	}
	/* Get data */
	end_rc = 0;
	do {
		if (getrep (socket, &reply, &rep_size, &rep_type) < 0) {
			dlf_errmsg(NULL, DLF05, "gettexts", sstrerror(serrno), fac_name);
			return(-1);
		}
		if (rep_type == MSG_DATA) {
			rep_buf_end = reply + rep_size;
			rbp = reply;
			unmarshall_BYTE(rbp, status);
			unmarshall_BYTE(rbp, fnum);
			*fac_no = fnum;
			
			while(rbp < rep_buf_end) {
				unmarshall_SHORT(rbp, msg_no);
				if (unmarshall_STRINGN (rbp, msg_text, DLF_MAXSTRVALLEN + 1) < 0) {
					serrno = EINVAL;
					free (reply);
					return (-1);
				}
				if (dlf_add_to_text_list(msg_no, msg_text, txt_list) < 0) {
					serrno = ENOMEM;
					free(reply);
					return (-1);
				}
			}
			free(reply);
		}
		else if (rep_type == MSG_ERR) {
			/* Error was printed by getrep() */
			free(reply);
			break;
		}
		else if (rep_type == DLF_RC) {
			end_rc++;
			break;
		}
	} while (status == 0);
	/* Get completion status */
	if (!end_rc) {
		if (getrep (socket, &reply, &rep_size, &rep_type) < 0) {
			dlf_errmsg(NULL, DLF05, "gettexts", sstrerror(serrno), fac_name);
			return(-1);
		}
		else if (rep_type != DLF_RC) {
			dlf_errmsg(NULL, DLF02, "gettexts", "Protocol error");
			return (-1);
		}
	}
	return (0);
}

int DLL_DECL dlf_gettexts_from_file(filename, fac_no, txt_list)
const char *filename;
unsigned int *fac_no;
dlf_msg_text_slist_t *txt_list;
{
        FILE *in_file;
	int i;
	int c;
	char *p;
	int msg_no;
	char fac_name[DLF_MAXFACNAMELEN + 1];
	char fac_number[16];
	char msg_number[16];
	char msg_text[DLF_MAXSTRVALLEN + 1];

	in_file = fopen(filename, "r");
	if (in_file == NULL) return (-1);
	
	/* Skip spaces */
	while ((c = fgetc (in_file)) == ' ' || c == '\t' || c == '\n');
	if (c == EOF) CLOSE_RETURN (-1); /* End of file or error */
	
	/* Get facility name */
	ungetc (c, in_file);
	for (i = 0, p = fac_name; (c = fgetc (in_file)) != ' ' && c!= '\t' && c != '\n' && c != EOF && i < DLF_MAXFACNAMELEN; i++) p[i] = c;
	p[i] = '\0'; /* Terminate string */
	if (c != ' ' && c != '\t') CLOSE_RETURN (-1); /* Parse error */

	/* Skip spaces */
	while ((c = fgetc (in_file)) == ' ' || c == '\t');
	if (c == EOF || c == '\n') CLOSE_RETURN (-1); /* End of file or error */

	/* Get facility number */
	ungetc (c, in_file);
	for (i = 0, p = fac_number; (c = fgetc (in_file)) != ' ' && c!= '\t' && c != '\n' && c != EOF && i < 4; i++) p[i] = c;
	p[i] = '\0'; /* Terminate string */
	if (!dlf_isinteger(fac_number)) CLOSE_RETURN (-1);
	*fac_no = atoi(fac_number);
	if (c == EOF ) CLOSE_RETURN (-1); /* file format error */
	ungetc (c, in_file);

	/* Skip until end of line */
	while ((c = fgetc (in_file)) != '\n' && c != EOF);
	if (c == EOF) CLOSE_RETURN (0); /* End of file reached */

	while (1) {
	  /* Skip spaces */
	  while ((c = fgetc (in_file)) == ' ' || c == '\t' || c == '\n');
	  if (c == EOF) CLOSE_RETURN (0); /* End of file reached */

	  /* Get message number */
	  ungetc (c, in_file);
	  for (i = 0, p = msg_number; (c = fgetc (in_file)) != ' ' && c!= '\t' && c != '\n' && c != EOF && i < 10; i++) p[i] = c;
	  p[i] = '\0'; /* Terminate string */
	  if (!dlf_isinteger(msg_number)) CLOSE_RETURN (-1);
	  if (c == EOF || c == '\n') CLOSE_RETURN (-1); /* file format error */
	  msg_no = atoi(msg_number);

	  /* Get message text until end of line */
	  /* Skip spaces */
	  while ((c = fgetc (in_file)) == ' ' || c == '\t');
	  if (c == EOF) CLOSE_RETURN (-1); /* End of file or error */

	  ungetc (c, in_file);
	  for (i = 0, p = msg_text; (c = fgetc (in_file)) != '\n' && c != EOF && i < DLF_MAXSTRVALLEN; i++) p[i] = c;
	  p[i] = '\0'; /* Terminate string */
	  if (c != '\n' && c != EOF) CLOSE_RETURN (-1); /* String is too long */

	  if (dlf_add_to_text_list(msg_no, msg_text, txt_list) < 0) {
		 serrno = ENOMEM;
		 CLOSE_RETURN (-1);
	  }
	  if (c == EOF) CLOSE_RETURN (0); /* End of file reached */
	}
}

int DLL_DECL dlf_add_to_text_list (msg_no, msg_text, txt_list)
int msg_no;
const char *msg_text;
dlf_msg_text_slist_t *txt_list;
{
	dlf_msg_text_t *txt;
	
	if ((txt = (dlf_msg_text_t*)malloc(sizeof(dlf_msg_text_t))) == NULL)
		return (-1);
	txt->msg_no = msg_no;
	if ((txt->msg_text = (char*)malloc(strlen(msg_text) + 1)) == NULL) {
		free(txt);
		return (-1);
	}
	strcpy(txt->msg_text, msg_text);
	txt->next = NULL;
	if (txt_list->head == NULL)
		txt_list->head = txt;
	if (txt_list->tail != NULL)
		txt_list->tail->next = txt;
	txt_list->tail = txt;
	return (0);
}


char DLL_DECL * dlf_format_str(char *buf, int buf_len, const char *fmt, ...) {
	
	va_list ap;
	int nb;
	
	va_start( ap, fmt );
	nb = Cvsnprintf (buf, buf_len, (char *)fmt, ap);
	va_end( ap );
	/* If the output was truncated put terminating zero at the end of the string.
	We don't check here whether the output was truncated, just put zero. */
	buf[buf_len - 1] = '\0';
	return (buf);
}

int DLL_DECL dlf_write (Cuuid_t request_id, int severity, int message_no,
						struct Cns_fileid *ns_invariant, int numparams, ...)  {
	
	int i;
	HYPER par_int;
	double par_double;
	char *par_name;
	char *par_string;
	Cuuid_t par_uuid;
	int par_type;
	int rv;
	int saved_rv;
	int num_char;
	dlf_log_message_t *log_message;
	dlf_log_dst_t *dst;
	struct timeval cur_time;
	struct tm tmres;
#if defined(_WIN32)
	struct tm *tmptr;
#endif
	static char time_fmt[] = "%04d%02d%02d%02d%02d%02d";
	int n;
	va_list ap;

	n = 0;
	for (dst = g_dlf_fac_info.dest_list.head; dst != NULL; dst = dst->next) {
	        if (dst->severity_mask & (1 << (severity-1))) n++;
	}
	if (n == 0) return (1); /* No log destinations have been specified for this severity */

	if ((log_message = (dlf_log_message_t*)dlf_new_log_message()) == NULL)
		return (-1);
	
	memcpy( &(log_message->request_id), &request_id, sizeof(Cuuid_t) );
	log_message->severity = severity;
	log_message->message_no = message_no;
	if (ns_invariant != NULL) {
	  log_message->ns_fileid.fileid = ns_invariant->fileid;
	  num_char = strlen(ns_invariant->server);
	  if (num_char < 0 || num_char > CA_MAXHOSTNAMELEN) {
	    dlf_free_log_message(log_message);
	    return (-1);
	  }
	  strcpy (log_message->ns_fileid.server, ns_invariant->server);
	}
	else {
	  log_message->ns_fileid.fileid = 0;
	  strcpy (log_message->ns_fileid.server, "N/A");
	}
#if !defined(_WIN32)  
	gettimeofday (&cur_time, NULL);
	/* Format time */
	
	localtime_r((time_t*)&cur_time.tv_sec, &tmres);
#else
	cur_time.tv_sec = time(NULL);
	cur_time.tv_usec = 0;
	tmptr = localtime((time_t*)&cur_time.tv_sec);
	tmres.tm_year = tmptr->tm_year;
	tmres.tm_mon = tmptr->tm_mon;
	tmres.tm_mday = tmptr->tm_mday;
	tmres.tm_hour = tmptr->tm_hour;
	tmres.tm_min = tmptr->tm_min;
	tmres.tm_sec = tmptr->tm_sec;
#endif
	n = Csnprintf (log_message->time, DLF_TIMESTRLEN + 1, time_fmt, 
		tmres.tm_year + 1900, tmres.tm_mon + 1, tmres.tm_mday,
		tmres.tm_hour, tmres.tm_min, tmres.tm_sec);
	log_message->time_usec = cur_time.tv_usec;
	
	log_message->pid = getpid();
	
	/*  log_message->cid = Cthread_self();*/
	Cglobals_getTid(&log_message->cid);
	log_message->facility_no = g_dlf_fac_info.fac_no;
	gethostname (log_message->hostname, sizeof(log_message->hostname));
	
	rv = 0;
	saved_rv = 0;
	va_start( ap, numparams );
	for (i = numparams; i > 0; --i) {
		
		par_name = va_arg( ap, char* );
		par_type = va_arg( ap, int );
		switch (par_type) {
		case DLF_MSG_PARAM_STR:
			par_string = va_arg( ap, char* );
			rv = dlf_add_str_parameter ( log_message, par_name, par_string );
			break;
		case DLF_MSG_PARAM_INT:
			par_int = va_arg( ap, int );
			rv = dlf_add_int_parameter ( log_message, par_name, par_int );
			break;
		case DLF_MSG_PARAM_INT64:
			par_int = va_arg( ap, HYPER );
			rv = dlf_add_int_parameter ( log_message, par_name, par_int );
			break;
		case DLF_MSG_PARAM_FLOAT:
		case DLF_MSG_PARAM_DOUBLE:
			par_double = va_arg( ap, double );
			rv = dlf_add_double_parameter ( log_message, par_name, par_double );
			break;
		case DLF_MSG_PARAM_TPVID:
			par_string = va_arg( ap, char* );
			rv = dlf_add_tpvid_parameter ( log_message, par_string );
			break;
		case DLF_MSG_PARAM_UUID:
			par_uuid = va_arg( ap, Cuuid_t );
			rv = dlf_add_subreq_id ( log_message, par_uuid );
			break;
		default:
			break;
		}
		if (rv < 0) {
			dlf_free_log_message (log_message);
			va_end( ap );
			return (rv);
		}
	}
	va_end( ap );
	for (dst = g_dlf_fac_info.dest_list.head; dst != NULL; dst = dst->next) {
		if (dst->severity_mask & (1 << (severity-1))) {
			switch (dst->dst_type) {
			case DLF_DST_FILE:
				if ((rv = dlf_write_to_file(dst, log_message)) < 0)
					saved_rv = rv;
				break;
			case DLF_DST_TCPHOST:
				if ((rv = dlf_send_to_host(dst, log_message, 0, 0)) < 0)
					saved_rv = rv;
				break;
			}
		}
	}
	dlf_free_log_message (log_message);
	return (saved_rv);
}

int DLL_DECL dlf_writep (Cuuid_t request_id, int severity, int message_no,
			 struct Cns_fileid *ns_invariant, int numparams,
			 struct dlf_write_param params[])  {
	
	int i;
	HYPER par_int;
	double par_double;
	char *par_name;
	char *par_string;
	Cuuid_t par_uuid;
	int par_type;
	int rv;
	int saved_rv;
	int num_char;
	dlf_log_message_t *log_message;
	dlf_log_dst_t *dst;
	struct timeval cur_time;
	struct tm tmres;
#if defined(_WIN32)
	struct tm *tmptr;
#endif
	static char time_fmt[] = "%04d%02d%02d%02d%02d%02d";
	int n;

	n = 0;
	for (dst = g_dlf_fac_info.dest_list.head; dst != NULL; dst = dst->next) {
	        if (dst->severity_mask & (1 << (severity-1))) n++;
	}
	if (n == 0) return (1); /* No log destinations have been specified for this severity */

	if ((log_message = (dlf_log_message_t*)dlf_new_log_message()) == NULL)
		return (-1);
	
	memcpy( &(log_message->request_id), &request_id, sizeof(Cuuid_t) );
	log_message->severity = severity;
	log_message->message_no = message_no;
	if (ns_invariant != NULL) {
	  log_message->ns_fileid.fileid = ns_invariant->fileid;
	  num_char = strlen(ns_invariant->server);
	  if (num_char < 0 || num_char > CA_MAXHOSTNAMELEN) {
	    dlf_free_log_message(log_message);
	    return (-1);
	  }
	  strcpy (log_message->ns_fileid.server, ns_invariant->server);
	}
	else {
	  log_message->ns_fileid.fileid = 0;
	  strcpy (log_message->ns_fileid.server, "N/A");
	}
#if !defined(_WIN32)  
	gettimeofday (&cur_time, NULL);
	/* Format time */
	
	localtime_r((time_t*)&cur_time.tv_sec, &tmres);
#else
	cur_time.tv_sec = time(NULL);
	cur_time.tv_usec = 0;
	tmptr = localtime((time_t*)&cur_time.tv_sec);
	tmres.tm_year = tmptr->tm_year;
	tmres.tm_mon = tmptr->tm_mon;
	tmres.tm_mday = tmptr->tm_mday;
	tmres.tm_hour = tmptr->tm_hour;
	tmres.tm_min = tmptr->tm_min;
	tmres.tm_sec = tmptr->tm_sec;
#endif
	n = Csnprintf (log_message->time, DLF_TIMESTRLEN + 1, time_fmt, 
		tmres.tm_year + 1900, tmres.tm_mon + 1, tmres.tm_mday,
		tmres.tm_hour, tmres.tm_min, tmres.tm_sec);
	log_message->time_usec = cur_time.tv_usec;
	
	log_message->pid = getpid();
	
	/*  log_message->cid = Cthread_self();*/
	Cglobals_getTid(&log_message->cid);
	log_message->facility_no = g_dlf_fac_info.fac_no;
	gethostname (log_message->hostname, sizeof(log_message->hostname));
	
	rv = 0;
	saved_rv = 0;
	for (i = 0; i < numparams; i++) {
		
		par_name = params[i].name;
		par_type = params[i].type;
		switch (par_type) {
		case DLF_MSG_PARAM_STR:
			par_string = params[i].par.par_string;
			rv = dlf_add_str_parameter ( log_message, par_name, par_string );
			break;
		case DLF_MSG_PARAM_INT:
			par_int = params[i].par.par_int;
			rv = dlf_add_int_parameter ( log_message, par_name, par_int );
			break;
		case DLF_MSG_PARAM_INT64:
			par_int = params[i].par.par_u64;
			rv = dlf_add_int_parameter ( log_message, par_name, par_int );
			break;
		case DLF_MSG_PARAM_FLOAT:
		case DLF_MSG_PARAM_DOUBLE:
			par_double = params[i].par.par_double;
			rv = dlf_add_double_parameter ( log_message, par_name, par_double );
			break;
		case DLF_MSG_PARAM_TPVID:
			par_string = params[i].par.par_string;
			rv = dlf_add_tpvid_parameter ( log_message, par_string );
			break;
		case DLF_MSG_PARAM_UUID:
			par_uuid = params[i].par.par_uuid;
			rv = dlf_add_subreq_id ( log_message, par_uuid );
			break;
		default:
			break;
		}
		if (rv < 0) {
			dlf_free_log_message (log_message);
			return (rv);
		}
	}
	for (dst = g_dlf_fac_info.dest_list.head; dst != NULL; dst = dst->next) {
		if (dst->severity_mask & (1 << (severity-1))) {
			switch (dst->dst_type) {
			case DLF_DST_FILE:
				if ((rv = dlf_write_to_file(dst, log_message)) < 0)
					saved_rv = rv;
				break;
			case DLF_DST_TCPHOST:
				if ((rv = dlf_send_to_host(dst, log_message, 0, 0)) < 0)
					saved_rv = rv;
				break;
			}
		}
	}
	dlf_free_log_message (log_message);
	return (saved_rv);
}

char DLL_DECL * dlf_uuid2hex(uuid, buf, buf_size)
const Cuuid_t uuid;
char *buf;
int buf_size;
{
	char *p;
	char *u;
	unsigned char x;
	int i;
	
	if (buf_size < (2 * sizeof(Cuuid_t) + 1)) return NULL;
	
	for (i = 0, p = buf, u = (char*)(&uuid); i < sizeof(Cuuid_t); ++i, u++) {
		x = (*u & 0xF0) >> 4;
		*p++ = (x <= 9) ? x + '0' : x - 10 + 'a';
		x = *u & 0x0F;
		*p++ = (x <= 9) ? x + '0' : x - 10 + 'a';
	}
	/* Terminate string */
	*p = '\0';
	return(buf);
}

int DLL_DECL dlf_write_to_file(dst, msg)
dlf_log_dst_t *dst;
dlf_log_message_t *msg;
{
	static char fmt1[] = "DATE=%s.%06d HOST=%s LVL=%s PRGN=%d PROG=%s PID=%d TID=%d MSGN=%d MSG=\"%s\" RQID=%s CNS=%s FID=%016llx";
	static char fmt2[] = " %s=\"%s\"";
	static char fmt3[] = " %s=%lld";
	static char fmt4[] = " %s=%f";
	static char fmt5[] = " %s=%s";
	
	int n;
#if !defined(_WIN32)
	struct flock64 fl_struct;
#endif
	char prtbuf[DLF_PRTBUF_LEN + 2];
	int restsize = DLF_PRTBUF_LEN;
	char *bufpos;
	char *txt;
	char *sev_name;
	dlf_msg_param_t *p;
	int fd;
	int write_to_stdout = 0;
	int written;
	char uuidhex[CUUID_STRING_LEN+1];
	int resuuid;
	
	if (strcmp("stdout", dst->name) == 0) {
	        write_to_stdout = 1;
	        fd = 1;
	}
	else {
	        write_to_stdout = 0;
	        if ((fd = open (dst->name, O_WRONLY | O_CREAT | O_APPEND, 0664)) < 0)
		     return (-1);
	}
	resuuid = Cuuid2string(uuidhex, sizeof(uuidhex), &msg->request_id );
	n = Csnprintf
		(prtbuf, restsize, fmt1, 
		msg->time, msg->time_usec, msg->hostname,
		((sev_name =
		dlf_get_severity_name(msg->severity)) != NULL) ? sev_name : "Unknown",
		g_dlf_fac_info.fac_no,
		g_dlf_fac_info.fac_name, msg->pid, msg->cid, 
		msg->message_no,
		((txt = dlf_get_msg_text(msg->message_no)) != NULL) ? txt : "No text",
		(resuuid >= 0) ? uuidhex : "Unknown", 
		msg->ns_fileid.server,
		msg->ns_fileid.fileid);
	if (n >= restsize) { /* Buffer too small */
	        if (!write_to_stdout) close(fd);
	        return (-1);
	}
	restsize -= n;
	bufpos = prtbuf + n;
	for (p = msg->param_list.head; p != NULL; p = p->next) {
		switch(p->type) {
		case DLF_MSG_PARAM_STR:
			n = Csnprintf (bufpos, restsize, fmt2, p->name, p->strval);
			break;
		case DLF_MSG_PARAM_TPVID:
			n = Csnprintf (bufpos, restsize, fmt5, p->name, p->strval);
			break;
		case DLF_MSG_PARAM_INT64:
			n = Csnprintf (bufpos, restsize, fmt3, p->name, p->numval);
			break;
		case DLF_MSG_PARAM_UUID:
			resuuid = Cuuid2string(uuidhex, sizeof(uuidhex), ((Cuuid_t*)p->strval) );
			n = Csnprintf (bufpos, restsize, fmt5, p->name, 
			(resuuid >= 0) ? uuidhex : "Unknown"); 
			break;
		case DLF_MSG_PARAM_DOUBLE:
			n = Csnprintf (bufpos, restsize, fmt4, p->name, p->dval);
			break;
		default:
			serrno = SEINTERNAL;
			if (!write_to_stdout) close(fd);
			return (-1);
		}
		if (n >= restsize) { /* buffer too small */
		        if (!write_to_stdout) close(fd);
			return (-1);
		}
		restsize -= n;
		bufpos += n;
	}
	n = Csnprintf (bufpos, restsize, "\n");
	write (fd, prtbuf, bufpos - prtbuf + 1);
	if (!write_to_stdout) close(fd);
	return (0);
}

int DLL_DECL dlf_calc_msg_size(msg)
dlf_log_message_t *msg;
{
	int size;
	dlf_msg_param_t *p;
	
	size = sizeof(U_BYTE)
		+ strlen(msg->time) + 1
		+ sizeof(int)
		+ sizeof(Cuuid_t)
		+ strlen(msg->hostname) + 1
#if !defined(_WIN32)
		+ sizeof(pid_t)
#else
		+ sizeof(int)
#endif
		+ sizeof(int)
	        + strlen(msg->ns_fileid.server) + 1
		+ sizeof(HYPER)
		+ sizeof(U_BYTE)
		+ sizeof(U_BYTE)
		+ sizeof(U_SHORT);
	
	for (p = msg->param_list.head; p != NULL; p = p->next) {
		switch(p->type) {
		case DLF_MSG_PARAM_TPVID:
		case DLF_MSG_PARAM_STR:
			size += sizeof(U_BYTE)
				+ strlen(p->name) + 1
				+ strlen(p->strval) + 1;
			break;
		case DLF_MSG_PARAM_INT64:
			size += sizeof(U_BYTE)
				+ strlen(p->name) + 1
				+ sizeof(HYPER);
			break;
		case DLF_MSG_PARAM_DOUBLE:
		/* Convert double to string since it is unclear
			how to transfer floating point numbers over the network */
			size += sizeof(U_BYTE)
				+ strlen(p->name) + 1
				+ Csnprintf ((char*)p->strval, DLF_MAXSTRVALLEN, "%f", p->dval) + 1;
			break;
		case DLF_MSG_PARAM_UUID:
			size += sizeof(U_BYTE)
				+ strlen(p->name) + 1
				+ sizeof (Cuuid_t);
			break;
		default:
			size = -1;
			serrno = EINVAL;
			return (size);
		}
	}
	return(size);
}

int DLL_DECL dlf_send_to_host(dst, msg, persist_flag, last_message)
dlf_log_dst_t *dst;
dlf_log_message_t *msg;
int persist_flag;
int last_message;
{
	
	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	char *sendbuf;
	uid_t uid;	
	dlf_msg_param_t *p;
	int socket;
	int required_buf_size;
	int required_buf_size_calc;
	char *reply;
	int rep_size;
	int rep_type;
	int rv;
	
	uid = geteuid();
	gid = getegid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		fprintf (stderr, DLF53);
		exit (USERR);
	}
#endif
	
	/* Build request header */
	
	required_buf_size_calc = dlf_calc_msg_size(msg);
	if (required_buf_size_calc < 0)
		return (-1);
	required_buf_size = required_buf_size_calc + 3*LONGSIZE + 2*LONGSIZE;
	if ((sendbuf = (char*)malloc(required_buf_size)) == NULL) {
		serrno = ENOMEM;
		return (-1);
	}
	sbp = sendbuf;
	
	marshall_LONG (sbp, DLF_MAGIC);
	marshall_LONG (sbp, DLF_STORE);
	
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
	
	/* Build request body */
	
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	
	if (!persist_flag) {
		marshall_BYTE (sbp, DLF_LAST_BUFFER);
	}
	else {
		if (last_message) {
			marshall_BYTE (sbp, 1);
		}
		else {
			marshall_BYTE (sbp, 0);
		}
	}
	
	marshall_STRING (sbp, msg->time);
	marshall_LONG (sbp, msg->time_usec);
	marshall_STRING (sbp, msg->hostname);
	marshall_UUID (sbp, msg->request_id);
	marshall_LONG(sbp, msg->pid);
	marshall_LONG(sbp, msg->cid);
	
	marshall_STRING(sbp, msg->ns_fileid.server);
	marshall_HYPER(sbp, msg->ns_fileid.fileid);
	
	marshall_BYTE(sbp, msg->facility_no);
	marshall_BYTE(sbp, msg->severity);
	marshall_SHORT(sbp, msg->message_no);
	
	for (p = msg->param_list.head; p != NULL; p = p->next) {
		switch(p->type) {
		case DLF_MSG_PARAM_TPVID:
		case DLF_MSG_PARAM_STR:
		case DLF_MSG_PARAM_DOUBLE:
			marshall_BYTE(sbp, p->type);
			marshall_STRING(sbp, p->name);
			marshall_STRING(sbp, p->strval);
			break;
		case DLF_MSG_PARAM_INT64:
			marshall_BYTE(sbp, p->type);
			marshall_STRING(sbp, p->name);
			marshall_HYPER(sbp, p->numval);
			break;
		case DLF_MSG_PARAM_UUID:
			marshall_BYTE(sbp, p->type);
			marshall_STRING(sbp, p->name);
			marshall_UUID(sbp, *((Cuuid_t*)p->strval));
			break;
		default:
			serrno = SEINTERNAL;
			free(sendbuf);
			return(-1);
			break;
		}
	}
	
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */
	if (persist_flag) /* We have persistent connection */
		socket = dst->descr;
	else
		socket = -1;
	if (send2dlf (&socket, dst, sendbuf, msglen) < 0) {
		free(sendbuf);
		return (-1);
	}
	
	if (persist_flag)
		dst->descr = socket;
	free(sendbuf);
	if (!persist_flag) {
		/* Get completion status */
		while ((rv = getrep (socket, &reply, &rep_size, &rep_type)) >= 0) {
			if (rep_type == MSG_ERR) {
				free (reply);
				continue;
			}
			else if (rep_type == DLF_RC) {
				break;
			}
			else {
				dlf_errmsg(NULL, DLF02, "dlf_send_to_host", "Protocol error");
				return (-1);
			}
		}
		if (rv < 0) {
			dlf_errmsg(NULL, DLF02, "dlf_send_to_host", sstrerror(serrno));
			return(-1);
		}
	}
	return(0);
}

int DLL_DECL islittleendian()
{
	union {
		long l;
		char c[sizeof(long)];
	} check;
	
	check.l = 1;
	return (check.c[0] == 1);
}
    
dlf_log_message_t DLL_DECL * dlf_new_log_message()
{
	dlf_log_message_t *ptr;
	
	ptr = (dlf_log_message_t*)malloc (sizeof(dlf_log_message_t));
	if (ptr == NULL) return (NULL);
	
	ptr->next_tapevid_idx = 1;
	ptr->next_subreq_idx = 1;
	ptr->param_list.head = NULL;
	ptr->param_list.tail = NULL;
	
	return(ptr);
}
    
void DLL_DECL dlf_free_log_message (log_message)
dlf_log_message_t *log_message;
{
	/* Delete parameters */
	dlf_delete_param_list (&log_message->param_list);
	free(log_message);
	return;
}
    
void DLL_DECL dlf_delete_param_list (param_list)
dlf_msg_param_list_t *param_list;
{
	dlf_msg_param_t *p;
	dlf_msg_param_t *p1;
	
	for (p = param_list->head; p != NULL; ) {
		p1 = p;
		p = p->next;
		free (p1);
	}
	param_list->head = NULL;
	param_list->tail = NULL;
	return;
}
    
int DLL_DECL dlf_log_message_init(log_message)
dlf_log_message_t *log_message;
{
	log_message->next_subreq_idx = 1;
	log_message->next_tapevid_idx = 1;
	log_message->param_list.head = NULL;
	log_message->param_list.tail = NULL;
	return (0);
}

    
dlf_msg_param_t DLL_DECL * new_dlf_param()
{
	return ( (dlf_msg_param_t*)malloc (sizeof(struct _dlf_msg_parameter)) );
}

int DLL_DECL dlf_add_str_parameter (log_message, par_name, par_string)
dlf_log_message_t *log_message;
const char *par_name;
const char *par_string;
{
	dlf_msg_param_t* param_p;
	char *p;

	param_p = (dlf_msg_param_t*)new_dlf_param();
	if (param_p == NULL) {
		serrno = ENOMEM;
		return (-1);
	}
	if (strlen(par_name) > DLF_MAXPARNAMELEN ||
		strlen(par_string) > DLF_MAXSTRVALLEN) {
		serrno = EINVAL;
		return (-1);
	}
	strcpy (param_p->name, par_name);
	strcpy (param_p->strval, par_string);
	/* Replace all occurrencies of new lines with spaces */
	for (p = param_p->strval; *p != 0; p++) {
	  if (*p == '\n') *p = ' ';
	}
	param_p->type = DLF_MSG_PARAM_STR;
	dlf_add_to_param_list(&log_message->param_list, param_p);
	return (0);
}
    
int DLL_DECL dlf_add_int_parameter (log_message, par_name, par_int)
dlf_log_message_t *log_message;
const char *par_name;
HYPER par_int;
{
	dlf_msg_param_t *param_p;
	
	param_p = (dlf_msg_param_t*)new_dlf_param();
	if (param_p == NULL) {
		serrno = ENOMEM;
		return (-1);
	}
	if (strlen(par_name) > DLF_MAXPARNAMELEN) {
		serrno = EINVAL;
		return (-1);
	}
	strcpy (param_p->name, par_name);
	param_p->numval = par_int;
	param_p->type = DLF_MSG_PARAM_INT64;
	dlf_add_to_param_list(&log_message->param_list, param_p);
	return (0);
}
    
int DLL_DECL dlf_add_double_parameter (log_message, par_name, par_double)
dlf_log_message_t *log_message;
const char *par_name;
double par_double;
{
	dlf_msg_param_t *param_p;
	
	param_p = (dlf_msg_param_t*)new_dlf_param();
	if (param_p == NULL) {
		serrno = ENOMEM;
		return (-1);
	}
	if (strlen(par_name) > DLF_MAXPARNAMELEN) {
		serrno = EINVAL;
		return (-1);
	}
	strcpy (param_p->name, par_name);
	param_p->dval = par_double;
	param_p->type = DLF_MSG_PARAM_DOUBLE;
	dlf_add_to_param_list(&log_message->param_list, param_p);
	return (0);
}
    

int DLL_DECL dlf_add_to_param_list (param_list, param)
dlf_msg_param_list_t *param_list;
dlf_msg_param_t *param;
{
	dlf_msg_param_t *p;
	
	p = param_list->tail;
	if (p == NULL) {
		param_list->head = param;
		param_list->tail = param;
		param->prev = NULL;
	} else {
		param_list->tail = param;
		p->next = param;
		param->prev=p;
	}
	param->next=NULL;
	return (0);
}

int DLL_DECL dlf_add_subreq_id (log_message, uuid)
dlf_log_message_t *log_message;
Cuuid_t uuid;
{
	static char fmt[] = "DLF.SRQID%d";
	int i;
	int n;
	dlf_msg_param_t *param_p;
	
	param_p = (dlf_msg_param_t*)new_dlf_param();
	if (param_p == NULL) {
		serrno = ENOMEM;
		return (-1);
	}
	i = log_message->next_subreq_idx++;
	n = Csnprintf (param_p->name, DLF_MAXPARNAMELEN, fmt, i);
	memcpy (param_p->strval, &uuid, sizeof(Cuuid_t));
	param_p->type = DLF_MSG_PARAM_UUID;
	dlf_add_to_param_list (&log_message->param_list, param_p);
	return(0);
}

int DLL_DECL dlf_add_tpvid_parameter ( log_message, tape_vid )
dlf_log_message_t *log_message;
const char *tape_vid;
{
	static char fmt[] = "DLF.TPVID%d";
	int i;
	int n;
	dlf_msg_param_t *param_p;
	
	param_p = (dlf_msg_param_t*)new_dlf_param();
	if (param_p == NULL) {
		serrno = ENOMEM;
		return (-1);
	}
	if (strlen(tape_vid) > DLF_MAXSTRVALLEN) {
		serrno = EINVAL;
		return (-1);
	}
	i = log_message->next_tapevid_idx++;
	n = Csnprintf (param_p->name, DLF_MAXPARNAMELEN, fmt, i);
	strcpy (param_p->strval, tape_vid);
	param_p->type = DLF_MSG_PARAM_TPVID;
	dlf_add_to_param_list (&log_message->param_list, param_p);
	return(0);
}

int DLL_DECL dlf_isinteger( par_str_val )
const char *par_str_val;
{
	char *p;
	
	int i = 0;
	
	p = (char *) par_str_val;
	if (*p != '+' && *p != '-' && !isdigit(*p)) return (0);
	if (isdigit(*p)) i++;
	for (++p; *p != '\0' && isdigit(*p); p++, i++);
	if (*p == '\0' && i != 0) return (1); else return(0);
}

int DLL_DECL  dlf_isfloat( par_str_val )
const char *par_str_val;
{
	char *p;
	
	int i = 0;
	int pointn = 0;
	
	p = (char *) par_str_val;
	if (*p != '+' && *p != '-' && *p !='.' && !isdigit(*p)) return (0);
	if (*p == '.') pointn++;
	if (isdigit(*p)) i++;
	for (++p; *p != '\0' && isdigit(*p); p++, i++);
	if (*p == '.') {
		if (pointn) return (0);
		else pointn++;
	}
	if (*p == '\0') {
		if(i != 0) return (1);
		else return(0);
	}
	for (++p; *p != '\0' && isdigit(*p); p++, i++);
	if (*p == '\0' && i != 0) return (1); 
	else return(0);
}

int  DLL_DECL dlf_hex2uuid( hex_str, uuid )
const char *hex_str;
Cuuid_t uuid;
{
	int i, x0, x1;
	char *p, *p1;
	
	if (strlen(hex_str) != (2 * sizeof(Cuuid_t))) return (-1);
	for (i = 0, p = (char *)hex_str, p1 = (char*)&uuid;
	i < 2 * sizeof(Cuuid_t);
	i += 2) {
		if ((x0 = dlf_hexto4bits(p[i])) < 0) return (-1);
		if ((x1 = dlf_hexto4bits(p[i + 1])) < 0) return (-1);
		*p1++ = (x0 << 4) | x1;
	}
	return (0);
}

int  DLL_DECL dlf_hexto4bits(c)
const int c;
{
	int i;
	
	i = (int) c;
	if (i >= '0' && i <= '9') i -= '0';
	else if (i >= 'a' && i <= 'f') i = i - 'a' + 10;
	else if (i >= 'A' && i <= 'F') i = i - 'A' + 10;
	else return (-1);
	return (i);
}

int DLL_DECL dlf_hex2u64 ( hex_str, u64_val )
const char *hex_str;
u_signed64 *u64_val;
{
	char *p;
	u_signed64 u64v;
	int x0;
	
	for (p = (char *)hex_str, u64v = 0; *p != '\0'; p++) {
		if ((x0 = dlf_hexto4bits(*p)) < 0) return (-1);
		u64v  = u64v * 16 + x0;
	}
	*u64_val = u64v;
	return (0);
}

