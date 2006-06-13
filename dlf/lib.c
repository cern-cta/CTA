/******************************************************************************************************
 *                                                                                                    *
 * lib.c - Castor Distribution Logging Facility                                                       *
 * Copyright (C) 2005 CERN IT/FIO (castor-dev@cern.ch)                                                *
 *                                                                                                    *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU *
 * General Public License as published by the Free Software Foundation; either version 2 of the       *
 * License, or (at your option) any later version.                                                    *
 *                                                                                                    *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without  *
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU     *
 * General Public License for more details.                                                           *
 *                                                                                                    *
 * You should have received a copy of the GNU General Public License along with this program; if not, *
 * write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307,  *
 * USA.                                                                                               *
 *                                                                                                    *
 ******************************************************************************************************/

/**
 * $Id
 */

/* headers */
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>

#include "Cglobals.h"
#include "Cnetdb.h"
#include "common.h"
#include "Cthread_api.h"
#include "dlf_api.h"
#include "getconfent.h"
#include "hash.h"
#include "lib.h"
#include "marshall.h"
#include "net.h"
#include "queue.h"
#include "server.h"
#include "u64subr.h"


/* hashes, tables and pools */
static target_t  *targets[API_MAX_TARGETS];     /**< target pool for targets/destinations   */
static msgtext_t *texts[DLF_MAX_MSGTEXTS];      /**< the clients message texts              */
static hash_t    *hashtexts = NULL;             /**< fast lookup of message texts           */

/* mutexes */
static int api_mutex;

/* api variables */
static long api_mode = MODE_DEFAULT;            /**< api mode                               */
static char api_facname[DLF_LEN_FACNAME + 1];   /**< facility name as provided to db_init() */
static char api_ucfacname[DLF_LEN_FACNAME + 1]; /**< facility name in upper case            */

/* prototype for common/socket_timeout.c */
EXTERN_C int DLL_DECL netconnect_timeout _PROTO((SOCKET, struct sockaddr *, size_t, int));


/*
 * dlf_error
 *   - this function is used to log internal error messages from the api. It has some duplication of
 *     code from dlf_write but this is necessary to prevent recursive loops! Errors generated here are
 *     not reported.
 */

void dlf_error(char *format, ...) {

	/* variables */
	struct timeval tv;     
	struct tm      tm_str;  

	int       rv;
	int       i;
	int       tid;
	int       fd;
	int       len;
	char      buffer[1024];
	char      hostname[DLF_LEN_HOSTNAME + 1];
	
	va_list   ap;

	/* timestamp */
	gettimeofday(&tv, NULL);
	localtime_r(&tv.tv_sec, &tm_str);
	len = strftime(buffer, sizeof(buffer), "DATE=%Y%m%d%H%M%S.", &tm_str);

	rv = gethostname(hostname, sizeof(hostname));
	if (rv < 0) {
		return;
	}
	Cglobals_getTid(&tid);

	/* main message */
	len += snprintf(&buffer[len], sizeof(buffer) - len - 1, "%06d HOST=%s LVL=ERROR FACILITY=DLF PID=%d TID=%d ",
			(int)tv.tv_usec,
			hostname,
			getpid(),
			tid);
	
	/* format message */
	va_start(ap, format);
	vsnprintf(&buffer[len], sizeof(buffer) - len - 1, format, ap);
	va_end(ap);

	/* terminate string */
	len = strlen(buffer);
	if (buffer[len - 1] != '\n') {
		sprintf(&buffer[len], "\n");
	}

	for (i = 0; i < API_MAX_TARGETS; i++) {
		if (targets[i] == NULL)
			continue;
		if (!IsFile(targets[i]->mode))   
			continue;                     /* not a file               */
		if (!(targets[i]->sevmask & severitylist[2].sevmask))
			continue;                     /* severity not of interest */

		/* write to stdout or file */
		if (!strncasecmp(targets[i]->path, "stdout", 6)) {
			fprintf(stdout, "%s", buffer);
			fflush(stdout);
		} else {
#ifdef O_LARGEFILE
  			fd = open(targets[i]->path, O_APPEND|O_WRONLY|O_CREAT|O_LARGEFILE, 0644);
#else
        		fd = open(targets[i]->path, O_APPEND|O_WRONLY|O_CREAT, 0644);
#endif
		    
			if (fd < 0) {
				continue;
			}
			write(fd, buffer, len + 1);
			close(fd);
		}
	}
}


/*
 * dlf_read
 */

int dlf_read(target_t *t, int *rtype, int *rcode) {

	/* variables */
	int       len;           
	int       rc; 
	int       err_no;
	int       type;          
	int       magic;        
	char      header[3 * LONGSIZE];  
	char      *rbp;          
 	
	/* extract header */
	len = netread_timeout(t->socket, header, sizeof(header), API_READ_TIMEOUT);
	if (len != sizeof(header)) {
		return APP_FAILURE;
	}

	/* initialise variables */
	rbp = header;
	
	unmarshall_LONG(rbp, magic);
	unmarshall_LONG(rbp, type);
	unmarshall_LONG(rbp, rc);

	/* correct protocol version ?
	 *   - this should never happen as the server shouldn't answer requests from clients using an
	 *     incorrect protocol version
	 */
	if (magic != DLF_MAGIC) {
		SetShutdown(t->mode);
		return APP_FAILURE;
	}

	/* process the response type */
	if (type == DLF_REP_RC) {
		netclose(t->socket);
		t->socket = -1;
	} else if (type == DLF_REP_ERR) {
		err_no = rc;
		if (err_no > DLF_ERR_MAX) {
			err_no = DLF_ERR_MAX;
		}
		dlf_error("MSG=\"%s\" SERVER=\"%s\"", t->server, dlf_errorlist[(err_no - 1)].err_str);
	} else if (type == DLF_REP_IRC) {

	} else {
		return APP_FAILURE;          /* unknown response type */
	}

	*rtype = type;
	*rcode = rc;

	return APP_SUCCESS;
}


/*
 * dlf_send
 */

int dlf_send(target_t *t, char *data, int len) {

	/* variables */
	struct sockaddr_in server_addr;
	struct hostent     *hp;
	
	int    rv;
	int    opts;

	/* already connected ? */
	if ((t->socket == -1) || (!IsConnected(t->mode))) {

		/* populate structure */
		server_addr.sin_family = AF_INET;
		server_addr.sin_port   = htons(t->port);

		hp = Cgethostbyname(t->server);
		if (hp == NULL) {
			dlf_error("MSG=\"Failed to resolve hostname\" SERVER=\"%s\"", t->server);
			return h_errno;
		}
		server_addr.sin_addr.s_addr = ((struct in_addr *)hp->h_addr)->s_addr;

		/* open socket */
		t->socket = socket(AF_INET, SOCK_STREAM, 0);
		if (t->socket < 0) {
			return errno;
		}
		
		/* keep connection alive */
		opts = 1;
		rv = setsockopt(t->socket, SOL_SOCKET, SO_KEEPALIVE, &opts, sizeof(opts));
		if (rv != APP_SUCCESS) {
			return errno;
		}

		/* connect */
		rv = netconnect_timeout(t->socket, (struct sockaddr *)&server_addr, sizeof(server_addr), API_CONNECT_TIMEOUT);
		if (rv < 0) {
			dlf_error("MSG=\"Failed to connect to remote host\" SERVER=\"%s\" TIMEOUT=\"%d\" ERROR=\"%s\"", t->server, API_CONNECT_TIMEOUT, neterror());
			ClrConnected(t->mode);
			return rv;
		}
		SetConnected(t->mode);	
	}

	/* attempt to send data to the server
	 *   - a failure here updates sets the socket to -1
	 */
	rv = netwrite_timeout(t->socket, data, len, API_WRITE_TIMEOUT);
	if (rv < 0) {
		netclose(t->socket);
		t->socket = -1;
		ClrConnected(t->mode);
		return APP_FAILURE;
	}
	
	return APP_SUCCESS;
}


/*
 * dlf_writep
 */

int dlf_writep(Cuuid_t reqid, int severity, int msg_no, struct Cns_fileid *ns, int numparams, dlf_write_param_t params[]) {

	/* variables */
	struct timeval tv;     
	struct tm      tm_str;  

	message_t *message;
	message_t *m;
	param_t   *param;          
	param_t   *p;            
	param_t   *p2;          
	int       rv;          
	int       fd;          
	int       i;             
	int       j; 
	int       k;
	int       error;            
	int       len;        
	int       found;        
	int       sevmask;        
	char      *text = NULL;     
	char      buffer[8192];        
	char      value[DLF_LEN_STRINGVALUE + 1];
	Cuuid_t   preqid;

	/* valid severity ? */
	for (j = 0, found = 0; severitylist[j].name != NULL; j++) {
		if (severitylist[j].sevno == severity) {
			found = 1;
			break;
		}
	}
	if (found == 0) {
		return APP_FAILURE;
	}
	sevmask = severitylist[j].sevmask;
	
	/* convert the msg_no to a message text
	 *   - this is only used by targets of type file. Here we specify the text as opposed to the 
	 *     number as this is more useful in standard log files
	 */
	snprintf(buffer, sizeof(buffer) - 1, "%d", msg_no);
	
	rv = hash_search(hashtexts, buffer, (void **)&text);
	if (rv != APP_SUCCESS) {
		return APP_FAILURE;
	}

	/* alocate memory for new message */
	message = (message_t *) malloc(sizeof(message_t));
	if (message == NULL) {
		return APP_FAILURE;
	}
	message->plist = NULL;
	message->size  = sizeof(unsigned char);
	
	/* timestamp */
	gettimeofday(&tv, NULL);
	localtime_r(&tv.tv_sec, &tm_str);
	strftime(message->timestamp, DLF_LEN_TIMESTAMP + 1, "%Y%m%d%H%M%S", &tm_str);
	message->timestamp[DLF_LEN_TIMESTAMP + 1] = '\0';
	message->timeusec = tv.tv_usec;
	message->size = strlen(message->timestamp) + sizeof(int) + 1;

	/* hostname */
	rv = gethostname(message->hostname, sizeof(message->hostname) - 1);
	if (rv < 0) {
		free_message(message);
		return APP_FAILURE;
	}

	/* request id */
	rv = Cuuid2string(message->reqid, sizeof(message->reqid) - 1, &reqid);
	if (rv < 0) {
		free_message(message);
		return APP_FAILURE;
	}

	/* pid and thread id */
	message->pid = getpid();
	Cglobals_getTid(&message->tid);

	/* nameserver info */
	if (ns != NULL) {
		strncpy(message->nshostname, ns->server, sizeof(message->nshostname) - 1);
		u64tostr(ns->fileid, message->nsfileid, -1);		
	} else {
		strcpy(message->nshostname, "N/A");
		strcpy(message->nsfileid, "0");
	}

	/* severity and message number */
	message->severity = severity;
	message->msg_no   = msg_no;

	/* update message size */
	message->size += strlen(message->hostname) + strlen(message->reqid) + strlen(message->nshostname)
		      +  strlen(message->nsfileid) + sizeof(unsigned char)  + sizeof(unsigned char)     
		      +  sizeof(unsigned short)    + sizeof(pid_t)          + sizeof(int) + 4;

       	/* process parameter */
	for (i = 0; i < numparams; i++) {

		/* allocate memory to hold parameter */
		param = (param_t *) malloc(sizeof(param_t));
		if (param == NULL) {
			free_message(message);
			return APP_FAILURE;
		}
				
		/* extract name and type */
		if (params[i].name != NULL) {
			strncpy(param->name, params[i].name, DLF_LEN_PARAMNAME);
			param->name[DLF_LEN_PARAMNAME] = '\0';
		} else {
			strcpy(param->name, "(null)");
		}
		param->name[DLF_LEN_PARAMNAME] = '\0';
		param->type = params[i].type;
	       		
		/* process type */
		if (param->type == DLF_MSG_PARAM_TPVID) {
			strncpy(value, params[i].par.par_string, DLF_LEN_TAPEID);
			strncpy(param->name, "TPVID", sizeof(param->name) - 1);
			value[DLF_LEN_TAPEID] = '\0';
		}
		else if (param->type == DLF_MSG_PARAM_STR) {
			strncpy(value, params[i].par.par_string, DLF_LEN_STRINGVALUE);
			value[DLF_LEN_STRINGVALUE] = '\0';
		}
		else if (param->type == DLF_MSG_PARAM_INT) {
			snprintf(value, DLF_LEN_NUMBERVALUE, "%d", params[i].par.par_int);
		}
		else if (param->type == DLF_MSG_PARAM_INT64) {
			snprintf(value, DLF_LEN_NUMBERVALUE, "%Ld", params[i].par.par_u64);
		}
		else if (param->type == DLF_MSG_PARAM_DOUBLE) {
			snprintf(value, DLF_LEN_NUMBERVALUE, "%f", params[i].par.par_double);
		}
		else if (param->type == DLF_MSG_PARAM_UUID) {
			preqid = params[i].par.par_uuid;
			rv = Cuuid2string(value, sizeof(message->reqid) - 1, &preqid);
			if (rv < 0) {
				free_param(param);
				free_message(message);
				return APP_FAILURE;
			}
			strncpy(param->name, "SUBREQID", sizeof(param->name) - 1);
		}

		/* unknown, free resources */
		else {
			free_param(param);
			free_message(message);
			return APP_FAILURE;
		}

		/* copy value */
		param->value = strdup(value);
		if (param->value == NULL) {
			free_param(param);
			free_message(message);
			return APP_FAILURE;
		}

		/* first parameter ? */
		if (message->plist == NULL) {
			message->plist = param;			
		} else {
			for (p = message->plist; p->next != NULL; p = p->next);
			p->next = param;
		}
		param->next = NULL;
	}

	/* replace all occurrences of the new line '\n' character in all parameter strings 
	 *   - in parameter names we simply collapse all '\n' and spaces ' '
	 */
	for (param = message->plist; param != NULL; param = param->next) {

		/* value */
		for (i = 0; (param->value[i] != '\0') && (i < strlen(param->value)); i++) {
			if (param->value[i] == '\n') {
				param->value[i] = ' ';
			}
		}
		param->value[i] = '\0';

		/* name */
		for (i = 0, k = 0; (param->name[i] != '\0') && (i < strlen(param->name)); i++, k++) {
			if ((param->name[i] == '\n') || (param->name[i] == ' ')) {
				k--;
				continue;
			}
			param->name[k] = param->name[i];
		}
		param->name[k] = '\0';

		message->size += sizeof(int) + strlen(param->name) + strlen(param->value) + 2;
	}

	/* deal with files first, they take priority! */
	for (i = 0; i < API_MAX_TARGETS; i++) {
		if (targets[i] == NULL)
			continue;
		if (!IsFile(targets[i]->mode))   
			continue;                     /* not a file               */
		if (!(targets[i]->sevmask & sevmask) &&
		    !(targets[i]->sevmask & severitylist[10].sevmask))
			continue;                     /* severity not of interest */
	
		/* construct message */
		len = snprintf(buffer, sizeof(buffer), 
			"DATE=%s.%06d HOST=%s LVL=%s FACILITY=%s PID=%d TID=%d MSG=\"%s\" REQID=%s NSHOSTNAME=%s NSFILEID=%s",
			message->timestamp,
			message->timeusec,
			message->hostname,
			severitylist[j].name,
			api_facname,
			message->pid,
			message->tid,
			text != NULL ? text : "Unknown",
			message->reqid,
			message->nshostname,
			message->nsfileid);

		/* buffer too small ? */
		if (len >= sizeof(buffer)) {
			continue;
		}
		
		/* add parameters */
		for (param = message->plist, error = 0; param != NULL; param = param->next) {
			if (param->type == DLF_MSG_PARAM_STR) {
				len += snprintf(&buffer[len], sizeof(buffer) - len - 1, " %s=\"%s\"", param->name, param->value);
			} else {
				len += snprintf(&buffer[len], sizeof(buffer) - len - 1, " %s=%s", param->name, param->value);
			}
			if (len >= sizeof(buffer)) {
				error = 1;
				break;
			}
		}
		if (error == 1) {
			continue;
		}
	
		/* terminate string */
		len = strlen(buffer);
		if (buffer[len - 1] != '\n') {
			sprintf(&buffer[len], "\n");
		}

		/* write to stdout or file */
		if (!strncasecmp(targets[i]->path, "stdout", 6)) {
			fprintf(stdout, "%s", buffer);
			fflush(stdout);
		} else {
#ifdef O_LARGEFILE
  			fd = open(targets[i]->path, O_APPEND|O_WRONLY|O_CREAT|O_LARGEFILE, 0644);
#else
        		fd = open(targets[i]->path, O_APPEND|O_WRONLY|O_CREAT, 0644);
#endif
		    
			if (fd < 0) {
				continue;
			}
			write(fd, buffer, len + 1);
			close(fd);
		}	
	}

	/* servers 
	 *   - push the message onto the internal message queue of each server, this requires adding a
	 *     message_t structure
	 */
	for (i = 0; i < API_MAX_TARGETS; i++) {
		if (targets[i] == NULL)
			continue;
		if (!IsServer(targets[i]->mode))
			continue;                     /* not a server             */
		if (!(targets[i]->sevmask & sevmask) &&
		    !(targets[i]->sevmask & severitylist[10].sevmask)) 
			continue;                     /* severity not of interest */
		
		/* it is necessary to create a copy of the message before passing it into the targets fifo
		 * queue. If we don't do this all queues will be sharing the same data as the queue just
		 * holds a pointer to a structure of type void.
		 */
		m = (message_t *) malloc(sizeof(message_t));
		if (m == NULL) {
			free_message(message);
			return APP_FAILURE;
		}
		m->size     = message->size;
		m->msg_no   = message->msg_no;
		m->pid      = message->pid;
		m->plist    = NULL;
		m->severity = message->severity;
		m->tid      = message->tid;
		m->timeusec = message->timeusec;

		strcpy(m->hostname,   message->hostname);
		strcpy(m->nsfileid,   message->nsfileid);
		strcpy(m->nshostname, message->nshostname);
		strcpy(m->reqid,      message->reqid);
		strcpy(m->timestamp,  message->timestamp);
		
		/* copy parameters */
		for (param = message->plist; param != NULL; param = param->next) {
			
			/* allocate memory for new parameter */
			p = (param_t *) malloc(sizeof(param_t));
			if (p == NULL) {
				free_message(m);
				free_message(message);
				return APP_FAILURE;
			}
			p->value = NULL;

			p->type = param->type;
			strcpy(p->name,  param->name);
			
			/* copy the parameter value */
			p->value = strdup(param->value);
			if (p->value == NULL) {
				free_param(param);
				free_message(m);
				free_message(message);
				return APP_FAILURE;
			}

			/* first parameter ? */
			if (m->plist == NULL) {
				m->plist = p;			
			} else {
				for (p2 = m->plist; p2->next != NULL; p2 = p2->next);
				p2->next = p;
			}
			p->next = NULL;
		}

		/* push message to servers queue, retry up to 3 times on a full queue */
		j = 0;
		do {
			rv = queue_push(targets[i]->queue, m);
			j++;
		} while ((rv == APP_QUEUE_FULL) && (j < 3));
		
		/* free message on queue failure */
		if (rv != APP_SUCCESS) {
			free_message(m);
		}
	}

	free_message(message);

	return APP_SUCCESS;
}


/*
 * dlf_write
 */

int dlf_write(Cuuid_t reqid, int severity, int msg_no, struct Cns_fileid *ns, int numparams, ...) {

	/* variables */
	dlf_write_param_t plist[numparams];
	char              *name;
	char              *string;
	int               i;
	int		  rv;
	int               ok;
	va_list           ap;

	/* translate the variable argument list to a dlf_write_param_t array */
       	va_start(ap, numparams);
	for (i = 0, ok = 1; i < numparams; i++) {

		string = NULL;
		name   = va_arg(ap, char *);
		if (name == NULL) {
			plist[i].name = NULL;
		} else {
			plist[i].name = strdup(name);
		}
		plist[i].type = va_arg(ap, int);
	
		/* process type */
		if ((plist[i].type == DLF_MSG_PARAM_TPVID) || (plist[i].type == DLF_MSG_PARAM_STR)) {
			string = va_arg(ap, char *);
			if (string == NULL) {
				plist[i].par.par_string = NULL;
			} else {
				plist[i].par.par_string = strdup(string);
			}
		}
		else if (plist[i].type == DLF_MSG_PARAM_INT) {
			plist[i].par.par_int = va_arg(ap, int);
		}
		else if (plist[i].type == DLF_MSG_PARAM_INT64) {
			plist[i].par.par_u64 = va_arg(ap, HYPER);
		}
		else if (plist[i].type == DLF_MSG_PARAM_DOUBLE) {
			plist[i].par.par_double = va_arg(ap, double);
		}
		else if (plist[i].type == DLF_MSG_PARAM_UUID) {
			plist[i].par.par_uuid = va_arg(ap, Cuuid_t);
		} 
		else if (plist[i].type == DLF_MSG_PARAM_FLOAT) {
			plist[i].par.par_double = va_arg(ap, double);
			plist[i].type = DLF_MSG_PARAM_DOUBLE;
		}
		else { 
			ok = 0;
			break;
		}	
	}
	va_end(ap);

	/* everything ok ? */
	rv = APP_FAILURE;
	if (ok == 1) {
		rv = dlf_writep(reqid, severity, msg_no, ns, numparams, plist);		
	}
	
	/* free resources */
	for (i = 0; i < numparams; i++) {
		if (plist[i].name != NULL)
			free(plist[i].name);
		if ((plist[i].type == DLF_MSG_PARAM_TPVID) || (plist[i].type == DLF_MSG_PARAM_STR)) {
			if (plist[i].par.par_string != NULL) { 
				free(plist[i].par.par_string);			
			}
		}
	}
	
	return rv;
}


/*
 * dlf_worker
 */

void dlf_worker(target_t *t) {

	/* variables */
	message_t *message;
	param_t   *param;
	uid_t     uid; 
	gid_t     gid;
	int       i;
	int       rv;               
	int       len;           
	int       code;    
	int       type;
	char      *sbp;   
	char      *buffer;     
	char      *ptr;       

	/* initialise variables */
	uid = geteuid();
	gid = getegid();

	/* worker thread loop
	 *   - deals with all outgoing connections and message transfers to the remote dlf server, this 
	 *     is done in an endless loop until a shutdown is requested.
	 *   - it is important not to sleep(3) in this loop, this can cause shutdown requests to be 
	 *     missed
	 */
	while (!IsShutdown(api_mode)) {

		/* the client-server communication protocol has the capacity for the server to notify 
		 * the client to slow down or pause message send for X seconds of time. This helps reduce
		 * the load on a heavily loaded DLF server.
		 */
		if (t->pause > time(NULL)) {
			sleep(1);
			continue;
		}

		/* initialised ?
		 *   - the thread cannot send any messages to the remote target/destination until
		 *     initialisation has occurred. This is because the thread doesn't know its facility
		 *     number yet
		 */
		if (!IsInitialised(t->mode)) {

			/* calculate message size */
			len = strlen(api_facname) + 1;
                        for (i = 0; i < DLF_MAX_MSGTEXTS; i++) {
                                if (texts[i] == NULL)
                                        continue;

                                len += sizeof(unsigned short) + strlen(texts[i]->msg_text) + 1;
			}			
	
			/* allocate memory for DLF_INIT message */
			buffer = (char *) malloc((5 * LONGSIZE) + len);
			if (buffer == NULL) {
				t->pause = time(NULL) + 10;
				continue;
			}

			/* build request header */
			sbp = buffer;
			marshall_LONG(sbp, DLF_MAGIC);     /* protocol version     */
			marshall_LONG(sbp, DLF_INIT);      /* request method       */ 

			ptr = sbp;
			len = 5 * LONGSIZE;

			marshall_LONG(sbp, len);           /* total message length */
			
			/* build request body */
			marshall_LONG(sbp, uid);
			marshall_LONG(sbp, gid);

			/* marshall the facility name */
			marshall_STRING(sbp, api_facname); 

			/* marshall the message texts */
			Cthread_mutex_lock(&api_mutex);
			for (i = 0; i < DLF_MAX_MSGTEXTS; i++) {
				if (texts[i] == NULL)
					continue;

				marshall_SHORT(sbp,  texts[i]->msg_no);
				marshall_STRING(sbp, texts[i]->msg_text);				
			}
			Cthread_mutex_unlock(&api_mutex);

			/* update message length */
			len = sbp - buffer;
			marshall_LONG(ptr, len);

		     	/* send data to remote host
			 *   - the api is asynchronous reporting errors would be pointless and only fill
			 *     up the log. All we do is try again.
			 */
			rv = dlf_send(t, buffer, len);
			if (rv != APP_SUCCESS) {
				if (IsShutdown(api_mode)) {
					break;
				}
				t->pause = time(NULL) + 10;
				free(buffer);
				continue;
			}
			
			/* read the response */
			rv = dlf_read(t, &type, &code);
			if (rv != APP_SUCCESS) {
				if (IsShutdown(api_mode)) {
					break;
				}
				t->pause = time(NULL) + 10;
				free(buffer);
				continue;
			}

			/* initialisation complete */
			if (type == DLF_REP_RC) {
				SetInitialised(t->mode);
				t->fac_no = code;
			} 		 

			free(buffer);			
			continue;
		}

		/* pop a message from the queue */
		rv = queue_pop(t->queue, (void **)&message);

		/* queue errors ? */
		if (rv == APP_QUEUE_TERMINATED) {
			break;                             /* queue terminated, signifies a shutdown */
		} else if (rv == APP_QUEUE_EMPTY) {
			t->pause = time(NULL) + 5;         /* no messages available                  */
			continue;
		} else if (rv != APP_SUCCESS) {
			t->pause = time(NULL) + 5;
			continue;
		}
		
		if (message == NULL) {
			t->pause = time(NULL) + 5;
			continue;
		}

		/* allocate memory for DLF_LOG message 
		 *   - the length of bytes to allocate should have been pre-calculated in dlf_write()
		 */
		buffer = (char *) malloc((5 * LONGSIZE) + message->size);
		if (buffer == NULL) {
			t->pause = time(NULL) + 60;
			continue;
		}

		/* build request header */
		sbp = buffer;
		marshall_LONG(sbp, DLF_MAGIC);             /* protocol version     */
		marshall_LONG(sbp, DLF_LOG);               /* request method       */ 
		
		ptr = sbp;
		len = 5 * LONGSIZE;
			
		marshall_LONG(sbp, len);                   /* total message length */
		
		/* build request body */
		marshall_LONG(sbp, uid);
		marshall_LONG(sbp, gid);
		
		/*  this is the last message in the queue ? */
		if (queue_size(t->queue) == 0) {
			marshall_BYTE(sbp, 1);
		} else {
			marshall_BYTE(sbp, 0);
		}

		/* marshall standard header components */
		marshall_STRING(sbp, message->timestamp);
		marshall_LONG(sbp, message->timeusec);
		marshall_STRING(sbp, message->hostname);
		marshall_STRING(sbp, message->reqid);
		marshall_LONG(sbp, message->pid);
		marshall_LONG(sbp, message->tid);
		marshall_STRING(sbp, message->nshostname);
		marshall_STRING(sbp, message->nsfileid);
		marshall_BYTE(sbp, t->fac_no);
		marshall_BYTE(sbp, message->severity);
		marshall_SHORT(sbp, message->msg_no);

		/* marshall parameters */
		for (param = message->plist; param != NULL; param = param->next) {
			marshall_BYTE(sbp, param->type);
			marshall_STRING(sbp, param->name);
			marshall_STRING(sbp, param->value);
		}
		
		/* update message length */
		len = sbp - buffer;
		marshall_LONG(ptr, len);

		/* send data to server */
		rv = dlf_send(t, buffer, len);
		if (rv != APP_SUCCESS) {
			if (IsShutdown(api_mode)) {
				break;
			}
			t->pause = time(NULL) + 20;
			free(buffer);
			free_message(message);
			continue;
		}

		/* read the response */
		rv = dlf_read(t, &type, &code);
		if (rv != APP_SUCCESS) {
			if (IsShutdown(api_mode)) {
				break;
			}
			t->pause = time(NULL) + 10;
			free(buffer);
			free_message(message);
			continue;
		}		

		/* instructed to throttle by the server ? */
		if ((type == DLF_REP_IRC) && (code > 0)) {
			usleep((code * 1000));
			if (IsShutdown(api_mode)) {
				break;
			}
		}

	       	free(buffer);
		free_message(message);
	}

	/* flag the shutdown */
	t->tid = -1;
	
	/* exit */
	Cthread_exit(0);
}


/*
 * dlf_regtext
 */

int dlf_regtext(unsigned short msg_no, const char *msg_text) {

	/* variables */
	msgtext_t  *entry;       
	int        i;       
	int        rv;      
	int        found;
	
	char       *message;
	char       num[10];  
	char       text[DLF_LEN_MSGTEXT + 1];

	/* although the fast lookup hash is thread safe, the message text global table isn't hence the
	 * need for this global mutex lock of the entire interface. 
	 */
	Cthread_mutex_lock(&api_mutex);

	/* not initialised ? */
	if (!IsInitialised(api_mode)) {
		Cthread_mutex_unlock(&api_mutex);
		return -1;
	}

	/* allocate memory */
	entry = (msgtext_t *) malloc(sizeof(msgtext_t));
	if (entry == NULL) {
		Cthread_mutex_unlock(&api_mutex);
		return -1;
	}
	entry->msg_text = NULL;

	if (strlen(msg_text) > DLF_LEN_STRINGVALUE) {
		free(entry);
		Cthread_mutex_unlock(&api_mutex);
		return -1;
	}
	
	/* find an available msgtext slot */
	for (i = 0, found = 0; i < DLF_MAX_MSGTEXTS; i++) {
		if (texts[i] == NULL) {
			found = 1;
			break;
		}
	}
	
	/* too many msgtexts defined ? */
	if (found == 0) {
		free(entry);
		Cthread_mutex_unlock(&api_mutex);
		return -1;
	}
	
	/* to provide fast msgtext to msgno lookups we use a global hash. Here we check to make sure
	 * the next entry will not be a duplicate
	 */
	snprintf(num,  sizeof(int), "%d", msg_no);
	snprintf(text, sizeof(text), "%s", msg_text);	

	message = strdup(text);
	if (message == NULL) {
		free(entry);
		return -1;
	}

	rv = hash_insert(hashtexts, num, message);
	if (rv == -1) {
		free(message);
		free(entry);
		Cthread_mutex_unlock(&api_mutex);
		return 0;     /* key already exits */
	}

	/* add entry to msgtext table */
	entry->msg_text = strdup(text);
	if (entry->msg_text == NULL) {
		free(message);
		free(entry);
		Cthread_mutex_unlock(&api_mutex);
		return -1;
	}
	entry->msg_no = msg_no;
	
	texts[i] = entry;
	
	Cthread_mutex_unlock(&api_mutex);
	return 0;
}


/*
 * dlf_init
 */

int dlf_init(const char *facility, char *errptr) {

	/* variables */
	struct servent *servent; 

	target_t   *t;
	int        i;
	int        j;  
	int        rv;      
	int        port;   
	int        found;   
	char       *value;       
	char       buffer[1024];    
	char       uri[10];    
 	char       envname[1024];  
     
	/* we can't pass an error message back if the pointer to the error is NULL */
	if (errptr == NULL) {
		return -1;
	}
	Cthread_init();

	/* this function shouldn't be called from within a thread as initialisation should only occur
	 * once and only once for the whole system. Re-initialisation is of course possible but a call
	 * to dlf_shutdown() must be made beforehand. Here we hold a global mutex to prevent multiple
	 * dlf_init()'s at the same time
	 */
	Cthread_mutex_lock(&api_mutex);

	/* already initialised ? */
	if (IsInitialised(api_mode)) {
		snprintf(errptr, CA_MAXLINELEN, "dlf_init(): interface already initialised");
		Cthread_mutex_unlock(&api_mutex);
		return -1;
	}
	SetInitialised(api_mode);

	/* flush pools and tables */
	for (i = 0; i < API_MAX_TARGETS; i++) {
		targets[i] = NULL;
	}
	for (i = 0; i < DLF_MAX_MSGTEXTS; i++) {
		texts[i] = NULL;
	}
	value = NULL;
	t     = NULL;

       	/* convert facility name to upper case */
	if (strlen(facility) > (sizeof(api_facname) - 1)) {
		snprintf(errptr, CA_MAXLINELEN, "dlf_init(): facility name exceeds %d characters in length", sizeof(api_facname) - 1);
		Cthread_mutex_unlock(&api_mutex);
		return -1;
	}
	strncpy(api_facname, facility, sizeof(api_facname) - 1);

	for (i = 0, api_ucfacname[0] = '\0'; facility[i] != '\0'; i++) {
		api_ucfacname[i] = toupper(facility[i]);
	}
	api_ucfacname[i] = '\0';	

	/* initialise global hashes for fast message text lookups */
	hash_create(&hashtexts, 100);

	/* the logging targets can be configured either through environment variables or via confents in
	 * the castor configuration file (e.g. /etc/castor/castor.conf). The format for the target name:
	 *   - <facility_name>_<severity_name>
	 */
	for (i = 0; severitylist[i].name != NULL; i++, value = NULL) {
		snprintf(envname, sizeof(envname) - 1, "%s_%s", api_ucfacname, severitylist[i].confentname);

		/* option defined ? 
		 *    - environment variables take precedince over confents
		 */
		if ((value = getenv(envname)) == NULL) {
			if ((value = getconfent(api_ucfacname, severitylist[i].confentname, 1)) == NULL) {
				continue;
			}
		}
	
		/* logging targets take the following format:
		 *   - <uri>://<[server|filepath]>:[port]
		 */
		if (sscanf(value, "%9[^://]://%1023s", uri, buffer) != 2) {
			continue;
		}

		/* invalid uniform resource identifer ? */
		if (!strcasecmp(uri, "file") && !strcasecmp(uri, "x-dlf")) {
			continue;
		}

		/* determine server port */
		if (sscanf(buffer, "%1023[^:]:%d", buffer, &port) == 2) {
			port = port;
		} else if ((value = getenv("DLF_PORT")) || (value = getconfent("DLF", "PORT", 0))) {
			port = atoi(value);
		} else if ((servent = getservbyname("dlf", "tcp"))) {
			port = servent->s_port;
		} else {
			port = DEFAULT_SERVER_PORT;
		}

		/* lookup the server and port or filepath and alter the severity mask accordingly
		 *   - note: multiple servers are allowed with different ports!
		 */
		for (j = 0, found = 0; j < API_MAX_TARGETS; j++) {
			if (targets[j] == NULL)
				continue;

			if (strcmp(targets[j]->path, buffer)) 
				continue;                            /* incorrect path */

			/* servers have an additional port attribute to consider */
			if (IsServer(targets[j]->mode)) {
				if (targets[j]->port != port) {
					continue;                    /* incorrect port */
				}
			} 

			/* alter severity mask */
			targets[j]->sevmask |= severitylist[i].sevmask;

			found = 1;
			break;
		}

		/* found an entry ? */
		if (found == 1) {
			continue;
		}

		/* find an available target slot */
		for (j = 0, found = 0; j < API_MAX_TARGETS; j++) {
			if (targets[j] == NULL) {
				found = 1;
				break;
			}
		}
		
		/* too many targets defined ? */
		if (found == 0) {
			snprintf(errptr, CA_MAXLINELEN, "dlf_init(): too many log destinations defined");
       			Cthread_mutex_unlock(&api_mutex);
			return -1;
		}

		/* create structure */
		t = (target_t *) malloc(sizeof(target_t));
		if (t == NULL) {
			snprintf(errptr, CA_MAXLINELEN, "dlf_init(): failed to malloc() : %s", strerror(errno));
			Cthread_mutex_unlock(&api_mutex);
			return -1;
		}

		/* initialise target_t structure
		 *   - files are fare less complicated then servers. We simple initialise everything too
		 *     NULL here and later create the server threads, queue and hash'ing structures once
		 *     the whole initialisation sequences has completed.
		 */
		t->pause    = time(NULL) + 3;
		t->port     = port;
		t->tid      = -1;
		t->index    = j;
		t->socket   = -1;
		t->queue    = NULL;
		t->mode     = MODE_DEFAULT;
		t->fac_no   = -1;
		t->sevmask  = severitylist[i].sevmask;

		/* set the type */
		if (!strcasecmp(uri, "file")) {
			strncpy(t->path, buffer, sizeof(t->path) - 1);
			SetFile(t->mode);
		} else {
			strncpy(t->server, buffer, sizeof(t->server) - 1);
			SetServer(t->mode);
		}

		targets[j] = t;
	}

	/* at this point we haven't initialised any threads, if we've exceed API_MAX_THREADS then the 
	 * whole initialisation phase fails!
	 */
	for (i = 0, j = 0; i < API_MAX_TARGETS; i++) {
		if (targets[i] == NULL)
			continue;
		if (IsServer(targets[i]->mode)) {
			j++;
		}
	}
	if (j > API_MAX_THREADS) {
		snprintf(errptr, CA_MAXLINELEN, "dlf_init(): too many servers defined, maximum threads would be exceeded");
		Cthread_mutex_unlock(&api_mutex);
		return -1;
	}
   
	/* create server threads */
	for (i = 0; i < API_MAX_TARGETS; i++) {
		if (targets[i] == NULL)
			continue;
		if (!IsServer(targets[i]->mode))
			continue;            /* not a server */
		
		/* create server specific bounded fifo queue */
		rv = queue_create(&targets[i]->queue, API_QUEUE_SIZE);
		if (rv != 0) {
			snprintf(errptr, CA_MAXLINELEN, "dlf_init(): failed to queue_create() : code %d", rv);
			Cthread_mutex_unlock(&api_mutex);
			return -1;
		}
	
		/* create thread */
		targets[i]->tid = Cthread_create_detached((void *)dlf_worker, (target_t *)targets[i]);
		if (targets[i]->tid == -1) {
			snprintf(errptr, CA_MAXLINELEN, "dlf_init(): failed to Cthread_create_detached() : %s", strerror(errno));
			Cthread_mutex_unlock(&api_mutex);
			return -1;
		}
	}
	
	/* destroy thread attributes and mutex */
	Cthread_mutex_unlock(&api_mutex);

	return 0;
}


/*
 * dlf_shutdown
 */

int dlf_shutdown(void) {

	/* variables */
	int    i;        
	int    j;         
	int    found;     

	/* interface already shutdown */
	Cthread_mutex_lock(&api_mutex);
	if (IsShutdown(api_mode)) {
		Cthread_mutex_unlock(&api_mutex);
		return 0;
	}
	SetShutdown(api_mode);

	/* worker threads are responsible for cleaning up their own thread related data at shutdown. 
	 * Here we simply wait upto a maximum of 10 seconds for all threads to exit
	 */
	for (i = 0; i < 10; i++) {
		for (j = 0, found = 0; j < API_MAX_TARGETS; j++) {
			if (targets[j] == NULL)
				continue;
			if (!IsServer(targets[j]->mode))
				continue;
			if (targets[j]->tid != -1) 
				found++;
		}

		/* all threads have exited ? */
		if (found == 0) {
			break;
		}
		sleep(1);
	}

	/* cleanup targets 
	 *   - some threads may still be running, we destroy its internal queue in the hope it will see
	 *     the shutdown
	 */
	for (i = 0; i < API_MAX_TARGETS; i++) {
		if (targets[i] == NULL)
			continue;

		/* if a connection to a server is established, close it */
		if (IsServer(targets[i]->mode)) {
			if (targets[i]->socket != -1) {
				netclose(targets[i]->socket);
			}
			queue_destroy(targets[i]->queue, (void *)free_message);
		}

		free(targets[i]);
	}

	/* destroy global hashes */
	hash_destroy(hashtexts, (void *)free);
	
	/* clear msg texts */
	free_msgtexts(texts);

	ClrInitialised(api_mode);
	ClrShutdown(api_mode);
	
	Cthread_mutex_unlock(&api_mutex);
	return 0;
}


/** End-of-File **/
