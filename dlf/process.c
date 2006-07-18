/******************************************************************************************************
 *                                                                                                    *
 * process.c - Castor Distribution Logging Facility                                                   *
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
 * $Id: process.c,v 1.3 2006/07/18 12:04:35 waldron Exp $
 */

/* headers */
#include <ctype.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#include "common.h"
#include "Cmutex.h"
#include "Cthread_api.h"
#include "dbi.h"
#include "dlf_api.h"
#include "errno.h"
#include "log.h"
#include "marshall.h"
#include "net.h"
#include "queue.h"
#include "serrno.h"


/* externs */
extern queue_t *queue;
extern int     server_mode;


/*
 * send_rep
 */

int sendrep(client_t *client, int type, int rc) {

	/* variables */
	char     *rbp;
	int      repsize;
	char     repbuf[3 * LONGSIZE];

	/* start header */
	rbp = repbuf;
	marshall_LONG(rbp, DLF_MAGIC);
	marshall_LONG(rbp, type);
	marshall_LONG(rbp, rc);

	/* send response */
	repsize = rbp - repbuf;
	if (netwrite_timeout(client->fd, repbuf, repsize, 5) != repsize) {
		log(LOG_DEBUG, "sendrep() - failed to netwrite_timeout client=%d:%s : %s\n", CLIENT_IDENT(client), neterror());
		if (type == DLF_REP_RC) {
			log(LOG_DEBUG, "sendrep() - client=%d:%s fd=%d calling netclose()\n", CLIENT_IDENT(client), client->fd);
			netclose(client->fd);
			client->fd = -1;
		}
		return APP_FAILURE;
	}
	log(LOG_DEBUG, "sendrep() - client=%d:%s fd=%d type=%d rc=%d length=%d\n", CLIENT_IDENT(client), client->fd, type, rc, repsize);

	if (type == DLF_REP_RC) {
		log(LOG_DEBUG, "sendrep() - client=%d:%s fd=%d calling netclose()\n", CLIENT_IDENT(client), client->fd);
		netclose(client->fd);
		client->fd = -1;
	}

       	return APP_SUCCESS;
}


/*
 * parse_init
 */

int parse_init(client_t *client, char *data, int len) {

	/* variables */
	char           *eob;
	char           *rbp;
	uid_t          uid;
	gid_t          gid;
	int            i;
	int            rc;
	int            rv;
	msgtext_t      *m;

	/* marshalling */
	msgtext_t      *texts[DLF_MAX_MSGTEXTS];
	char           facility[DLF_LEN_FACNAME + 1];
	char           buffer[DLF_LEN_MSGTEXT + 1];

	/* flush message text table */
	for (i = 0; i < DLF_MAX_MSGTEXTS; i++) {
		texts[i] = NULL;
	}

	/* security related info */
	rbp    = data;
	eob    = rbp + len;
	unmarshall_LONG(rbp, uid);
	unmarshall_LONG(rbp, gid);

	/* facility name */
	rv = unmarshall_STRINGN(rbp, facility, DLF_LEN_FACNAME + 1);
	if (rv < 0) {
		sendrep(client, DLF_REP_ERR, DLF_ERR_MARSHALL);
		return APP_FAILURE;
	}

	log(LOG_DEBUG, "parse_init() - client=%d:%s uid=%d gid=%d length=%d facility=%s\n", 
	    CLIENT_IDENT(client), uid, gid, len, facility);

	/* extract key value pair message texts
	 *   - we store the values in a msgtext structure and pass this to the database layer for
	 *     processing.
	 */
	for (i = 0; rbp < eob; i++) {

		/* too many message texts ? */
		if (i > DLF_MAX_MSGTEXTS) {
			break;
		}

		/* allocate memory for msgtext */
		m = (msgtext_t *) malloc(sizeof(msgtext_t));
		if (m == NULL) {
			log(LOG_ERR, "parse_init() - failed to malloc client=%d:%s : %s\n", CLIENT_IDENT(client), strerror(errno));
			sendrep(client, DLF_REP_ERR, DLF_ERR_SYSERR);
			free_msgtexts(texts);
			return APP_FAILURE;
		}
		m->msg_text = NULL;

		/* message number */
		unmarshall_SHORT(rbp, m->msg_no);

		/* message text */
		rv = unmarshall_STRINGN(rbp, buffer, DLF_LEN_MSGTEXT + 1);
		if (rv < 0) {
			sendrep(client, DLF_REP_ERR, DLF_ERR_MARSHALL);
			free_msgtexts(texts);
			free(m);
			return APP_FAILURE;
		}
 
		/* copy the text */
		m->msg_text = strdup(buffer);
		if (m->msg_text == NULL) {
			log(LOG_ERR, "parse_init() - failed to malloc client=%d:%s : %s\n", CLIENT_IDENT(client), strerror(errno));
			sendrep(client, DLF_REP_ERR, DLF_ERR_SYSERR);
			free_msgtexts(texts);
			free(m);
			return APP_FAILURE;
		}

		log(LOG_DEBUG, "parse_init() - client=%d:%s count=%d msg_no=%d msg_text=\"%s\"\n", CLIENT_IDENT(client), i, m->msg_no, m->msg_text);

		texts[i] = m;
	}

	/* database call */
	rv = db_initfac(facility, texts, &rc);
	if (rv == APP_SUCCESS) {
		log(LOG_DEBUG, "parse_init() - client=%d:%s resolved facilty=%s to id=%d\n", CLIENT_IDENT(client), facility, rc);
		sendrep(client, DLF_REP_RC, rc);
	} else {
		if (rc == -1) {
			sendrep(client, DLF_REP_ERR, DLF_ERR_UNKNOWNFAC);
		} else {
			sendrep(client, DLF_REP_ERR, DLF_ERR_DB);
		}
	}

	/* free resources */
	free_msgtexts(texts);

	return rv;
}


/*
 * parse_log
 */

int parse_log(client_t *client, char *data, int len) {

	/* variables */
	struct timeval tv;

	char          *eob;
	char          *rbp;
	gid_t         gid;
	int           rv;
	int           i;
	param_t       *p;
	param_t       *param;
	uid_t         uid;
	message_t     *message;
	U_BYTE        last;
	char          value[DLF_LEN_STRINGVALUE + 1];

	/* number conversions */
	HYPER              p_int64;
	int                p_int;
	double             p_double;

	/* intialise variables */
	message = NULL;
	param   = NULL;

        /* security related info */
	rbp    = data;
	eob    = rbp + len;
	unmarshall_LONG(rbp, uid);
	unmarshall_LONG(rbp, gid);
	unmarshall_BYTE(rbp, last);

	log(LOG_DEBUG, "parse_log() - client=%d:%s uid=%d gid=%d last=%d\n", 
	    CLIENT_IDENT(client), uid, gid, last);

	/* allocate memory for message */
	message = (message_t *) malloc(sizeof(message_t));
	if (message == NULL) {
		log(LOG_ERR, "parse_log() - failed to malloc client=%d:%s : %s\n", CLIENT_IDENT(client), strerror(errno));
		sendrep(client, DLF_REP_ERR, DLF_ERR_SYSERR);
		return APP_FAILURE;
	}
	message->plist = NULL;

	/* statistics */
	rv = gettimeofday(&tv, NULL);
	if (rv < 0) {
		sendrep(client, DLF_REP_ERR, DLF_ERR_SYSERR);
		free_message(message);
		return APP_FAILURE;
	}
	message->received = ((double)tv.tv_sec * 1000) + ((double)tv.tv_usec / 1000);

	/* timestamps */
	rv = unmarshall_STRINGN(rbp, message->timestamp, DLF_LEN_TIMESTAMP + 1);
	if (rv < 0) {
		sendrep(client, DLF_REP_ERR, DLF_ERR_MARSHALL);
		free_message(message);
		return APP_FAILURE;
	}

	unmarshall_LONG(rbp, message->timeusec);

	/* hostname */
	rv = unmarshall_STRINGN(rbp, message->hostname, DLF_LEN_HOSTNAME + 1);
	if (rv < 0) {
		sendrep(client, DLF_REP_ERR, DLF_ERR_MARSHALL);
		free_message(message);
		return APP_FAILURE;
	}

	/* request id, convert to hex string */
	rv = unmarshall_STRINGN(rbp, message->reqid, sizeof(message->reqid));
	if (rv < 0) {
		sendrep(client, DLF_REP_ERR, DLF_ERR_MARSHALL);
		free_message(message);
		return APP_FAILURE;
	}

	/* pid and thread id */
	unmarshall_LONG(rbp, message->pid);
	unmarshall_LONG(rbp, message->tid);

	/* name server info */
	rv = unmarshall_STRINGN(rbp, message->nshostname, DLF_LEN_HOSTNAME + 1);
	if (rv < 0) {
		sendrep(client, DLF_REP_ERR, DLF_ERR_MARSHALL);
		free_message(message);
		return APP_FAILURE;
	}

	/* file id */
	rv = unmarshall_STRINGN(rbp, message->nsfileid, sizeof(message->nsfileid));
	if (rv < 0) {
		sendrep(client, DLF_REP_ERR, DLF_ERR_MARSHALL);
		free_message(message);
		return APP_FAILURE;
	}

	/* facility, severity, message number */
	unmarshall_BYTE(rbp, message->facility);
	unmarshall_BYTE(rbp, message->severity);
	unmarshall_SHORT(rbp, message->msg_no);

	/* process remaining parameters */
	while (rbp < eob) {

		/* allocate memory for parameter */
		param = (param_t *)malloc(sizeof(param_t));
		if (param == NULL) {
			log(LOG_ERR, "parse_log() - failed to malloc client=%d:%s : %s\n", CLIENT_IDENT(client), strerror(errno));
			sendrep(client, DLF_REP_ERR, DLF_ERR_SYSERR);
			free_message(message);
			return APP_FAILURE;
		}
		param->value = NULL;

		/* extract type and name */
		unmarshall_BYTE(rbp, param->type);
		rv = unmarshall_STRINGN(rbp, param->name, DLF_LEN_PARAMNAME + 1);
		if (rv < 0) {
			sendrep(client, DLF_REP_ERR, DLF_ERR_MARSHALL);
			free_message(message);
			free_param(param);
			return APP_FAILURE;
		}

		/* extract value */
		if (param->type == DLF_MSG_PARAM_STR) {
			rv = unmarshall_STRINGN(rbp, value, DLF_LEN_STRINGVALUE + 1);
		} else if (param->type == DLF_MSG_PARAM_TPVID) {
			rv = unmarshall_STRINGN(rbp, value, DLF_LEN_TAPEID + 1);
		} else if (param->type == DLF_MSG_PARAM_UUID) {
			rv = unmarshall_STRINGN(rbp, value, CUUID_STRING_LEN);
		} else {
			rv = unmarshall_STRINGN(rbp, value, DLF_LEN_STRINGVALUE + 1);
		}
		if (rv < 0) {
			sendrep(client, DLF_REP_ERR, DLF_ERR_MARSHALL);
			free_message(message);
			free_param(param);
			return APP_FAILURE;
		}

		/* copy the parameter value */
		param->value = strdup(value);
		if (param->value == NULL) {
			log(LOG_ERR, "parse_log() - failed to malloc client=%d:%s : %s\n", CLIENT_IDENT(client), strerror(errno));
			sendrep(client, DLF_REP_ERR, DLF_ERR_SYSERR);
			free_message(message);
			free_param(param);
			return APP_FAILURE;
		}

		/* check for a valid number */
		if (param->type == DLF_MSG_PARAM_INT) {
			rv = sscanf(param->value, "%d", &p_int);
		}
		else if (param->type == DLF_MSG_PARAM_INT64) {
			rv = sscanf(param->value, "%Ld", &p_int64);
		}
		else if (param->type == DLF_MSG_PARAM_DOUBLE) {
			rv = sscanf(param->value, "%lf", &p_double);
		}
		else {
			rv = 1;
		}

		if (rv != 1) {
			free_param(param);
			continue;
		}

		/* unknown type ? */
		if (param->type > 7) {
			sendrep(client, DLF_REP_ERR, DLF_ERR_UNKNOWNTYPE);
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

	/* push to central fifo queue, retry up to 3 times on full queue
	 *   - any other errors are ignored.
	 */
	i = 0;
	if (!IsServerPurge(server_mode) && !IsSuspendedQueue(server_mode)) {
		do {
			rv = queue_push(queue, message);
			i++;
		} while ((rv == APP_QUEUE_FULL) && (i < 3));
	}

	/* free message on queue failure */
	if (rv != APP_SUCCESS) {
		free_message(message);
	}

	/* client throttling required ?
	 *   - if the servers queue reaches THROTTLE_THRESHOLD capacity notify the client to begin
	 *     throttling messages. every 1000 messages over the threshold incures a THROTTLE_PENALTIY
	 *     upto a maximum of MAX_THROTTLE_TIME seconds
	 */
	if (THROTTLE_THRESHOLD > 0) {
		rv = 0;
		if (queue_size(queue) > ((MAX_QUEUE_SIZE) / 100 * THROTTLE_THRESHOLD)) {
			rv = (((MAX_QUEUE_SIZE - queue_size(queue)) / 1000) * THROTTLE_PENALTIY);
			if (rv > MAX_THROTTLE_TIME) {
				rv = MAX_THROTTLE_TIME;
			}
		}
	}

	if (last == 0) {
		sendrep(client, DLF_REP_IRC, rv);  /* acknowledgement, continue    */
	} else {
		sendrep(client, DLF_REP_RC, 0);    /* end of transaction, netclose */
	}

	/* server purge mode ? */
	if (IsServerPurge(server_mode)) {
		for (i = queue_size(queue); i > 0; i--) {
			rv = queue_pop(queue, (void **)&message);
			if (rv == APP_SUCCESS) {
				free_message(message);
			}
		}
	}

	return APP_SUCCESS;
}


/*
 * process_request
 */

int process_request(client_t *client) {

	/* variables */
	char      *data;
	char      *rbp;
	char      header[3 * LONGSIZE];
	int       len;
	int       magic;
	int       msglen;
	int       rv;
	int       type;

	/* extract header */
	len = netread_timeout(client->fd, header, sizeof(header), 0);
	if (len != sizeof(header)) {
		sendrep(client, DLF_REP_ERR, DLF_ERR_PROTO);
		return APP_FAILURE;
	}

	/* initialise variables */
	rv     = APP_FAILURE;
	msglen = 0;
	rbp    = header;

	unmarshall_LONG(rbp, magic);
	unmarshall_LONG(rbp, type);
	unmarshall_LONG(rbp, msglen);

	/* valid protocol version ? */
	if (magic != DLF_MAGIC) {
		log(LOG_ERR, "process_request() - protocol violation from client=%d:%s\n", CLIENT_IDENT(client));
		sendrep(client, DLF_REP_ERR, DLF_ERR_PROTO);
		return APP_FAILURE;
	}
	len = msglen - sizeof(header);

	/* allocate memory for message */
	data = (char *) malloc(len);
	if (data == NULL) {
		log(LOG_ERR, "process_request() - failed to malloc client=%d:%s : %s\n", CLIENT_IDENT(client), strerror(errno));
		sendrep(client, DLF_REP_ERR, DLF_ERR_SYSERR);
		return APP_FAILURE;
	}
	log(LOG_DEBUG, "process_request() - client=%d:%s magic=0x%x type=%d length=%d\n", 
	    CLIENT_IDENT(client), magic, type, len);

	/* read message
	 *   - if we fail to read the message here, there is a strong likelihood we'll fail to send an
	 *     error message back so we simply disconnect the client
	 */
	rv = netread_timeout(client->fd, data, len, 0);
	if (rv < 0) {
		free(data);
		return APP_FAILURE;
	}

	/* process the request type */
	if (type == DLF_INIT) {
		rv = parse_init(client, data, len);
		if (rv == APP_SUCCESS) {

			/* update statistics */
			Cthread_mutex_lock(&client->handler->mutex);
			client->handler->inits++;
			Cthread_mutex_unlock(&client->handler->mutex);
		}
	} else if (type == DLF_LOG) {
		rv = parse_log(client, data, len);
		if (rv == APP_SUCCESS) {

			/* update statistics */
			Cthread_mutex_lock(&client->handler->mutex);
			client->handler->messages++;
			Cthread_mutex_unlock(&client->handler->mutex);
		}
	} else {
		sendrep(client, DLF_REP_ERR, DLF_ERR_INVALIDREQ);
		free(data);
		return APP_FAILURE;
	}

	/* free resources */
	free(data);

	return rv;
}


/** End-of-File **/
