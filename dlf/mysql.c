/******************************************************************************************************
 *                                                                                                    *
 * mysql.c - Castor Distribution Logging Facility                                                     *
 * Copyright (C) 2005 CERN IT/FIO (castor-dev@cern.ch)                                                *
 *                                                                                                    *
 * DLF is free software; you can redistribute it and/or modify it under the terms of the GNU          *
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
 * $Id: mysql.c,v 1.5 2006/08/02 16:24:55 waldron Exp $
 */

/* headers */
#include <errno.h>
#include <mysql.h>
#include <errmsg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "Cthread_api.h"
#include "dbi.h"
#include "dlf_api.h"
#include "hash.h"
#include "log.h"
#include "queue.h"
#include "serrno.h"


/* pools */
static database_t   *dpool[MAX_THREADS];     /**< thread pool for database connections  */

/* hashes */
static hash_t       *hosthash    = NULL;     /**< hostname hash                         */
static hash_t       *nshosthash  = NULL;     /**< name server hostname hash             */

/* mutexes
 *   - used to generate msg sequence numbers
 */
static int database_mutex;

/* database sequences */
static long   seq_id       = -1;             /**< primary key id for dlf_messages       */
static long   seq_hostid   = -1;             /**< primary key id for dlf_host_map       */
static long   seq_nshostid = -1;             /**< primary key id for dlf_nshost_map     */

/* global variables */
static time_t last_monitoring = 0;           /**< time of last call to dlf_monitoring() */
static time_t last_mode       = 0;           /**< time of last call to dlf_mode()       */

/* externs */
extern int       server_mode;                /**< server mode                           */
extern long      server_start;               /**< server startup time                   */
extern queue_t   *queue;                     /**< internal server fifo queue            */


/**
 * structures
 */

struct database_t {
	int                tid;
	int                index;
	long               mode;
	int                mutex;
	time_t             last_connect;

	MYSQL              mysql;

	/* statistics
	 *   - must be purged at a regular interval, otherwise overflows in the counters will occur
	 */
	unsigned long      commits;
	unsigned long      errors;
	unsigned long      inserts;
	unsigned long      rollbacks;
	unsigned long      selects;
	unsigned long      updates;
	unsigned long      messages;
	unsigned long      cursors;
	unsigned long      inits;
	double             response;
};


/*
 * db_init
 */

int DLL_DECL db_init(int threads) {

	/* variables */
	MYSQL_RES    *res = NULL;
	MYSQL_ROW    row;
	database_t   *db;
	char         func[30];
	int          i;
	int          rv;
	unsigned int mysql_errnum;

	/* flush thread pool */
	for (i = 0; i < MAX_THREADS; i++) {
		dpool[i] = NULL;
	}
	db = NULL;

	/* suspend database insertion until init completion
	 *   - at this point data may already be waiting for insertion in the central queue, allowing
	 *     the newly created threads in the next section to begin working on data would cause data
	 *     corruption as the sequence numbers have yet to be determined.
 	 */
	SetSuspendedDB(server_mode);

	/* initialise hashes */
	rv = hash_create(&hosthash, 512);
	if (rv != APP_SUCCESS) {
		log(LOG_ERR, "db_init() - failed to initialise host hash\n");
		return APP_FAILURE;
	}

	rv = hash_create(&nshosthash, 100);
	if (rv != APP_SUCCESS) {
		log(LOG_ERR, "db_init() - failed to initialise name server host hash\n");
		return APP_FAILURE;
	}

	last_monitoring = time(NULL);
	last_mode       = time(NULL) - 60;

	if (threads <= 0) {
		return APP_SUCCESS;
	}

	/* check if the mysql client was compiled as thread-safe
	 *   - if not reset the number of threads to 1
	 */
	if (mysql_thread_safe() == 0) {
		log(LOG_NOTICE, "db_init() - mysql client libraries not compiled as thread-safe\n");
		threads = 1;
	}

	/* create database threads */
	for (i = 0; i < threads; i++) {

		/* create structure */
		db = (database_t *) malloc(sizeof(database_t));
		if (db == NULL) {
			log(LOG_ERR, "db_init() - failed to malloc() database structure : %s\n", strerror(errno));
			return APP_FAILURE;
		}

		/* initialise database_t structure */
		db->index        = i;
		db->mode         = MODE_DEFAULT;
		db->last_connect = 0;

		/* initialise statistics */
		db->commits = db->errors  = db->inserts  = db->rollbacks = 0;
		db->selects = db->updates = db->messages = db->cursors   = 0;
		db->inits   = 0;

		/* create thread */
		db->tid = Cthread_create_detached((void *)db_worker, (database_t *)db);
		if (db->tid == APP_FAILURE) {
			log(LOG_EMERG, "db_init() - failed to Cthread_create_detached() : %s\n", sstrerror(serrno));
			return APP_FAILURE;
		}

		/* assign thread to database pool */
		dpool[i] = db;
	}

	/* initialise internal sequence numbers
	 *   - failure to determine the sequence numbers for tables dlf_messages, dlf_nshost_map and
	 *     dlf_host_map result in db_shutdown(). This error is unrecoverable!
	 */

	/* use the first thread in the pool */
	rv = db_open(dpool[0], 3);
	if (rv != APP_SUCCESS) {
		log(LOG_ERR, "db_init() - maximum connection attempts reached\n");
		return APP_FAILURE;
	}

	/* execute query */
	Cthread_mutex_lock(&dpool[0]->mutex);
	SetActive(dpool[0]->mode);

	db->selects++;
	if (mysql_query(&dpool[0]->mysql, "SELECT * FROM dlf_sequences") != APP_SUCCESS) {
		strcpy(func, "mysql_query()");
		goto error;
	}

	/* store the result */
	if ((res = mysql_store_result(&dpool[0]->mysql)) == NULL) {
		strcpy(func, "mysql_store_result()");
		goto error;
	}

	/* loop over results */
	while ((row = mysql_fetch_row(res))) {
		if (!strcasecmp(row[0], "id"))
			seq_id       = atol(row[1]);    /* table: dlf_messages   */
		if (!strcasecmp(row[0], "hostid"))
			seq_hostid   = atol(row[1]);    /* table: dlf_host_map   */
		if (!strcasecmp(row[0], "nshostid"))
			seq_nshostid = atol(row[1]);    /* table: dlf_nshost_map */
	}
	mysql_free_result(res);
	ClrActive(dpool[0]->mode);

	/* all the sequences numbers where updated ? */
	if ((seq_id == -1) || (seq_hostid == -1) || (seq_nshostid == -1)) {
		log(LOG_ERR, "db_init() - failed to determine initial sequence vales\n");
		dpool[0]->errors++;
		Cthread_mutex_unlock(&dpool[0]->mutex);
		return APP_FAILURE;
	}

	Cthread_mutex_unlock(&dpool[0]->mutex);

	/* unsuspend */
	ClrSuspendedDB(server_mode);

	return APP_SUCCESS;

 error:

	/* display error message */
	mysql_errnum = mysql_errno(&db->mysql);
	if ((mysql_errnum == CR_SERVER_GONE_ERROR) || (mysql_errnum == CR_SERVER_LOST)) {
		ClrConnected(dpool[0]->mode);
	}

	log(LOG_ERR, "db_init() - %s : %s\n", func, mysql_error(&dpool[0]->mysql));

	mysql_free_result(res);
	dpool[0]->errors++;
	ClrActive(dpool[0]->mode);

	Cthread_mutex_unlock(&dpool[0]->mutex);
	return APP_FAILURE;
}


/*
 * db_open
 */

int DLL_DECL db_open(database_t *db, unsigned int retries) {

	/* variables */
	char    hostname[100];
	char    username[100];
	char    password[100];
	char    func[30];
	char    buf[100];
	int     count;

	FILE    *fp;

	/* initialise variables */
	username[0] = password[0] = hostname[0] = '\0';
	count = 0;

	/* connection already open */
	if (IsConnected(db->mode)) {
		return APP_SUCCESS;
	}

	/* throttle connection attempts */
	if (db->last_connect > (time(NULL) - 10)) {
		return APP_FAILURE;
	}

	/* try to establish a connection until the maximum number of retries has been reached */
	do {
		/* throttle the connection attempts */
		if (count != 0) {
			sleep(10);
		}
		count++;

		/* read connection string
		 *    - the string should be in the format <username>/<password>@<hostname>
		 *    - all parameters are mandatory and must not be blank
		 */
		fp = fopen(DLFCONFIG, "r");
		if (fp == NULL) {
			log(LOG_ERR, "db_open() - failed to open config '%s' : %s\n",
				DLFCONFIG, strerror(errno));
			continue;
		}

		/* read the first line */
		if (fgets(buf, sizeof(buf) - 1, fp) == NULL) {
			fclose(fp);
			continue;
		}

		fclose(fp);

		/* extract values */
		if (sscanf(buf, "%25[^/]/%25[^@]@%25s", username, password, hostname) != 3) {
			log(LOG_ERR, "db_open() - invalid connect string in '%s'", DLFCONFIG);
			continue;
		}

		log(LOG_DEBUG, "db_open() - attempting connection to %s@%s\n", username, hostname);

		/* open connection */
		if (mysql_init(&db->mysql) == NULL) {
			log(LOG_CRIT, "db_open() - failed to mysql_init() MYSQL handler : memory allocation error\n");
			return APP_FAILURE;
		}
		db->last_connect = time(NULL);

		if ((mysql_real_connect(&db->mysql, hostname, username, password, "dlf", 0, NULL, 0)) == NULL) {
			strcpy(func, "mysql_real_connect()");
			goto error;
		}

		/* turn off automatic commits, we want a transactional database mode */
		if (mysql_autocommit(&db->mysql, 0) != APP_SUCCESS) {
			strcpy(func, "mysql_autocommit()");
			goto error;
		}

		/* flag new connection status */
		db->last_connect = 0;
		SetConnected(db->mode);
		log(LOG_DEBUG, "db_open() - connection established\n");

		return APP_SUCCESS;

	error:
		log(LOG_ERR, "db_open() - %s : %s\n", func, mysql_error(&db->mysql));

	} while ((retries == 0) || (count < retries));

	return APP_FAILURE;
}


/*
 * Shutdown database layer
 */

int DLL_DECL db_shutdown(void) {

	/* variables */
	int       i;

	/* destroy database thread pool */
	for (i = 0; i < MAX_THREADS; i++) {
		if (dpool[i] != NULL) {

			/* close database connection if connected */
			Cthread_mutex_lock(&dpool[i]->mutex);

			if (IsConnected(dpool[i]->mode)) {
				mysql_close(&dpool[i]->mysql);
				log(LOG_NOTICE, "db_shutdown() - connection closed\n");
			}
			Cthread_mutex_unlock(&dpool[i]->mutex);

			free(dpool[i]);
		}
	}

	/* destroy hashes */
	hash_destroy(hosthash,   NULL);
	hash_destroy(nshosthash, NULL);

	return APP_SUCCESS;
}


/*
 * db_initfac
 */

int DLL_DECL db_initfac(char *facility, msgtext_t *texts[], int *fac_no) {

	/* variables */
	MYSQL_RES    *res = NULL;
	MYSQL_ROW    row;
	database_t   *db;
	char         func[30];
       	char         query[1024];
	int          i;
	int          id;
	int          found;
	unsigned int mysql_errnum;

	/* attempt to find a database connection which is currently connected and not in a busy state
	 *   - if no connection can be found pick any connected connection
	 */
	for (i = 0, found = 0; i < MAX_THREADS; i++) {
		if (dpool[i] == NULL)
			continue;
		if (IsConnected(dpool[i]->mode) && !IsActive(dpool[i]->mode)) {
			found = 1;
			break;
		}
	}

	/* pick any ? */
	if (found != 1) {
		for (i = 0, found = 0; i < MAX_THREADS; i++) {
			if (dpool[i] == NULL)
				continue;
			if (IsConnected(dpool[i]->mode)) {
				found = 1;
				break;
			}
		}
	}

	/* no connections available ? */
	if (found == 0) {
		log(LOG_ERR, "db_initfac() - no database connections available\n");
		Cthread_mutex_unlock(&database_mutex);
		return APP_FAILURE;
	}
	db = dpool[i];

	/* inserting msg texts bypasses the central fifo queue and has priority over normal incoming
	 * data. As threads are heavily coupled to the queue concept it is necessary to hijack another
	 * threads db connection. This requires us to hold a mutex over the statistics data
	 *   - any delays here will impact insertion globally across the system
	 */
       	Cthread_mutex_lock(&db->mutex);
	db->inits++;

	/* execute query */
	SetActive(db->mode);

	db->selects++;
	snprintf(query, sizeof(query), "SELECT fac_no FROM dlf_facilities WHERE fac_name = '%s'", facility);
	if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
		strcpy(func, "mysql_query()");
		goto error;
	}

	/* store the result */
	if ((res = mysql_store_result(&db->mysql)) == NULL) {
		strcpy(func, "mysql_store_result()");
		goto error;
	}

	/* nothing found ? - facility not registered */
	if (mysql_num_rows(res) == 0) {
		goto not_found;
	}

	/* fetch the first row which contains the facility number */
	if ((row = mysql_fetch_row(res)) == NULL) {
		strcpy(func, "mysql_fetch_row()");
		goto error;
	}
	id = atoi(row[0]);
	*fac_no = id;

	/* the arrays msg_text array passed to this function cannot simply be insert'ed into the database
	 * checks must be performed first to determine whether the message is new to the system or
	 * already exists and requires an update.
	 */

	for (i = 0; i < DLF_MAX_MSGTEXTS; i++) {
		if (texts[i] == NULL)
			continue;

		/* clear any previous results gathered */
		mysql_free_result(res);

		/* entry already exists ? */
		db->selects++;
		snprintf(query, sizeof(query), "SELECT COUNT(*) FROM dlf_msg_texts WHERE (fac_no = '%d') AND (msg_no = '%d')",
				       id, texts[i]->msg_no);
		if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
			strcpy(func, "mysql_query()");
			goto error;
		}

		/* store the result */
		if ((res = mysql_store_result(&db->mysql)) == NULL) {
			strcpy(func, "mysql_store_result()");
			goto error;
		}

		/* fetch the first row which tells us if the message already exists for the given
		 * facility
		 */
		if ((row = mysql_fetch_row(res)) == NULL) {
			strcpy(func, "mysql_fetch_row()");
			goto error;
		}

		/* found results, update as opposed to select */
		if (atoi(row[0]) == 1) {

			/* construct query */
			db->updates++;
			snprintf(query, sizeof(query), "UPDATE dlf_msg_texts \
                                                        SET msg_text = '%s'  \
                                                        WHERE (fac_no = '%d') AND (msg_no = '%d')",
				 texts[i]->msg_text, id, texts[i]->msg_no);

			/* execute query */
			if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
				strcpy(func, "mysql_query()");
				goto error;
			}
		} else {

			/* construct query */
			db->inserts++;
			snprintf(query, sizeof(query), "INSERT INTO dlf_msg_texts (fac_no, msg_no, msg_text) \
                                                        VALUES ('%d', '%d', '%s')",
				 id, texts[i]->msg_no, texts[i]->msg_text);

			/* execute query */
			if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
				strcpy(func, "mysql_query()");
				goto error;
			}
		}
	}

	/* commit all changes */
	db->commits++;
	(void)mysql_query(&db->mysql, "COMMIT");

	mysql_free_result(res);
 	ClrActive(db->mode);

	Cthread_mutex_unlock(&db->mutex);
	return APP_SUCCESS;

not_found:

	ClrActive(db->mode);

	*fac_no = -1;

	mysql_free_result(res);
	Cthread_mutex_unlock(&db->mutex);
	return APP_FAILURE;

error:

	/* display error message */
	mysql_errnum = mysql_errno(&db->mysql);
	if ((mysql_errnum == CR_SERVER_GONE_ERROR) || (mysql_errnum == CR_SERVER_LOST)) {
		ClrConnected(db->mode);
	}
	db->errors++;

	log(LOG_ERR, "db_initfac() - %s : %s %d\n", func, mysql_error(&db->mysql), mysql_errnum);

	mysql_free_result(res);
	ClrActive(db->mode);

	Cthread_mutex_unlock(&db->mutex);
	return APP_FAILURE;
}


/*
 * db_hostid
 */

int DLL_DECL db_hostid(database_t *db, char *hostname, int *id) {

        /* variables */
	MYSQL_RES    *res = NULL;
	MYSQL_ROW    row;
	char         func[30];
	char         query[1024];
        int          rv;
	int          hostid;
	int          *value = NULL;
	void         *v     = NULL;
	unsigned int mysql_errnum;

        /* hostname defined ? */
        if (hostname == NULL) {
                return APP_FAILURE;
        }

        /* cached ? */
        rv = hash_search(hosthash, hostname, &v);
        if (rv == APP_SUCCESS) {
		*id = *(int *)v;
                return APP_SUCCESS;
        }

        Cthread_mutex_lock(&database_mutex);

        /* re-check the hash
         *   - the last thread may have inserted the data we are looking for.
         */
        rv = hash_search(hosthash, hostname, &v);
        if (rv == APP_SUCCESS) {
		*id = *(int *)v;
                Cthread_mutex_unlock(&database_mutex);
                return APP_SUCCESS;
        }

	/* execute query */
	Cthread_mutex_lock(&db->mutex);
	SetActive(db->mode);

	db->selects++;
	snprintf(query, sizeof(query), "SELECT hostid FROM dlf_host_map WHERE hostname = '%s'", hostname);
	if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
		strcpy(func, "mysql_query()");
		goto error;
	}

	/* store the result */
	if ((res = mysql_store_result(&db->mysql)) == NULL) {
		strcpy(func, "mysql_store_result()");
		goto error;
	}

	/* nothing found ? - must be a new host */
	if (mysql_num_rows(res) == 0) {
		goto not_found;
	}

	/* fetch first row */
	if ((row = mysql_fetch_row(res)) == NULL) {
		strcpy(func, "mysql_fetch_row()");
		goto error;
	}
	hostid = atoi(row[0]);
	ClrActive(db->mode);

	/* cache the value */
	value = malloc(sizeof(int));
	if (value == NULL) {
		log(LOG_CRIT, "db_hostid() - failed to malloc hash entry : %s", strerror(errno));

		db->errors++;

		mysql_free_result(res);
		Cthread_mutex_unlock(&db->mutex);
		Cthread_mutex_unlock(&database_mutex);

		return APP_FAILURE;
	}
	*value = hostid;

        hash_insert(hosthash, hostname, value);
        *id = hostid;

	mysql_free_result(res);
        Cthread_mutex_unlock(&db->mutex);
        Cthread_mutex_unlock(&database_mutex);

        return APP_SUCCESS;

not_found:

        /* insert a new host */
	hostid = seq_hostid++;
	mysql_free_result(res);

	/* register the new hostname */
        db->inserts++;
	snprintf(query, sizeof(query), "INSERT INTO dlf_host_map (hostid, hostname) VALUES ('%d', '%s')",
		 hostid, hostname);
	if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
		strcpy(func, "mysql_query()");
		goto error;
	}

	/* update sequences */
	db->updates++;
	snprintf(query, sizeof(query), "UPDATE dlf_sequences SET seq_no = (seq_no + 1) WHERE seq_name = 'hostid'");
	if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
		strcpy(func, "mysql_query()");
		goto error;
	}

	/* commit all changes */
	db->commits++;
	(void)mysql_query(&db->mysql, "COMMIT");

        *id = hostid;

        ClrActive(db->mode);

        Cthread_mutex_unlock(&db->mutex);
        Cthread_mutex_unlock(&database_mutex);

        return APP_SUCCESS;

error:

	/* display error message */
	mysql_errnum = mysql_errno(&db->mysql);
	if ((mysql_errnum == CR_SERVER_GONE_ERROR) || (mysql_errnum == CR_SERVER_LOST)) {
		ClrConnected(db->mode);
	}
	db->errors++;

	log(LOG_ERR, "db_hostid() - %s : %s\n", func, mysql_error(&db->mysql));

	/* attempt to rollback
	 *   - only necessary if we fail in the insert, we ignore errors here
	 */
	if (IsConnected(db->mode)) {
		if (mysql_rollback(&db->mysql) != 0) {
			log(LOG_ERR, "db_hostid() : failed to rollback : %s\n", mysql_error(&db->mysql));
		}
		db->rollbacks++;
	}

	mysql_free_result(res);
	ClrActive(db->mode);

	Cthread_mutex_unlock(&db->mutex);
	Cthread_mutex_unlock(&database_mutex);

	return APP_FAILURE;
}


/* db_nshostid
 *   - this function is almost identical to db_hostid() apart from the querying of different tables and
 *     updating of different sequences. We could have created one generic function to do both hostnames
 *     and ns hostnames but it would just be full of IF statements!!
 */

int DLL_DECL db_nshostid(database_t *db, char *hostname, int *id) {

        /* variables */
	MYSQL_RES    *res   = NULL;
	MYSQL_ROW    row;
	char         func[30];
	char         query[1024];
        int          rv;
	int          nshostid;
	int          *value = NULL;
	void         *v     = NULL;
	unsigned int mysql_errnum;

        /* hostname defined ? */
        if (hostname == NULL) {
                return APP_FAILURE;
        }

        /* cached ? */
        rv = hash_search(nshosthash, hostname, &v);
        if (rv == APP_SUCCESS) {
		*id = *(int *)v;
                return APP_SUCCESS;
        }

        Cthread_mutex_lock(&database_mutex);

        /* re-check the hash
         *   - the last thread may have inserted the data we are looking for.
         */
        rv = hash_search(nshosthash, hostname, &v);
        if (rv == APP_SUCCESS) {
		*id = *(int *)v;
                Cthread_mutex_unlock(&database_mutex);
                return APP_SUCCESS;
        }

	/* execute query */
	Cthread_mutex_lock(&db->mutex);
	SetActive(db->mode);

	db->selects++;
	snprintf(query, sizeof(query), "SELECT nshostid FROM dlf_nshost_map WHERE nshostname = '%s'", hostname);
	if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
		strcpy(func, "mysql_query()");
		goto error;
	}

	/* store the result */
	if ((res = mysql_store_result(&db->mysql)) == NULL) {
		strcpy(func, "mysql_store_result()");
		goto error;
	}

	/* nothing found ? - must be a new host */
	if (mysql_num_rows(res) == 0) {
		goto not_found;
	}

	/* fetch first row */
	if ((row = mysql_fetch_row(res)) == NULL) {
		strcpy(func, "mysql_fetch_row()");
		goto error;
	}
	nshostid = atoi(row[0]);
	ClrActive(db->mode);

	/* cache the value */
	value = malloc(sizeof(int));
	if (value == NULL) {
		log(LOG_CRIT, "db_nshostid() - failed to malloc hash entry : %s", strerror(errno));

		db->errors++;
		mysql_free_result(res);
		Cthread_mutex_unlock(&db->mutex);
		Cthread_mutex_unlock(&database_mutex);

		return APP_FAILURE;
	}
	*value = nshostid;

        hash_insert(nshosthash, hostname, value);
        *id = nshostid;

	mysql_free_result(res);
        Cthread_mutex_unlock(&db->mutex);
        Cthread_mutex_unlock(&database_mutex);

        return APP_SUCCESS;

not_found:

        /* insert a new host */
	nshostid = seq_nshostid++;
	mysql_free_result(res);

	/* register the new hostname */
        db->inserts++;
	snprintf(query, sizeof(query), "INSERT INTO dlf_nshost_map (nshostid, nshostname) VALUES ('%d', '%s')",
		 nshostid, hostname);
	if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
		strcpy(func, "mysql_query()");
		goto error;
	}

	/* update sequences */
	db->updates++;
	snprintf(query, sizeof(query), "UPDATE dlf_sequences SET seq_no = (seq_no + 1) WHERE seq_name = 'nshostid'");
	if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
		strcpy(func, "mysql_query()");
		goto error;
	}

	/* commit all changes */
	db->commits++;
	(void)mysql_query(&db->mysql, "COMMIT");

        *id = nshostid;

        ClrActive(db->mode);

        Cthread_mutex_unlock(&db->mutex);
        Cthread_mutex_unlock(&database_mutex);

        return APP_SUCCESS;

error:

	/* display error message */
	mysql_errnum = mysql_errno(&db->mysql);
	if ((mysql_errnum == CR_SERVER_GONE_ERROR) || (mysql_errnum == CR_SERVER_LOST)) {
		ClrConnected(db->mode);
	}
	db->errors++;

	log(LOG_ERR, "db_nshostid() - %s : %s\n", func, mysql_error(&db->mysql));

	/* attempt to rollback
	 *   - only necessary if we fail in the insert, we ignore errors here
	 */
	if (IsConnected(db->mode)) {
		if (mysql_rollback(&db->mysql) != 0) {
			log(LOG_ERR, "db_nshostid() : failed to rollback : %s\n", mysql_error(&db->mysql));
		}
		db->rollbacks++;
	}

	mysql_free_result(res);
	ClrActive(db->mode);

	Cthread_mutex_unlock(&db->mutex);
	Cthread_mutex_unlock(&database_mutex);

	return APP_FAILURE;
}


/*
 * db_worker
 */

void DLL_DECL db_worker(database_t *db) {

	/* variables */
	struct timeval tv;
	message_t    *message;
	param_t      *param;
	char         func[50];
	char         query[4096];
	int          rv;
	int          id;
	int          hostid;
	int          nshostid;
	unsigned int mysql_errnum;

	/* initialise table and statistic variables */
	db->commits  = db->errors = db->inserts = db->rollbacks = db->updates = db->messages = 0;
	db->response = 0.0;

	/* initialise thread specific variables for mysql */
	if (mysql_thread_safe() != 0) {
		mysql_thread_init();
	}

	/* worker thread
	 *   - extract messages from the central fifo queue and insert the message into the mysql db
	 *     this is done one row at a time. Not very efficient!!!
	 */
	while (!IsShutdown(server_mode)) {

		/* server suspended ?
		 *    - this maybe a user suspension via a signal or we are still in an initialisation
	 	 *      determining the unique id for the first message, either way we sleep and loop
		 */
		if (IsSuspendedDB(server_mode)) {
			sleep(1);
			continue;
		}

		/* connected to database ? */
		if (!IsConnected(db->mode)) {
			if ((rv = db_open(db, 1)) != APP_SUCCESS) {
				continue;
			}
		}
		message = NULL;

		/* pop a message from the queue */
		rv = queue_pop(queue, (void **)&message);

		/* queue errors */
		if (rv == APP_QUEUE_TERMINATED) {
			break;                        /* queue terminated, shutdown  */
		}
		else if (rv == APP_QUEUE_EMPTY) {
			sleep(1);                     /* nothing available           */
			continue;
		}
		else if (rv != APP_SUCCESS) {
			log(LOG_CRIT, "db_worker() - failed to queue_pop()\n");
			break;
		}

		/* message defined ? */
		if (message == NULL) {
			continue;
		}

		Cthread_mutex_lock(&db->mutex);
		db->messages++;
		Cthread_mutex_unlock(&db->mutex);

		/* accumulate the sql response time */
		rv = gettimeofday(&tv, NULL);
		if (rv == APP_SUCCESS) {
			Cthread_mutex_lock(&db->mutex);
			db->response += ((((double)tv.tv_sec * 1000) +
				  	 ((double)tv.tv_usec / 1000)) - message->received) * 0.001;
			Cthread_mutex_unlock(&db->mutex);
		}

		/* determine the next available unique id (primary key)
		 *   - the next number in the oracle sequence is determined at initialisation and updated
		 *     using a global mutex.
		 *   - a failure here is a clear indicator that something serious has gone wrong and the
		 *     thread will exit.
		 */
		if ((rv = Cthread_mutex_lock(&database_mutex)) != APP_SUCCESS) {
			break;
		}

		id = seq_id;
		seq_id++;

		/* release mutex lock */
		if ((rv = Cthread_mutex_unlock(&database_mutex)) != APP_SUCCESS) {
			break;
		}

		/* hostname resolution (nshostname, hostname)
		 *   - if an error occurs attempting to resolve the hostname to an id, we simply ignore
		 *     the message
		 */
		if (db_hostid(db, message->hostname, &hostid) != APP_SUCCESS) {
			free_message(message);
			continue;
		}

		if (db_nshostid(db, message->nshostname, &nshostid) != APP_SUCCESS) {
			free_message(message);
			continue;
		}

		/* insert message into database */
		Cthread_mutex_lock(&db->mutex);
		SetActive(db->mode);

		/* table: dlf_messages */
		db->inserts++;
		snprintf(query, sizeof(query), "INSERT INTO dlf_messages                                                                          \
                                               (id, timestamp, timeusec, reqid, hostid, facility, severity, msg_no, pid, tid, nshostid, nsfileid) \
                                               VALUES ('%ld','%s','%d','%s','%d','%d','%d','%d','%d','%d','%d','%s');",
			 id, message->timestamp,
			 message->timeusec,
			 message->reqid,
			 hostid,
			 message->facility,
			 message->severity,
			 message->msg_no,
			 message->pid,
			 message->tid,
			 nshostid,
			 message->nsfileid);
		if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
			strcpy(func, "mysql_query() into table: dlf_messages");
			goto error;
		}

		/* process parameters */
		for (param = message->plist; param != NULL; param = param->next) {

			/* table: dlf_str_param_values */
			if (param->type == DLF_MSG_PARAM_STR) {
				snprintf(query, sizeof(query), "INSERT INTO dlf_str_param_values   \
                                                               (id, timestamp, name, value)        \
                                                               VALUES ('%ld', '%s', '%s', '%s')",
					 id, message->timestamp, param->name, param->value);

				strcpy(func, "mysql_query() into table dlf_str_param_values");
			}

			/* table: dlf_reqid_map */
			else if (param->type == DLF_MSG_PARAM_UUID) {
				snprintf(query, sizeof(query), "INSERT INTO dlf_reqid_map         \
                                                               (id, timestamp, reqid, subreqid)   \
                                                               VALUES ('%ld', '%s', '%s', '%s')",
					 id, message->timestamp, message->reqid, param->value);

				strcpy(func, "mysql_query() into table dlf_reqid_map");
			}

			/* table: dlf_num_param_values */
			else if ((param->type == DLF_MSG_PARAM_DOUBLE) ||
				 (param->type == DLF_MSG_PARAM_INT) ||
				 (param->type == DLF_MSG_PARAM_INT64)) {
				snprintf(query, sizeof(query), "INSERT INTO dlf_num_param_values \
                                                                (id, timestamp, name, value)     \
                                                                VALUES ('%ld', '%s', '%s', '%s')",
					 id, message->timestamp, param->name, param->value);

				strcpy(func, "mysql_query() into table: dlf_num_param_values");
			}

			/* table: dlf_tape_ids */
			else if (param->type == DLF_MSG_PARAM_TPVID) {
				snprintf(query, sizeof(query), "INSERT INTO dlf_tape_ids        \
                                                                (id, timestamp, tapevid)        \
                                                                VALUES ('%ld', '%s', '%s')",
					 id, message->timestamp, param->value);

				strcpy(func, "mysql_query() into table: dlf_tape_ids");
			} else {
				continue;
			}

			/* execute query */
			db->inserts++;
			if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
				goto error;
			}
		}

		/* update sequences */
		db->updates++;
		snprintf(query, sizeof(query), "UPDATE dlf_sequences SET seq_no = (seq_no + 1) WHERE seq_name = 'id'");
		if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
			strcpy(func, "mysql_query() into table: dlf_sequences");
			goto error;
		}

		/* commit all changes */
		db->commits++;
		(void)mysql_query(&db->mysql, "COMMIT");

		ClrActive(db->mode);
		Cthread_mutex_unlock(&db->mutex);

		/* free message */
		free_message(message);

		continue;

	error:

		/* display error message */
		mysql_errnum = mysql_errno(&db->mysql);
		if ((mysql_errnum == CR_SERVER_GONE_ERROR) || (mysql_errnum == CR_SERVER_LOST)) {
			ClrConnected(db->mode);
		}
		db->errors++;

		log(LOG_ERR, "db_worker() - %s : %s\n", func, mysql_error(&db->mysql));

		if (mysql_rollback(&db->mysql) != 0) {
			log(LOG_ERR, "db_worker() - failed to rollback : %s\n", mysql_error(&db->mysql));
		}
		db->rollbacks++;

		ClrActive(db->mode);
		Cthread_mutex_unlock(&db->mutex);

		/* free message */
		free_message(message);
	}

	if (mysql_thread_safe() != 0) {
		mysql_thread_end();
	}

	/* exit */
	Cthread_exit(0);
}


/*
 * db_stats (not implemented)
 */

int DLL_DECL db_stats(unsigned int interval) {

	return APP_SUCCESS;
}


/*
 * db_monitoring
 */

int DLL_DECL db_monitoring(handler_t **hpool, unsigned int interval) {

	/* variables */
	struct       tm tm_str;
	database_t   *db;
	char         query[1024];
	char         timestr[15];
	time_t       now;
	int          i;
	int          found;
	int          s_hosthash;
	int          s_nshosthash;
	unsigned int mysql_errnum;

	/* database accumulated stats */
	int db_threads, h_threads, s_uptime;

	unsigned long db_commits    = 0, db_errors    = 0, db_inserts  = 0, db_rollbacks = 0, db_hashtexts = 0;
	unsigned long db_selects    = 0, db_updates   = 0, db_cursors  = 0, db_messages  = 0, db_inits = 0;
	unsigned long h_connections = 0, h_clientpeak = 0, h_messages  = 0, h_errors     = 0;
	unsigned long h_inits       = 0, h_timeouts   = 0;

	unsigned int size   = 0;
	double response     = 0.0;
	double db_hashstats = -1.0;


	/* interval passed ? */
	if ((time(NULL) - last_monitoring) < interval) {
		return APP_SUCCESS;
	}
	last_monitoring = time(NULL);

        /* attempt to find a database connection which is currently connected and not in a busy state
         *   - if no connection can be found pick any connected connection
         */
        for (i = 0, found = 0; i < MAX_THREADS; i++) {
                if (dpool[i] == NULL)
                        continue;
                if (IsConnected(dpool[i]->mode) && !IsActive(dpool[i]->mode)) {
                        found = 1;
                        break;
                }
        }

        /* pick any ? */
        if (found != 1) {
                for (i = 0, found = 0; i < MAX_THREADS; i++) {
                        if (dpool[i] == NULL)
                                continue;
                        if (IsConnected(dpool[i]->mode)) {
                                found = 1;
                                break;
                        }
                }
        }
	db = dpool[i];

	/* handler based statistics */
	for (i = 0, h_threads = 0; i < MAX_THREADS; i++) {
		if (hpool[i] == NULL)
			continue;

		Cthread_mutex_lock(&hpool[i]->mutex);

		h_threads++;
		h_connections += hpool[i]->connections;
		h_clientpeak  += hpool[i]->clientpeak;
		h_messages    += hpool[i]->messages;
		h_inits       += hpool[i]->inits;
		h_errors      += hpool[i]->errors;
		h_timeouts    += hpool[i]->timeouts;

		hpool[i]->connections = hpool[i]->clientpeak = hpool[i]->timeouts = 0;
		hpool[i]->messages    = hpool[i]->inits      = hpool[i]->errors   = 0;

		Cthread_mutex_unlock(&hpool[i]->mutex);
	}

	/* database based statistics */
        for (i = 0, db_threads = 0; i < MAX_THREADS; i++) {
                if (dpool[i] == NULL)
                        continue;

                Cthread_mutex_lock(&dpool[i]->mutex);

		db_threads++;
		db_commits    += dpool[i]->commits;
		db_errors     += dpool[i]->errors;
		db_inserts    += dpool[i]->inserts;
		db_rollbacks  += dpool[i]->rollbacks;
		db_selects    += dpool[i]->selects;
		db_updates    += dpool[i]->updates;
		db_messages   += dpool[i]->messages;
		db_cursors    += dpool[i]->cursors;
		db_inits      += dpool[i]->inits;

		/* average response rate */
		if (dpool[i]->messages > 0) {
			response += (dpool[i]->response / dpool[i]->messages);
		}

		/* reset counters */
		dpool[i]->commits  = dpool[i]->errors  = dpool[i]->inserts  = dpool[i]->rollbacks = 0;
		dpool[i]->selects  = dpool[i]->updates = dpool[i]->messages = dpool[i]->cursors   = 0;
		dpool[i]->inits    = 0;
		dpool[i]->response = 0.0;

		Cthread_mutex_unlock(&dpool[i]->mutex);
	}

	if (response > 0) {
		response = (response / db_threads);
	}
	size = queue_size(queue);

	/* create the table timestamp */
	now = time(NULL);
	localtime_r(&now, &tm_str);
	strftime(timestr, DLF_LEN_TIMESTAMP + 1, "%Y%m%d%H%M%S", &tm_str);

	/* no database connections available ?
	 *   - we could have checked this earlier, however we need to reset the counters otherwise
	 *     overflows will occur
	 */
	if (found == 0) {
		log(LOG_ERR, "db_monitoring() : no database connections available\n");
		return APP_FAILURE;
	}

	/* execute query */
	Cthread_mutex_lock(&db->mutex);
	SetActive(db->mode);

	/* calculate the hash statistics */
	s_hosthash   = hash_stat(hosthash);
	s_nshosthash = hash_stat(nshosthash);

	if ((s_hosthash != APP_FAILURE) && (s_nshosthash != APP_FAILURE)) {
		db_hashstats = s_hosthash + s_nshosthash;
	} else {
		db_hashstats = -1.0;
	}
	s_uptime = (time(NULL) - server_start);

	db->inserts++;
	snprintf(query, sizeof(query), "INSERT INTO dlf_monitoring VALUES                        \
                 ('%s',  '%lu', '%lu', '%lu', '%lu', '%lu', '%lu', '%lu', '%lu', '%lu', '%lu',   \
                  '%lu', '%lu', '%lu', '%lu', '%lu', '%lu', '%lu', '%lu', '%lu', '%lu', '%lu',   \
                  '%lf', '%lu')",
		 timestr,       h_threads,    h_messages,   h_inits,      h_errors,   
		 h_connections, h_clientpeak, h_timeouts,   db_threads,   db_commits,  
		 db_errors,     db_inserts,   db_rollbacks, db_selects,   db_updates,  
		 db_cursors,    db_messages,  db_inits,     db_hashtexts, s_uptime,    
		 server_mode,   size,         response,     interval);

	if (mysql_query(&db->mysql, query) != APP_SUCCESS) {
		goto error;
	}

	/* commit all changes */
	db->commits++;
	(void)mysql_query(&db->mysql, "COMMIT");

	ClrActive(db->mode);
	Cthread_mutex_unlock(&db->mutex);

	return APP_SUCCESS;

error:

	/* display error message */
	mysql_errnum = mysql_errno(&db->mysql);
	if ((mysql_errnum == CR_SERVER_GONE_ERROR) || (mysql_errnum == CR_SERVER_LOST)) {
		ClrConnected(db->mode);
	}
	db->errors++;

	log(LOG_ERR, "db_monitoring() : %s\n", mysql_error(&db->mysql));

	db->errors++;
	ClrActive(db->mode);
	Cthread_mutex_unlock(&db->mutex);

	return APP_FAILURE;
}


/*
 * db_mode
 */

int DLL_DECL db_mode(unsigned int interval) {

	/* variables */
	MYSQL_RES    *res = NULL;
	MYSQL_ROW    row;
	database_t   *db;
	char         func[30];
	int          i;
	int          found;
	unsigned int mysql_errnum;

	/* interval passed ? */
	if ((time(NULL) - last_mode) < interval) {
		return APP_SUCCESS;
	}
	last_mode = time(NULL);

        /* attempt to find a database connection which is currently connected and not in a busy state
         *   - if no connection can be found pick any connected connection
         */
        for (i = 0, found = 0; i < MAX_THREADS; i++) {
                if (dpool[i] == NULL)
                        continue;
                if (IsConnected(dpool[i]->mode) && !IsActive(dpool[i]->mode)) {
                        found = 1;
                        break;
                }
        }

        /* pick any ? */
        if (found != 1) {
                for (i = 0, found = 0; i < MAX_THREADS; i++) {
                        if (dpool[i] == NULL)
                                continue;
                        if (IsConnected(dpool[i]->mode)) {
                                found = 1;
                                break;
                        }
                }
        }
	db = dpool[i];

	if (found == 0) {
		log(LOG_ERR, "db_mode() : no database connections available\n");
		return APP_FAILURE;
	}

	/* execute query */
	Cthread_mutex_lock(&db->mutex);
	SetActive(db->mode);

	db->selects++;
	if (mysql_query(&db->mysql, "SELECT name, enabled FROM dlf_mode") != APP_SUCCESS) {
		goto error;
	}

	/* store the result */
	if ((res = mysql_store_result(&db->mysql)) == NULL) {
		strcpy(func, "mysql_store_result()");
		goto error;
	}

	/* loop over results */
	while ((row = mysql_fetch_row(res))) {
		if (!strcasecmp(row[0], "queue_purge")) {
			if (atoi(row[1])) SetServerPurge(server_mode);
			else ClrServerPurge(server_mode);			
		} 
		if (!strcasecmp(row[0], "queue_suspend")) {
			if (atoi(row[1])) SetSuspendedQueue(server_mode);
			else ClrSuspendedQueue(server_mode);
		}
		if (!strcasecmp(row[0], "database_suspend")) {
			if (atoi(row[1])) SetSuspendedDB(server_mode);
			else ClrSuspendedDB(server_mode);
		}
	}
	mysql_free_result(res);

	ClrActive(db->mode);
	Cthread_mutex_unlock(&db->mutex);

	return APP_SUCCESS;

error:

	/* display error message */
	mysql_errnum = mysql_errno(&db->mysql);
	if ((mysql_errnum == CR_SERVER_GONE_ERROR) || (mysql_errnum == CR_SERVER_LOST)) {
		ClrConnected(dpool[0]->mode);
	}

	log(LOG_ERR, "db_mode() : %s\n", mysql_error(&dpool[0]->mysql));

	mysql_free_result(res);
	db->errors++;
	ClrActive(dpool[0]->mode);

	Cthread_mutex_unlock(&db->mutex);
	return APP_FAILURE;
}


/** End-of-File **/

