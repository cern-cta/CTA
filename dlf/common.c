/******************************************************************************************************
 *                                                                                                    *
 * common.c - Castor Distribution Logging Facility                                                    *
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
 * $Id: common.c,v 1.2 2006/06/19 06:52:55 waldron Exp $
 */

/* headers */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "dlf_api.h"

/*
 * Free message
 */

void DLL_DECL free_message(void *arg) {

	/* variables */
	param_t   *p, *param;
	message_t *message;

	if (arg == NULL) {
		return;
	}
	
	/* initialise variables */
	message = (message_t *)arg;

	/* free parameters */
	for (p = message->plist; p != NULL; p = param) {
		param = p->next;
		free_param(p);
	}
       	
	free(message);
}


/*
 * Free parameter
 */

void DLL_DECL free_param(param_t *param) {
	free(param->value);
	free(param);
}


/*
 * Free msgtext 
 */

void DLL_DECL free_msgtexts(msgtext_t *texts[]) {

	/* variables */
	int       i;

	/* free entries */
	for (i = 0; i < DLF_MAX_MSGTEXTS; i++) {
		if (texts[i] != NULL) {
			free(texts[i]->msg_text);
			free(texts[i]);
		}
	}
}


/** End-of-File **/

