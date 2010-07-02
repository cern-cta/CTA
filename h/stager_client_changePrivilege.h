/******************************************************************************
 *                      stager/stager_addPrivilege.c
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile $ $Revision $ $Release$ $Date $ $Author $
 *
 * common code for the add and remove privilege comman lines
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef STAGER_CLIENT_CHANGEPRIVILEGE
#define STAGER_CLIENT_CHANGEPRIVILEGE

#include <sys/types.h>
#include "osdep.h"
#include "stager_api.h"

/**
 * changePrivilege
 *
 * fully handles a add/removePrivilege command line
 *
 * @param argc number of command line arguments
 * @param argv the command line
 * @param isAdd whether it is an addPrivilege command. Otherwise
 * it is a removePrivilege command
 */
EXTERN_C int changePrivilege _PROTO
((int argc, char *argv[], int isAdd));

#endif  /* STAGER_CLIENT_CHANGEPRIVILEGE */
