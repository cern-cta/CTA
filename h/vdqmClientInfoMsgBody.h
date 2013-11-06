/******************************************************************************
 *                h/vdqmClientInfoMsgBody.h
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
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef H_VDQMCLIENTINFOMSGBODY_H
#define H_VDQMCLIENTINFOMSGBODY_H 1

#include "h/Castor_limits.h"
#include <stdint.h>

/**
 * The body of a VDQM_CLIENTINFO message.
 */
typedef struct {
  int32_t volReqId;
  int32_t clientCallbackPort;
  int32_t clientUID;
  int32_t clientGID;
  char    clientHost[CA_MAXHOSTNAMELEN+1];
  char    dgn[CA_MAXDGNLEN+1];
  char    drive[CA_MAXUNMLEN+1];
  char    clientName[CA_MAXUSRNAMELEN+1];
} vdqmClientInfoMsgBody_t;

#define VDQMCLIENTINFOMSGBODY_MAXSIZE (              \
    LONGSIZE              + /* volReqID           */ \
    LONGSIZE              + /* clientCallbackPort */ \
    LONGSIZE              + /* clientUID          */ \
    LONGSIZE              + /* clientGID          */ \
    CA_MAXHOSTNAMELEN + 1 + /* clientHost         */ \
    CA_MAXDGNLEN      + 1 + /* dgn                */ \
    CA_MAXUNMLEN      + 1 + /* drive              */ \
    CA_MAXUSRNAMELEN  + 1)  /* clientName         */

#define VDQMCLIENTINFOMSGBODY_MINSIZE (              \
    LONGSIZE              + /* volReqID           */ \
    LONGSIZE              + /* clientCallbackPort */ \
    LONGSIZE              + /* clientUID          */ \
    LONGSIZE              + /* clientGID          */ \
    1                     + /* clientHost         */ \
    1                     + /* dgn                */ \
    1                     + /* drive              */ \
    1)                      /* clientName         */

#endif /* H_VDQMCLIENTINFOMSGBODY_H */
