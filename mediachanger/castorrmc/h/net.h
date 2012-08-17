/******************************************************************************
 *                      net.h
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
 * @author castor dev team
 *****************************************************************************/

#ifndef _NET_H
#define _NET_H

#include <osdep.h>
#include <sys/types.h>
#include <sys/socket.h>

EXTERN_C int   netread (int, char *, int); /* Network receive function     */
EXTERN_C int   netwrite(int, char *, int); /* Network send function        */
EXTERN_C char* neterror(void);             /* Network error function       */

EXTERN_C ssize_t  netread_timeout (int, void *, ssize_t, int);
EXTERN_C ssize_t  netwrite_timeout (int, void *, ssize_t, int);
EXTERN_C int netconnect_timeout (int, struct sockaddr *, size_t, int);

#endif /* _NET_H */
