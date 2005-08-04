/******************************************************************************
 *                      vdqmMacros.h
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
 * @(#)RCSfile: vdqmMacros.h  Revision: 1.0  Release Date: Aug 4, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _VDQMMACROS_H_
#define _VDQMMACROS_H_

#include <vdqm_constants.h>
#include "marshall.h"

/**
 * Some useful macros, taken from the old vdqm code, 
 * to do the (un)marshalling
 */
#define ADMINREQ(X) ( X == VDQM_HOLD || X == VDQM_RELEASE || \
    X == VDQM_SHUTDOWN )

#define DO_MARSHALL(X,Y,Z,W) { \
    if ( W == SendTo ) {marshall_##X(Y,Z);} \
    else {unmarshall_##X(Y,Z);} }
    
#define DO_MARSHALL_STRING(Y,Z,W,N) { \
    if ( W == SendTo ) {marshall_STRING(Y,Z);} \
    else { if(unmarshall_STRINGN(Y,Z,N)) { serrno=EINVAL; return -1; } } }

    
#define REQTYPE(Y,X) ( X == VDQM_##Y##_REQ || \
    X == VDQM_DEL_##Y##REQ || \
    X == VDQM_GET_##Y##QUEUE || (!strcmp(#Y,"VOL") && X == VDQM_PING) || \
    (!strcmp(#Y,"DRV") && X == VDQM_DEDICATE_DRV) )
    
    
/**
 * Definition of SendTo and RceiveFrom for DO_MARSHALL
 */
typedef enum direction {SendTo, ReceiveFrom} direction_t;    

#endif //_VDQMMACROS_H_
