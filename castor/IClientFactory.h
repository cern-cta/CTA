/******************************************************************************
 *                      IClientFactory.h
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
 * @(#)$RCSfile: IClientFactory.h,v $ $Revision: 1.2 $ $Release$ $Date: 2004/12/17 15:33:36 $ $Author: jdurand $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_ICLIENTFACTORY_H 
#define CASTOR_ICLIENTFACTORY_H 1

/// Forward declarations for the C world
struct C_IClient_t;

/**
 * converts a client to a human readable string.
 * In case of error, 0 is returned, serrno is set and
 * an error string is allocated in errorMsg
 * @param cl the Client to convert
 * @param an allocated string in case of error. The caller
 * is responsible for its deallocation
 * @result the resulting string, or 0 if an error occured
 */
const char* C_IClientFactory_client2String
(struct C_IClient_t* cl, const char** errorMsg);
    
/**
 * creates a Client from its human readable
 * string representation. Note that the caller is
 * responsible for the deallocation of the returned
 * Client.
 * @param st the human readable string.
 * @param an allocated string in case of error. The caller
 * is responsible for its deallocation
 * @result the newly allocated client, or 0 in case of error
 */
struct C_IClient_t* C_IClientFactory_string2Client
(char* st, const char** errorMsg);

#endif // CASTOR_ICLIENTFACTORY_H
