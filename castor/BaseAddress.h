/******************************************************************************
 *                      BaseAddress.h
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_BASEADDRESS_H 
#define CASTOR_BASEADDRESS_H 1

/**
 * Forward declaration of IAddress for the C world
 */
struct C_BaseAddress_t;

/*
 * constructor
 */
int C_BaseAddress_create(const char* cnvSvcName,
                         const unsigned int cnvSvcType,
                         struct C_BaseAddress_t** addr);

/*
 * destructor
 */
int C_BaseAddress_delete(struct C_BaseAddress_t* addr);

/**
 * Cast into IAddress
 */
struct C_IAddress_t* C_BaseAddress_getIAddress(struct C_BaseAddress_t* obj);

/**
 * Dynamic cast from Request
 */
struct C_BaseAddress_t* C_BaseAddress_fromIAddress(struct C_IAddress_t* obj);

#endif // CASTOR_BASEADDRESS_H
