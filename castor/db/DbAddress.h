/******************************************************************************
 *                      DbAddress.h
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
 * @(#)$RCSfile: DbAddress.h,v $ $Revision: 1.4 $ $Release$ $Date: 2004/10/05 13:37:28 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef DB_DBADDRESS_H 
#define DB_DBADDRESS_H 1

/**
 * Forward declaration of IAddress for the C world
 */
typedef struct Cdb_DbAddress_t Cdb_DbAddress_t;

/*
 * constructor
 */
int Cdb_DbAddress_create(const u_signed64 id,
                         const char* cnvSvcName,
                         const unsigned int cnvSvcType,
                         Cdb_DbAddress_t** addr);

/*
 * destructor
 */
int Cdb_DbAddress_delete(Cdb_DbAddress_t* addr);

/**
 * Get the id of this address
 */
u_signed64 Cdb_DbAddress_id(struct Cdb_DbAddress_t* obj);

/**
 * Set the id of this address
 */
void Cdb_DbAddress_setId(struct Cdb_DbAddress_t* obj, u_signed64 id);

/**
 * Cast into BaseAddress
 */
struct C_BaseAddress_t* Cdb_DbAddress_getBaseAddress(struct Cdb_DbAddress_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cdb_DbAddress_t* Cdb_DbAddress_fromBaseAddress(struct C_BaseAddress_t* obj);

#endif // DB_DBADDRESS_H
