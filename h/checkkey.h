/******************************************************************************
 *                      checkkey.h
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
 * @(#)$RCSfile: checkkey.h,v $ $Revision: 1.1 $ $Release$ $Date: 2008/07/31 13:10:27 $ $Author: sponcec3 $
 *
 * function declarations for checkkey
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef RFIO_CHECKKEY_H 
#define RFIO_CHECKKEY_H 1

int connecttpread(char * host, u_short aport);

int checkkey(int sock, u_short  key);

#endif /* RFIO_CHECKKEY_H */
