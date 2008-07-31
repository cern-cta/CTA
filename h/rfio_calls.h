/******************************************************************************
 *                      rfio_calls.h
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
 * @(#)$RCSfile: rfio_calls.h,v $ $Revision: 1.1 $ $Release$ $Date: 2008/07/31 13:17:11 $ $Author: sponcec3 $
 *
 * header file for functions implemented in rfio_calls.c
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef RFIO_RFIO_CALLS_H 
#define RFIO_RFIO_CALLS_H 1

int  srchk(
#if defined(_WIN32)
           SOCKET  s
#else
           int     s
#endif
           );

int   srerrmsg(
#if defined(_WIN32)
               SOCKET  s
#else
               int     s
#endif
               );

int srstatfs(int s);

int   srfread(
#if defined(_WIN32)
               SOCKET  s,
#else
               int     s,
#endif
               FILE    *fp);

int   srfwrite(
#if defined(_WIN32)
               SOCKET  s,
#else
               int     s,
#endif
               FILE    *fp);

int   srpclose(
#if defined(_WIN32)
               SOCKET  s,
#else
               int     s,
#endif
               FILE    *fs);

#endif // RFIO_RFIO_CALLS_H
