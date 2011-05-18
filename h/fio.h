/******************************************************************************
 *                      fio.h
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
 * @(#)$RCSfile: fio.h,v $ $Revision: 1.1 $ $Release$ $Date: 2007/12/07 13:26:07 $ $Author: sponcec3 $
 *
 * header file of fio.c
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef H_FIO_H 
#define H_FIO_H 1

int usf_open(int *unit, char *file, int *append, int *trunc);
int udf_open(int *unit, char *file, int *lrecl , int *trunc);
int usf_write(int *unit, char *buf, int *nwrit);
int udf_write(int *unit, char *buf, int *nrec, int *nwrit);
int usf_read(int *unit, char *buf, int *nwant);
int udf_read(int *unit, char *buf, int *nrec, int *nwant);
int uf_close(int *unit);
void uf_cread(int *unit, char *buf, int *nrec, int *nwant, int *ngot, int *irc);


#endif /* H_FIO_H */
