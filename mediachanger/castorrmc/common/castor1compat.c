/******************************************************************************
 *                      castor1compat.c
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
 * This file only exists so that applications that were using concurrently
 * CASTOR1 and CASTOR2, using use_castor2_api to decide which can
 * still run.
 * It should be removed as soon as possible
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include <stage_api.h>

int use_castor2_api() {
  return 1;
}

int DLL_DECL stage_setoutbuf(buffer, buflen)
     char *buffer;
     int buflen;
{ return -1; }

int DLL_DECL stage_seterrbuf(buffer, buflen)
     char *buffer;
     int buflen;
{ return -1; }

int DLL_DECL stage_iowc(req_type,t_or_d,flags,openflags,openmode,hostname,
                        pooluser,nstcp_input,stcp_input,nstcp_output,
                        stcp_output,nstpp_input,stpp_input)
     int req_type;
     char t_or_d;
     u_signed64 flags;
     int openflags;
     mode_t openmode;
     char *hostname;
     char *pooluser;
     int nstcp_input;
     struct stgcat_entry *stcp_input;
     int *nstcp_output;
     struct stgcat_entry **stcp_output;
     int nstpp_input;
     struct stgpath_entry *stpp_input;
{ return -1; }

int DLL_DECL stage_updc_filchg(stghost,hsmstruct)
     char *stghost;
     stage_hsm_t *hsmstruct;
{ return -1; }
