/******************************************************************************
 *                      dlf_write.cpp
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
 * @(#)$RCSfile: Dlf.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/04/05 11:51:33 $ $Author: sponcec3 $
 *
 * C++ interface to DLF
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/dlf/Dlf.hpp"

// -----------------------------------------------------------------------
// dlf_writep
// -----------------------------------------------------------------------
void castor::dlf::dlf_init
(char* facilityName, Message messages[]) {
  // First set the DLF error buffer. Otherwise it may
  // write to standard ouput
  char *dlfErrBuf;
  dlfErrBuf = (char *)malloc(CA_MAXLINELEN+1);
  (void)dlf_seterrbuf(dlfErrBuf,CA_MAXLINELEN);
  // initialize DLF, ignore output so that we don't give up
  // if the DLF server is not available
  ::dlf_init(facilityName);
  // Enter messages in the facility, even if initialization
  // was not successful since it could be used for local
  // logging
  int i = 0;
  extern dlf_facility_info_t g_dlf_fac_info;
  while (messages[i].number >= 0) {
    ::dlf_add_to_text_list(messages[i].number,
                           messages[i].text.c_str(),
                           &g_dlf_fac_info.text_list);
    ::dlf_entertext(facilityName,
                    messages[i].number,
                    messages[i].text.c_str());
    i++;
  }
}

// -----------------------------------------------------------------------
// dlf_writep
// -----------------------------------------------------------------------
void castor::dlf::dlf_writep
(Cuuid_t uuid,
 int severity,
 int message_no,
 int numparams,
 castor::dlf::Param params[],
 struct Cns_fileid *ns_invariant) {
  // Place holder for the C version of the parameters
  dlf_write_param_t cparams[numparams];
  // Translate paramters from C++ to C
  for (int i = 0; i < numparams; i++) {
    cparams[i] = params[i].cParam();
  }
  ::dlf_writep(uuid, severity, message_no,
               ns_invariant, numparams, cparams);
}
