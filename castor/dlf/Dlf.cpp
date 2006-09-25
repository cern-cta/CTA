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
 * @(#)Dlf.cpp,v 1.1 $Release$ 2005/04/05 11:51:33 sponcec3
 *
 * C++ interface to DLF
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/dlf/Dlf.hpp"

// -----------------------------------------------------------------------
// dlf_init
// -----------------------------------------------------------------------
void castor::dlf::dlf_init
(char* facilityName, Message messages[]) {
  // Initialise the DLF interface, ignore any errors that may be generated
  char dlfErrBuf[CA_MAXLINELEN+1];  

  ::dlf_init(facilityName, dlfErrBuf);

  // Register the facility's messages with the interface. We do this even
  // if the interface fails to initialisation as it is used for local 
  // logging
  dlf_addMessages(0, messages);
}

// -----------------------------------------------------------------------
// dlf_addMessages
// -----------------------------------------------------------------------
void castor::dlf::dlf_addMessages (int offset, Message messages[]) {
  int i = 0;
  while (messages[i].number >= 0) {
    ::dlf_regtext(offset + messages[i].number,
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
  // dlf_write_param_t cparams[numparams]; // Doesn't work on windows compiler!!!
  dlf_write_param_t* cparams = new dlf_write_param_t[numparams];
  // Translate paramters from C++ to C
  for (int i = 0; i < numparams; i++) {
    cparams[i] = params[i].cParam();
  }
  ::dlf_writep(uuid, severity, message_no,
               ns_invariant, numparams, cparams);
  delete[] cparams;
}
