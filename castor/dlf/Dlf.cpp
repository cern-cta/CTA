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
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include <errno.h>


//-----------------------------------------------------------------------------
// dlf_getPendingMessages
//-----------------------------------------------------------------------------
std::vector<std::pair<int, castor::dlf::Message*> >&
castor::dlf::dlf_getPendingMessages () throw() {
  static std::vector<std::pair<int, castor::dlf::Message*> > pendingMessages;
  return pendingMessages;
}

//-----------------------------------------------------------------------------
// dlf_init
//-----------------------------------------------------------------------------
void castor::dlf::dlf_init
(const char* facilityName, castor::dlf::Message messages[])
  throw (castor::exception::Exception) {
  // Initialise the DLF interface
  if (::dlf_init(facilityName) != 0) {
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to initialize DLF: " << sstrerror(errno);
    throw ex;
  }
  // Register the facility's messages with the interface. We do this even
  // if the interface fails to initialisation as it is used for local
  // logging
  dlf_addMessages(0, messages);
  // Also register the pending messages
  for (std::vector<std::pair<int, Message*> >::const_iterator it =
         dlf_getPendingMessages().begin();
       it != dlf_getPendingMessages().end();
       it++) {
    dlf_addMessages(it->first, it->second);
    delete[](it->second);
  }
  dlf_getPendingMessages().clear();
}

//-----------------------------------------------------------------------------
// dlf_addMessages
//-----------------------------------------------------------------------------
void castor::dlf::dlf_addMessages (int offset, Message messages[])
  throw () {
  if (::dlf_isinitialized()) {
    int i = 0;
    while (messages[i].number >= 0) {
      ::dlf_regtext(offset + messages[i].number,
                    messages[i].text.c_str());
      i++;
    }
  } else {
    // replicate message array
    int len = 0;
    while (messages[len].number >= 0) {
      len++;
    }
    Message* lmessages = new Message[len+1];
    lmessages[len].number = -1;
    for (int i = 0; i < len; i++) {
      lmessages[i].number = messages[i].number;
      lmessages[i].text = messages[i].text;
    }
    // and store it for further usage when DLF will be initialized
    dlf_getPendingMessages().push_back(std::pair<int, Message*>(offset, lmessages));
  }
}

//-----------------------------------------------------------------------------
// dlf_writep
// wrapper of the dlf writep that compounds the Cns_fileid struct
//-----------------------------------------------------------------------------
void castor::dlf::dlf_writep
(Cuuid_t uuid,
 int severity,
 int message_no,
 u_signed64 fileId,
 std::string nsHost,
 int numparams,
 castor::dlf::Param params[])
  throw() {

  struct Cns_fileid ns_invariant;
  ns_invariant.fileid = fileId;
  strncpy(ns_invariant.server, nsHost.c_str(), sizeof(ns_invariant.server) - 1);

  castor::dlf::dlf_writep(uuid, severity, message_no, numparams, params, &ns_invariant);
}

//-----------------------------------------------------------------------------
// dlf_writep
//-----------------------------------------------------------------------------
void castor::dlf::dlf_writep
(Cuuid_t uuid,
 int severity,
 int message_no,
 int numparams,
 castor::dlf::Param params[],
 struct Cns_fileid *ns_invariant) throw() {
  // Place holder for the C version of the parameters
  // dlf_write_param_t cparams[numparams]; // Doesn't work on windows compiler!!!
  dlf_write_param_t* cparams = new dlf_write_param_t[numparams];
  // Translate parameters from C++ to C
  for (int i = 0; i < numparams; i++) {
    cparams[i] = params[i].cParam();
  }
  ::dlf_writep(uuid, severity, message_no,
               ns_invariant, numparams, cparams);
  delete[] cparams;
}

//-----------------------------------------------------------------------------
// dlf_writepc
//-----------------------------------------------------------------------------
void castor::dlf::dlf_writepc
(const char *file,
 const int line,
 const char *function,
 Cuuid_t uuid,
 int severity,
 int message_no,
 int numparams,
 castor::dlf::Param params[],
 struct Cns_fileid *ns_invariant) throw() {
  // Place holder for the C version of the parameters, allocate 3 more
  // parameters for the context parameters: file, line and function
  const int numcontextparams = 3;
  const int numcparams = numparams + numcontextparams;
  dlf_write_param_t* cparams = new dlf_write_param_t[numcparams];

  // Fill the context parameters: file, line and function
  cparams[0].name             = (char*)"File";
  cparams[0].type             = DLF_MSG_PARAM_STR;
  cparams[0].value.par_string = (char *)file;
  cparams[1].name             = (char*)"Line";
  cparams[1].type             = DLF_MSG_PARAM_INT;
  cparams[1].value.par_int    = line;
  cparams[2].name             = (char*)"Function";
  cparams[2].type             = DLF_MSG_PARAM_STR;
  cparams[2].value.par_string = (char*)function;

  // Translate parameters from C++ to C
  for (int i = 0; i < numparams; i++) {
    cparams[i+numcontextparams] = params[i].cParam();
  }
  ::dlf_writep(uuid, severity, message_no,
               ns_invariant, numcparams, cparams);
  delete[] cparams;
}
