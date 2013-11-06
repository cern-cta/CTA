/******************************************************************************
 *                      DLFInit.cpp
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
 *
 * Initialization of the DLF messages for the stager common part
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/stager/DLFInit.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/exception/Exception.hpp"

//------------------------------------------------------------------------------
// DLFInitInstance
//------------------------------------------------------------------------------
namespace castor {

  namespace stager {

    castor::stager::DLFInit DLFInitInstance;

  }  // namespace stager

} // namespace castor

//------------------------------------------------------------------------------
// DLFInit
//------------------------------------------------------------------------------
castor::stager::DLFInit::DLFInit() {
  castor::dlf::Message messages[] =
    {{  0, "Exception caught in RemoteGCSvc::nsFilesDeleted" },
     {  1, "Exception caught in RemoteGCSvc::stgFilesDeleted" },
     {  2, "CC: Adding message to client connection" },
     {  3, "CC: Closing client connection" },
     {  4, "CC: Deleting message" },
     {  5, "CC: No more messages in queue" },
     {  6, "CC: Write failure" },
     {  7, "CC: Send successful" },
     {  8, "RR: Could not establish communication pipe" },
     {  9, "RR: Could not create thread" },
     { 10, "RR: Request Replier thread started" },
     { 11, "RR: Request Replier statistics" },
     { 12, "RR: Finished processing - terminating" },
     { 13, "RR: Waiting to terminate connection queue" },
     { 14, "RR: Polling file descriptors" },
     { 15, "RR: Poll returned - timed out" },
     { 16, "RR: Poll returned - interrupted, continuing" },
     { 17, "RR: Error in poll" },
     { 18, "RR: Found existing connection" },
     { 19, "RR: Added FD to client connection" },
     { 20, "RR: Exception while Creating new ClientConnection" },
     { 21, "RR: Deleting connection" },
     { 22, "RR: GC Check active time" },
     { 23, "RR: Cleanup client connection - in DONE_FAILURE - to remove" },
     { 24, "RR: Cleanup client connection - terminate is true and no more messages - to remove" },
     { 25, "RR: Cleanup client connection - timeout" },
     { 26, "RR: Processing poll Array" },
     { 27, "RR: Could not look up Connection for FD" },
     { 28, "RR: Connection dropped" },
     { 29, "RR: POLLERR received" },
     { 30, "RR: Got EndConnectionException closing connection" },
     { 31, "RR: No more messages to send, waiting" },
     { 32, "RR: Exception caught in sending data" },
     { 33, "RR: Resending data POLLOUT" },
     { 34, "RR: CCCR Resending data POLLIN" },
     { 35, "RR: Unknown status" },
     { 36, "RR: Locking m_clientQueue" },
     { 37, "RR: Info before processing" },
     { 38, "RR: No client in queue, removing lock" },
     { 39, "RR: Error reading" },
     { 40, "RR: Info after processing" },
     { 41, "RR: Unlocking m_clientQueue" },
     { 42, "RR: Requesting RequestReplier termination" },
     { 43, "RR: Adding Response" },
     { 44, "RR: Adding Response to m_ClientQueue" },
     { 45, "RR: Adding EndResponse to m_ClientQueue" },
     { 46, "RR: Error writing to communication pipe with RRThread" },

     { -1, "" }};
  try {
    castor::dlf::dlf_addMessages(DLF_BASE_STAGERLIB, messages);
  } catch (castor::exception::Exception& ex) {
    // We failed to insert our messages into DLF
    // So we cannot really log to DLF.
    // On the other hand, we cannot be sure that that standard out is usable.
    // So we have to ignore the error. Note really nice
  }
}
