/**
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "cta/RetrievalFileTransfer.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrievalFileTransfer::RetrievalFileTransfer() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrievalFileTransfer::~RetrievalFileTransfer() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrievalFileTransfer::RetrievalFileTransfer(
  const TapeFileLocation &tapeFile,
  const std::string &id, 
  const std::string &userRequestId,
  const uint32_t copyNb,
  const std::string &remoteFile):
  FileTransfer(id, userRequestId, copyNb, remoteFile),
  m_tapeFile(tapeFile) {
}

//------------------------------------------------------------------------------
// getTapeFile
//------------------------------------------------------------------------------
const cta::TapeFileLocation &cta::RetrievalFileTransfer::getTapeFile() const
  throw() {
  return m_tapeFile;
}
