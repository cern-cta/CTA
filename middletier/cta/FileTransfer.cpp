/*
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

#include "cta/FileTransfer.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileTransfer::FileTransfer():
  m_copyNb(0) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::FileTransfer::~FileTransfer() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileTransfer::FileTransfer(
  const std::string &id,
  const std::string &userRequestId,
  const uint32_t copyNb,
  const std::string &remoteFile):
  m_id(id),
  m_userRequestId(userRequestId),
  m_copyNb(copyNb),
  m_remoteFile(remoteFile) {
}

//------------------------------------------------------------------------------
// getId
//------------------------------------------------------------------------------
const std::string &cta::FileTransfer::getId() const throw() {
  return m_id;
}

//------------------------------------------------------------------------------
// getUserRequestId
//------------------------------------------------------------------------------
const std::string &cta::FileTransfer::getUserRequestId() const throw() {
  return m_userRequestId;
}

//------------------------------------------------------------------------------
// getCopyNb
//------------------------------------------------------------------------------
uint32_t cta::FileTransfer::getCopyNb() const throw() {
  return m_copyNb;
}

//------------------------------------------------------------------------------
// getRemoteFile 
//------------------------------------------------------------------------------
const std::string &cta::FileTransfer::getRemoteFile() const throw() {
  return m_remoteFile;
}
