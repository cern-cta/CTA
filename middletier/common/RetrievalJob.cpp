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

#include "middletier/common/RetrievalJob.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrievalJob::RetrievalJob():
  m_state(RetrievalJobState::NONE),
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrievalJob::RetrievalJob(
  const RetrievalJobState::Enum state,
  const std::string &srcPath,
  const std::string &dstUrl,
  const UserIdentity &creator,
  const time_t creationTime):
  m_srcPath(srcPath),
  m_dstUrl(dstUrl),
  m_creationTime(creationTime),
  m_creator(creator) {
}

//------------------------------------------------------------------------------
// setState
//------------------------------------------------------------------------------
void cta::RetrievalJob::setState(const RetrievalJobState::Enum state) {
  m_state = state;
}

//------------------------------------------------------------------------------
// getState
//------------------------------------------------------------------------------
cta::RetrievalJobState::Enum cta::RetrievalJob::getState() const throw() {
  return m_state;
}

//------------------------------------------------------------------------------
// getStateStr
//------------------------------------------------------------------------------
const char *cta::RetrievalJob::getStateStr() const throw() {
  return RetrievalJobState::toStr(m_state);
}

//------------------------------------------------------------------------------
// getSrcPath
//------------------------------------------------------------------------------
const std::string &cta::RetrievalJob::getSrcPath() const throw() {
  return m_srcPath;
}

//------------------------------------------------------------------------------
// getDstUrl
//------------------------------------------------------------------------------
const std::string &cta::RetrievalJob::getDstUrl() const throw() {
  return m_dstUrl;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::RetrievalJob::getCreationTime() const throw() {
  return m_creationTime;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::RetrievalJob::getCreator() const throw() {
  return m_creator;
}
