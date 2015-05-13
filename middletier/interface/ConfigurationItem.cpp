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

#include "middletier/interface/ConfigurationItem.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ConfigurationItem::ConfigurationItem() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ConfigurationItem::~ConfigurationItem() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ConfigurationItem::ConfigurationItem(const UserIdentity &creator,
  const std::string &comment, const time_t creationTime):
  m_creator(creator),
  m_comment(comment),
  m_creationTime(creationTime) {
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::ConfigurationItem::getCreator() const throw() {
  return m_creator;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
const std::string &cta::ConfigurationItem::getComment() const throw() {
  return m_comment;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::ConfigurationItem::getCreationTime() const throw() {
  return m_creationTime;
}
