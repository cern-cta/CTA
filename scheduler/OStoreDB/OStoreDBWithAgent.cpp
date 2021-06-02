/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "scheduler/OStoreDB/OStoreDBWithAgent.hpp"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
cta::OStoreDBWithAgent::OStoreDBWithAgent(cta::objectstore::Backend & be, cta::objectstore::AgentReference & ar, 
  catalogue::Catalogue & catalogue, log::Logger & logger): cta::OStoreDB(be, catalogue, logger) {
  cta::OStoreDB::setAgentReference(&ar);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
cta::OStoreDBWithAgent::~OStoreDBWithAgent() throw () {
  cta::OStoreDB::setAgentReference(nullptr);
}