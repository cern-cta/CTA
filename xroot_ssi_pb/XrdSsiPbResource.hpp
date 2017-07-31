/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Interface to XRootD SSI Resource class
 * @copyright      Copyright 2017 CERN
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

#ifndef __XRD_SSI_PB_RESOURCE_H
#define __XRD_SSI_PB_RESOURCE_H

#include <XrdSsi/XrdSsiResource.hh>
#include <XrdSsi/XrdSsiEntity.hh>
#include "XrdSsiPbException.hpp"



namespace XrdSsiPb {

/*!
 * Get the client username from the Resource.
 *
 * The following fields are available in the client:
 *
 * prot            char*          Protocol used
 * name            const char*    Entity's name
 * host            const char*    Entity's host name or address
 * vorg            const char*    Entity's virtual organization
 * role            const char*    Entity's role
 * grps            const char*    Entity's group names
 * endorsements    const char*    Protocol-specific endorsements
 * creds           const char*    Raw client credentials or certificate
 * credslen        int            Length of the 'creds' field
 * rsvd            int            Reserved
 * tident          const char*    Trace identifier (always preset)
 */

static const XrdSsiEntity &getClient(const XrdSsiResource &resource)
{
#ifdef XRDSSI_DEBUG
   std::cerr << "[DEBUG] Resource name: " << resource.rName << std::endl
             << "[DEBUG] Resource user: " << resource.rUser << std::endl
             << "[DEBUG] Resource info: " << resource.rInfo << std::endl
             << "[DEBUG] Hosts to avoid: " << resource.hAvoid << std::endl
             << "[DEBUG] Affinity: ";

   switch(resource.affinity)
   {
      case XrdSsiResource::None:     std::cerr << "None" << std::endl; break;
      case XrdSsiResource::Default:  std::cerr << "Default" << std::endl; break;
      case XrdSsiResource::Weak:     std::cerr << "Weak" << std::endl; break;
      case XrdSsiResource::Strong:   std::cerr << "Strong" << std::endl; break;
      case XrdSsiResource::Strict:   std::cerr << "Strict" << std::endl; break;
   }

   std::cerr << "[DEBUG] Resource options: "
             << (resource.rOpts & XrdSsiResource::Reusable ? "Resuable " : "")
             << (resource.rOpts & XrdSsiResource::Discard  ? "Discard"   : "")
             << std::endl;
#endif

   if(resource.client == nullptr)
   {
      throw XrdSsiException("getClient(): resource.client is a null pointer");
   }

   return *(resource.client);
}

} // namespace XrdSsiPb

#endif
