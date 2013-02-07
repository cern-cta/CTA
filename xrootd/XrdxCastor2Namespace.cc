/*******************************************************************************
 *                      XrdxCastor2Namespace.cc
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2012  CERN
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
 * @author Elvin Sindrilaru & Andreas Peters - CERN
 *
 ******************************************************************************/

/*-----------------------------------------------------------------------------*/
#include "XrdxCastor2Fs.hh"
/*-----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*-----------------------------------------------------------------------------*/

bool
XrdxCastor2Fs::Map( XrdOucString inmap, XrdOucString& outmap )
{
  XrdOucString prefix;
  XrdOucString* nsmap;
  int pos = inmap.find( '/', 1 );

  if ( pos == STR_NPOS )
    //TODO: remove the goto routine :D
    goto checkroot;

  prefix.assign( inmap, 0, pos );

  // 1st check the namespace with deepness 1
  if ( !( nsmap = nstable->Find( prefix.c_str() ) ) ) {
    // 2nd check the namespace with deepness 0 e.g. look for a root mapping

checkroot:
    if ( !( nsmap = nstable->Find( "/" ) ) ) {
      return false;
    }

    pos = 0;
    prefix = "/";
  }

  outmap.assign( nsmap->c_str(), 0, nsmap->length() - 1 );
  XrdOucString hmap = inmap;
  hmap.erase( prefix, 0, prefix.length() );
  outmap += hmap;
  return true;
}
