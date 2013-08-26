// ----------------------------------------------------------------------
// File: File/Structures.cc
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "Structures.hh"
#include "../Exception/Exception.hh"

void Tape::AULFile::VOL1::fill(std::string vsn) {
  setString(label, "VOL1");
  setString(VSN, vsn);
  setString(lblStandart,"3");
  setString(ownerID, "CASTOR"); /* TODO: check do we need CASTOR's STAGERSUPERUSER */
}

void Tape::AULFile::VOL1::verify() {
  if (cmpString(label,"VOL1")) 
    throw Tape::Exceptions::Errnum(std::string("Failed check for the VOL1: ")+
            Tape::AULFile::toString(label));
  if (!cmpString(VSN,""))
    throw Tape::Exceptions::Errnum(std::string("Failed check for the VSN: ") + 
            Tape::AULFile::toString(VSN));
  if (cmpString(lblStandart,"3"))
    throw Tape::Exceptions::Errnum(
            std::string("Failed check for the label standart: ") +
            Tape::AULFile::toString(lblStandart));
  if (cmpString(ownerID,"CASTOR"))
    throw Tape::Exceptions::Errnum(
            std::string("Failed check for the ownerID: ") +
            Tape::AULFile::toString(ownerID));
  
  /* now we check all other fields which must be spaces */
  if (cmpString(accessibility,""))
    throw Tape::Exceptions::Errnum("accessibility is not empty");         
  if (cmpString(reserved1,""))
    throw Tape::Exceptions::Errnum("reserved1 is not empty");
  if (cmpString(implID,""))
    throw Tape::Exceptions::Errnum("implID is not empty");
  if (cmpString(reserved2,""))
    throw Tape::Exceptions::Errnum("reserved2 is not empty");  
}
