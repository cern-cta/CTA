/******************************************************************************
 *                      StageInMain.cpp
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
 * @(#)$RCSfile: StageInMain.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/06/02 08:00:37 $ $Author: sponcec3 $
 *
 * This file is the main part of the stagein program.
 * This part was separated in order to be able to reuse
 * the implemenatation in StageIn.cpp inside Mttest
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/client/StageIn.hpp"

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(int argc, char** argv) {
  castor::client::StageIn req;
  req.run(argc, argv);
  return 0;
}
