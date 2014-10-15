/******************************************************************************
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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/
#pragma once

namespace castor {
namespace tape   {
namespace tapegateway  {
    
  /**
   * Abstract class specifiying the interface to be followed by functors that
   * take no parameters and return a boolean.
   */
  class BoolFunctor {
  public:

    /**
     * operator() which takes no parameters and returns a boolean.
     */
    virtual bool operator()() const = 0;

    /**
     * Virtual destructor.
     */
    virtual ~BoolFunctor() throw();

  }; // class BoolFunctor

} // namespace tapegateway
} // namespace tape
} // namespace castor      

