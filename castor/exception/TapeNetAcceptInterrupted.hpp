/******************************************************************************
 *                      castor/exception/TapeNetAcceptInterrupted.hpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_EXCEPTION_TAPE_NEXTACCEPTINTERRUPTED_HPP 
#define CASTOR_EXCEPTION_TAPE_NEXTACCEPTINTERRUPTED_HPP 1

#include "castor/exception/Exception.hpp"

#include <sys/types.h>

namespace castor {

  namespace exception {

    /**
     * castor::tape::net::acceptConnection interrupted.
     */
    class TapeNetAcceptInterrupted : public castor::exception::Exception {
      
    public:
      
      /**
       * Constructor.
       *
       * @param remainingTime The number of remaining seconds when
       * castor::tape::net::acceptConnection was interrupted.
       */
      TapeNetAcceptInterrupted(const time_t remainingTime) throw();

      /**
       * Returns the number of remaining seconds when
       * castor::tape::net::acceptConnection was interrupted.
       */
      time_t remainingTime() throw();


    private:

      /**
       * The number of remaining seconds when
       * castor::tape::net::acceptConnection was interrupted.
       */
      const time_t m_remainingTime;

    };
      
  } // end of namespace exception

} // end of namespace castor

#endif // CASTOR_EXCEPTION_TAPE_NEXTACCEPTINTERRUPTED_HPP
