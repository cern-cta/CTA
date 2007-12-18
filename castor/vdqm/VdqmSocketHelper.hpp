/******************************************************************************
 *                      VdqmSocketHelper.hpp
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
 * @author castor dev team
 *****************************************************************************/
 
#ifndef CASTOR_VDQM_VDQMSOCKETHELPER_HPP
#define CASTOR_VDQM_VDQMSOCKETHELPER_HPP 1


namespace castor {

  namespace vdqm {

    /**
     * Provides VDQM specific helper functions to work with sockets used by the
     * VDQM.
     */
    class VdqmSocketHelper {

    public:

      /**
       * Reads the first four Bytes of the header. This function was added to 
       * support also the older VDQM protocol. The magic number defines, which
       * protocol is used.
       * 
       * @return The magic number
       * @exception In case of error
       */
      static unsigned int readMagicNumber(const int socket)
      throw (castor::exception::Exception);

      /**
       * This function is used internally to write the header buffer 
       * on the socket.
       * 
       * @param hdrbuf The header buffer, which contains the data for the client
       * @exception In case of error
       */
      static void vdqmNetwrite(const int socket, void* hdrbuf) 
      throw (castor::exception::Exception);
          
      /**
       * This function is used internally to read the header 
       * from the socket to the overgiven buffer.
       * 
       * @param hdrbuf The header buffer, where the data will be written to
       * @exception In case of error
       */
      static void vdqmNetread(const int socket, void* hdrbuf) 
      throw (castor::exception::Exception);       

    }; // class VdqmSocketHelper

  } // namespace vdqm

} // namespace castor

#endif // CASTOR_VDQM_VDQMSOCKETHELPER_HPP
