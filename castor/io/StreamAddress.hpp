/******************************************************************************
 *                      StreamAddress.hpp
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
 * @(#)$RCSfile: StreamAddress.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/11/29 15:40:35 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef IO_STREAMADDRESS_HPP 
#define IO_STREAMADDRESS_HPP 1

// Include Files
#include "castor/io/biniostream.h"
#include "castor/BaseAddress.hpp"
#include "castor/Constants.hpp"

namespace castor {

  namespace io {

    /**
     * An address containing a reference to a binary stream
     */
    class StreamAddress : public BaseAddress {

    public:

      /**
       * constructor
       * @param stream the stream where to put the data
       * @param cnvSvcName the conversion service able to deal with this address
       * In this later case, the type will be deduced from the id.
       */
      StreamAddress(biniostream& stream,
                    const std::string cnvSvcName,
                    const unsigned int cnvSvcType);

      /*
       * destructor
       */
      virtual ~StreamAddress() throw() {}

      /**
       * gets the id of this address
       */
      virtual biniostream& stream() const { return m_stream; }
      
    private:

      /**
       * the id of this address
       */
      biniostream& m_stream;

    };

  } // end of namespace io

} // end of namespace castor

#endif // IO_STREAMADDRESS_HPP
