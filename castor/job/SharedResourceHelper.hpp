/******************************************************************************
 *                      SharedResourceHelper.hpp
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
 * @(#)$RCSfile: SharedResourceHelper.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2008/07/29 06:22:21 $ $Author: waldron $
 *
 * Helper used to download the resource file associated with a job
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef SHARED_RESOURCE_HELPER_HPP
#define SHARED_RESOURCE_HELPER_HPP 1

// Include files
#include "castor/exception/Exception.hpp"
#include <string>
#include <stdarg.h>

namespace castor {

  namespace job {

    /**
     * SharedResourceHelper class
     */
    class SharedResourceHelper {

    public:

      /**
       * Default Constructor
       */
      SharedResourceHelper() throw();

      /**
       * Default destructor
       */
      virtual ~SharedResourceHelper() throw() {};

      /**
       * Download the data at the m_url location and return the result back
       * to the callee.
       * @exception Exception in case of error
       */
      virtual std::string download() throw(castor::exception::Exception);

      /**
       * Set the value of m_url
       * @param url the new value of m_url
       */
      virtual void setUrl(std::string url) throw() { m_url = url;}

    private:

      /// The location of the file to download into memory.
      std::string m_url;

      /// The content of data downloaded from m_url
      std::string m_content;

    };

  } // End of namespace job

} // End of namespace castor

#endif // SHARED_RESOURCE_HELPER_HPP
