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
#include <curl/curl.h>
#include <curl/easy.h>
#include <curl/types.h>

extern "C" {
  #ifndef LSBATCH_H
    #include "lsf/lsbatch.h"
    #include "lsf/lssched.h"
  #endif
}

namespace castor {

  namespace job {

    /**
     * SharedResourceHelper class
     */
    class SharedResourceHelper {

    public:

      /**
       * Default constructor
       */
      SharedResourceHelper();

      /**
       * Initialize the cURL interface and create a new cURL easy handle
       * @param retryAttempts defines how many times the helper should retry
       * to download a file upon failure.
       * @param retryInterval defines how long the helper should sleep in
       * seconds between failed attempts.
       * @note Retries are only supported for HTTP based URI's
       * @exception Exception in case of error
       */
      SharedResourceHelper
      (unsigned int retryAttempts, unsigned int retryInterval)
	throw(castor::exception::Exception);

      /**
       * Default destructor
       */
      virtual ~SharedResourceHelper() throw();

      /**
       * Download the data at the m_url location and return the result back
       * to the callee. In case of failure the method will retry up to
       * retryAttempt times.
       * @param debug flag to indicate whether cURL debugging should be
       * enabled
       * @exception Exception in case of error
       */
      virtual std::string download(bool debug)
	throw(castor::exception::Exception);

      /**
       * Returns the error text/description of the last cURL error.
       * @return the value of m_errorBuffer
       */
      std::string errorBuffer() const {
	return m_errorBuffer;
      }

      /**
       * Returns the max number of retries the helper should perform upon
       * encountering an error.
       * @return the value of m_retryAttempts
       */
      unsigned int retryAttempts() const {
	return m_retryAttempts;
      }

      /**
       * Returns the retry interval in seconds between retry attempts.
       * @return the value of m_retryInterval
       */
      unsigned int retryInterval() const {
	return m_retryInterval;
      }

      /**
       * Set the value of m_url. For http:// downloads the LSB_MASTERNAME
       * variable will be replaced by the name of the current LSF master
       * @param url the new value of m_url
       */
      virtual void setUrl(std::string url)
	throw (castor::exception::Exception);

    private:

      /// The cURL easy handle
      CURL *m_curl;

      /// The error code of the last call to libcurl
      CURLcode m_errorCode;

      /// The error message associated with the error code
      char m_errorBuffer[CURL_ERROR_SIZE];

      /// How many times should we attempt to download the m_url
      unsigned int m_retryAttempts;

      /// The interval to wait between download attempts
      unsigned int m_retryInterval;

      /// The location of the file to download into memory. file:// and
      /// http:// are supported protocols
      std::string m_url;

      /// The content of data downloaded from m_url
      std::string m_content;

    };

  } // End of namespace job

} // End of namespace castor

#endif // SHARED_RESOURCE_HELPER_HPP
