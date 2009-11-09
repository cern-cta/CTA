/******************************************************************************
 *                      Param.hpp
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
 * @(#)$RCSfile: Param.hpp,v $ $Revision: 1.12 $ $Release$ $Date: 2009/08/18 09:42:51 $ $Author: waldron $
 *
 * A parameter for the DLF (Distributed Logging System)
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef DLF_PARAM_HPP
#define DLF_PARAM_HPP 1

// Include Files
#include "dlf_api.h"
#include "castor/IObject.hpp"
#include "castor/stager/TapeVid.hpp"
#include "castor/dlf/IPAddress.hpp"
#include "castor/dlf/TimeStamp.hpp"
#include <string.h>
#include <stdlib.h>

namespace castor {

  namespace dlf {

    /**
     * A parameter for the DLF.
     */
    class Param {

    public:

      /**
       * Constructor for strings
       */
      Param(const char* name, std::string value) :
        m_deallocate(true) {
        m_cParam.name = (char*) name;
        m_cParam.type = DLF_MSG_PARAM_STR;
        if (!strcmp(name, "TPVID")) {
          m_cParam.type = DLF_MSG_PARAM_TPVID;
        }
        m_cParam.value.par_string = strdup(value.c_str());
      };

      /**
       * Constructor for C strings
       */
      Param(const char* name, const char* value) :
        m_deallocate(true) {
        m_cParam.name = (char*) name;
        m_cParam.type = DLF_MSG_PARAM_STR;
        if (!strcmp(name, "TPVID")) {
          m_cParam.type = DLF_MSG_PARAM_TPVID;
        }
        if (0 != value) {
          m_cParam.value.par_string = strdup(value);
        } else {
          m_cParam.value.par_string = 0;
        }
      };

      /**
       * Constructor for uuids
       */
      Param(const char* name, Cuuid_t value) :
        m_deallocate(false) {
        m_cParam.name = (char*) name;
        m_cParam.type = DLF_MSG_PARAM_UUID;
        m_cParam.value.par_uuid = value;
      };

      /**
       * Constructor for SubRequest uuids
       */
      Param(Cuuid_t value) :
        m_deallocate(false) {
        m_cParam.name = NULL;
        m_cParam.type = DLF_MSG_PARAM_UUID;
        m_cParam.value.par_uuid = value;
      };

      /**
       * Constructor for int
       */
      Param(const char* name, const long int value) :
        m_deallocate(false) {
        m_cParam.name = (char*) name;
        m_cParam.type = DLF_MSG_PARAM_INT;
        m_cParam.value.par_int = value;
      };

      /**
       * Constructor for int
       */
      Param(const char* name, const long unsigned int value) :
        m_deallocate(false) {
        m_cParam.name = (char*) name;
        m_cParam.type = DLF_MSG_PARAM_INT;
        m_cParam.value.par_int = value;
      };

      /**
       * Constructor for int
       */
      Param(const char* name, const int value) :
        m_deallocate(false) {
        m_cParam.name = (char*) name;
        m_cParam.type = DLF_MSG_PARAM_INT;
        m_cParam.value.par_int = value;
      };

      /**
       * Constructor for int
       */
      Param(const char* name, const unsigned int value) :
        m_deallocate(false) {
        m_cParam.name = (char*) name;
        m_cParam.type = DLF_MSG_PARAM_INT;
        m_cParam.value.par_int = value;
      };

      /**
       * Constructor for u_signed64
       */
      Param(const char* name, u_signed64 value) :
        m_deallocate(false) {
        m_cParam.name = (char*) name;
        m_cParam.type = DLF_MSG_PARAM_INT64;
        m_cParam.value.par_u64 = value;
      };

      /**
       * Constructor for floats
       */
      Param(const char* name, float value) :
        m_deallocate(false) {
        m_cParam.name = (char*) name;
        m_cParam.type = DLF_MSG_PARAM_DOUBLE;
        m_cParam.value.par_double = value;
      };

      /**
       * Constructor for doubles
       */
      Param(const char* name, double value) :
        m_deallocate(false) {
        m_cParam.name = (char*) name;
        m_cParam.type = DLF_MSG_PARAM_DOUBLE;
        m_cParam.value.par_double = value;
      };

      /**
       * Constructor for Tape VIDS
       */
      Param(const char* name, castor::stager::TapeVid value) :
        m_deallocate(false) {
        m_cParam.name = (char*) name;
        m_cParam.type = DLF_MSG_PARAM_TPVID;
        if (0 != value.vid()) {
          m_cParam.value.par_string = strdup(value.vid());
        } else {
          m_cParam.value.par_string = 0;
        }
      };

      /**
       * Constructor for IPAddress
       */
      Param(const char* name, castor::dlf::IPAddress value);

      /**
       * Constructor for TimeStamp
       */
      Param(const char* name, castor::dlf::TimeStamp value);

      /**
       * Constructor for objects
       */
      Param(const char* name, castor::IObject* value);

      /**
       *
       */
      ~Param() {
        if (m_deallocate && 0 != m_cParam.value.par_string) {
          free(m_cParam.value.par_string);
        }
      };

    public:

      /**
       * Gets the corresponding C parameter for the
       * DLF C interface
       */
      dlf_write_param_t cParam() {
        return m_cParam;
      }

    private:

      /// the parameter, in a C structure
      dlf_write_param_t m_cParam;

      /// Whether the param value should be deallocated
      bool m_deallocate;

    };

  } // end of namespace dlf

} // end of namespace castor

#endif // DLF_PARAM_HPP
