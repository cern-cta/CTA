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
 *
 * A parameter for the CASTOR log system
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef CASTOR_LOG_PARAM_HPP
#define CASTOR_LOG_PARAM_HPP 1

// Include Files
#include "castor/IObject.hpp"
#include "castor/stager/TapeVid.hpp"
#include "castor/log/IPAddress.hpp"
#include "castor/log/TimeStamp.hpp"
#include "h/Cuuid.h"

#include <string.h>
#include <stdlib.h>

/* Parameter types */
#define LOG_MSG_PARAM_DOUBLE  1     /* Double precision floating point value */
#define LOG_MSG_PARAM_INT64   2     /* 64 bit integers */
#define LOG_MSG_PARAM_STR     3     /* General purpose string */
#define LOG_MSG_PARAM_TPVID   4     /* Tape visual identifier string */
#define LOG_MSG_PARAM_UUID    5     /* Subrequest identifier */
#define LOG_MSG_PARAM_FLOAT   6     /* Single precision floating point value */
#define LOG_MSG_PARAM_INT     7     /* Integer parameter */
#define LOG_MSG_PARAM_UID     8
#define LOG_MSG_PARAM_GID     9
#define LOG_MSG_PARAM_STYPE   10
#define LOG_MSG_PARAM_SNAME   11
#define LOG_MSG_PARAM_RAW     12    /* raw (set of) parameters, in key=value format */

namespace castor {
namespace log {

/**
 * Provided for C++ users. Refer to: castor/log/Param.hpp
 */
struct castor_write_param_t {
  char *name;                /* Name of the parameter */
  int  type;                 /* Parameter type, one of LOG_MSG_PARAM_* */
  union {
    char       *par_string;  /* Value for type LOG_PARAM_STRING */
    int        par_int;      /* Value for type LOG_PARAM_INT */
    u_signed64 par_u64;      /* Value for type LOG_PARAM_INT64 */
    double     par_double;   /* Value for type LOG_PARAM_DOUBLE */
    Cuuid_t    par_uuid;     /* Value for type LOG_PARAM_UUID */
  } value;
};

/**
 * A parameter for the CASTOR logging system.
 */
class Param {

public:

  /**
   * Constructor for strings
   */
  Param(const char* name, std::string value) :
    m_deallocate(true) {
    m_cParam.name = (char*) name;
    m_cParam.type = LOG_MSG_PARAM_STR;
    if (!strcmp(name, "TPVID")) {
      m_cParam.type = LOG_MSG_PARAM_TPVID;
    }
    m_cParam.value.par_string = strdup(value.c_str());
  };

  /**
   * Constructor for C strings
   */
  Param(const char* name, const char* value) :
    m_deallocate(true) {
    m_cParam.name = (char*) name;
    m_cParam.type = LOG_MSG_PARAM_STR;
    if (!strcmp(name, "TPVID")) {
      m_cParam.type = LOG_MSG_PARAM_TPVID;
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
    m_cParam.type = LOG_MSG_PARAM_UUID;
    m_cParam.value.par_uuid = value;
  };

  /**
   * Constructor for SubRequest uuids
   */
  Param(Cuuid_t value) :
    m_deallocate(false) {
    m_cParam.name = NULL;
    m_cParam.type = LOG_MSG_PARAM_UUID;
    m_cParam.value.par_uuid = value;
  };

  /**
   * Constructor for int
   */
  Param(const char* name, const long int value) :
    m_deallocate(false) {
    m_cParam.name = (char*) name;
    m_cParam.type = LOG_MSG_PARAM_INT;
    m_cParam.value.par_int = value;
  };

  /**
   * Constructor for int
   */
  Param(const char* name, const long unsigned int value) :
    m_deallocate(false) {
    m_cParam.name = (char*) name;
#if defined __x86_64__
    m_cParam.type = LOG_MSG_PARAM_INT64;
    m_cParam.value.par_u64 = value;
#else
    m_cParam.type = LOG_MSG_PARAM_INT;
    m_cParam.value.par_int = value;
#endif
  };

  /**
   * Constructor for int
   */
  Param(const char* name, const int value) :
    m_deallocate(false) {
    m_cParam.name = (char*) name;
    m_cParam.type = LOG_MSG_PARAM_INT;
    m_cParam.value.par_int = value;
  };

  /**
   * Constructor for int
   */
  Param(const char* name, const unsigned int value) :
    m_deallocate(false) {
    m_cParam.name = (char*) name;
    m_cParam.type = LOG_MSG_PARAM_INT;
    m_cParam.value.par_int = value;
  };

  /**
   * Constructor for u_signed64
   */
  Param(const char* name, u_signed64 value) :
    m_deallocate(false) {
    m_cParam.name = (char*) name;
    m_cParam.type = LOG_MSG_PARAM_INT64;
    m_cParam.value.par_u64 = value;
  };

  /**
   * Constructor for floats
   */
  Param(const char* name, float value) :
    m_deallocate(false) {
    m_cParam.name = (char*) name;
    m_cParam.type = LOG_MSG_PARAM_DOUBLE;
    m_cParam.value.par_double = value;
  };

  /**
   * Constructor for doubles
   */
  Param(const char* name, double value) :
    m_deallocate(false) {
    m_cParam.name = (char*) name;
    m_cParam.type = LOG_MSG_PARAM_DOUBLE;
    m_cParam.value.par_double = value;
  };

  /**
   * Constructor for Tape VIDS
   */
  Param(const char* name, castor::stager::TapeVid value) :
    m_deallocate(false) {
    m_cParam.name = (char*) name;
    m_cParam.type = LOG_MSG_PARAM_TPVID;
    if (0 != value.vid()) {
      m_cParam.value.par_string = strdup(value.vid());
    } else {
      m_cParam.value.par_string = 0;
    }
  };

  /**
   * Constructor for Raw parameters
   */
  Param(const char* rawParams) :
    m_deallocate(true) {
    m_cParam.name = (char*)"";
    m_cParam.type = LOG_MSG_PARAM_RAW;
    m_cParam.value.par_string = strdup(rawParams);
  };

  /**
   * Constructor for IPAddress
   */
  Param(const char* name, castor::log::IPAddress value);

  /**
   * Constructor for TimeStamp
   */
  Param(const char* name, castor::log::TimeStamp value);

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
   * CASTOR logging C interface
   */
  castor_write_param_t cParam() {
    return m_cParam;
  }

private:

  /// the parameter, in a C structure
  castor_write_param_t m_cParam;

  /// Whether the param value should be deallocated
  bool m_deallocate;

}; // class Param

} // namespace log
} // namespace castor

#endif // CASTOR_LOG_PARAM_HPP
