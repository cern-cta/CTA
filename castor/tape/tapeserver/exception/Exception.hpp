/******************************************************************************
 *                      Exception.hpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "../../../exception/Exception.hpp"
#include <exception>
#include <string>

namespace castor {
namespace tape {
  class Exception: public castor::exception::Exception {
  public:
    Exception(const std::string& what): castor::exception::Exception(SEINTERNAL) { setWhat(what); }
    // Copy operator needed to throw anonymous instance (throw myClass("some failure."))
    Exception(const Exception &ex);
    virtual ~Exception() throw() {};
  protected:
    void setWhat(const std::string &w);
  };

  namespace exceptions {
    class Errnum: public Exception {
    };
    
    class InvalidArgument: public Exception {
    public: InvalidArgument(const std::string& what = ""): Exception(what) {};
    };
    
    class EndOfFile: public castor::exception::Exception {
    public:
      EndOfFile(const std::string & w): castor::exception::Exception(w) {}
      virtual ~EndOfFile() throw() {}
    };
    /**
     * Used
     */
    class ErrorFlag : public castor::exception::Exception {
    public:
      ErrorFlag(): castor::exception::Exception("Internal exception, should not be seen") {}
      virtual ~ErrorFlag() throw() {}
    };
    
    
    class MemException: public castor::tape::Exception {
    public:
      MemException(const std::string & what): Exception(what) {}
      virtual ~MemException() throw() {}
    };

  }
} //namespace tape
} //namespace castor

