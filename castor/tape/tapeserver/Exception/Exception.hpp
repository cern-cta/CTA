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
#include <exception>
#include <string>

namespace Tape {
  
  namespace Exceptions {
    class Backtrace {
    public:
      Backtrace();
      operator std::string() const { return m_trace; }
    private:
      std::string m_trace;
    };
  }
  
  class Exception: public std::exception {
  public:
    Exception(const std::string& what) { setWhat(what); }
    virtual ~Exception() throw() {};
    virtual const char * what() const throw();
    virtual const char * shortWhat() const throw();
    Tape::Exceptions::Backtrace backtrace;
  protected:
    void setWhat(const std::string &w);
  private:
    std::string m_what;
    std::string m_shortWhat;
  };

  namespace Exceptions {
    class Errnum: public Tape::Exception {
    public:
      Errnum(std::string what = "");
      virtual ~Errnum() throw() {};
      int ErrorNumber() { return m_errnum; }
      std::string strError() { return m_strerror; }
      /* We rely on base version:
       *  virtual const char * what() const throw() */
    protected:
      int m_errnum;
      std::string m_strerror;
    };
    
    class InvalidArgument: public Tape::Exception {
    public: InvalidArgument(std::string what = ""): Tape::Exception(what) {};
    };
  }
}
