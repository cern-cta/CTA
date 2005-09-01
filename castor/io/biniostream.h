/******************************************************************************
 *                      biniostream.h
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
 * @(#)$RCSfile: biniostream.h,v $ $Revision: 1.4 $ $Release$ $Date: 2005/09/01 10:03:16 $ $Author: mbraeger $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef IO_BINIOSTREAM_H
#define IO_BINIOSTREAM_H 1

#include <sstream>
#include "osdep.h"

namespace castor {

  namespace io {

    /**
     * A binary stream based on stringstream
     */
    class biniostream : public std::stringstream {
    public:
      biniostream(std::string& s) : std::stringstream(s) {}
      biniostream() : std::stringstream() {}

      biniostream& operator<< (char c) {
        write((char*)&c, sizeof(char));
        return *this;
      }

      biniostream& operator<< (unsigned char c) {
        write((char*)&c, sizeof(unsigned char));
        return *this;
      }

      biniostream& operator<< (signed char c) {
        write((char*)&c, sizeof(signed char));
        return *this;
      }

      biniostream& operator<< (int i) {
        write((char*)&i, sizeof(int));
        return *this;
      }

      biniostream& operator<< (unsigned int i) {
        write((char*)&i, sizeof(unsigned int));
        return *this;
      }

      biniostream& operator<< (short s) {
        write((char*)&s, sizeof(short));
        return *this;
      }

      biniostream& operator<< (unsigned short s) {
        write((char*)&s, sizeof(unsigned short));
        return *this;
      }

      biniostream& operator<< (long l) {
        write((char*)&l, LONGSIZE);
        return *this;
      }

      biniostream& operator<< (unsigned long l) {
        write((char*)&l, LONGSIZE);
        return *this;
      }

      biniostream& operator<< (const char* cp) {
        int len = strlen(cp)+1;
        write((char*)&len, sizeof(int));
        write(cp,len);
        return *this;
      }

      biniostream& operator<< (bool b) {
        write((char*)&b, sizeof(bool));
        return *this;
      }

      biniostream& operator<< (float f) {
        write((char*)&f, sizeof(float));
        return *this;
      }

      biniostream& operator<< (double d) {
        write((char*)&d, sizeof(double));
        return *this;
      }

      biniostream& operator<< (long double d) {
        write((char*)&d, sizeof(long double));
        return *this;
      }

      biniostream& operator<< (u_signed64 d) {
        write((char*)&d, sizeof(u_signed64));
        return *this;
      }

      ////////////////////////////////////////////////////////////
      //
      // Input Binary Operators
      //
      ////////////////////////////////////////////////////////////
      biniostream& operator>> (char& c) {
        read((char*)&c, sizeof(char));
        return *this;
      }

      biniostream& operator>> (unsigned char& c) {
        read((char*)&c, sizeof(unsigned char));
        return *this;
      }

      biniostream& operator>> (signed char& c) {
        read((char*)&c, sizeof(signed char));
        return *this;
      }

      biniostream& operator>> (int& i) {
        read((char*)&i, sizeof(int));
        return *this;
      }

      biniostream& operator>> (unsigned int& i) {
        read((char*)&i, sizeof(unsigned int));
        return *this;
      }

      biniostream& operator>> (short& s) {
        read((char*)&s, sizeof(short));
        return *this;
      }

      biniostream& operator>> (unsigned short& s) {
        read((char*)&s, sizeof(unsigned short));
        return *this;
      }

      biniostream& operator>> (long& l) {
        read((char*)&l, LONGSIZE);
        return *this;
      }

      biniostream& operator>> (unsigned long& l) {
        read((char*)&l, LONGSIZE);
        return *this;
      }

      biniostream& operator>> (char* cp) {
        int len;
        read((char*)&len, sizeof(int));
        read(cp,len);
        return *this;
      }

      biniostream& operator>> (bool& b) {
        read((char*)&b, sizeof(bool));
        return *this;
      }

      biniostream& operator>> (float& f) {
        read((char*)&f, sizeof(float));
        return *this;
      }

      biniostream& operator>> (double& d) {
        read((char*)&d, sizeof(double));
        return *this;
      }

      biniostream& operator>> (long double& d) {
        read((char*)&d, sizeof(long double));
        return *this;
      }

      biniostream& operator>> (u_signed64& d) {
        read((char*)&d, sizeof(u_signed64));
        return *this;
      }

    };

  } // end of namespace io

} // end of namespace castor

template <class charT, class traits, class Allocator>
  castor::io::biniostream&
  operator>> (castor::io::biniostream& os,
              std::basic_string <charT, traits, Allocator>& s) {
  s.erase();
  int len;
  os >> len;
  char* buf = (char*)malloc(len+1);
  os.read(buf, len);
  buf[len] = 0;
  s += buf;
  free(buf);
  return os;
}

template <class charT, class traits, class Allocator>
  castor::io::biniostream &
  operator<< (castor::io::biniostream& os,
              const std::basic_string <charT, traits, Allocator>& s) {
  int len = s.length();
  os << len;
  os.write(s.data(), len);
  return os;
}


#endif // IO_BINIOSTREAM_H
