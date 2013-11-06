/******************************************************************************
 *                      castor/tape/utils/SmartFdList.hpp
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

#ifndef CASTOR_TAPE_UTILS_SMARTFDLIST
#define CASTOR_TAPE_UTILS_SMARTFDLIST

#include "castor/exception/Exception.hpp"

#include <list>


namespace castor {
namespace tape   {
namespace utils  {

/**
 * A smart file descriptor list that owns a list of basic file descriptors.
 * When the smart file descriptor list goes out of scope, it will close all
 * of the file descriptor it owns.
 */
class SmartFdList: public std::list<int> {

public:

  /**
   * Destructor.
   *
   * Closes the owned file descriptor if release() has not been called
   * previously.
   */
  ~SmartFdList();

  /**
   * Appends a copy of the specified file descriptor to the end of list.
   * A file descriptor must not be closed twice, there an exception is thrown
   * if the file descriptor already exists in the list.
   */
  void push_back(const int &fd) throw(castor::exception::Exception);

  /**
   * Releases the specified file descriptor.  After a call to this function,
   * the desctructor of the smart file descriptor list will not close the
   * specified file descriptor.
   *
   * @return The released file descriptor.
   */
  int release(const int fd) throw(castor::exception::Exception);


private:

  /**
   * Not implemented copy-constructor to prevent users from trying to create a new
   * copy of an object of this class.
   */
  SmartFdList(const SmartFdList &obj) throw();

  /**
   * Not implemented assignment-operator to prevent users from trying to assign one
   * object of this class to another.
   */
  SmartFdList &operator=(SmartFdList& obj) throw();

}; // class SmartFdList

} // namespace utils
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_UTILS_SMARTFDLIST
