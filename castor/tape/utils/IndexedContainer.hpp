/******************************************************************************
 *                 castor/tape/utils/IndexedContainer.hpp
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

#ifndef CASTOR_TAPE_UTILS_INDEXEDCONTAINER_HPP
#define CASTOR_TAPE_UTILS_INDEXEDCONTAINER_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/exception/OutOfMemory.hpp"
#include "castor/tape/utils/utils.hpp"


namespace castor {
namespace tape   {
namespace utils  {

/**
 * An indexed container with a fixed maximum size of NB_ELEMENTS, used to store
 * data of type T.
 *
 * This container is designed to provide a "no search" solution for both
 * inserting and getting data pointers.
 *
 * All NB_ELEMENTS elements of the container are allocated when the container
 * is created.  Each element maybe free or in-use. Each element has a next
 * member which allows the element to in an internal list of free elements. All
 * elements are in the list of free elements when the container is created.
 *
 * The insert function inserts a data pointer into the container using the
 * the internal linked-list of free elements and returns the array index of the
 * element used to store the data pointer.  This index should then be used
 * later to get the data pointer back and to remove the data pointer from the
 * container.
 *
 * The internal linked-listed of free elements enables a data pointer to be
 * inserted without a search for a free element.  The fact that the elements
 * are in array, enables a data pointer to be retrieved based on an array index
 * without the need for a search.
 */
template <typename T, int NB_ELEMENTS> class IndexedContainer {

public:

  /**
   * Constructor.
   */
  IndexedContainer() throw() {
    for(int i=0; i<NB_ELEMENTS; i++) {
      Element &element = m_elements[i];

      element.index  = i;
      element.isFree = true;

      if(i == NB_ELEMENTS-1) {
        element.next = NULL;
      } else {
        element.next = &m_elements[i+1];
      }
    }

    m_free = m_elements;
  }

  /**
   * Inserts the specified data and returns the index of the elements used to
   * store a copy of that data.
   *
   * Throws an OutOfMemory exception if there are no free elements.
   *
   * @param data The data to be inserted.
   * @return The index of the element used to store the data.
   */
  int insert(const T &data) throw(exception::OutOfMemory) {

    if(m_free == NULL) {
      exception::OutOfMemory ex;

      ex.getMessage() << "No more free elements to store data pointer: "
        "Maximum number of elements=" << NB_ELEMENTS;

      throw(ex);
    }

    // Get and remove the free element from the list of free elements
    Element &element = *m_free;
    m_free = element.next;

    // Store the data and update the state of the element
    element.isFree = false;
    element.data   = data;
    element.next   = NULL;

    return element.index;
  }

  /**
   * Returns a reference to the stored data within the element with the
   * specified array index.
   *
   * Throws InvalidArgument if the index is invalid, i.e. no in range.
   *
   * Throws NoEntry if corresponding element is free.
   *
   * @param index The array index of the list element.
   * @return A reference to the data.
   */
  T &get(const int index)
    throw(exception::InvalidArgument, exception::NoEntry) {

    Element &element = getElement(index);

    // Throw an exception if the element is free
    if(element.isFree) {
      exception::NoEntry ex;

      ex.getMessage()
        << "Element is free"
           ": Index=" << index;

      throw ex;
    }

    return element.data;
  }

  /**
   * Returns a reference to the stored data within the element with the
   * specified array index, and effectively removes the data from the
   * container.
   *
   * The data is effectively removed by adding the corresponding element to the
   * the internal list of free elements.
   *
   * Throws InvalidArgument if the index is invalid.
   *
   * Throws NoEntry if corresponding element is free.
   *
   * @param index The array index of the list element.
   * @return The data pointer.
   */
  T &remove(const int index) throw(exception::InvalidArgument,
    exception::NoEntry) {

    Element &element = getElement(index);

    // Throw an exception if the element is free
    if(element.isFree) {
      exception::NoEntry ex;

      ex.getMessage()
        << "Element is free"
           ": Index=" << index;

      throw ex;
    }

    // Add the element to the list of free elements
    element.next = m_free;
    m_free = &element;

    // Get the data and update the state of the element
    T &data = element.data;
    element.isFree  = true;

    return data;
  }


  /**
   * Writes a textual representation of the IndexedContainer to the specified
   * output stream.
   */
  void write(std::ostream &os) {
    os << "{";

    os << "m_elements={";

    for(int i=0; i<NB_ELEMENTS; i++) {
      Element &element = m_elements[i];

      // Write a comma if this element is not the first
      if(i>0) {
        os << ",";
      }

      os << "{";
      os << "ADDRESS=" << &element;
      os << ",index="  << element.index;
      os << ",isFree=" << boolToString(element.isFree);
      os << ",data="   << element.data;
      os << ",next="   << element.next;
      os << "}";
    }

    os << "}"; // Closing brace of m_elements

    os << ",m_free=" << m_free;

    os << "}"; // Closing brace of IndexedContainer
  }


private:

  /**
   * An element.
   */
  struct Element {
    int     index;
    bool    isFree;
    T       data;
    Element *next;
  };

  /**
   * All of the elements, both free and used.
   */
  Element m_elements[NB_ELEMENTS];

  /**
   * Pointer to the first free element or NULL if there are no more free
   * elements.
   */
  Element *m_free;

  /**
   * Returns the element with the specified array index.
   *
   * Throws InvalidArgument if the index is invalid.
   *
   * @param index The array index of the element.
   * @return The data pointer.
   */
  Element &getElement(const int index) throw(exception::InvalidArgument) {

    // Throw an exception if the index is invalid
    if(index < 0 || index >=NB_ELEMENTS) {
      exception::InvalidArgument ex;

      ex.getMessage()
        << "Invalid index"
           ": Actual=" << index
        << " Minimum=0 Maximum=" << (NB_ELEMENTS-1);

      throw ex;
    }

    return m_elements[index];
  }

}; // IndexedContainer

} // namespace utils
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_UTILS_INDEXEDCONTAINER_HPP
