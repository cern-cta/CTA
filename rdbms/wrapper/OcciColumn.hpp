/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <memory>
#include <occi.h>
#include <string.h>
#include <typeinfo>

namespace cta::rdbms::wrapper {

/**
 * A class to help with preparing batch inserts and updatesi with the OCCI
 * interface.
 */
class OcciColumn {
public:
  /**
   * Constructor.
   *
   * @param colName The name of the column.
   * @param nbRows The number of rows in the column.
   */
  OcciColumn(const std::string &colName, const size_t nbRows);

  /**
   * Returns the name of the column.
   *
   * @return The name of the column.
   */
  const std::string &getColName() const;

  /**
   * Returns the number of rows in the column.
   *
   * @return The number of rows in the column.
   */
  size_t getNbRows() const;

  /**
   * Sets the length of the field at the specified index to the length of the
   * specified value.
   *
   * Please note that this method does not record the specified value.  This
   * method only stores the length of the value.
   *
   * This method tag dispatches using std::is_integral.
   *
   * @param index The index of the field.
   * @param value The value whose length is to be recorded.
   */
  template<typename T> void setFieldLenToValueLen(const size_t index, const T &value) {
    // Tag dispatch using std::is_integral
    setFieldLenToValueLen(index, value, std::is_integral<T>());
  }

  /**
   * Returns the array of field lengths.
   *
   * @return The array of field lengths.
   */
  ub2 *getFieldLengths();

  /**
   * Returns the buffer of column field values.
   *
   * @return The buffer of column field values.
   */
  char *getBuffer();

  /**
   * Returns the maximum of the field lengths.
   *
   * @return the maximum of the field lengths.
   */
  ub2 getMaxFieldLength() const;

  /**
   * Sets the field at the specified index to the specified value.
   *
   * This method tag dispatches using std::is_integral.
   *
   * @param index The index of the field.
   * @param value The value of the field.
   */
  template<typename T> void setFieldValue(const size_t index, const T &value) {
    setFieldValue(index, value, std::is_integral<T>());
  }

  /**
   * Sets the field at the specified index to the specified raw byte array.
   *
   * This method tag dispatches using std::is_integral.
   *
   * @param index The index of the field.
   * @param value The value of the field.
   */
  void setFieldValueToRaw(size_t index, const std::string &blob);

  /**
   * Sets the length of the field at the specified index.
   *
   * @param index The index of the field.
   * @param length The length of the field.
   */
  void setFieldLen(const size_t index, const ub2 length);

private:

  /**
   * Sets the length of the field at the specified index to the length of the
   * specified value.
   *
   * Please note that this method does not record the specified value.  This
   * method only stores the length of the value.
   *
   * The third unnamed parameter of this method is used to implement tag
   * dispatching with std::is_integral.
   *
   * @param index The index of the field.
   * @param value The value whose length is to be recorded.
   */
  template<typename T> void setFieldLenToValueLen(const size_t index, const T &value, std::false_type) {
    setFieldLen(index, value.length() + 1);
  }

  /**
   * Sets the length of the field at the specified index to the length of the
   * specified value.
   *
   * Please note that this method does not record the specified value.  This
   * method only stores the length of the value.
   *
   * The third unnamed parameter of this method is used to implement tag
   * dispatching with std::is_integral.
   *
   * @param index The index of the field.
   * @param value The value whose length is to be recorded.
   */
  template<typename T> void setFieldLenToValueLen(const size_t index, const T &value, std::true_type) {
    setFieldLen(index, std::to_string(value).length() + 1);
  }

  /**
   * Sets the field at the specified index to the specified value.
   *
   * The third unnamed parameter of this method is used to implement tag
   * dispatching with std::is_integral.
   *
   * @param index The index of the field.
   * @param value The value of the field.
   */
  template<typename T> void setFieldValue(const size_t index, const T &value, std::true_type) {
    copyStrIntoField(index, std::to_string(value));
  }

  /**
   * Sets the field at the specified index to the specified value.
   *
   * The third unnamed parameter of this method is used to implement tag
   * dispatching with std::is_integral.
   *
   * @param index The index of the field.
   * @param value The value of the field.
   */
  template<typename T> void setFieldValue(const size_t index, const T &value, std::false_type) {
    copyStrIntoField(index, value);
  }

  /**
   * Copies the specified string value into the field at the specified index.
   *
   * @param index The index of the field.
   * @param str The string value.
   */
  void copyStrIntoField(const size_t index, const std::string &str);

  /**
   * The name of the column.
   */
  std::string m_colName;

  /**
   * The number of rows in the column.
   */
  size_t m_nbRows;

  /**
   * The buffer of column field values.
   */
  std::unique_ptr<char[]> m_buffer;

  /**
   * The array of field lengths.
   */
  std::unique_ptr<ub2[]> m_fieldLengths;

  /**
   * The maximum of all the field lengths.
   */
  ub2 m_maxFieldLength;

}; // OcciColumn

} // namespace cta::rdbms::wrapper
