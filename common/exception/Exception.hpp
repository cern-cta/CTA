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

#include <exception>
#include <sstream>
#include <string>

#include "Backtrace.hpp"

namespace cta::exception {

/**
 * Exception class used for error handling in CTA
 */
class Exception : public std::exception {
public:
  /**
   * Constructor
   *
   * @param context        optional context string added to the message at initialisation time
   * @param embedBacktrace whether to embed a backtrace of where the exception was thrown
   */
  Exception(std::string_view context = "", bool embedBacktrace = true);

  /**
   * Copy constructor
   *
   * Default copy constructor is implicitly deleted as it would be ill-defined
   */
  Exception(const Exception& rhs);

  /**
   * Assignment constructor
   *
   * Default assignment constructor is implicitly deleted as it it would be ill-defined
   */
  Exception& operator=(const Exception& rhs);

  /**
   * Empty Destructor
   *
   * Needed for std::exception inheritance
   */
  ~Exception() override = default;

  /**
   * Get the message explaining why this exception was raised
   *
   * @return the value of m_message
   */
  std::ostringstream& getMessage() { return m_message; }

  /**
   * Get the message explaining why this exception was raised
   *
   * @return the value of m_message
   */
  const std::ostringstream& getMessage() const { return m_message; }

  /**
   * Get the value of m_message as a string, for const-correctness
   *
   * @return the value of m_message
   */
  std::string getMessageValue() const { return m_message.str(); }

  /**
   * Get the backtrace
   *
   * @return backtrace in a standard string.
   */
  std::string const backtrace() const {
    return m_backtrace.str();
  }

  /**
   * Updates the m_what member with a concatenation of the message and the stack trace
   *
   * @return pointer to m_what's contents
   */
  virtual const char* what() const noexcept;

private:
  /**
   * Message explaining why this exception was raised
   */
  std::ostringstream m_message;

  /**
   * Placeholder for the what result. It has to be a member of the object, and not on the stack of the "what" function.
   */
  mutable std::string m_what;

protected:
  void setWhat(std::string_view w);

  /**
   * Backtrace object. Its constructor does the heavy lifting of generating the backtrace.
   */
  Backtrace m_backtrace;
};

} // namespace cta::exception

#define CTA_GENERATE_EXCEPTION_CLASS(A) class A: public cta::exception::Exception { using Exception::Exception; }
