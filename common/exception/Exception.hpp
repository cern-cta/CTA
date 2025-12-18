/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "Backtrace.hpp"

#include <exception>
#include <sstream>
#include <string>

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
  explicit Exception(std::string_view context = "", bool embedBacktrace = true);

  /**
   * Copy constructor
   *
   * Default copy constructor is implicitly deleted as it would be ill-defined
   */
  Exception(const Exception& rhs);

  /**
   * Default move constructor
   */
  Exception(Exception&& rhs) noexcept = default;

  /**
   * Assignment operator
   *
   * Default assignment constructor is implicitly deleted as it would be ill-defined
   */
  Exception& operator=(const Exception& rhs);

  /**
   * Default move assignment operator
   */
  Exception& operator=(Exception&& rhs) noexcept = default;

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
  const std::string backtrace() const { return m_backtrace.str(); }

  /**
   * Updates the m_what member with a concatenation of the message and the stack trace
   *
   * @return pointer to m_what's contents
   */
  const char* what() const noexcept override;

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

}  // namespace cta::exception

#define CTA_GENERATE_EXCEPTION_CLASS(A)        \
  class A : public cta::exception::Exception { \
    using Exception::Exception;                \
  }
