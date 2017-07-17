/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Class to convert a XRootD SSI Google Protocol Buffers errors into a std::exception
 * @copyright      Copyright 2017 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __XRD_SSI_EXCEPTION_H
#define __XRD_SSI_EXCEPTION_H

#include <stdexcept>
#include <XrdSsi/XrdSsiErrInfo.hh>

namespace XrdSsiPb {

/*!
 * Framework exception thrown by Google Protocol Buffers layer
 */

class PbException : public std::exception
{
public:
   PbException(const std::string &err_msg) : m_err_msg(err_msg) {}

   const char* what() const noexcept { return m_err_msg.c_str(); }

private:
   std::string m_err_msg;
};



/*!
 * Framework exception thrown by XRootD/SSI layer
 */

class XrdSsiException : public PbException
{
public:
   XrdSsiException(const std::string &err_msg) : PbException(err_msg) {}
   XrdSsiException(const XrdSsiErrInfo &eInfo) : PbException(eInfo.Get()) {}
};

} // namespace XrdSsiPb

#endif
