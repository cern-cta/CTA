/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Command-line tool for CTA Admin commands
 * @description    CTA Admin command using Google Protocol Buffers and XRootD SSI transport
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

#pragma once



namespace cta {
namespace admin {

class CtaAdminCmd
{
public:
   CtaAdminCmd(int argc, const char *const *const argv);

   /*!
    * Send the protocol buffer across the XRootD SSI transport
    */
   void send();

private:
   /*!
    * Throw an exception with usage help
    */
   void throwUsage();

   /*
    * Member variables
    */
   std::string              m_execname;         //!< Executable name of this program
   std::vector<std::string> m_requestTokens;    //!< The command line parameters represented as a vector of strings
   cta::xrd::Request        m_request;          //!< Protocol Buffer for the command and parameters
};

}} // namespace cta::admin

