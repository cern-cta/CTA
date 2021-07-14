/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

/*!
* Simple command line option class for simpler commands like cta-verify-file or cta-send-event
*/
class StringOption {
public:
    /*!
    * Constructor
    */
    StringOption(const std::string &long_opt, const std::string &short_opt, const bool is_optional) :
            m_long_opt(long_opt),
            m_short_opt(short_opt),
            m_is_optional(is_optional),
            m_is_present(false),
            m_value("") {}

    /*!
    * Check if the supplied key matches the option
    */
    bool operator==(const std::string &option) const {
        return option == m_short_opt || option == m_long_opt;
    }

    /*!
    * Return whether the option is optional
    */
    bool is_optional() const {
        return m_is_optional;
    }

    /*!
    * Return whether the option is present on the command line
    */
    bool is_present() const {
        return m_is_present;
    }

    /*!
    * Sets the option as present on the command line
    */
    void set_present() {
        m_is_present = true;
    }

    /*!
    * Return the value of the option given from the command line
    */
    const std::string &get_value() const {
        return m_value;
    }

    /*!
    * Return the name of the option (it's long command option)
    */
    const std::string &get_name() const {
        return m_long_opt;
    }

    /*!
    * Sets the value of the option from the command line
    */
    void set_value(const std::string &mValue) {
        m_value = mValue;
    }

private:

    //member variables
    const std::string m_long_opt;      //!< Long command option
    const std::string m_short_opt;     //!< Short command option
    const bool        m_is_optional;   //!< Option is optional or compulsory
    bool              m_is_present;    //!< Option is present on the command line
    std::string       m_value;         //!< Option value
};


