/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::utils {

/**
 * @brief Copies a file to a particular destination and makes the destination file read-only.
 *
 * @param sourcePath The path of the file to copy into the directory.
 * @param newFileName The name the file should have in the runtime directory.
 * @param restricted Set to true if the destination file should have restricted permissions (see restrictFilePermissions). False otherwise.
 * @return The path of the destination file after the copy has completed.
 */
std::string copyFile(const std::string& sourcePath, const std::string& newFileName, bool restricted = true);

/**
 * @brief Create a new file in the given destination with the provided contents.
 *
 * @param contents The contents to populate the new file with.
 * @param newFileName The name the file should have in the runtime directory.
 * @param restricted Set to true if the destination file should have restricted permissions (see restrictFilePermissions). False otherwise.
 * @return The path of the destination file after the creation has completed.
*/
std::string createFileWithContents(const std::string& contents, const std::string& newFileName, bool restricted = true);

/**
 * @brief Restricts the permissions of the given file.
 *
 * Specifically sets:
 * - owner:  read + write
 * - group:  read
 * - others: read
 *
 * This is equivalent to 0644 permissions.
 *
 * Intended for regular files so they are world-readable but writable
 * only by the owning process.
 *
 * @param path The file path whose permissions should be restricted.
 */
void restrictFilePermissions(const std::string& path);

}  // namespace cta::utils
