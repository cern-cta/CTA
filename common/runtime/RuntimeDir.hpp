/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::runtime {

/**
 * @brief RAII class that creates the runtime directory /run/cta/<pid>/ and allows
 * for populating this class with a number of files.
 *
 */
class RuntimeDir final {
public:
  /**
   * @brief Creates a new runtime directory: /run/cta/<pid> where <pid> is the pid of the current process.
   * If a runtime directory already existed, it will be cleaned up.
   */
  RuntimeDir();

  /**
   * @brief Allows for the usage of an existing runtime directory. For this, the provided directory must already exist and have the correct permissions.
   * The directory will not be cleaned up afterwards.
   */
  explicit RuntimeDir(const std::string& dirPath);

  /**
   * @brief If a predefined runtime directory was provided, this does nothing. If the default runtime directory was created, this cleans up said runtime directory.
   *
   */
  ~RuntimeDir();

  /**
   * @brief Copies a file into the runtime directory. Files copied into the runtime directory are read-only.
   *
   * @param sourcePath The path of the file to copy into the directory.
   * @param newFileName The name the file should have in the runtime directory.
   * @return The path of the destination file after the copy has completed.
   */
  std::string copyFile(const std::string& sourcePath, const std::string& newFileName);

  /**
   * @brief Create a File object
   *
   * @param contents The contents to populate the new file with.
   * @param newFileName The name the file should have in the runtime directory.
   * @return The path of the destination file after the creation has completed.
   */
  std::string createFile(const std::string& contents, const std::string& newFileName);

  /**
   * @brief Returns the path of the runtime directory.
   *
   * @return The path of the runtime directory.
   */
  std::string runtimeDirPath() const;

private:
  /**
 * @brief Safely deletes the runtime directory. If the directory does not exist, this is a NOOP.
 * Performs various safety checks to ensure it is not unintentionally deleting a different directory.
 *
 */
  void deleteRuntimeDir() const;

  /**
 * @brief Restricts the permissions of the given directory.
 *
 * Specifically sets:
 * - owner:  read + write + exec
 * - group:  read + exec
 * - others: read + exec
 *
 * This is equivalent to 0755 permissions.
 *
 * Intended for directories so that only the owning process may modify
 * contents, while other users may read and traverse.
 *
 * @param path The directory path whose permissions should be restricted.
 */
  void restrictDirPermissions(const std::string& path) const;

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
  void restrictFilePermissions(const std::string& path) const;

  const std::string m_dirPath;
  const bool m_shouldCleanUp;
};

}  // namespace cta::runtime
