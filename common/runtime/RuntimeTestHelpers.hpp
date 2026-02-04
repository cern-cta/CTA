/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <utility>

namespace unitTests {

/**
 * @brief Creates a temporary file with the provided content. The suffix can be used to add file extensions (e.g .toml).
 *
 */
class TempFile {
public:
  explicit TempFile(const std::string& content, const std::string& suffix = "") {
    std::string fileName = "test_XXXXXX" + suffix;
    std::string path = (std::filesystem::temp_directory_path() / fileName).string();

    const int fd = ::mkstemps(path.data(), static_cast<int>(suffix.size()));
    if (fd == -1) {
      throw std::runtime_error("mkstemps failed: " + std::string {std::strerror(errno)});
    }

    if (::write(fd, content.data(), content.size()) != static_cast<ssize_t>(content.size())) {
      ::close(fd);
      throw std::runtime_error("failed to write temp file");
    }

    ::close(fd);
    m_path = std::move(path);
  }

  ~TempFile() {
    std::error_code ec;
    std::filesystem::remove(m_path, ec);
  }

  const std::string& path() const { return m_path; }

private:
  std::string m_path;
};

// Since we want to (be able to) initialise this with an initialiser list, we need a struct to "own" the storage of the argument list
struct Argv {
  int count = 0;
  std::vector<std::string> storage;  // owns the strings
  std::vector<char*> argv;           // points to the storage

  Argv(std::initializer_list<std::string> args) : storage(args) {
    count = static_cast<int>(storage.size());
    argv.reserve(storage.size() + 1);

    for (auto& s : storage) {
      argv.push_back(s.data());
    }
    argv.push_back(nullptr);  // argv must be null-terminated
  }

  char** data() { return argv.data(); }
};

}  // namespace unitTests
