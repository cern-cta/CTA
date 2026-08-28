# C++ Conventions

Follow [General Coding Conventions](general.md). The C++ standard is selected in `project.json`; formatting is defined by `.clang-format` and applied through [Pre-commit Hooks](../../tools-and-environment/pre-commit.md).

## Files and namespaces

- Headers MUST use `.hpp` and implementation files `.cpp` for new code. Prefer one class per file, allowing small, closely related types to stay together.
- Headers MUST use `#pragma once` before includes and declarations; copyright comments may precede it. Headers SHOULD include what they need rather than rely on transitive includes.
- Include project headers with quotes and paths from the project root; use angle brackets for external and system headers.
- Place declarations and definitions in the appropriate project namespace. Use enclosing namespace blocks for definitions in source files.
- `using namespace` directives MUST NOT appear at namespace scope in headers or at global scope in implementation files. Keep any use local and limited.
- Avoid namespace-scope `using` declarations in headers that expose unrelated names to consumers. Class-scope declarations, such as exposing base-class overloads, are permitted.

## Classes and resource ownership

- Prefer values and standard containers for ownership. Use RAII for resources that require cleanup.
- Prefer `std::unique_ptr` for dynamically allocated objects with a single owner. Use `std::shared_ptr` only when ownership is actually shared; raw pointers and references should express non-owning access.
- Constructors callable with one argument MUST be `explicit` unless an implicit conversion is intentional and justified.
- Overridden virtual methods MUST use `override`.
- Base-class destructors SHOULD be public and virtual when deletion through the base is supported, or protected and non-virtual when it is prohibited.
- Prefer the Rule of Zero. When defining custom destruction, copy, or move behaviour, explicitly consider all special member functions and default or delete them as appropriate.
- Keep class data private, except for simple data-holder structs. Avoid getters and setters that expose implementation details without a useful interface.
- Do not rely on virtual dispatch to derived-class overrides during construction or destruction.

## Functions and initialization

- Mark methods `const` when they do not modify observable object state.
- Pass inexpensive-to-copy inputs by value and expensive read-only inputs by `const&`. For ownership-taking parameters, use a value or an appropriate ownership type and move where needed; being cheap to move alone does not make a read-only argument cheap to pass by value.
- Initialize objects when declaring them. Initialize class members in the order of their declarations.
- Prefer named constants for values whose meaning is not obvious. Ordinary literals do not need names solely to avoid appearing in code.
- Prefer typed language features such as `constexpr`, inline functions, and templates over macros where practical. Keep necessary platform and conditional-compilation macros focused.

## Documentation

- Document class purpose and non-obvious public contracts, including ownership, errors, and thread-safety expectations where relevant.
- Use `///` comments for Doxygen API documentation, with `///<` for trailing member documentation.
- Omit `@brief` for single-line documentation comments; use it to mark the summary in multiline documentation blocks.
  Keep blank lines within a documentation block prefixed with `///`.
- Do not add comments that merely restate trivial methods or parameter names.
- End documentation comments with a full stop.
- Public and private methods in the header files should be documented.
- For member variables, only add a documentation comment if it's worth explaining why it's there.
- In the documentation comments, keep it clear and concise. Maintaining the comments should not be more work than maintaining the implementation.
- Tests don't need documentation-style comments.

For broader guidance, see the [C++ Core Guidelines](https://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines).
