# C++ Conventions

## Files & Organization

- Each header file MUST begin with `#pragma once` to prevent multiple inclusion.
- Header files MUST use the `.hpp` extension; implementation files MUST use the `.cpp` extension.
- Project headers MUST be included with `""` and external/system headers with `<>`.
- Project headers MUST be referenced using full paths from the project root (not relative paths).
- Declarations in headers and definitions in source files MUST be placed inside the correct project namespace.
- When defining namespaced entities in a source file, the surrounding `namespace {}` clauses MUST be present.

## Classes

- Constructors that can be called with a single argument MUST be marked `explicit`.
- When overriding virtual methods, the `override` keyword MUST be used.
- Base class destructors intended for polymorphic use MUST be `virtual`.
- Prefer the [Rule of Zero](https://en.cppreference.com/w/cpp/language/rule_of_three#Rule_of_zero). If manual resource management is required, the [Rule of Five](https://en.cppreference.com/w/cpp/language/rule_of_three.html#Rule_of_five) MUST be followed.
- Data members SHOULD NOT be public. Public data members MAY be used for simple data holder structs.

## Methods

- Methods that do not modify observable object state MUST be marked `const`.
- Parameter passing:
    - Cheap-to-move or small types MAY be passed by value (and moved as needed).
    - Expensive-to-copy types SHOULD be passed by `const&`.

## General

- RAII MUST be used for resource management.
- Owning raw pointers MUST NOT be used. Ownership MUST be expressed via `std::unique_ptr` or `std::shared_ptr`.
- Magic numbers MUST NOT appear in code. Use `constexpr`, `enum class`, or named constants instead.
- Macros SHOULD NOT be used except where unavoidable for conditional compilation or platform-specific concerns.
- `using namespace` directives MUST NOT appear in header files.
- `using namespace` directives MUST NOT appear at global scope in implementation files.
- `using namespace` MAY be used inside a limited local scope (e.g., inside a function) when it improves readability without risking namespace pollution.
- `using` declarations (e.g., `using std::string;`) MAY be used in implementation files but MUST NOT appear in header files.
- Virtual methods MUST NOT be called in constructors or destructors.
- Variables SHOULD be defined and initialized in declaration order.

## Documentation

- Each class SHOULD have a top-level Doxygen comment describing its purpose and usage.
- Each method SHOULD have a Doxygen comment describing its functionality.
- Use Doxygen Javadoc style for all class and method comments.
- Names of classes, methods, parameters, and variables SHOULD be descriptive and unambiguous.

!!! tip

    - Prefer static polymorphism (templates) over dynamic polymorphism when appropriate and when code size/compile times remain acceptable.
    - Avoid exposing unnecessary getters and setters; add them when invariants or encapsulation require it.
    - Use forward declarations to reduce dependencies and compilation time where possible.
    - Prefer `constexpr`, `inline`, or templates instead of macros.
    - Prefer brace initialization (`{}`) for consistency and to avoid narrowing.
