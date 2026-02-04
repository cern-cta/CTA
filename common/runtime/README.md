# Runtime Library

The files in this directory aim to provide a basic framework for every tool and every application in CTA.
Its main goals are (in order of importance):

- Improving consistency between the various apps and tools.
- Reducing the amount of mistakes a developer can introduce by accident.
- Reducing the amount of boilerplate code needed to set up a new tool/app.

This runtime library does this by providing a common framework that is easy to use and **checks as much as possible at compile time**.

Each application/tool has the same two inputs:

1. The commandline arguments.
2. The config file.

The main idea behind the implementation library here is to separate between (1) what the code representation of these two inputs look like and (2) how this code representation is populated from what the user provided.

Both the commandline arguments and the config file are internally defined as simple immutable struct (a data class if you will).
In addition, the main App is specified as a simple class.
This means that the developer defines three things (in this order):
1. The struct to store the commandline options
2. The struct to store the config
3. The class of your main application

Then the way you define an app is as follows:

```c++
runtime::Application<CustomApp, CustomConfig, CustomCliOptions> app(appName, cliOptions);
```

The cliOptions are expected to be populated beforehand. The `CustomConfig` is populated inside of `Application.hpp`. Similarly, `CustomApp` is initialised there as well. `Application.hpp` performs various compile time checks on all types passed into it (`CustomApp`, `CustomConfig`, `CustomCliOptions`). In that sense, it acts a bit as a builder pattern at compile time. Based on the config structs/types you pass into, it will enforce various constraints on `CustomApp`. This ensures every config and every app has the same consistent layout, while at the same time allowing maximum flexibility.

## Config Loading

Config loading is done using a combination of [tomlplusplus](https://github.com/marzer/tomlplusplus) to read TOML files and [reflect-cpp](https://github.com/getml/reflect-cpp) to populate the struct.

The only thing the developer needs to take care of is that the structure of the config struct matches the structure of TOML files. Both in terms of types and in terms of hierarchy and names. See e.g. `maintd/` for an example of what this looks like.

## Examples

As a general rule of thumb, you can also check the unit tests for various examples.

## Basic Example with Custom Config

```c++
struct CustomConfig {
  // Add this if the app/tool uses the catalogue
  cta::runtime::CatalogueConfig catalogue;
  // Add this if the app/tool uses the scheduler
  cta::runtime::SchedulerConfig scheduler;
  // Must always be present; all tools and apps support logging
  cta::runtime::LoggingConfig logging;
  // Add this if the app should be able to produce telemetry
  cta::runtime::TelemetryConfig telemetry;
  // Add this if the app needs a separate health endpoint
  // Don't add this if the app already natively exposes a health endpoint (e.g. a REST API)
  cta::runtime::HealthServerConfig health_server;
  // Add this if the app has experimental options
  // For now, this must be added if telemetry is there as telemetry is considered experimental
  cta::runtime::ExperimentalConfig experimental;
  // Add this if the app uses XRootD
  cta::runtime::XRootDConfig xrootd;
  // Put whatever you want here; will be populated from the config file and available in the run() function
  MyCustomConfStruct customConf;
};

class CustomApp {
public:
  CustomApp() = default;
  ~CustomApp() = default;
  void stop(); // Every app MUST have a stop() function
  // Consuming the CLI options
  int run(const CustomConfig& config, cta::log::Logger& log);
  // Alternatively this would compile as well:
  // int run(const CustomConfig& config, runtime::CommonCliOptions& opts, cta::log::Logger& log);
  bool isLive() const; // Since there is a health_server config, it MUST have an isLive() function
  bool isReady() const; // Since there is a health_server config, it MUST have an isReady() function
};

int main(const int argc, char** const argv) {
  using namespace cta;
  return runtime::safeRun([argc, argv]() {
    const std::string appName = "cta-custom-app";
    runtime::ArgParser<runtime::CommonCliOptions> argParser(appName);
    auto cliOptions = argParser.parse(argc, argv);
    runtime::Application<CustomApp, CustomConfig, runtime::CommonCliOptions> app(appName, cliOptions);
    return app.run();
  });
}
```

## App with Custom CLI options

```c++
struct CustomCliOptions : public cta::runtime::CommonCliOptions {
  std::string iAmExtra;
};

// Typically if you want to add custom CLI options, it means your app should consume them.
// As such, the run() method of CustomApp example would change to:
int run(const CustomConfig& config, const CustomCliOptions& opts, cta::log::Logger& log);

int main(const int argc, char** const argv) {
  using namespace cta;
  return runtime::safeRun([argc, argv]() {
    const std::string appName = "cta-custom-app";
    runtime::ArgParser<CustomCliOptions> argParser(appName);
    // Note we need to add an explicit parser for this new field
    argParser.withStringArg(&CustomCliOptions::iAmExtra, "extra", 'e', "STUFF", "my description");
    auto cliOptions = argParser.parse(argc, argv);
    runtime::Application<CustomApp, CustomConfig, CustomCliOptions> app(appName, cliOptions);
    return app.run();
  });
}
```

## Most minimal example you can get away with

```c++
struct MinimalConfig {
  cta::runtime::LoggingConfig logging;
};

class MinimalApp {
public:
  MinimalApp() = default;
  ~MinimalApp() = default;
  void stop(); // Every app MUST have a stop() function
  // Consuming the CLI options
  int run(const MinimalConfig& config, cta::log::Logger& log);
  // Alternatively this would compile as well:
  // int run(const MinimalConfig& config, runtime::CommonCliOptions& opts, cta::log::Logger& log);
};

int main(const int argc, char** const argv) {
  using namespace cta;
  return runtime::safeRun([argc, argv]() {
    const std::string appName = "cta-custom-app";
    runtime::ArgParser<runtime::CommonCliOptions> argParser(appName);
    auto cliOptions = argParser.parse(argc, argv);
    runtime::Application<MinimalApp, MinimalConfig, runtime::CommonCliOptions> app(appName, cliOptions);
    return app.run();
  });
}
```
