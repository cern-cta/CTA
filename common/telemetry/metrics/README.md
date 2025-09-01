# OpenTelemetry Metrics

This directory defines all the instruments for collecting metrics.

## Motivation

There are two important reasons why all instruments are declared under `instruments/` instead of locally in the places where they are used.
First, it makes it easy for a developer to see which instruments exist and which attributes they have.
Second, it makes it easy to change/update/reinit all instruments when the telemetry config changes (e.g. from NOOP to OTLP backend).
Third, it ensures only a single instance of a given instrument exists at the same time.

To give more context on the third point: in OpenTelemetry, it is important that duplicate instruments do not exist.
Consider the scenario of a counter with a given set of attributes that has been incremented to 70.
Now a new counter with the exact same attributes is initialised, because it is e.g. a class member and a new instance of the class was created.
This new counter might not be incremented and have the value 4.

Whenever metrics are now exported, the OpenTelemetry Collector cannot distinguish between these two instruments, resulting in incorrect and confusing metrics.
As such, it is vital that we only have one instance of a given instrument-attribute combination at any given time.

## Implementation

Declaring all instruments at a global level presents a few challenges:
- All instruments must be initialised when the program starts to a NOOP to prevent nullpointer exceptions. It is difficult to guarantee that no instruments will be accessed before the telemetry config is read.
- It must be possible to reinitialise all instruments. We do not know the telemetry config when the program starts (only after we have read the config file), so it is crucial to be able to update all instruments according to the config.

To solve these two problems, we have the `InstrumentRegistrar` (found in `InstrumentRegistry.hpp`).
The purpose of the InstrumentRegistrar is to be able to run and register an instrument init function at program start time.
By declaring the `InstrumentRegistrar` as a static variable in an anonymous namespace, we ensure that this is executed at start time.
This solves the two problems above:
- It ensures all instruments are initialised to a NOOP when the program starts.
  This is important to ensure we are not accessing a nullptr when using an instrument.
  For applications that don't require telemetry, it should not be mandatory to explicitly initialise this to a NOOP.
  Take e.g. the CLI tools: they require the catalogue library which has telemetry. It should work out of the box.
  Additionally, it also bypasses the need for thread-safety in the initialisation of the instruments
  because the init functions are only called once at any given time (at startup, after initialisation, at reset)
- It gives us a list of init functions that we can use to reinit all instruments when necessary.


Finally, it also makes the instrument files fully self-contained. If so desired, this allows us to put instrument files into their respective libraries.
This would improve the locality somewhat (instruments are bundled together with the code that they instrument), but it makes it a bit more difficult to get a full overview of all existing instruments.
