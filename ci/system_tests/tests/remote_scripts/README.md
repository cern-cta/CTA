# Remote test scripts

Scripts in this directory run inside test pods when a test needs access to their local environment. Keep test orchestration and assertions in Python where practical.

Follow [System Tests](../../../../docs/content/dev/guides/testing/system-tests.md#write-system-tests) when adding or changing tests. `unused/` contains historical scripts that are not called by the current suite.
