# Usage

This section presents code examples to use the `cta-sdk`.

## Initializing the Client

The `get_cta_client` function is the entry point for creating a client. You must specify a backend to use.

```python
from cta.sdk import CtaClient, get_cta_client

# Initialize the client with the "CLI" backend
cta_client: CtaClient = get_cta_client("CLI")
```

## Retrieving Resources

Daos are accessed as properties on the `cta_client` object. Use the `get()` and `list()` methods to retrieve resources.

```python
# Get the manager for tapes
tape_manager = cta_client.tape

# Get a specific tape by its VID
tape = tape_manager.get("L12345")

print(f"Tape {tape.vid} has capacity {tape.capacity} bytes.")

# List all drives
drives = list(cta_client.drive.list())
drive_names = [d.name for d in drives]
print(f"Available drives: {drive_names}")
```

!!! warning
    The `.list()` methods don't return a python list but an iterator over the specified resources.  
    You need to call built-in `list()` function to get an actual list. 

!!! tip
    Most of the time you don't need a real list but you just need to iterate over the resources. 

## Creating and Updating Resources

Use the `create()` and `update()` methods to manage resources. Methods use keyword-only arguments for clarity.

```python
# Create a new disk instance
cta_client.disk_instance.create("my-new-instance", comment="Created via SDK")

# Update the comment on an existing instance
cta_client.disk_instance.update("my-new-instance", comment="Updated via SDK")

# Create a new tape pool
cta_client.tape_pool.create(
    "my-pool",
    media_type="LTO9",
    virtual_organisation="test-vo",
    comment="Test pool"
)
```

## Querying Tape Files

The tape_file manager supports multiple lookup methods for finding files on tapes:

```python
# Get all copies of a file by archive ID
copies = cta_client.tape_file.get_all_copies(12345)

# Get all copies by disk file ID and disk instance
copies = cta_client.tape_file.get_all_copies("eos-file-id", "disk-instance-name")

# Get a specific copy on a given tape (by archive ID + VID)
tape_file = cta_client.tape_file.get(12345, vid="L12345")

# List all files on a tape
file_iterator = cta_client.tape_file.list(vid="L12345") # this is lazy evaluated
```

## Data-Oriented Resources

All CTA resources, such as Tapes, Drives, and Mount Policies, are represented as Python `dataclasses`. These objects provide a structured and predictable way to access resource attributes. Because they are strongly typed, you get the full benefit of IDE features like autocompletion and static analysis, making your code clearer and less prone to errors.

!!! note
    While these resources are structured as dataclasses, they are not immutable.
    However most of the time you will not modify them.

!!! warning
    However some of class attributes are annoted with `Final`. Those attributes are not supposed to be changed and should
    be treated as immutable.

## Error Handling

CTA-SDK defines several exceptions that can be raised by the daos. All exceptions inherit from the `CtaSdkError` base exception:

```python
from cta.sdk import get_cta_client
from cta.sdk.exceptions import (
    CtaSdkError,
    ResourceNotFoundError,
    ResourceAlreadyExistsError,
    ResourceConflictError,
    BackendError
)

cta_client = get_cta_client("CLI")

# Handle specific exceptions
try:
    cta_client.tape.get("NonExistentVID")
except ResourceNotFoundError as e:
    print(f"Tape not found: {e}")
except ResourceAlreadyExistsError as e:
    print(f"Resource already exists: {e}")
except ResourceConflictError as e:
    print(f"Conflict occurred: {e}")
except BackendError as e:
    print(f"Backend error (likely cta-admin command failure): {e}")
except CtaSdkError as e:
    print(f"Unexpected SDK error: {e}")
```


!!! info
    See also [**Exception Types** reference](../../../../api/python/cta-sdk/reference/exceptions.md)
