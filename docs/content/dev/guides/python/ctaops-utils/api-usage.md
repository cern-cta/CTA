# Usage

## Configuration Management

**`ctaops.utils.config`**

**Step 1**: Define your configuration schema using dataclasses

```python
from dataclasses import dataclass, field
from ctaops.utils.logging import LoggingConfiguration

@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    username: str = ""
    password: str = ""
    host: str = "localhost"
    port: int = 5432
    database: str = ""

@dataclass
class AppConfig:
    """Application configuration."""
    timeout: int = 30
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    logging: LoggingConfiguration = field(default_factory=LoggingConfiguration)

    def validate(self) -> "AppConfig":
        """Validate the configuration after loading."""
        # Add custom validation logic here
        if self.timeout <= 0:
            raise ValueError("Timeout must be positive")
        if not self.database.username:
            raise ValueError("Database username is required")
        if not self.database.password:
            raise ValueError("Database password is required")
        return self
```

??? Example "Corresponding Configuration File"

    ```toml
    timeout = 60

    [database]
    username = "myuser"
    password = "mypassword"
    host = "localhost"
    port = 5432
    database = "mydb"

    [logging]
    file_path = "app.log"
    level = "INFO"
    ```

**Step 2**: Load configuration from multiple sources

```python
from pathlib import Path
from ctaops.utils.config import CtaOpsConfigLoader

# Create loader instance
loader = CtaOpsConfigLoader(AppConfig)

# Load from TOML file (optional)
loader.load_from_toml(Path("/etc/myapp/config.toml"), file_exist_error="ignore")

# Override with dictionary (e.g., from Cli args)
loader.load_from_dict({
    "database": {
        "username": "admin",
        "password": "secret"
    },
    "logging": {
        "level": "DEBUG"
    }
})

# Set individual nested values
loader.set("database", "host", value="db.example.com")

# Get validated configuration
config = loader.get_safe_config()
```

!!! info
    See also different loading mode for [`load_from_toml()`](../../../../api/python/ctaops-utils/reference.md)

??? tip
    You can chain all operations in one line.

    ```python
    from pathlib import Path
    from ctaops.utils.config import CtaOpsConfigLoader

    config = (
        CtaOpsConfigLoader(AppConfig)
        .load_from_toml(Path("config.toml"), file_exist_error="ignore")
        .load_from_dict(cli_args)
        .set("logging", "level", value="INFO")
        .get_safe_config()
    )
    ```

**Step 3**: Use the configuration

```python
# Access configuration values
print(f"Database: {config.database.host}:{config.database.port}")
print(f"Timeout: {config.timeout}s")
```



## Logging Configuration

**`ctaops.utils.logging`**

```python
from pathlib import Path
from ctaops.utils.logging import configure_logging, LoggingConfiguration

# Configure logging
configure_logging(
    LoggingConfiguration(
        file_path="/var/log/ctaops/myapp.log",
        max_log_file_size=1_000_000_000,  # 1 GB
        keep_file_count=5,
        date_format="%Y-%m-%d %H:%M:%S",
        level="INFO"
    )
)

# Use standard logging
from logging import getLogger
LOGGER = getLogger("myapp")
LOGGER.info("Application started")
LOGGER.warning("This is a warning")
LOGGER.error("An error occurred")
```

!!! info
    See also [Package reference](../../../../api/python/ctaops-utils/reference.md).

## Dataclass Utilities

**`ctaops.utils.dataclasses`**

Utility functions for working with nested dataclasses and flattening complex structures.

**`load_data()`**

Recursively loads data from a dictionary into a dataclass instance, handling nested dataclasses automatically.

```python
from dataclasses import dataclass, field
from ctaops.utils.dataclasses import load_data

@dataclass
class Address:
    street: str = ""
    city: str = ""

@dataclass
class Person:
    name: str = ""
    address: Address = field(default_factory=Address)

# Create instance and load data
person = Person()
load_data(person, {
    "name": "Alice",
    "address": {
        "street": "123 Main St",
        "city": "Springfield"
    }
})

print(person.name)  # "Alice"
print(person.address.city)  # "Springfield"
```

**`flatten_dataclass_instance()`**

Converts a dataclass instance to a flat dictionary with dot-notation keys, useful for displaying or comparing complex nested structures.

```python
from dataclasses import dataclass, field
from ctaops.utils.dataclasses import flatten_dataclass_instance

@dataclass
class DatabaseConfig:
    host: str = "localhost"
    port: int = 5432

@dataclass
class AppConfig:
    app_name: str = "MyApp"
    database: DatabaseConfig = field(default_factory=DatabaseConfig)

config = AppConfig(database=DatabaseConfig(host="db.example.com"))

# Flatten to dot-notation dictionary
flat = flatten_dataclass_instance(config)
print(flat)
# Output: {'app_name': 'MyApp', 'database.host': 'db.example.com', 'database.port': 5432}
```

**`flatten_dataclass_class()`**

Extracts type information from a dataclass definition, returning paths and their types in dot-notation format. Useful for schema introspection and validation.

```python
from dataclasses import dataclass, field
from ctaops.utils.dataclasses import flatten_dataclass_class

@dataclass
class DatabaseConfig:
    host: str = "localhost"
    port: int = 5432

@dataclass
class AppConfig:
    app_name: str = "MyApp"
    database: DatabaseConfig = field(default_factory=DatabaseConfig)

# Get type information
schema = flatten_dataclass_class(AppConfig)
for path, types in schema:
    print(f"{path}: {types}")
# Output:
# app_name: frozenset({<class 'str'>})
# database.host: frozenset({<class 'str'>})
# database.port: frozenset({<class 'int'>})
```

---

## Type Annotation Utilities

**`ctaops.utils.typing`**

Utilities for normalizing and working with Python type annotations, particularly useful for handling complex types like `Union`, `Optional`, and generic types.

**`normalize_types()`**

Normalizes Python type annotations to a set of instantiable types. This is useful for runtime type checking and validation.

```python
from typing import Optional, Union
from ctaops.utils.typing import normalize_types

# Handle Optional types
types = normalize_types(Optional[int])
print(types)  # frozenset({<class 'int'>, <class 'NoneType'>})

# Handle Union types
types = normalize_types(Union[str, int])
print(types)  # frozenset({<class 'str'>, <class 'int'>})

# Handle Literal types
from typing import Literal
types = normalize_types(Literal["DEBUG", "INFO", "WARNING"])
print(types)  # frozenset({<class 'str'>})

# Handle Annotated types
from typing import Annotated
types = normalize_types(Annotated[str, "some metadata"])
print(types)  # frozenset({<class 'str'>})
```

!!! info 
    For **Supported Type Constructs** see also [ctaops.utils reference](../../../../api/python/ctaops-utils/reference.md).

!!! note
    **Performance:** The function is cached with LRU (Least Recently Used) caching for fast repeated calls.

## Complete Example

Here's a complete example showing how to use all modules together:

```python
from dataclasses import dataclass, field
from pathlib import Path
import logging

from ctaops.utils.config import CtaOpsConfigLoader
from ctaops.utils.logging import configure_logging, LoggingConfiguration
from ctaops.utils.dataclasses import flatten_dataclass_instance

# 1. Define configuration schema
@dataclass
class AppConfig:
    app_name: str = "MyApp"
    timeout: int = 30
    logging: LoggingConfiguration = field(default_factory=LoggingConfiguration)

    def validate(self) -> "AppConfig":
        if self.timeout <= 0:
            raise ValueError("Timeout must be positive")
        if not self.logging.file_path:
            raise ValueError("Log file path is required")
        return self

# 2. Load configuration
config = (
    CtaOpsConfigLoader(AppConfig)
    .load_from_toml(Path("/etc/myapp/config.toml"), file_exist_error="ignore")
    .set("logging", "file_path", value="/var/log/myapp/app.log")
    .set("logging", "level", value="INFO")
    .get_safe_config()
)

# 3. Configure logging
configure_logging(config.logging)

# 4. Use the application
logger = logging.getLogger(__name__)
logger.info(f"{config.app_name} started with timeout={config.timeout}s")

# 5. Optionally: Flatten configuration for display/debugging
flat_config = flatten_dataclass_instance(config)
for key, value in sorted(flat_config.items()):
    print(f"  {key}: {value}")
```
