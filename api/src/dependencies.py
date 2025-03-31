
from .internal.catalogue.catalogue import Catalogue

with open("/etc/cta/cta-catalogue.conf") as f:
    connection_string = f.read().strip()
    # Fix for sql alchemy
    if connection_string.startswith("postgresql:postgresql://"):
        connection_string = connection_string.replace("postgresql:", "", 1)
_catalogue = Catalogue(connection_string)

def get_catalogue() -> Catalogue:
    return _catalogue

# Eventually we can do the same thing with the postgres scheduler here
