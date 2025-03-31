
from .internal.catalogue.catalogue import Catalogue

with open("/etc/cta/catalogue.conf") as f:
    connection_string = f.read().strip()
_catalogue = Catalogue(connection_string)

def get_catalogue() -> Catalogue:
    return _catalogue

# Eventually we can do the same thing with the postgres scheduler here
