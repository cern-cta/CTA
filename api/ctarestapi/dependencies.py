from functools import lru_cache
from ctarestapi.internal.catalogue import Catalogue
import os


def get_connection_string() -> str:
    conn_str = os.environ.get("CTA_CATALOGUE_CONF")
    if not conn_str:
        print("Environment variable CTA_CATALOGUE_CONF missing. Looking for file alternative...")
        with open("/etc/cta/cta-catalogue.conf") as f:
            conn_str = f.read().strip()

    if conn_str.startswith("postgresql:postgresql://"):
        conn_str = conn_str.replace("postgresql:", "", 1)
    return conn_str


# lru cache is just a cheap wrapper to ensure we can nicely mock the get_catalogue method
@lru_cache
def get_catalogue() -> Catalogue:
    return Catalogue(get_connection_string())


# Eventually we can do the same thing with the postgres scheduler here
