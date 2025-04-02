from sqlalchemy import create_engine
from .drive_queries import DriveQueries


class Catalogue:

    def __init__(self, connection_string: str):
        engine = create_engine(connection_string)
        self.drives = DriveQueries(engine)
