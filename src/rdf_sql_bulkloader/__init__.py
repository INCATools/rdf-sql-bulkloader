"""rdf-sql-bulkloader package."""
from importlib import metadata

__version__ = metadata.version(__name__)

from rdf_sql_bulkloader.loaders.sqlite3_bulkloader import SqliteBulkloader
