"""Command line interface for rdf-sql-bulkloader."""
import logging
from pathlib import Path

import click

from rdf_sql_bulkloader import SqliteBulkloader, __version__

logger = logging.getLogger(__name__)


@click.group()
@click.option("-v", "--verbose", count=True)
@click.option("-q", "--quiet")
@click.version_option(__version__)
def main(verbose: int, quiet: bool):
    """CLI for rdf-sql-bulkloader.

    :param verbose: Verbosity while running.
    :param quiet: Boolean to be quiet or verbose.
    """
    if verbose >= 2:
        logger.setLevel(level=logging.DEBUG)
    elif verbose == 1:
        logger.setLevel(level=logging.INFO)
    else:
        logger.setLevel(level=logging.WARNING)
    if quiet:
        logger.setLevel(level=logging.ERROR)


@main.command()
@click.option("--output", "-o", required=True)
@click.option(
    "--force/--no-force", default=False, show_default=True, help="Recreates db if already present"
)
@click.option(
    "--rdftab-compatibility/--no-rdftab-compatibility", default=True, show_default=True,
    help="Creates a statements table, compatible with rdftab"
)
@click.argument("files", nargs=-1)
def load_sqlite(files, output, force: bool, rdftab_compatibility: bool):
    """Run the rdf-sql-bulkloader's demo command."""
    output_path = Path(output)
    if output_path.exists():
        if force:
            output_path.unlink()
        else:
            raise ValueError(f"Path exists {output_path}")
    loader = SqliteBulkloader(output)
    loader.rdftab_compatibility = rdftab_compatibility
    if len(files) > 1:
        logging.warning("Blank nodes may be shared TODO FIX ME")
    for file in files:
        print(file)
        loader.bulkload(file)


if __name__ == "__main__":
    main()
