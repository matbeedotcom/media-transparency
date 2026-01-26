"""CLI entry points for MITDS.

Provides command-line tools for:
- Data ingestion
- Entity resolution
- Analysis
"""

import click

from .ingest import cli as ingest_cli
from .analyze import cli as analyze_cli


@click.group()
@click.version_option(version="0.1.0", prog_name="mitds")
def main():
    """MITDS - Media Influence Topology & Detection System.

    Command-line tools for managing data ingestion,
    entity resolution, and analysis.
    """
    pass


main.add_command(ingest_cli, name="ingest")
main.add_command(analyze_cli, name="analyze")


if __name__ == "__main__":
    main()
