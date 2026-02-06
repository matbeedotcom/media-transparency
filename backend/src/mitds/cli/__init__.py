"""CLI entry points for MITDS.

Provides command-line tools for:
- Data ingestion
- Entity resolution
- Analysis
- Development utilities
"""

import click

from .ingest import cli as ingest_cli
from .analyze import cli as analyze_cli
from .resolve import cli as resolve_cli
from .detect import cli as detect_cli
from .research import cli as research_cli
from .cases import case_group
from .dev import cli as dev_cli
from .verify import cli as verify_cli


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
main.add_command(resolve_cli, name="resolve")
main.add_command(detect_cli, name="detect")
main.add_command(research_cli, name="research")
main.add_command(case_group, name="case")
main.add_command(dev_cli, name="dev")
main.add_command(verify_cli, name="verify")


if __name__ == "__main__":
    main()
