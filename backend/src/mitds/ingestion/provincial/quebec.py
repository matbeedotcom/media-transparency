"""Quebec Corporation Registry ingester.

Ingests data from the Quebec Registraire des Entreprises via Données Québec.

Data source: https://www.donneesquebec.ca/recherche/dataset/registre-des-entreprises
Format: ZIP containing multiple CSV files (UTF-8 with French characters)
Update frequency: Daily
License: CC-BY-NC-SA 4.0

Quebec has the best bulk data availability among Canadian provinces with
daily updates covering all corporation types including:
- Société par actions (S.A.) - Business corporations
- Société en nom collectif (S.E.N.C.) - General partnerships
- Coopérative - Cooperatives
- OBNL / Organisme sans but lucratif - Non-profit organizations
- Association - Associations

Key identifier: NEQ (Numéro d'entreprise du Québec)

Note: The data is provided as a ZIP file containing 6 CSV files.
The main file used is the enterprise identification file.
"""

import io
import zipfile
from datetime import date, datetime
from typing import Any, Literal
from uuid import UUID

from .base import BaseProvincialCorpIngester
from .models import Address, ProvincialCorporationRecord, ProvincialCorpStatus, ProvincialCorpType

# Check for Playwright availability
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# Quebec Enterprise Registry data download
# NOTE: The data is now provided as a ZIP file containing 6 CSV files
# The ASPX endpoint serves a downloadable ZIP archive
QUEBEC_DATA_URL = "https://www.registreentreprises.gouv.qc.ca/RQAnonymeGR/GR/GR03/GR03A2_22A_PIU_RecupDonnPub_PC/FichierDonneesOuvertes.aspx"

# Expected column headers for validation
QUEBEC_EXPECTED_COLUMNS = [
    "NEQ",
    "Nom",
    "Autre nom",  # English name or alternate name
    "Type personne",  # Entity type
    "Régime juridique",  # Legal regime
    "Forme juridique",  # Legal form
    "État",  # Status
    "Date d'immatriculation",  # Registration date
    "Adresse - Rue",
    "Adresse - Ville",
    "Adresse - Province",
    "Adresse - Code postal",
]


class QuebecCorporationIngester(BaseProvincialCorpIngester):
    """Ingester for Quebec's Enterprise Registry (Registraire des Entreprises).

    Downloads and parses the CSV file from Données Québec,
    extracting corporation information with bilingual name handling.

    Usage:
        ingester = QuebecCorporationIngester()
        result = await ingester.run(IngestionConfig(incremental=True))
    """

    @property
    def province(self) -> str:
        """Return the Quebec province code."""
        return "QC"

    @property
    def data_format(self) -> Literal["csv"]:
        """Return the data format (CSV)."""
        return "csv"

    def get_data_url(self) -> str:
        """Return the URL to download Quebec enterprise data."""
        return QUEBEC_DATA_URL

    def get_expected_columns(self) -> list[str] | None:
        """Return expected column names for validation."""
        # Return None to skip strict validation since Quebec data may vary
        # We'll handle missing columns gracefully in parse_record
        return None

    def get_csv_encoding(self) -> str:
        """Return CSV encoding (UTF-8 for French characters)."""
        return "utf-8"

    def get_csv_delimiter(self) -> str:
        """Return CSV delimiter (comma)."""
        return ","

    async def download_data(self) -> bytes:
        """Download and extract the Quebec enterprise data from ZIP.

        The Quebec data is provided as a ZIP file containing multiple CSV files.
        This method downloads the ZIP using Playwright (due to bot protection)
        and extracts the main enterprise file.

        Returns:
            Raw bytes of the extracted CSV file
        """
        url = self.get_data_url()
        self.logger.info(f"Downloading {self.province} data from {url}")

        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError(
                "Playwright is required for Quebec ingestion due to bot protection. "
                "Install with: pip install playwright && playwright install chromium"
            )

        from playwright.async_api import async_playwright
        import tempfile
        import os

        zip_data = None

        async with async_playwright() as p:
            self.logger.info("Launching browser for Quebec download...")

            # Launch browser
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                accept_downloads=True,
            )

            page = await context.new_page()

            try:
                # Navigate to the download page
                self.logger.info(f"Navigating to {url}")

                # For direct download URLs, the navigation will be aborted as the
                # browser immediately triggers a download. We set up expect_download
                # first and don't wait for the page to load.
                async with page.expect_download(timeout=60000) as download_info:
                    # Don't use wait_until since navigation will be aborted
                    # The download starts immediately when hitting this ASPX URL
                    try:
                        await page.goto(url, timeout=60000)
                    except Exception as nav_error:
                        # Navigation may fail with ERR_ABORTED for direct downloads
                        # This is expected - the download should still trigger
                        self.logger.debug(f"Navigation aborted (expected for direct download): {nav_error}")

                download = await download_info.value
                self.logger.info(f"Download started: {download.suggested_filename}")

                # Save to temp file
                with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
                    tmp_path = tmp.name

                await download.save_as(tmp_path)
                self.logger.info(f"Downloaded to {tmp_path}")

                # Read the file
                with open(tmp_path, "rb") as f:
                    zip_data = f.read()

                # Clean up
                os.unlink(tmp_path)

            finally:
                await browser.close()

        if not zip_data:
            raise ValueError("Failed to download Quebec data")

        # Extract the main enterprise CSV from the ZIP
        try:
            with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
                # List all files in the ZIP
                file_list = zf.namelist()
                self.logger.info(f"ZIP contains {len(file_list)} files: {file_list}")

                # Look for the main enterprise file (typically contains "entreprise" or "identification")
                main_file = None
                for name in file_list:
                    name_lower = name.lower()
                    if name_lower.endswith('.csv'):
                        # Prefer files with identification/enterprise keywords
                        if any(kw in name_lower for kw in ['identification', 'entreprise', 'etablissement']):
                            main_file = name
                            break

                # Fallback to first CSV if no specific match
                if not main_file:
                    for name in file_list:
                        if name.lower().endswith('.csv'):
                            main_file = name
                            break

                if not main_file:
                    raise ValueError(f"No CSV file found in Quebec data ZIP. Files: {file_list}")

                self.logger.info(f"Extracting main file: {main_file}")
                return zf.read(main_file)

        except zipfile.BadZipFile as e:
            self.logger.error(f"Invalid ZIP file from Quebec: {e}")
            raise ValueError(f"Quebec data is not a valid ZIP file: {e}")

    def parse_record(self, row: tuple) -> ProvincialCorporationRecord | None:
        """Parse a single row from the Quebec CSV file.

        Expected columns (approximate indices may vary):
        0: NEQ (Quebec Enterprise Number)
        1: Nom (Legal name in French)
        2: Autre nom (Alternative/English name)
        3: Type personne (Entity type)
        4: Régime juridique (Legal regime)
        5: Forme juridique (Legal form)
        6: État (Status)
        7: Date d'immatriculation (Registration date)
        8-11: Address fields

        Args:
            row: Tuple of values from the CSV row

        Returns:
            ProvincialCorporationRecord if valid, None to skip the row
        """
        if len(row) < 8:
            return None

        # Extract fields (indices based on typical Quebec CSV structure)
        neq = str(row[0]).strip() if row[0] else None
        name_french = str(row[1]).strip() if row[1] else None
        name_english = str(row[2]).strip() if len(row) > 2 and row[2] else None
        entity_type = str(row[3]).strip() if len(row) > 3 and row[3] else ""
        legal_regime = str(row[4]).strip() if len(row) > 4 and row[4] else ""
        legal_form = str(row[5]).strip() if len(row) > 5 and row[5] else ""
        status = str(row[6]).strip() if len(row) > 6 and row[6] else "unknown"
        reg_date_str = str(row[7]).strip() if len(row) > 7 and row[7] else None

        # Skip rows with missing required fields
        if not neq or not name_french:
            return None

        # Use French name as primary, English as secondary
        primary_name = name_french

        # Parse registration date
        incorporation_date = self._parse_date(reg_date_str)

        # Build address if available
        address = None
        if len(row) > 11:
            street = str(row[8]).strip() if row[8] else None
            city = str(row[9]).strip() if row[9] else None
            province = str(row[10]).strip() if row[10] else "QC"
            postal = str(row[11]).strip() if row[11] else None

            if city or postal:
                address = Address(
                    street_address=street,
                    city=city,
                    province=province,
                    postal_code=postal,
                )

        # Combine legal form/regime for type classification
        corp_type_raw = f"{legal_form} ({legal_regime})" if legal_regime else legal_form

        return ProvincialCorporationRecord(
            name=primary_name,
            name_french=name_french if name_french != primary_name else None,
            registration_number=neq,
            business_number=None,  # Quebec doesn't always include BN in bulk data
            corp_type_raw=corp_type_raw or entity_type or "unknown",
            status_raw=status,
            incorporation_date=incorporation_date,
            jurisdiction=self.province,
            registered_address=address,
            source_url=self.get_data_url(),
        )

    def _parse_date(self, value: str | None) -> date | None:
        """Parse a date value from the Quebec CSV.

        Quebec typically uses YYYY-MM-DD format.

        Args:
            value: Date string

        Returns:
            Parsed date or None if invalid/empty
        """
        if not value:
            return None

        value_str = str(value).strip()
        if not value_str:
            return None

        # Quebec date formats
        formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(value_str, fmt).date()
            except ValueError:
                continue

        return None

    def map_corp_type(self, raw: str) -> ProvincialCorpType:
        """Map Quebec corporation type to standard classification.

        Quebec legal forms:
        - Société par actions (S.A.) -> FOR_PROFIT
        - Société en nom collectif (S.E.N.C.) -> FOR_PROFIT
        - Société en commandite -> FOR_PROFIT
        - Coopérative -> COOPERATIVE
        - OBNL / Organisme sans but lucratif -> NONPROFIT
        - Personne morale sans but lucratif -> NONPROFIT
        - Association -> NONPROFIT

        Args:
            raw: Raw corporation type string

        Returns:
            ProvincialCorpType enum value
        """
        raw_lower = raw.lower()

        # Cooperatives
        if "coopérative" in raw_lower or "coop" in raw_lower:
            return ProvincialCorpType.COOPERATIVE

        # Non-profits
        if any(kw in raw_lower for kw in [
            "obnl", "sans but lucratif", "organisme", "association",
            "fondation", "syndicat", "non lucratif"
        ]):
            return ProvincialCorpType.NONPROFIT

        # For-profit corporations
        if any(kw in raw_lower for kw in [
            "société par actions", "s.a.", "société en nom collectif",
            "s.e.n.c.", "société en commandite", "compagnie", "inc.",
            "ltée", "limitée", "business"
        ]):
            return ProvincialCorpType.FOR_PROFIT

        # Professional corporations
        if "professionnel" in raw_lower:
            return ProvincialCorpType.PROFESSIONAL

        # Extraprovincial
        if "extraprovincial" in raw_lower or "étranger" in raw_lower:
            return ProvincialCorpType.EXTRAPROVINCIAL

        return ProvincialCorpType.UNKNOWN

    def map_status(self, raw: str) -> ProvincialCorpStatus:
        """Map Quebec status to standard classification.

        Quebec status values:
        - Immatriculée -> ACTIVE
        - Radiée -> DISSOLVED
        - Radiée d'office -> STRUCK
        - Fusionnée -> AMALGAMATED
        - Continuée -> CONTINUED_OUT
        - Dissoute -> DISSOLVED

        Args:
            raw: Raw status string

        Returns:
            ProvincialCorpStatus enum value
        """
        raw_lower = raw.lower()

        if "immatriculée" in raw_lower or "active" in raw_lower:
            return ProvincialCorpStatus.ACTIVE
        elif "radiée d'office" in raw_lower:
            return ProvincialCorpStatus.STRUCK
        elif "radiée" in raw_lower or "dissoute" in raw_lower:
            return ProvincialCorpStatus.DISSOLVED
        elif "fusionnée" in raw_lower:
            return ProvincialCorpStatus.AMALGAMATED
        elif "continuée" in raw_lower:
            return ProvincialCorpStatus.CONTINUED_OUT
        elif "suspendue" in raw_lower:
            return ProvincialCorpStatus.SUSPENDED
        elif "révoquée" in raw_lower:
            return ProvincialCorpStatus.REVOKED

        return ProvincialCorpStatus.UNKNOWN


async def run_quebec_corps_ingestion(
    incremental: bool = True,
    limit: int | None = None,
    target_entities: list[str] | None = None,
    run_id: UUID | None = None,
) -> dict[str, Any]:
    """Run Quebec corporation ingestion.

    Main entry point for running the Quebec ingester. Can be called
    from CLI, API, or directly from Python code.

    Args:
        incremental: Use incremental sync (only changed records)
        limit: Maximum records to process (for testing)
        target_entities: Specific organization names to ingest
        run_id: Optional run ID from API layer

    Returns:
        Ingestion result dictionary with statistics

    Example:
        result = await run_quebec_corps_ingestion(
            incremental=True,
            limit=100,
        )
        print(f"Processed: {result['records_processed']}")
    """
    from ..base import IngestionConfig

    ingester = QuebecCorporationIngester()

    config = IngestionConfig(
        incremental=incremental,
        limit=limit,
        target_entities=target_entities,
    )

    result = await ingester.run(config, run_id=run_id)

    return {
        "run_id": str(result.run_id),
        "source": result.source,
        "status": result.status,
        "started_at": result.started_at.isoformat(),
        "completed_at": result.completed_at.isoformat() if result.completed_at else None,
        "records_processed": result.records_processed,
        "records_created": result.records_created,
        "records_updated": result.records_updated,
        "duplicates_found": result.duplicates_found,
        "errors": result.errors,
    }
