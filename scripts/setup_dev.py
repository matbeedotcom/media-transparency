#!/usr/bin/env python3
"""
Development environment setup script for MITDS.

This script:
1. Verifies infrastructure is running
2. Runs database migrations
3. Loads sample data
"""

import asyncio
import subprocess
import sys
from pathlib import Path


async def run_verify() -> bool:
    """Run infrastructure verification."""
    print("\n[1/3] Verifying infrastructure...")
    result = subprocess.run(
        [sys.executable, "verify_infrastructure.py"],
        cwd=Path(__file__).parent,
    )
    return result.returncode == 0


async def run_migrations() -> bool:
    """Run Alembic migrations."""
    print("\n[2/3] Running database migrations...")
    backend_dir = Path(__file__).parent.parent / "backend"

    if not (backend_dir / "migrations").exists():
        print("  \033[93m! Migrations directory not found, skipping\033[0m")
        return True

    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=backend_dir,
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        print("  \033[92m✓ Migrations complete\033[0m")
        return True
    else:
        if "Target database is not up to date" in result.stderr:
            print("  \033[93m! Database already up to date\033[0m")
            return True
        print(f"  \033[91m✗ Migration error: {result.stderr}\033[0m")
        return False


async def run_load_data() -> bool:
    """Load sample data."""
    print("\n[3/3] Loading sample data...")
    result = subprocess.run(
        [sys.executable, "load_sample_data.py"],
        cwd=Path(__file__).parent,
    )
    return result.returncode == 0


async def main() -> int:
    """Run full development setup."""
    print("=" * 60)
    print("MITDS Development Environment Setup")
    print("=" * 60)

    # Step 1: Verify infrastructure
    if not await run_verify():
        print("\n\033[91mSetup failed: Infrastructure not available\033[0m")
        print("\nStart infrastructure with:")
        print("  cd infrastructure && docker-compose up -d")
        return 1

    # Step 2: Run migrations
    if not await run_migrations():
        print("\n\033[91mSetup failed: Migration error\033[0m")
        return 1

    # Step 3: Load sample data
    if not await run_load_data():
        print("\n\033[91mSetup failed: Could not load sample data\033[0m")
        return 1

    print("\n" + "=" * 60)
    print("\033[92mDevelopment environment ready!\033[0m")
    print("=" * 60)
    print("\nStart the backend:")
    print("  cd backend && uvicorn main:app --reload")
    print("\nStart the frontend:")
    print("  cd frontend && npm run dev")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
