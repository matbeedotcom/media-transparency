#!/usr/bin/env python3
"""
Infrastructure verification script for MITDS.

Checks that all required services are running and accessible.
"""

import asyncio
import sys
from dataclasses import dataclass
from enum import Enum


class ServiceStatus(Enum):
    OK = "OK"
    ERROR = "ERROR"
    UNKNOWN = "UNKNOWN"


@dataclass
class ServiceCheck:
    name: str
    status: ServiceStatus
    message: str


async def check_postgres() -> ServiceCheck:
    """Check PostgreSQL connectivity."""
    try:
        import asyncpg

        conn = await asyncpg.connect(
            host="localhost",
            port=5432,
            user="mitds",
            password="mitds_dev_password",
            database="mitds",
        )
        version = await conn.fetchval("SELECT version()")
        await conn.close()
        return ServiceCheck("PostgreSQL", ServiceStatus.OK, f"Connected - {version[:50]}...")
    except ImportError:
        return ServiceCheck("PostgreSQL", ServiceStatus.UNKNOWN, "asyncpg not installed")
    except Exception as e:
        return ServiceCheck("PostgreSQL", ServiceStatus.ERROR, str(e))


async def check_neo4j() -> ServiceCheck:
    """Check Neo4j connectivity."""
    try:
        from neo4j import AsyncGraphDatabase

        driver = AsyncGraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", "neo4j_dev_password"),
        )
        async with driver.session() as session:
            result = await session.run("RETURN 1 AS test")
            await result.single()
        await driver.close()
        return ServiceCheck("Neo4j", ServiceStatus.OK, "Connected and responsive")
    except ImportError:
        return ServiceCheck("Neo4j", ServiceStatus.UNKNOWN, "neo4j driver not installed")
    except Exception as e:
        return ServiceCheck("Neo4j", ServiceStatus.ERROR, str(e))


async def check_redis() -> ServiceCheck:
    """Check Redis connectivity."""
    try:
        import redis.asyncio as redis

        client = redis.Redis(host="localhost", port=6379)
        pong = await client.ping()
        await client.close()
        if pong:
            return ServiceCheck("Redis", ServiceStatus.OK, "Connected - PONG received")
        return ServiceCheck("Redis", ServiceStatus.ERROR, "No PONG response")
    except ImportError:
        return ServiceCheck("Redis", ServiceStatus.UNKNOWN, "redis not installed")
    except Exception as e:
        return ServiceCheck("Redis", ServiceStatus.ERROR, str(e))


async def check_minio() -> ServiceCheck:
    """Check MinIO connectivity."""
    try:
        import boto3
        from botocore.config import Config

        client = boto3.client(
            "s3",
            endpoint_url="http://localhost:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            config=Config(signature_version="s3v4"),
        )
        buckets = client.list_buckets()
        bucket_names = [b["Name"] for b in buckets.get("Buckets", [])]
        return ServiceCheck(
            "MinIO",
            ServiceStatus.OK,
            f"Connected - Buckets: {', '.join(bucket_names) or 'none'}",
        )
    except ImportError:
        return ServiceCheck("MinIO", ServiceStatus.UNKNOWN, "boto3 not installed")
    except Exception as e:
        return ServiceCheck("MinIO", ServiceStatus.ERROR, str(e))


def print_result(check: ServiceCheck) -> None:
    """Print a service check result."""
    # Use ASCII-safe symbols for Windows compatibility
    status_symbols = {
        ServiceStatus.OK: "\033[92m[OK]\033[0m",
        ServiceStatus.ERROR: "\033[91m[FAIL]\033[0m",
        ServiceStatus.UNKNOWN: "\033[93m[?]\033[0m",
    }
    symbol = status_symbols[check.status]
    print(f"  {symbol} {check.name}: {check.message}")


async def main() -> int:
    """Run all infrastructure checks."""
    print("\n" + "=" * 60)
    print("MITDS Infrastructure Verification")
    print("=" * 60 + "\n")

    print("Checking services...\n")

    checks = await asyncio.gather(
        check_postgres(),
        check_neo4j(),
        check_redis(),
        check_minio(),
    )

    for check in checks:
        print_result(check)

    print()

    # Summary
    ok_count = sum(1 for c in checks if c.status == ServiceStatus.OK)
    error_count = sum(1 for c in checks if c.status == ServiceStatus.ERROR)

    if error_count > 0:
        print(f"\033[91mResult: {error_count} service(s) not available\033[0m")
        print("\nTo start infrastructure:")
        print("  cd infrastructure")
        print("  docker-compose up -d")
        print()
        return 1
    elif ok_count == len(checks):
        print(f"\033[92mResult: All {ok_count} services operational\033[0m\n")
        return 0
    else:
        print(f"\033[93mResult: {ok_count}/{len(checks)} services verified\033[0m\n")
        return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
