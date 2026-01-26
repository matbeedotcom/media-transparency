#!/usr/bin/env python3
"""
Quick API test script for MITDS.

Tests basic API endpoints to verify the system is working.
"""

import sys

import httpx

BASE_URL = "http://localhost:8000/api/v1"
ROOT_URL = "http://localhost:8000"


def test_health() -> bool:
    """Test health endpoint."""
    try:
        response = httpx.get(f"{ROOT_URL}/health", timeout=5.0)
        if response.status_code == 200:
            print("  \033[92m[OK]\033[0m Health check: OK")
            return True
        print(f"  \033[91m[FAIL]\033[0m Health check: {response.status_code}")
        return False
    except httpx.ConnectError:
        print("  \033[91m[FAIL]\033[0m Health check: Cannot connect to API")
        return False
    except Exception as e:
        print(f"  \033[91m[FAIL]\033[0m Health check: {e}")
        return False


def test_entity_search() -> bool:
    """Test entity search endpoint."""
    try:
        response = httpx.get(
            f"{BASE_URL}/entities",
            params={"q": "Alpha", "limit": 5},
            timeout=10.0,
        )
        if response.status_code == 200:
            data = response.json()
            count = data.get("total", 0)
            print(f"  \033[92m[OK]\033[0m Entity search: Found {count} results for 'Alpha'")
            return True
        print(f"  \033[91m[FAIL]\033[0m Entity search: {response.status_code}")
        return False
    except Exception as e:
        print(f"  \033[91m[FAIL]\033[0m Entity search: {e}")
        return False


def test_funding_clusters() -> bool:
    """Test funding clusters endpoint."""
    try:
        response = httpx.get(
            f"{BASE_URL}/relationships/funding-clusters",
            params={"min_shared_funders": 2},
            timeout=10.0,
        )
        if response.status_code == 200:
            data = response.json()
            count = len(data.get("clusters", []))
            print(f"  \033[92m[OK]\033[0m Funding clusters: Found {count} clusters")
            return True
        print(f"  \033[91m[FAIL]\033[0m Funding clusters: {response.status_code}")
        return False
    except Exception as e:
        print(f"  \033[91m[FAIL]\033[0m Funding clusters: {e}")
        return False


def test_ingestion_status() -> bool:
    """Test ingestion status endpoint."""
    try:
        response = httpx.get(f"{BASE_URL}/ingestion/status", timeout=5.0)
        if response.status_code == 200:
            data = response.json()
            sources = data.get("sources", [])
            print(f"  \033[92m[OK]\033[0m Ingestion status: {len(sources)} sources configured")
            return True
        print(f"  \033[91m[FAIL]\033[0m Ingestion status: {response.status_code}")
        return False
    except Exception as e:
        print(f"  \033[91m[FAIL]\033[0m Ingestion status: {e}")
        return False


def main() -> int:
    """Run all API tests."""
    print("\n" + "=" * 60)
    print("MITDS API Tests")
    print("=" * 60 + "\n")

    tests = [
        ("Health Check", test_health),
        ("Entity Search", test_entity_search),
        ("Funding Clusters", test_funding_clusters),
        ("Ingestion Status", test_ingestion_status),
    ]

    results = []
    for name, test_func in tests:
        print(f"Testing {name}...")
        results.append(test_func())

    passed = sum(results)
    total = len(results)

    print()
    if passed == total:
        print(f"\033[92mAll {total} tests passed!\033[0m\n")
        return 0
    else:
        print(f"\033[91m{passed}/{total} tests passed\033[0m")
        print("\nMake sure the API server is running:")
        print("  cd backend && uvicorn main:app --reload\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
