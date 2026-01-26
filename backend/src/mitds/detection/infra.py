"""Infrastructure sharing detection for MITDS.

Provides detection of shared technical infrastructure across outlets:
- WHOIS/DNS lookup for domain registration details
- Hosting provider detection via IP and ASN analysis
- Analytics tag detection (Google Analytics, GTM, Facebook Pixel)
- Shared infrastructure scoring and relationship creation
"""

import asyncio
import hashlib
import re
import socket
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

import httpx


class InfraSignalType(str, Enum):
    """Types of infrastructure signals that indicate sharing."""

    SAME_REGISTRAR = "same_registrar"
    SAME_NAMESERVER = "same_nameserver"
    SAME_IP = "same_ip"
    SAME_ASN = "same_asn"
    SAME_HOSTING = "same_hosting"
    SAME_CDN = "same_cdn"
    SAME_ANALYTICS = "same_analytics"
    SAME_GTM = "same_gtm"
    SAME_PIXEL = "same_pixel"
    SAME_ADSENSE = "same_adsense"
    SAME_SSL_ISSUER = "same_ssl_issuer"
    SAME_CMS = "same_cms"
    SSL_SAN_OVERLAP = "ssl_san_overlap"


@dataclass
class InfraSignal:
    """A single infrastructure sharing signal."""

    signal_type: InfraSignalType
    value: str
    weight: float = 1.0
    description: str = ""


@dataclass
class DNSResult:
    """DNS lookup result for a domain."""

    domain: str
    nameservers: list[str] = field(default_factory=list)
    a_records: list[str] = field(default_factory=list)
    aaaa_records: list[str] = field(default_factory=list)
    mx_records: list[str] = field(default_factory=list)
    txt_records: list[str] = field(default_factory=list)
    cname: str | None = None
    error: str | None = None


@dataclass
class WHOISResult:
    """WHOIS lookup result for a domain."""

    domain: str
    registrar: str | None = None
    registration_date: datetime | None = None
    expiry_date: datetime | None = None
    nameservers: list[str] = field(default_factory=list)
    registrant_name: str | None = None
    registrant_org: str | None = None
    registrant_country: str | None = None
    admin_email: str | None = None
    raw_text: str | None = None
    error: str | None = None


@dataclass
class HostingResult:
    """Hosting provider detection result."""

    ip_address: str
    asn: str | None = None
    asn_name: str | None = None
    hosting_provider: str | None = None
    cdn_provider: str | None = None
    ip_range: str | None = None
    country: str | None = None
    is_shared_hosting: bool = False


@dataclass
class AnalyticsResult:
    """Analytics tag detection result."""

    domain: str
    google_analytics_ids: list[str] = field(default_factory=list)
    google_tag_manager_ids: list[str] = field(default_factory=list)
    facebook_pixel_ids: list[str] = field(default_factory=list)
    adsense_ids: list[str] = field(default_factory=list)
    other_trackers: dict[str, list[str]] = field(default_factory=dict)
    cms_detected: str | None = None
    technologies: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass
class SSLResult:
    """SSL certificate analysis result."""

    domain: str
    issuer: str | None = None
    subject_alt_names: list[str] = field(default_factory=list)
    valid_from: datetime | None = None
    valid_until: datetime | None = None
    fingerprint: str | None = None
    error: str | None = None


@dataclass
class InfrastructureProfile:
    """Complete infrastructure profile for a domain."""

    domain: str
    scanned_at: datetime = field(default_factory=datetime.utcnow)
    dns: DNSResult | None = None
    whois: WHOISResult | None = None
    hosting: list[HostingResult] = field(default_factory=list)
    analytics: AnalyticsResult | None = None
    ssl: SSLResult | None = None


@dataclass
class SharedInfrastructureMatch:
    """Match result between two domains sharing infrastructure."""

    domain_a: str
    domain_b: str
    signals: list[InfraSignal] = field(default_factory=list)
    total_score: float = 0.0
    confidence: float = 0.0

    def add_signal(self, signal: InfraSignal) -> None:
        """Add a signal and update scores."""
        self.signals.append(signal)
        self.total_score += signal.weight
        self.confidence = min(1.0, self.total_score / 10.0)


class DNSLookupService:
    """Service for performing DNS lookups."""

    def __init__(self, timeout: float = 5.0):
        self.timeout = timeout

    async def lookup(self, domain: str) -> DNSResult:
        """Perform DNS lookup for a domain."""
        result = DNSResult(domain=domain)

        try:
            loop = asyncio.get_event_loop()

            # A records
            try:
                a_records = await loop.run_in_executor(
                    None, lambda: socket.getaddrinfo(domain, None, socket.AF_INET)
                )
                result.a_records = list(set(r[4][0] for r in a_records))
            except socket.gaierror:
                pass

            # AAAA records
            try:
                aaaa_records = await loop.run_in_executor(
                    None, lambda: socket.getaddrinfo(domain, None, socket.AF_INET6)
                )
                result.aaaa_records = list(set(r[4][0] for r in aaaa_records))
            except socket.gaierror:
                pass

            # Try to get nameservers via dnspython if available
            try:
                import dns.resolver

                ns_answers = await loop.run_in_executor(
                    None, lambda: dns.resolver.resolve(domain, "NS")
                )
                result.nameservers = [str(rdata) for rdata in ns_answers]
            except Exception:
                pass

            # Try MX records
            try:
                import dns.resolver

                mx_answers = await loop.run_in_executor(
                    None, lambda: dns.resolver.resolve(domain, "MX")
                )
                result.mx_records = [str(rdata.exchange) for rdata in mx_answers]
            except Exception:
                pass

        except Exception as e:
            result.error = str(e)

        return result


class WHOISLookupService:
    """Service for performing WHOIS lookups."""

    REGISTRAR_PATTERNS = {
        r"godaddy": "GoDaddy",
        r"namecheap": "Namecheap",
        r"cloudflare": "Cloudflare",
        r"google\s*(domains|llc)": "Google Domains",
        r"network\s*solutions": "Network Solutions",
        r"tucows": "Tucows",
        r"gandi": "Gandi",
        r"porkbun": "Porkbun",
        r"hostinger": "Hostinger",
        r"ionos|1&1": "IONOS",
        r"ovh": "OVH",
    }

    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout

    async def lookup(self, domain: str) -> WHOISResult:
        """Perform WHOIS lookup for a domain."""
        result = WHOISResult(domain=domain)

        try:
            import whois

            loop = asyncio.get_event_loop()
            w = await loop.run_in_executor(None, lambda: whois.whois(domain))

            if w:
                result.registrar = self._normalize_registrar(w.registrar)
                result.registration_date = self._parse_date(w.creation_date)
                result.expiry_date = self._parse_date(w.expiration_date)
                result.nameservers = self._normalize_nameservers(w.name_servers)

                if hasattr(w, "name"):
                    result.registrant_name = w.name
                if hasattr(w, "org"):
                    result.registrant_org = w.org
                if hasattr(w, "country"):
                    result.registrant_country = w.country

        except Exception as e:
            result.error = str(e)

        return result

    def _normalize_registrar(self, registrar: str | None) -> str | None:
        if not registrar:
            return None
        registrar_lower = registrar.lower()
        for pattern, canonical in self.REGISTRAR_PATTERNS.items():
            if re.search(pattern, registrar_lower):
                return canonical
        return registrar

    def _parse_date(self, date_value: Any) -> datetime | None:
        if not date_value:
            return None
        if isinstance(date_value, list):
            date_value = date_value[0]
        if isinstance(date_value, datetime):
            return date_value
        return None

    def _normalize_nameservers(self, nameservers: Any) -> list[str]:
        if not nameservers:
            return []
        if isinstance(nameservers, str):
            nameservers = [nameservers]
        return [ns.lower().rstrip(".") for ns in nameservers if ns]


class HostingDetector:
    """Detect hosting providers via IP/ASN analysis."""

    ASN_PROVIDERS = {
        "AS13335": ("Cloudflare", "cdn"),
        "AS16509": ("Amazon AWS", "hosting"),
        "AS15169": ("Google Cloud", "hosting"),
        "AS8075": ("Microsoft Azure", "hosting"),
        "AS20940": ("Akamai", "cdn"),
        "AS54113": ("Fastly", "cdn"),
        "AS14061": ("DigitalOcean", "hosting"),
        "AS63949": ("Linode/Akamai", "hosting"),
        "AS20473": ("Vultr", "hosting"),
        "AS26496": ("GoDaddy", "hosting"),
        "AS16276": ("OVH", "hosting"),
        "AS24940": ("Hetzner", "hosting"),
        "AS397998": ("Vercel", "hosting"),
        "AS209242": ("Netlify", "hosting"),
    }

    IP_PATTERNS = {
        r"^104\.1[6-9]\.": "Cloudflare",
        r"^104\.2[0-7]\.": "Cloudflare",
        r"^13\.[0-9]+\.": "Amazon AWS",
        r"^52\.[0-9]+\.": "Amazon AWS",
        r"^35\.[0-9]+\.": "Google Cloud",
        r"^34\.[0-9]+\.": "Google Cloud",
    }

    def __init__(self, timeout: float = 5.0):
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def detect(self, ip_address: str) -> HostingResult:
        """Detect hosting provider for an IP address."""
        result = HostingResult(ip_address=ip_address)

        for pattern, provider in self.IP_PATTERNS.items():
            if re.match(pattern, ip_address):
                result.hosting_provider = provider
                break

        try:
            if not self._client:
                self._client = httpx.AsyncClient(timeout=self.timeout)

            response = await self._client.get(
                f"http://ip-api.com/json/{ip_address}",
                params={"fields": "status,country,isp,org,as,hosting"},
            )

            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    result.country = data.get("country")

                    as_field = data.get("as", "")
                    if as_field:
                        asn_match = re.match(r"(AS\d+)", as_field)
                        if asn_match:
                            result.asn = asn_match.group(1)

                    result.asn_name = data.get("org") or data.get("isp")
                    result.is_shared_hosting = data.get("hosting", False)

                    if result.asn and result.asn in self.ASN_PROVIDERS:
                        provider, ptype = self.ASN_PROVIDERS[result.asn]
                        if ptype == "cdn":
                            result.cdn_provider = provider
                        else:
                            result.hosting_provider = provider

        except Exception:
            pass

        return result

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None


class AnalyticsDetector:
    """Detect analytics tags and tracking codes from web pages."""

    PATTERNS = {
        "google_analytics": [r"UA-\d{4,10}-\d{1,4}", r"G-[A-Z0-9]{10,}"],
        "google_tag_manager": [r"GTM-[A-Z0-9]{6,}"],
        "facebook_pixel": [r"fbq\s*\(\s*['\"]init['\"]\s*,\s*['\"](\d{15,})['\"]"],
        "adsense": [r"ca-pub-\d{16}"],
    }

    CMS_PATTERNS = {
        r"wp-content|wp-includes": "WordPress",
        r"drupal\.js": "Drupal",
        r"Joomla!": "Joomla",
        r"ghost\.io": "Ghost",
        r"squarespace\.com": "Squarespace",
        r"wix\.com": "Wix",
        r"shopify\.com": "Shopify",
        r"webflow\.com": "Webflow",
    }

    def __init__(self, timeout: float = 15.0, user_agent: str | None = None):
        self.timeout = timeout
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        self._client: httpx.AsyncClient | None = None

    async def detect(self, domain: str) -> AnalyticsResult:
        """Detect analytics tags for a domain."""
        result = AnalyticsResult(domain=domain)

        try:
            if not self._client:
                self._client = httpx.AsyncClient(
                    timeout=self.timeout,
                    headers={"User-Agent": self.user_agent},
                    follow_redirects=True,
                )

            response = await self._client.get(f"https://{domain}")
            html = response.text

            result.google_analytics_ids = self._find_all_patterns(
                html, self.PATTERNS["google_analytics"]
            )
            result.google_tag_manager_ids = self._find_all_patterns(
                html, self.PATTERNS["google_tag_manager"]
            )
            result.facebook_pixel_ids = self._find_all_patterns(
                html, self.PATTERNS["facebook_pixel"]
            )
            result.adsense_ids = self._find_all_patterns(
                html, self.PATTERNS["adsense"]
            )

            for pattern, cms in self.CMS_PATTERNS.items():
                if re.search(pattern, html, re.IGNORECASE):
                    result.cms_detected = cms
                    result.technologies.append(cms)
                    break

            result.technologies.extend(self._detect_technologies(html))

        except Exception as e:
            result.error = str(e)

        return result

    def _find_all_patterns(self, text: str, patterns: list[str]) -> list[str]:
        results = set()
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0] if match else ""
                if match:
                    results.add(match)
        return list(results)

    def _detect_technologies(self, html: str) -> list[str]:
        technologies = []
        tech_patterns = {
            "React": r"react\.production|__REACT_DEVTOOLS",
            "Vue.js": r"vue\.js|__VUE__",
            "Angular": r"ng-app|angular\.js",
            "jQuery": r"jquery\.js|jquery\.min\.js",
            "Next.js": r"_next/static|__NEXT_DATA__",
            "Cloudflare": r"cloudflareinsights|cf-ray",
        }
        for tech, pattern in tech_patterns.items():
            if re.search(pattern, html, re.IGNORECASE):
                technologies.append(tech)
        return technologies

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None


class SSLAnalyzer:
    """Analyze SSL certificates for infrastructure sharing signals."""

    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout

    async def analyze(self, domain: str) -> SSLResult:
        """Analyze SSL certificate for a domain."""
        result = SSLResult(domain=domain)

        try:
            import ssl

            loop = asyncio.get_event_loop()

            def get_cert():
                context = ssl.create_default_context()
                with socket.create_connection((domain, 443), timeout=self.timeout) as sock:
                    with context.wrap_socket(sock, server_hostname=domain) as ssock:
                        return ssock.getpeercert()

            cert = await loop.run_in_executor(None, get_cert)

            if cert:
                issuer = cert.get("issuer", ())
                for item in issuer:
                    for key, value in item:
                        if key == "organizationName":
                            result.issuer = value
                            break

                san = cert.get("subjectAltName", ())
                result.subject_alt_names = [name for _, name in san]

                not_before = cert.get("notBefore")
                not_after = cert.get("notAfter")
                if not_before:
                    result.valid_from = datetime.strptime(
                        not_before, "%b %d %H:%M:%S %Y %Z"
                    )
                if not_after:
                    result.valid_until = datetime.strptime(
                        not_after, "%b %d %H:%M:%S %Y %Z"
                    )

                fingerprint_data = f"{result.issuer}:{','.join(sorted(result.subject_alt_names))}"
                result.fingerprint = hashlib.sha256(fingerprint_data.encode()).hexdigest()[:16]

        except Exception as e:
            result.error = str(e)

        return result


class InfrastructureScorer:
    """Score shared infrastructure between domains."""

    SIGNAL_WEIGHTS = {
        InfraSignalType.SAME_REGISTRAR: 0.5,
        InfraSignalType.SAME_NAMESERVER: 1.5,
        InfraSignalType.SAME_IP: 3.0,
        InfraSignalType.SAME_ASN: 0.5,
        InfraSignalType.SAME_HOSTING: 0.3,
        InfraSignalType.SAME_CDN: 0.2,
        InfraSignalType.SAME_ANALYTICS: 4.0,
        InfraSignalType.SAME_GTM: 4.5,
        InfraSignalType.SAME_PIXEL: 3.5,
        InfraSignalType.SAME_ADSENSE: 5.0,
        InfraSignalType.SAME_SSL_ISSUER: 0.3,
        InfraSignalType.SAME_CMS: 0.2,
        InfraSignalType.SSL_SAN_OVERLAP: 4.0,
    }

    def compare(
        self,
        profile_a: InfrastructureProfile,
        profile_b: InfrastructureProfile,
    ) -> SharedInfrastructureMatch:
        """Compare two infrastructure profiles and return match result."""
        match = SharedInfrastructureMatch(
            domain_a=profile_a.domain,
            domain_b=profile_b.domain,
        )

        if profile_a.whois and profile_b.whois:
            self._compare_whois(profile_a.whois, profile_b.whois, match)

        if profile_a.dns and profile_b.dns:
            self._compare_dns(profile_a.dns, profile_b.dns, match)

        if profile_a.hosting and profile_b.hosting:
            self._compare_hosting(profile_a.hosting, profile_b.hosting, match)

        if profile_a.analytics and profile_b.analytics:
            self._compare_analytics(profile_a.analytics, profile_b.analytics, match)

        if profile_a.ssl and profile_b.ssl:
            self._compare_ssl(profile_a.ssl, profile_b.ssl, match)

        return match

    def _compare_whois(
        self, a: WHOISResult, b: WHOISResult, match: SharedInfrastructureMatch
    ) -> None:
        if a.registrar and b.registrar and a.registrar == b.registrar:
            match.add_signal(InfraSignal(
                signal_type=InfraSignalType.SAME_REGISTRAR,
                value=a.registrar,
                weight=self.SIGNAL_WEIGHTS[InfraSignalType.SAME_REGISTRAR],
                description=f"Same registrar: {a.registrar}",
            ))

        ns_overlap = set(a.nameservers) & set(b.nameservers)
        for ns in ns_overlap:
            match.add_signal(InfraSignal(
                signal_type=InfraSignalType.SAME_NAMESERVER,
                value=ns,
                weight=self.SIGNAL_WEIGHTS[InfraSignalType.SAME_NAMESERVER],
                description=f"Shared nameserver: {ns}",
            ))

    def _compare_dns(
        self, a: DNSResult, b: DNSResult, match: SharedInfrastructureMatch
    ) -> None:
        ip_overlap = set(a.a_records) & set(b.a_records)
        for ip in ip_overlap:
            match.add_signal(InfraSignal(
                signal_type=InfraSignalType.SAME_IP,
                value=ip,
                weight=self.SIGNAL_WEIGHTS[InfraSignalType.SAME_IP],
                description=f"Same IP address: {ip}",
            ))

    def _compare_hosting(
        self,
        a: list[HostingResult],
        b: list[HostingResult],
        match: SharedInfrastructureMatch,
    ) -> None:
        a_asns = {h.asn for h in a if h.asn}
        b_asns = {h.asn for h in b if h.asn}
        asn_overlap = a_asns & b_asns

        for asn in asn_overlap:
            match.add_signal(InfraSignal(
                signal_type=InfraSignalType.SAME_ASN,
                value=asn,
                weight=self.SIGNAL_WEIGHTS[InfraSignalType.SAME_ASN],
                description=f"Same ASN: {asn}",
            ))

        a_hosts = {h.hosting_provider for h in a if h.hosting_provider and not h.is_shared_hosting}
        b_hosts = {h.hosting_provider for h in b if h.hosting_provider and not h.is_shared_hosting}
        host_overlap = a_hosts & b_hosts

        for host in host_overlap:
            match.add_signal(InfraSignal(
                signal_type=InfraSignalType.SAME_HOSTING,
                value=host,
                weight=self.SIGNAL_WEIGHTS[InfraSignalType.SAME_HOSTING],
                description=f"Same hosting provider: {host}",
            ))

    def _compare_analytics(
        self,
        a: AnalyticsResult,
        b: AnalyticsResult,
        match: SharedInfrastructureMatch,
    ) -> None:
        ga_overlap = set(a.google_analytics_ids) & set(b.google_analytics_ids)
        for ga_id in ga_overlap:
            match.add_signal(InfraSignal(
                signal_type=InfraSignalType.SAME_ANALYTICS,
                value=ga_id,
                weight=self.SIGNAL_WEIGHTS[InfraSignalType.SAME_ANALYTICS],
                description=f"Same Google Analytics ID: {ga_id}",
            ))

        gtm_overlap = set(a.google_tag_manager_ids) & set(b.google_tag_manager_ids)
        for gtm_id in gtm_overlap:
            match.add_signal(InfraSignal(
                signal_type=InfraSignalType.SAME_GTM,
                value=gtm_id,
                weight=self.SIGNAL_WEIGHTS[InfraSignalType.SAME_GTM],
                description=f"Same GTM container: {gtm_id}",
            ))

        pixel_overlap = set(a.facebook_pixel_ids) & set(b.facebook_pixel_ids)
        for pixel_id in pixel_overlap:
            match.add_signal(InfraSignal(
                signal_type=InfraSignalType.SAME_PIXEL,
                value=pixel_id,
                weight=self.SIGNAL_WEIGHTS[InfraSignalType.SAME_PIXEL],
                description=f"Same Facebook Pixel: {pixel_id}",
            ))

        adsense_overlap = set(a.adsense_ids) & set(b.adsense_ids)
        for adsense_id in adsense_overlap:
            match.add_signal(InfraSignal(
                signal_type=InfraSignalType.SAME_ADSENSE,
                value=adsense_id,
                weight=self.SIGNAL_WEIGHTS[InfraSignalType.SAME_ADSENSE],
                description=f"Same AdSense publisher: {adsense_id}",
            ))

        if a.cms_detected and b.cms_detected and a.cms_detected == b.cms_detected:
            match.add_signal(InfraSignal(
                signal_type=InfraSignalType.SAME_CMS,
                value=a.cms_detected,
                weight=self.SIGNAL_WEIGHTS[InfraSignalType.SAME_CMS],
                description=f"Same CMS: {a.cms_detected}",
            ))

    def _compare_ssl(
        self, a: SSLResult, b: SSLResult, match: SharedInfrastructureMatch
    ) -> None:
        if a.issuer and b.issuer and a.issuer == b.issuer:
            match.add_signal(InfraSignal(
                signal_type=InfraSignalType.SAME_SSL_ISSUER,
                value=a.issuer,
                weight=self.SIGNAL_WEIGHTS[InfraSignalType.SAME_SSL_ISSUER],
                description=f"Same SSL issuer: {a.issuer}",
            ))

        san_overlap = set(a.subject_alt_names) & set(b.subject_alt_names)
        san_overlap -= {a.domain, b.domain, f"*.{a.domain}", f"*.{b.domain}"}
        if san_overlap:
            for san in san_overlap:
                match.add_signal(InfraSignal(
                    signal_type=InfraSignalType.SSL_SAN_OVERLAP,
                    value=san,
                    weight=self.SIGNAL_WEIGHTS[InfraSignalType.SSL_SAN_OVERLAP],
                    description=f"SSL SAN overlap: {san}",
                ))


class InfrastructureDetector:
    """Main detector class for infrastructure analysis."""

    def __init__(
        self,
        dns_service: DNSLookupService | None = None,
        whois_service: WHOISLookupService | None = None,
        hosting_detector: HostingDetector | None = None,
        analytics_detector: AnalyticsDetector | None = None,
        ssl_analyzer: SSLAnalyzer | None = None,
        scorer: InfrastructureScorer | None = None,
    ):
        self.dns = dns_service or DNSLookupService()
        self.whois = whois_service or WHOISLookupService()
        self.hosting = hosting_detector or HostingDetector()
        self.analytics = analytics_detector or AnalyticsDetector()
        self.ssl = ssl_analyzer or SSLAnalyzer()
        self.scorer = scorer or InfrastructureScorer()

    async def analyze_domain(self, domain: str) -> InfrastructureProfile:
        """Perform full infrastructure analysis on a domain."""
        profile = InfrastructureProfile(domain=domain)

        dns_task = self.dns.lookup(domain)
        whois_task = self.whois.lookup(domain)
        analytics_task = self.analytics.detect(domain)
        ssl_task = self.ssl.analyze(domain)

        results = await asyncio.gather(
            dns_task, whois_task, analytics_task, ssl_task,
            return_exceptions=True,
        )

        if isinstance(results[0], DNSResult):
            profile.dns = results[0]

            hosting_tasks = [
                self.hosting.detect(ip) for ip in profile.dns.a_records[:5]
            ]
            if hosting_tasks:
                hosting_results = await asyncio.gather(
                    *hosting_tasks, return_exceptions=True
                )
                profile.hosting = [
                    r for r in hosting_results if isinstance(r, HostingResult)
                ]

        if isinstance(results[1], WHOISResult):
            profile.whois = results[1]

        if isinstance(results[2], AnalyticsResult):
            profile.analytics = results[2]

        if isinstance(results[3], SSLResult):
            profile.ssl = results[3]

        return profile

    async def find_shared_infrastructure(
        self,
        domains: list[str],
        min_score: float = 1.0,
    ) -> list[SharedInfrastructureMatch]:
        """Find shared infrastructure across a list of domains."""
        profiles = {}
        tasks = [self.analyze_domain(domain) for domain in domains]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for domain, result in zip(domains, results):
            if isinstance(result, InfrastructureProfile):
                profiles[domain] = result

        matches = []
        domain_list = list(profiles.keys())

        for i, domain_a in enumerate(domain_list):
            for domain_b in domain_list[i + 1:]:
                match = self.scorer.compare(
                    profiles[domain_a],
                    profiles[domain_b],
                )
                if match.total_score >= min_score:
                    matches.append(match)

        matches.sort(key=lambda m: m.total_score, reverse=True)
        return matches

    async def close(self) -> None:
        """Clean up resources."""
        await self.hosting.close()
        await self.analytics.close()


async def create_shared_infra_relationships(
    matches: list[SharedInfrastructureMatch],
    domain_to_outlet: dict[str, UUID],
    min_confidence: float = 0.3,
) -> list[dict[str, Any]]:
    """Create SHARED_INFRA relationships from detected infrastructure matches."""
    from ..graph.builder import GraphBuilder

    created = []
    builder = GraphBuilder()

    for match in matches:
        if match.confidence < min_confidence:
            continue

        outlet_a = domain_to_outlet.get(match.domain_a)
        outlet_b = domain_to_outlet.get(match.domain_b)

        if not outlet_a or not outlet_b:
            continue

        properties = {
            "shared_signals": [
                {
                    "type": sig.signal_type.value,
                    "value": sig.value,
                    "weight": sig.weight,
                }
                for sig in match.signals
            ],
            "total_score": match.total_score,
            "detection_timestamp": datetime.utcnow().isoformat(),
        }

        signal_types = {sig.signal_type for sig in match.signals}
        if InfraSignalType.SAME_ANALYTICS in signal_types or \
           InfraSignalType.SAME_GTM in signal_types or \
           InfraSignalType.SAME_ADSENSE in signal_types:
            properties["sharing_category"] = "analytics"
        elif InfraSignalType.SAME_IP in signal_types:
            properties["sharing_category"] = "hosting"
        elif InfraSignalType.SSL_SAN_OVERLAP in signal_types:
            properties["sharing_category"] = "certificate"
        else:
            properties["sharing_category"] = "infrastructure"

        result = await builder.create_shared_infra_relationship(
            source_id=outlet_a,
            target_id=outlet_b,
            confidence=match.confidence,
            properties=properties,
        )

        created.append({
            "domain_a": match.domain_a,
            "domain_b": match.domain_b,
            "outlet_a": str(outlet_a),
            "outlet_b": str(outlet_b),
            "confidence": match.confidence,
            "signals_count": len(match.signals),
            "relationship_id": str(result.relationship_id) if result else None,
        })

    return created
