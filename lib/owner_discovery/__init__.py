"""Owner/decision-maker discovery for hotels.

Waterfall enrichment layers:
  1. RDAP domain lookup (registrant name/email)
  2. Historical WHOIS via Wayback Machine (pre-GDPR registrant data)
  3. DNS intelligence (MX, SOA, SPF → email provider + admin email)
  4. Website scraping (/about, /team, /contact pages + LLM extraction)
  5. Review response mining (Google/TripAdvisor owner responses)
  6. Email pattern discovery + verification (SMTP, O365 autodiscover)
"""
