# International Government Hotel Data Sources

Research on government hotel/accommodation registers for international expansion beyond US/Florida.

## Summary

| Country | Source | Quality | Format | Access |
|---------|--------|---------|--------|--------|
| **Ireland** | Fáilte Ireland | ⭐⭐⭐⭐⭐ | CSV | [data.gov.ie](https://data.gov.ie/organization/failte-ireland) |
| **Portugal** | RNT | ⭐⭐⭐⭐ | Web search | [rnt.turismodeportugal.pt](https://rnt.turismodeportugal.pt/) |
| **Australia (NSW)** | Liquor Licence List | ⭐⭐⭐⭐⭐ | CSV, geocoded | [data.nsw.gov.au](https://data.nsw.gov.au/data/dataset/liquor-licence-premises-list) |
| **Australia (VIC)** | Liquor Licences | ⭐⭐⭐⭐ | Excel | [data.vic.gov.au](https://discover.data.vic.gov.au/dataset/victorian-liquor-licences-by-location) |
| **Australia (SA)** | Liquor & Gaming | ⭐⭐⭐⭐ | Excel | [data.sa.gov.au](https://data.sa.gov.au/data/dataset/liquor-gaming-licences) |
| **Australia (All)** | ABN Bulk Extract | ⭐⭐⭐ | CSV | [data.gov.au](https://data.gov.au/data/dataset/abn-bulk-extract) |
| **Spain** | INE Statistics | ⭐⭐ | Stats only | [ine.es](https://www.ine.es/dyngs/INEbase/en/categoria.htm?c=Estadistica_P&cid=1254735576863) |
| **Italy** | BDSR (regional) | ⭐⭐ | Fragmented | Regional portals |
| **France** | Atout France | ⭐⭐ | Classification | [atout-france.fr](https://www.atout-france.fr) |
| **Germany** | IHK | ⭐ | No public data | N/A |
| **UK** | None yet | ⭐ | In development | N/A |

---

## Best Opportunities (Priority 1)

### Ireland - Fáilte Ireland

**The closest equivalent to DBPR in Europe.**

- **URL**: https://data.gov.ie/organization/failte-ireland
- **Format**: CSV download, API available
- **Coverage**: All registered/approved accommodation in Ireland
- **Fields**: Property name, registration number, rating, address
- **Update frequency**: Regular
- **Cost**: Free (Open Data)

**How to access:**
```bash
# Visit data.gov.ie and download CSV directly
# Or use their Open Data API
```

### Portugal - RNT (Registo Nacional de Turismo)

**Centralized national tourism register.**

- **URL**: https://rnt.turismodeportugal.pt/
- **Registers**:
  - RNET - Hotels/resorts (Empreendimentos turísticos)
  - RNAL - Local accommodation (Alojamento Local)
  - RNAVT - Travel agencies
  - RNAAT - Tourism entertainment
- **Access**: Public searchable database
- **Fields**: Name, address, registration number, type

**Scraping approach:**
- Search interface at https://rnt.turismodeportugal.pt/RNT/Pesquisa_AL.aspx
- May need to paginate through results

---

## Australia (Priority 2)

Australia is fragmented by state, but liquor licence registers are excellent hotel data sources since hotels need liquor licenses.

### NSW - Liquor Licence Premises List

**Best Australian source - monthly CSV with geocodes.**

- **URL**: https://data.nsw.gov.au/data/dataset/liquor-licence-premises-list
- **Format**: CSV (monthly updates)
- **Fields**:
  - Licence number, name, licensee name
  - Venue address + **geocodes (lat/lng)**
  - Trading hours
  - Gaming info
  - SA2 area
- **Filter**: By licence type "Hotel"

### Victoria - Liquor Licences by Location

- **URL**: https://discover.data.vic.gov.au/dataset/victorian-liquor-licences-by-location
- **Format**: Excel (monthly snapshots)
- **Coverage**: All active liquor licences in VIC

### South Australia - Liquor & Gaming Licences

- **URL**: https://data.sa.gov.au/data/dataset/liquor-gaming-licences
- **Format**: Excel
- **Search portal**: https://secure.cbs.sa.gov.au/LGPubReg/

### Queensland - OLGR

- **URL**: https://secure.olgr.qld.gov.au/forms/lls
- **Access**: Search portal (no bulk download)
- **Cost**: $44.15/search or annual subscription
- **Contact**: OLGRlicensing@justice.qld.gov.au for bulk data

### National - ABN Bulk Extract

- **URL**: https://data.gov.au/data/dataset/abn-bulk-extract
- **Filter by**: ANZSIC code **4400** (Accommodation)
- **Coverage**: All registered Australian businesses
- **Use case**: Cross-reference with liquor data to get national coverage

---

## Partially Centralized (Priority 3)

### Spain

**No single hotel list, but statistics available.**

- SES.HOSPEDAJES - Guest registration system (not public hotel list)
- INE (Instituto Nacional de Estadística) - Tourism statistics only
- Regional registers exist but fragmented by autonomous community

### Italy

**Becoming centralized but still regional.**

- CIN (Codice Identificativo Nazionale) - National ID being rolled out
- BDSR (National Database) - Active in 10 regions: Abruzzo, Calabria, Liguria, Lombardy, Marche, Molise, Puglia, Sardinia, Sicily, Veneto
- CIR (Regional codes) - Still used in other regions
- Each region has different registration portal

### France

**Voluntary classification system.**

- Atout France publishes classified establishments (1-5 star)
- Classification is voluntary, not mandatory
- Publishes list of classified hotels on their website
- May need to request data directly

---

## Fragmented / Not Available (Skip for Now)

### Germany
- All businesses register with local Gewerbeamt
- Membership in IHK (Chamber of Commerce) is mandatory
- **Data is NOT publicly available**

### UK
- No centralized register currently exists
- Government announced "Tourist Accommodation Registration Scheme" in development
- Expected to launch in future

### Australia (Other States)
- WA, TAS, NT have smaller markets
- NT has some data on data.nt.gov.au

---

## Implementation Approach

### Phase 1: Quick Wins
1. **Ireland** - Download Fáilte Ireland CSV from data.gov.ie
2. **NSW** - Download liquor licence CSV, filter for hotels
3. **Victoria** - Download liquor Excel, filter for hotels
4. **South Australia** - Download liquor Excel

### Phase 2: Scraping Required
1. **Portugal** - Build scraper for RNT search portal
2. **Queensland** - Contact OLGR for bulk access or scrape public search

### Phase 3: National Coverage
1. **Australia** - Download ABN bulk extract, filter by ANZSIC 4400
2. **Cross-reference** with liquor data to dedupe

### Phase 4: Future Expansion
1. Monitor UK registration scheme development
2. Explore Italian regional portals
3. Consider Spain regional registers if high-value market

---

## Data Quality Comparison

| Source | Has Website | Has Phone | Has Geocodes | Has Email |
|--------|-------------|-----------|--------------|-----------|
| Ireland (Fáilte) | Sometimes | Sometimes | No | No |
| Portugal (RNT) | Sometimes | Sometimes | No | No |
| NSW Liquor | No | No | **Yes** | No |
| VIC Liquor | No | No | Partial | No |
| ABN Extract | No | No | No | No |

**Note**: Like DBPR, most government sources don't include websites. Will need Google enrichment step.

---

## Enrichment Pipeline

For all international sources, follow same pattern as DBPR:

```
1. Ingest government data
2. Dedupe against existing hotels
3. Google Places API to get:
   - Website
   - Phone
   - Coordinates (if missing)
   - Rating/reviews
4. Run booking engine detection
5. Export qualified leads
```

---

## Cost Analysis

| Source | Data Cost | Enrichment Cost (est.) |
|--------|-----------|------------------------|
| Ireland | Free | ~$0.02/hotel (Google) |
| Portugal | Free | ~$0.02/hotel |
| NSW | Free | Geocoded already |
| VIC | Free | ~$0.01/hotel |
| SA | Free | ~$0.01/hotel |
| QLD | $44/search or subscription | TBD |

---

## Next Steps

- [ ] Download Ireland Fáilte CSV and analyze fields
- [ ] Download NSW liquor CSV and count hotels
- [ ] Build Portugal RNT scraper
- [ ] Contact Queensland OLGR for pricing on annual subscription
- [ ] Create unified ingestor service for international sources
