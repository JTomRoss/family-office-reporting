"""Quick smoke test for Goldman Sachs parsers."""
from pathlib import Path
from parsers.goldman_sachs.etf import GoldmanSachsEtfParser
from parsers.goldman_sachs.custody import GoldmanSachsCustodyParser

# ETF
print("=" * 60)
print("GOLDMAN SACHS ETF")
print("=" * 60)
p = GoldmanSachsEtfParser()
fp = Path("documentos/Cartolas/202512 Boatview - ETF - GoldmanSachs.pdf")
score = p.detect(fp)
print(f"Detection score: {score}")
result = p.safe_parse(fp)
print(f"Status: {result.status}")
print(f"Account: {result.account_number}")
print(f"Currency: {result.currency}")
b = result.balances or {}
print(f"Total portfolio: {b.get('total_portfolio')}")
print(f"Period: {b.get('period')}")
act = b.get("portfolio_activity", {})
print(f"Opening: {act.get('opening_value')}")
print(f"Closing: {act.get('closing_value')}")
alloc = b.get("asset_allocation", {})
print(f"Asset allocation categories: {len(alloc)}")
for cat, data in alloc.items():
    print(f"  {cat}: ${data.get('market_value')}")
print(f"Rows (holdings): {len(result.rows)}")
for r2 in result.rows:
    print(f"  - {r2.data.get('name', '?')} = ${r2.data.get('market_value', '?')}")
print(f"Warnings: {result.warnings}")

# Custody
print()
print("=" * 60)
print("GOLDMAN SACHS CUSTODY (MANDATO)")
print("=" * 60)
p2 = GoldmanSachsCustodyParser()
fp2 = Path("documentos/Cartolas/202512 Boatview - Mandato - GoldmanSachs.pdf")
score2 = p2.detect(fp2)
print(f"Detection score: {score2}")
result2 = p2.safe_parse(fp2)
print(f"Status: {result2.status}")
print(f"Account: {result2.account_number}")
b2 = result2.balances or {}
print(f"Total portfolio: {b2.get('total_portfolio')}")
print(f"Period: {b2.get('period')}")
subs = b2.get("sub_portfolios", [])
print(f"Sub-portfolios: {len(subs)}")
for sp in subs:
    print(f"  - {sp.get('name')} ({sp.get('number')})")
act2 = b2.get("portfolio_activity", {})
print(f"Opening: {act2.get('opening_value')}")
print(f"Closing: {act2.get('closing_value')}")
print(f"Rows (holdings): {len(result2.rows)}")
q = result2.qualitative_data or {}
sub_ov = q.get("sub_portfolio_overviews", [])
print(f"Sub-portfolio overviews extracted: {len(sub_ov)}")
for so in sub_ov[:3]:
    print(f"  {so.get('portfolio_name')}: ${so.get('total_portfolio')}")
print(f"Warnings: {result2.warnings}")
