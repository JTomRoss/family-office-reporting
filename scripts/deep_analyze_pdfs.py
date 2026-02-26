"""Deep analysis of each PDF to find positions/holdings data pages."""
import pdfplumber
import os
import sys

# Fix encoding for Windows console
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

def deep_analyze(path, label):
    sep = '=' * 90
    print(f"\n{sep}")
    print(f"DEEP ANALYSIS: {label}")
    print(f"FILE: {os.path.basename(path)}")
    print(sep)
    
    with pdfplumber.open(path) as pdf:
        print(f"Total pages: {len(pdf.pages)}")
        
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ''
            tables = page.extract_tables() or []
            lines = text.strip().split('\n')
            
            # Skip nearly-empty pages
            if len(lines) <= 2 and not tables:
                continue
            
            # Look for keywords that indicate data pages
            text_lower = text.lower()
            has_positions = any(w in text_lower for w in [
                'position', 'holding', 'portfolio', 'market value',
                'security', 'instrument', 'quantity', 'shares',
                'cusip', 'isin', 'asset allocation', 'summary',
                'beginning', 'ending', 'net asset', 'balance',
                'saldo', 'patrimonio', 'inversiones', 'cartera',
                'acciones', 'fondos', 'bonos', 'renta',
                'liquidity', 'equity', 'fixed income', 'bonds',
                'account summary', 'consolidated', 'statement of assets',
                'total assets', 'net assets'
            ])
            
            if not has_positions and i > 5:
                continue  # Skip non-data pages after page 5
            
            print(f"\n--- Page {i+1} ---")
            print(f"  Lines: {len(lines)}, Tables: {len(tables)}")
            
            # Print all lines for data-relevant pages
            for line in lines:
                print(f"  | {line}")
            
            # Show tables in detail
            for ti, table in enumerate(tables):
                if table:
                    print(f"  TABLE {ti} ({len(table)} rows x {len(table[0]) if table[0] else 0} cols):")
                    for ri, row in enumerate(table[:5]):  # first 5 rows
                        print(f"    [{ri}] {row}")
                    if len(table) > 5:
                        print(f"    ... ({len(table)-5} more rows)")
            
            # Stop after checking enough pages for each doc
            if i > 15:
                remaining = len(pdf.pages) - i - 1
                if remaining > 0:
                    print(f"\n  ... ({remaining} more pages not analyzed)")
                break


if __name__ == "__main__":
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(base)
    
    cartolas = {
        "JPMorgan ETF": "documentos/Cartolas/20251231-statements-0007-ETF - JPMorgan.pdf",
        "JPMorgan Brokerage": "documentos/Cartolas/20251231-statements-9001-Brokerage - JPMorgan.pdf",
        "JPMorgan Mandato": "documentos/Cartolas/20251231-statements-2600-Mandato - JPMorgan.pdf",
        "JPMorgan Mandato Bonos": "documentos/Cartolas/202512 Ect Intl JPM NY BO (1100) - Mandato Bonos - JPMorgan.pdf",
        "BBH Mandato": "documentos/Cartolas/202512 Boatview - Mandato - BBH.pdf",
        "UBS Miami Mandato": "documentos/Cartolas/202512 Boatview UBS Miami (432) - Mandato.pdf",
        "UBS Suiza Mandato": "documentos/Cartolas/202512 Boatview UBS Suiza (Portfolio 1) - Mandato.pdf",
        "BICE Brokerage": "documentos/Cartolas/Cartola Ecoterra Internacional SpA 20251231 - Brokerage - BICE.pdf",
        "GoldmanSachs ETF": "documentos/Cartolas/202512 Boatview - ETF - GoldmanSachs.pdf",
        "GoldmanSachs Mandato": "documentos/Cartolas/202512 Boatview - Mandato - GoldmanSachs.pdf",
    }
    
    # Analyze one at a time specified by argv, or all
    if len(sys.argv) > 1:
        key = sys.argv[1]
        if key in cartolas:
            deep_analyze(cartolas[key], key)
        else:
            print(f"Unknown key: {key}. Available: {list(cartolas.keys())}")
    else:
        for key, path in cartolas.items():
            if os.path.exists(path):
                deep_analyze(path, key)
            else:
                print(f"MISSING: {path}")
