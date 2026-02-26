"""Quick analysis of all PDFs in documentos/ to understand structure."""
import pdfplumber
import os

def analyze_dir(directory, label):
    print(f"\n{'#'*80}")
    print(f"# {label}")
    print(f"{'#'*80}")
    
    for fname in sorted(os.listdir(directory)):
        if not fname.endswith('.pdf'):
            continue
        path = os.path.join(directory, fname)
        sep = '=' * 80
        print(f"\n{sep}")
        print(f"FILE: {fname}")
        print(sep)
        
        with pdfplumber.open(path) as pdf:
            print(f"Pages: {len(pdf.pages)}")
            
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ''
                tables = page.extract_tables()
                lines = text.split('\n')
                
                print(f"\n--- Page {i+1} ({len(lines)} lines, {len(tables)} tables) ---")
                
                # Show first 20 lines of text
                for line in lines[:20]:
                    print(f"  {line}")
                if len(lines) > 20:
                    print(f"  ... ({len(lines)-20} more lines)")
                
                # Show table structure
                for ti, table in enumerate(tables):
                    if table and len(table) > 0:
                        headers = table[0][:8] if table[0] else []
                        print(f"  TABLE {ti}: {len(table)} rows")
                        print(f"    headers: {headers}")
                        if len(table) > 1:
                            print(f"    row[1]:  {table[1][:8] if table[1] else []}")
                        if len(table) > 2:
                            print(f"    row[2]:  {table[2][:8] if table[2] else []}")
                
                # Only show first 3 pages per file for brevity
                if i >= 2 and len(pdf.pages) > 3:
                    remaining = len(pdf.pages) - 3
                    print(f"\n  ... ({remaining} more pages not shown)")
                    break


if __name__ == "__main__":
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(base)
    
    analyze_dir("documentos/Cartolas", "CARTOLAS (Bank Statements)")
    analyze_dir("documentos/Reporting", "REPORTING")
