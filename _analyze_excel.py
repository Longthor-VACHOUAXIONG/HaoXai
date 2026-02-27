import pandas as pd
import os

folder = r'D:\MyFiles\Program_Last_version\ViroDB_structure_latest_V - Copy\DataExcel'
out = r'D:\MyFiles\Program_Last_version\ViroDB_structure_latest_V - Copy\_excel_analysis.txt'

with open(out, 'w', encoding='utf-8') as f:
    for fname in sorted(os.listdir(folder)):
        if not fname.endswith(('.xlsx', '.xls')):
            continue
        path = os.path.join(folder, fname)
        try:
            xl = pd.ExcelFile(path, engine='openpyxl')
            f.write(f"\n{'='*80}\n")
            f.write(f"FILE: {fname}\n")
            f.write(f"{'='*80}\n")
            for sname in xl.sheet_names:
                df = pd.read_excel(path, sheet_name=sname, engine='openpyxl')
                cols = list(df.columns)
                f.write(f"\n  Sheet: {sname} ({len(df)} rows, {len(cols)} cols)\n")
                f.write(f"  Columns:\n")
                for i, c in enumerate(cols):
                    dtype = str(df[c].dtype)
                    non_null = df[c].notna().sum()
                    sample_val = ''
                    for v in df[c].dropna().head(1):
                        sample_val = str(v)[:60]
                    f.write(f"    {i+1:3d}. {str(c):<35} type={dtype:<12} non_null={non_null:>5}/{len(df)}  sample='{sample_val}'\n")
                f.write(f"\n")
        except Exception as e:
            f.write(f"\n{'='*80}\n")
            f.write(f"FILE: {fname} - ERROR: {e}\n")
            f.write(f"{'='*80}\n")

print(f"Analysis written to {out}")
