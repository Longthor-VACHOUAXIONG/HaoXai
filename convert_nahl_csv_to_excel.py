import pandas as pd
import os

def convert_nahl_csv_to_excel():
    """Convert nahl_host.csv to Excel format"""
    print("=== Converting nahl_host.csv to Excel ===")
    
    # Input and output paths
    csv_path = os.path.join('DataExcel', 'nahl_host.csv')
    excel_path = os.path.join('DataExcel', 'nahl_host.xlsx')
    
    try:
        # Read the CSV file
        print(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        
        print(f"CSV file contains {len(df)} rows and {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Show sample data
        print(f"\nSample data (first 3 rows):")
        print(df.head(3).to_string())
        
        # Write to Excel
        print(f"\nConverting to Excel: {excel_path}")
        df.to_excel(excel_path, index=False)
        
        print(f"✅ Successfully converted to Excel!")
        print(f"Excel file saved: {excel_path}")
        
        # Verify the Excel file was created
        if os.path.exists(excel_path):
            file_size = os.path.getsize(excel_path)
            print(f"File size: {file_size:,} bytes")
        else:
            print("❌ Excel file was not created")
            
    except Exception as e:
        print(f"❌ Error converting CSV to Excel: {e}")

if __name__ == "__main__":
    convert_nahl_csv_to_excel()
