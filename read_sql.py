import pyodbc
import urllib.request
import ssl
import os
import csv
from datetime import datetime
import pandas as pd

# ---------- DATATYPE FIX FUNCTION ----------
def fix_datatypes(df):

    numeric_int_cols = [
        "PATIENTNO", "PATZIP", "PATPHONE", "PATMOBILENO",
        "RXNO", "QUANT", "DAYS", "CLASS", "PRESPHONE"
    ]

    numeric_float_cols = ["PATIENTCOPAY"]

    def _safe_str(val):
        if pd.isna(val):
            return ""
        return str(val).strip()

    def _to_int_safe(val):
        s = _safe_str(val)
        if s.replace('.', '', 1).isdigit():
            try:
                return int(float(s))
            except:
                return val
        return val

    def _to_float_safe(val):
        s = _safe_str(val)
        if s.replace('.', '', 1).isdigit():
            try:
                return float(s)
            except:
                return val
        return val

    for col in df.columns:
        # Clean spaces (keep originals for non-strings safe)
        df[col] = df[col].apply(lambda v: _safe_str(v))

        # Convert to INT where needed
        if col in numeric_int_cols:
            df[col] = df[col].apply(_to_int_safe)

        # Convert to FLOAT where needed
        elif col in numeric_float_cols:
            df[col] = df[col].apply(_to_float_safe)

        # Convert empty → None
        df[col] = df[col].replace({"": None, "null": None, "None": None})

    return df


# SQL Server connection settings
server = '54.225.130.181,1983'
database = 'FinerrApp'
username = 'finerrprod'
password = 'x.pFXt#U:jCSZYGm8s6RAb'

connection_string = f"""
DRIVER={{ODBC Driver 17 for SQL Server}};
SERVER={server};
DATABASE={database};
UID={username};
PWD={password};
"""

ssl_context = ssl._create_unverified_context()

# ---------- MASTER FILE PATH ----------
MASTER_FILE = r"C:\testdata\master.csv"
MASTER_JSON = r"C:\testdata\Ewing_Pharmacy_Patients.json"

def process_pharmacy_data(pharmacy_name, pharmacy_id, conn):
    """Process and save data for a specific pharmacy"""
    print(f"\n📊 Processing {pharmacy_name} (ID: {pharmacy_id})...")
    
    cursor = conn.cursor()
    
    # Get top 2 records for this pharmacy
    cursor.execute(f"""
        SELECT TOP(2) *
        FROM FileProcessLog
        WHERE PharmacyId = {pharmacy_id}
        AND FileName LIKE '%RX_FINERR NIGHTLY%'
        ORDER BY 1 DESC
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print(f"⚠ No data found for {pharmacy_name}")
        return
    
    # Create folder for output
    folder = r"C:\testdata"
    os.makedirs(folder, exist_ok=True)
    
    # Process each row
    all_data = []
    for row in rows:
        file_info = row[3]
        file_url = file_info.split("|")[1]
        print(f"  File URL: {file_url}")
        
        file_name = file_url.split("/")[-1]
        download_path = os.path.join(folder, file_name)
        converted_file = os.path.join(folder, "converted_" + file_name)
        
        try:
            print(f"  ⬇ Downloading...")
            
            with urllib.request.urlopen(file_url, context=ssl_context) as response:
                file_data = response.read()
            
            with open(download_path, "wb") as f:
                f.write(file_data)
            
            print(f"  ✅ File downloaded")
            
            # Convert PATDOB format
            print(f"  🔄 Converting PATDOB...")
            with open(download_path, "r", encoding="utf-8") as infile:
                reader = csv.reader(infile)
                rows_csv = list(reader)
            
            header = [h.strip() for h in rows_csv[0]]
            dob_index = header.index("PATDOB")
            
            with open(converted_file, "w", newline="", encoding="utf-8") as outfile:
                writer = csv.writer(outfile)
                writer.writerow(header)
                
                for row_csv in rows_csv[1:]:
                    try:
                        dob_raw = row_csv[dob_index].strip()
                        dt = datetime.strptime(dob_raw, "%m/%d/%Y %I:%M:%S %p")
                        row_csv[dob_index] = dt.strftime("%m%d%Y")
                    except:
                        pass
                    writer.writerow(row_csv)
            
            # Read and add to all_data
            today_df = pd.read_csv(converted_file, dtype=str)
            all_data.append(today_df)
            
            # Cleanup
            try:
                if os.path.exists(download_path):
                    os.remove(download_path)
                if os.path.exists(converted_file):
                    os.remove(converted_file)
            except:
                pass
                
        except Exception as e:
            print(f"  ❌ Error processing file: {e}")
    
    if all_data:
        # Combine all new data
        new_df = pd.concat(all_data, ignore_index=True)
        new_df = new_df.drop_duplicates(keep="last")
        new_df = fix_datatypes(new_df)
        
        # Save separate files for this pharmacy
        pharmacy_filename = pharmacy_name.replace(" ", "_").lower()
        csv_file = os.path.join(folder, f"{pharmacy_filename}_data.csv")
        json_file = os.path.join(folder, f"{pharmacy_filename}_data.json")
        
        # ============================================
        # 🔄 MERGE WITH EXISTING DATA + REMOVE DUPLICATES
        # ============================================
        print(f"  🔄 Checking for existing data...")
        
        if os.path.exists(csv_file):
            # Load existing data
            existing_df = pd.read_csv(csv_file, dtype=str)
            print(f"  📁 Found existing file with {len(existing_df)} records")
            
            # Combine new + existing data
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            print(f"  📊 Combined total: {len(combined_df)} records")
            
            # Remove duplicates (keep the last occurrence - newest data)
            combined_df = combined_df.drop_duplicates(keep="last")
            print(f"  ✂️  After removing duplicates: {len(combined_df)} records")
            
            merged_df = combined_df
        else:
            print(f"  ✨ New file (no existing data)")
            merged_df = new_df
        
        # Apply datatype fixes
        merged_df = fix_datatypes(merged_df)
        
        # Save updated files
        merged_df.to_csv(csv_file, index=False)
        merged_df.to_json(json_file, orient="records", indent=4)
        
        print(f"  ✅ Updated: {csv_file} ({len(merged_df)} total records)")
        print(f"  ✅ Updated: {json_file}")

try:
    conn = pyodbc.connect(connection_string)
    print("✅ Connected to SQL Server")
    
    # First, let's check available columns
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TOP 1 * FROM FileProcessLog
    """)
    
    columns = [description[0] for description in cursor.description]
    print("\n📋 Available columns in FileProcessLog:")
    for col in columns:
        print(f"  - {col}")
    
    # Query for pharmacy IDs by name
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TOP 2 * 
        FROM FileProcessLog 
        WHERE PharmacyId IN (62, 224)
        ORDER BY PharmacyId DESC
    """)
    
    pharmacy_mapping = cursor.fetchall()
    print("\n📋 Found Pharmacies:")
    
    # Process Ewing Pharmacy (ID: 62)
    process_pharmacy_data("Ewing Pharmacy", 62, conn)
    
    # Process Zarchy Pharmacy (ID: 224 - update if different)
    process_pharmacy_data("Zarchy Pharmacy", 224, conn)
    
    print("\n✅ All pharmacy data processed successfully!")

except Exception as e:
    print("❌ Error:", e)

finally:
    try:
        conn.close()
    except:
        pass
