import numpy as np
import streamlit as st
import pandas as pd
import re
import numpy as np
import gspread
import io
from oauth2client.service_account import ServiceAccountCredentials

# --- 1. SHARED RESOURCES ---
@st.cache_resource
def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gcp_service_account"], scope)
    return gspread.authorize(creds)

def make_url(sheet_id):
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"

@st.cache_data(ttl=600)
def load_google_sheet(url, sheet_name=0):
    try:
        client = get_gspread_client()
        sheet = client.open_by_url(url)
        if isinstance(sheet_name, str): worksheet = sheet.worksheet(sheet_name)
        else: worksheet = sheet.get_worksheet(sheet_name)
        data = worksheet.get_all_values()
        return pd.DataFrame(data)
    except Exception as e: 
        print(f"Error loading {sheet_name}: {e}")
        return None

def write_to_sheet(url, sheet_name, df):
    try:
        client = get_gspread_client()
        sh = client.open_by_url(url)
        try: ws = sh.worksheet(sheet_name); ws.clear()
        except: ws = sh.add_worksheet(title=sheet_name, rows=100, cols=20)
        df_str = df.fillna("").astype(str)
        if df.index.name is not None: df_str = df_str.reset_index()
        data = [df_str.columns.values.tolist()] + df_str.values.tolist()
        ws.resize(rows=len(data), cols=len(data[0]))
        ws.update(data)
        return True
    except: return False

def get_saved_reports(url):
    try:
        client = get_gspread_client()
        sh = client.open_by_url(url)
        titles = [ws.title for ws in sh.worksheets()]
        reports = set()
        for t in titles:
            if t.startswith("Rep_"):
                parts = t.split('_') 
                if len(parts) >= 2: reports.add(parts[1])
        return sorted(list(reports), reverse=True)
    except: return []

# --- 2. DATA PROCESSING HELPERS ---
def normalize_store_name(name, report_type='CS'):
    if pd.isna(name): return "UNKNOWN"
    name = str(name).upper().strip()

    if report_type == 'CS':
        # CS Mappings
        if name == 'COMPASS ONE': return 'CS COMPASS ONE'
        if name == 'CS GREAT WORLD CITY-AM' : return 'CS GREAT WORLD CITY'
        if name == 'CS GREAT WORLD CITY-PM' : return 'CS GREAT WORLD CITY'
        if name == 'MP TANGLIN-AM' : return 'MP TANGLIN'
        if name == 'MP TANGLIN-PM' : return 'MP TANGLIN'
        if name == 'CS PARKWAY PARADE-PM' : return 'CS PARKWAY PARADE'
        if name == 'CS I12 KATONG-PM' : return 'CS I12 KATONG'
        if name == 'CS 1 HOLLAND-PM' : return 'CS 1 HOLLAND'
        if name == 'CS CHANCERY COURT 2-PM' : return 'CS CHANCERY COURT'
        if name == 'CHANCERY COURT 2' : return 'CS CHANCERY COURT'
        if name == 'CS ONE HOLLAND VILLAGE-PM' : return 'CS ONE HOLLAND VILLAGE'
        if name == 'ONE HOLLAND VILLAGE' : return 'CS ONE HOLLAND VILLAGE'
        if name == 'ANCHORPOINT 3' :  return 'CS ANCHORPOINT 3'
        if name == 'JOO CHIAT' : return 'CS JOO CHIAT'
        if name == 'JS SENTOSA QUAYSIDE-PM' : return 'JS SENTOSA QUAYSIDE'
        if name == 'CS ALOCASSIA-PM' : return 'CS ALOCASSIA'
        if name == 'CS CLUNY COURT-PM' : return 'CS CLUNY COURT'
        if name == 'CS MARINA ONE-PM' : return 'CS MARINA ONE'
        if name == 'CS GUTHRIE HOUSE-PM' : return 'CS GUTHRIE HOUSE'
        if name == 'CS ORCHARD HOTEL-AM' : return 'CS ORCHARD HOTEL'
        if name == 'CS ORCHARD HOTEL-PM' : return 'CS ORCHARD HOTEL'
        if name == 'CS RAIL MALL-PM' : return 'CS RAIL MALL'
        if name == 'CS RAIL MALL-AM' : return 'CS RAIL MALL'
        if name == 'CS SERANGOON NEX-PM' : return 'CS SERANGOON NEX'
        if name == 'CS UNITED SQUARE-PM' : return 'CS UNITED SQUARE'
        if name == 'MP HILLVIEW-AM' : return 'MP HILLVIEW'
        if name == 'MP HILLVIEW-PM' : return 'MP HILLVIEW'
        if name == 'PASIR RIS MALL' : return 'CS PASIR RIS MALL'

        # CS Cleanup
        replacements = { ' MARKET': '', ' SUPERMARKET': '', 'SINGAPORE': '', ' PTE LTD': ''}
        for old, new in replacements.items(): name = name.replace(old, new)
        return " ".join(name.split()).strip()

    elif report_type == 'SS':
        # SS Mappings
        if name == 'AJ': return 'SS ALJUNIED 118'
        if 'AJA118' in name: return 'SS ALJUNIED 118'
        if name == 'BC': return 'SS BEDOK 209'
        if name == 'BS': return 'SS BEDOK 151'
        if name == 'BH': return 'SS BISHAN 512'
        if name == 'EM': return 'SS ELIAS MALL'
        if name == 'HG': return 'SS HOUGANG 377'
        if name == 'J9': return 'SS JUNCTION NINE'
        if name == 'JT': return 'SS JUNCTION TEN'
        if name == 'KL': return 'SS KALLANG'
        if name == 'KN': return 'SS KINEX'
        if name == 'CT': return 'SS THE CATHAY'
        if name == 'TH': return 'SS TANGLIN HALT 88'
        if name == 'TC': return 'SS TAMPINES CENTRAL 506'
        if name == 'TY': return 'SS TOA PAYOH 181'
        if name == 'W5': return 'SS WOODLANDS 573E'
        
        return name
    
    elif report_type == 'NTUC':
        name = re.sub(r'^\d+\s*-\s*', '', name)
        name = name.replace('FPX-', '').strip()
        if name == 'BUKIT TIMAH PLAZA-PM' : return 'BUKIT TIMAH PLAZA'
        if name == 'CLEMENTI MALL-PM' : return 'CLEMENTI MALL'
        return name
    
    elif report_type == 'CS_DRY':
        if name == 'COMPASS ONE': return 'CS COMPASS ONE'
        return name
    
    elif report_type == 'SS_DRY':
        if name  == 'AJ': return 'SS ALJUNIED 118'
        if 'AJA118' in name: return 'SS ALJUNIED 118'
        if name == 'BC': return 'SS BEDOK 209'
        if name == 'BS': return 'SS BEDOK 151'
        if name == 'BH': return 'SS BISHAN 512'
        if name == 'EM': return 'SS ELIAS MALL'
        if name == 'HG': return 'SS HOUGANG 377'
        if name == 'J9': return 'SS JUNCTION NINE'
        if name == 'JT': return 'SS JUNCTION TEN'
        if name == 'KL': return 'SS KALLANG'
        if name == 'KN': return 'SS KINEX'
        if name == 'CT': return 'SS THE CATHAY'
        if name == 'TH': return 'SS TANGLIN HALT 88'
        if name == 'TC': return 'SS TAMPINES CENTRAL 506'
        if name == 'TY': return 'SS TOA PAYOH 181'
        if name == 'W5': return 'SS WOODLANDS 573E'
        if name == 'TOTAL' : return 'UNKNOWN'
        return name
    
    elif report_type == 'NTUC_DRY':
        name = re.sub(r'^\d+\s*-\s*', '', name)
        name = name.replace('FPX-', '').strip()
        if name == 'BUKIT TIMAH PLAZA-PM' : return 'BUKIT TIMAH PLAZA'
        if name == 'CLEMENTI MALL-PM' : return 'CLEMENTI MALL'
        return name
        

    return name

def clean_id(val):
    if pd.isna(val) or val == '': return "0"
    s = str(val).strip().upper()
    if s == 'NAN' or s == 'NONE': return "0"
    if "HCZX" in s: return "0"
    s = s.split('-')[0].strip()
    if s.endswith('.0'): s = s[:-2]
    return s

def clean_currency(val):
    if pd.isna(val) or str(val).strip() == "": return 0.0
    s = str(val).strip().replace('$', '').replace(' ', '')
    
    if s.endswith(",000"):
        s = s[:-4]
        if s.count('.') > 1:
            s = s.replace('.', '')
        return float(s)
    
    if ',' in s and '.' not in s:
        s = s.replace(',', '.')
        return float(s)

    if ',' in s and '.' in s:
        if s.rfind(',') < s.rfind('.'):
            s = s.replace(',', '')
        else:
            s = s.replace('.', '').replace(',', '.')

    try:return float(s)
    except: return 0.0 

def parse_uom_factor(uom_str):
    if pd.isna(uom_str): return 1.0
    s = str(uom_str).upper().strip()
    if 'KG' in s: return 1.0
    match = re.search(r'(\d+)G', s)
    if match: return float(match.group(1)) / 1000.0
    return 1.0

def clean_header(header):
    return str(header).replace('\n', ' ').replace('\r', ' ').strip().upper()

def strict_rename(df, map_dict):
    df.columns = [clean_header(c) for c in df.columns]
    new_cols = {}
    used_targets = set()
    for col in df.columns:
        for target, keywords in map_dict.items():
            if target in used_targets: continue
            if target == 'NAV' and "CUSTOMER" in col: continue 
            if any(k.upper() in col for k in keywords):
                keyword_has_desc = any("DESC" in k.upper() for k in keywords)
                if "DESC" in col and not keyword_has_desc: continue
                new_cols[col] = target
                used_targets.add(target)
                break
    # Remove duplicates immediately to prevent Series ambiguity
    temp = df.rename(columns=new_cols)
    return temp.loc[:, ~temp.columns.duplicated()]

def find_correct_header_row(df_in, required_map, source_name="File"):
    if df_in is None: return None
    def check_df(d):
        temp = strict_rename(d.copy(), required_map)
        found = [k for k in required_map.keys() if k in temp.columns]
        if source_name == "DB Sheet":
            return 'Article' in temp.columns and 'NAV' in temp.columns
        return len(found) >= (len(required_map) - 1)

    for r in range(min(20, len(df_in))):
        candidate_header = df_in.iloc[r]
        if not any(isinstance(x, str) and len(x)>1 for x in candidate_header): continue
        candidate_df = df_in.iloc[r+1:].copy()
        candidate_df.columns = candidate_header
        if check_df(candidate_df): return candidate_df
    
    st.error(f"❌ Error: Header not found in {source_name}")
    return None

# --- 3. MAIN PROCESS DATA FUNCTION ---
@st.cache_data
def process_data(df_sales_raw, df_db_raw, df_dist_raw, df_waste_raw, report_type):
    master_name_map = {}
    nav_to_article_map = {} 

    if report_type =="CS":
        db_cols = {'Article': ['Article', 'ITEMCODE'], 'NAV': ['NAV', 'NAV_CODE', 'No.'], 'ArtDesc': ['Article Description', 'ArticleDesc'], 'NavDesc': ['NAV description', 'Description']}
        sales_cols ={'Article': ['Article', 'ITEMCODE'], 'Qty': ['Quantity','QTY','SALESQTY','Billed Quantity'], 'Val': ['Amount','SALESAMOUNT','Total Amount'], 'Store': ['STOREDESC', 'Store name'], 'Date': ['TRXDATE','Date'], 'Name': ['ITEMDESC', 'Description', 'Name']}
        dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['Your Reference', 'key'], 'UOM': ['Unit of Measure', 'UOM'], 'Name': ['USOFT product description', 'Description', 'Name'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date','Date'], 'Chain': ['External Doc No.']}
        waste_cols = {'NAV': ['NAV', 'NAV_CODE'], 'Qty': ['QTY', 'Quantity'], 'Weight': ['WEIGHT'], 'Store': ['Store', 'LONG_NAME'], 'Val': ['Amount', 'TOT_AMT'], 'Date': ['DATE', 'Date'], 'Chain': ['MAIN_CODE']}

    elif report_type == "SS":
        db_cols = {'Article': ['ITEM CODE', 'Article'], 'NAV': ['NAV CODE', 'NAV'], 'ArtDesc': ['DESCRIPTION', 'Article Description'], 'NavDesc': ['NAV Description']}
        # Sales: ITEM CODE, OUTLET, QTY, SALES BEF GST
        sales_cols = {'Article': ['ITEM CODE', 'Article'], 'Qty': ['QTY', 'Quantity'], 'Val': ['SALES BEF GST', 'Total Amount', 'Amount'], 'Store': ['OUTLET', 'Store'], 'Date': ['Date', 'TRXDATE'], 'Name': ['DESCRIPTION', 'Name']}
        dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['Your Reference', 'key'], 'UOM': ['Unit of Measure', 'UOM'], 'Name': ['USOFT product description', 'Description', 'Name'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date','Date'], 'Chain': ['External Doc No.']}
        waste_cols = {'NAV': ['NAV', 'NAV_CODE'], 'Qty': ['QTY', 'Quantity'], 'Weight': ['WEIGHT'], 'Store': ['Store', 'LONG_NAME'], 'Val': ['Amount', 'TOT_AMT'], 'Date': ['DATE', 'Date'], 'Chain': ['MAIN_CODE']}

    elif report_type =="NTUC":
        db_cols = {'Article': ['cno_sku'], 'NAV': ['id'], 'ArtDesc': ['name1'], 'NavDesc': ['name2']}
        sales_cols = {'Store': ['1st Column'], 'Raw_Item': ['2nd Column']}
        dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['Your Reference', 'key'], 'UOM': ['Unit of Measure', 'UOM'], 'Name': ['USOFT product description', 'Description', 'Name'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date','Date'], 'Chain': ['External Doc No.']}
        waste_cols = {'NAV': ['NAV', 'NAV_CODE'], 'Qty': ['QTY', 'Quantity'], 'Weight': ['WEIGHT'], 'Store': ['Store', 'LONG_NAME'], 'Val': ['Amount', 'TOT_AMT'], 'Date': ['DATE', 'Date'], 'Chain': ['MAIN_CODE']}

    
    elif report_type == "CS_DRY":
        db_cols = {'Article': ['cno_sku'], 'NAV': ['partno'], 'ArtDesc': ['name2'], 'NavDesc': ['name2']}
        sales_cols ={'Article': ['Article', 'ITEMCODE'], 'Qty': ['Quantity','QTY','SALESQTY','Billed Quantity'], 'Val': ['Amount','SALESAMOUNT','Total Amount'], 'Store': ['STOREDESC', 'Store name'], 'Date': ['TRXDATE','Date'], 'Name': ['ITEMDESC', 'Description', 'Name']}
        dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['External Doc No.'], 'UOM': ['Unit of Measure', 'UOM'], 'Name': ['USOFT product description', 'Description', 'Name'], 'Cost': ['Unit Price Excl. GST'], 'Date': ['Posting Date','Date'], 'Chain': ['Customer']}

    elif report_type == "SS_DRY":
        db_cols = {'Article': ['cno_sku'], 'NAV': ['partno'], 'ArtDesc': ['name2'], 'NavDesc': ['name2']}
        sales_cols = {'Article': ['ITEMCODE', 'Article'], 'Qty': ['QTY', 'Quantity'], 'Val': ['SALES BEF GST', 'Total Amount', 'Amount'], 'Store': ['OUTLET', 'Store'], 'Date': ['YEAR', 'TRXDATE'], 'Name': ['DESCRIPTION', 'Name']}
        dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['External Doc No.'], 'UOM': ['Unit of Measure', 'UOM'], 'Name': ['USOFT product description', 'Description', 'Name'], 'Cost': ['Unit Price Excl. GST'], 'Date': ['Posting Date','Date'], 'Chain': ['Transfer-to Code']}

    elif report_type == "NTUC_DRY":
        db_cols = {'Article': ['cno_sku'], 'NAV': ['partno'], 'ArtDesc': ['name2'], 'NavDesc': ['name2']}
        sales_cols = {'Store': ['1st Column'], 'Raw_Item': ['2nd Column']}
        dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['External Doc No.'], 'UOM': ['Unit of Measure', 'UOM'], 'Name': ['USOFT product description', 'Description', 'Name'], 'Cost': ['Unit Price Excl. GST'], 'Date': ['Posting Date','Date'], 'Chain': ['Transfer-to Code']}
        # No wastage file for CS_DRY
    # Common Maps
    # dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['Your Reference', 'key'], 'UOM': ['Unit of Measure', 'UOM'], 'Name': ['USOFT product description', 'Description', 'Name'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date','Date'], 'Chain': ['External Doc No.']}
    # waste_cols = {'NAV': ['NAV', 'NAV_CODE'], 'Qty': ['QTY', 'Quantity'], 'Weight': ['WEIGHT'], 'Store': ['Store', 'LONG_NAME'], 'Val': ['Amount', 'TOT_AMT'], 'Date': ['DATE', 'Date'], 'Chain': ['MAIN_CODE']}



    # --- A. DATABASE ---
    df_db = find_correct_header_row(df_db_raw,db_cols, "DB Sheet")
    if df_db is None: return None
    df_db = strict_rename(df_db, db_cols)

    if report_type == "NTUC":
        df_db['NAV'] = df_db['NAV'].astype(str).apply(lambda x: x.split('-')[0] if '-' in x else x)

    df_db['Article'] = df_db['Article'].apply(clean_id)
    df_db['NAV'] = df_db['NAV'].apply(clean_id)
    df_db = df_db[df_db['NAV'] != "0"].drop_duplicates('Article')
    
    db_mapping_forward = df_db.set_index('Article')['NAV'].to_dict()
    df_valid_db = df_db[df_db['NAV'] != "0"]
    nav_to_article_map = df_valid_db.drop_duplicates('NAV').set_index('NAV')['Article'].to_dict()

    if 'ArtDesc' in df_db.columns:
        df_db['Final_Name'] = df_db['ArtDesc']
        if 'NavDesc' in df_db.columns:
            df_db['Final_Name'] = df_db['Final_Name'].fillna(df_db['NavDesc'])
        df_db['Final_Name'] = df_db['Final_Name'].fillna("Unknown DB Item")
        master_name_map.update(df_db.set_index('NAV')['Final_Name'].to_dict())

    # --- B. SALES ---
    if report_type == "NTUC" or report_type == "NTUC_DRY":
        id_vars = ['Store', 'Raw_Item']
        melt_val = pd.DataFrame()
        melt_qty = pd.DataFrame()

        try:
            client = get_gspread_client()
            sales_url = st.session_state['urls']['s'] 
            sh = client.open_by_url(sales_url)

            # 1. FETCH & PROCESS "Quantity" TAB
            try:
                ws_qty = sh.worksheet("Quantity")
                df_qty_raw = pd.DataFrame(ws_qty.get_all_values())
                df_qty_clean = find_correct_header_row(df_qty_raw, sales_cols, "Qty Sheet")
                df_qty_clean = strict_rename(df_qty_clean, sales_cols)
                
                # Exclude 'METRIC' column explicitly to avoid data corruption
                date_cols_q = [c for c in df_qty_clean.columns if c not in id_vars and 'METRIC' not in str(c).upper()]
                melt_qty = df_qty_clean.melt(id_vars=id_vars, value_vars=date_cols_q, var_name='Date', value_name='Qty')
            except Exception as e:
                st.warning(f"Error loading Quantity tab: {e}")

            # 2. FETCH & PROCESS "Sales" TAB (Value)
            try:
                try: ws_val = sh.worksheet("Sales") 
                except: ws_val = sh.get_worksheet(0)
                
                df_val_raw = pd.DataFrame(ws_val.get_all_values())
                df_val_clean = find_correct_header_row(df_val_raw, sales_cols, "Sales Sheet")
                df_val_clean = strict_rename(df_val_clean, sales_cols) # FIX: Use df_val_clean here
                
                # Exclude 'METRIC' column explicitly
                date_cols_v = [c for c in df_val_clean.columns if c not in id_vars and 'METRIC' not in str(c).upper()]
                melt_val = df_val_clean.melt(id_vars=id_vars, value_vars=date_cols_v, var_name='Date', value_name='Val')
            except Exception as e:
                st.warning(f"Error loading Sales tab: {e}")

        except Exception as e:
            st.error(f"Critical GSheet Error: {e}")
            return None

        # 3. Handle Empty Dataframes
        if melt_val.empty: 
            st.error("Could not fetch Sales Value data.")
            return None
        if melt_qty.empty:
            melt_qty = melt_val.copy()[id_vars + ['Date']]
            melt_qty['Qty'] = 0

        # 4. Cleanup & Merge 
        # Clean currency symbols just in case ($) and convert to numeric
        melt_val['Val'] = melt_val['Val'].apply(clean_currency)
        
        melt_qty['Qty'] = pd.to_numeric(melt_qty['Qty'], errors='coerce').fillna(0)
        
        # Standardize Date formats 
        melt_val['Date'] = pd.to_datetime(melt_val['Date'], dayfirst=True, errors='coerce')
        melt_qty['Date'] = pd.to_datetime(melt_qty['Date'], dayfirst=True, errors='coerce')
        
        # Filter out invalid dates (e.g. if 'Metric' column slipped in)
        melt_val = melt_val.dropna(subset=['Date'])
        melt_qty = melt_qty.dropna(subset=['Date'])

        df_sales = pd.merge(melt_val, melt_qty, on=['Store', 'Raw_Item', 'Date'], how='outer').fillna(0)

        # 5. Extract Article Code
        df_sales['Article'] = df_sales['Raw_Item'].astype(str).str.extract(r'(\d+)\s*$')
        df_sales['Name'] = df_sales['Raw_Item'].astype(str).str.rsplit('-', n=1).str[0].str.strip()

    else:
        # Standard Logic (CS / SS)
        df_sales = find_correct_header_row(df_sales_raw, sales_cols, "Sales Sheet")
        if df_sales is None: return None
        df_sales = strict_rename(df_sales, sales_cols)

    # For SS_DRY, set Sales_Qty and Sales_Val to 0.0 if Store is 'TOTAL'
    if report_type == "SS_DRY" and 'Store' in df_sales.columns:
        mask_total = df_sales['Store'].astype(str).str.upper() == 'TOTAL'
        df_sales.loc[mask_total, 'Qty'] = 0.0
        df_sales.loc[mask_total, 'Val'] = 0.0
    
    df_sales['Article'] = df_sales['Article'].apply(clean_id)
    df_sales['NAV'] = df_sales['Article'].map(db_mapping_forward).fillna("0")
    if 'Name' in df_sales.columns:
        sales_names = df_sales[df_sales['NAV'] != "0"].set_index('NAV')['Name'].to_dict()
        for k, v in sales_names.items():
            if k not in master_name_map: master_name_map[k] = v
            
    df_sales = df_sales[df_sales['NAV'] != "0"]
    df_sales['Store'] = df_sales['Store'].apply(lambda x: normalize_store_name(x, report_type))
    df_sales['Qty'] = df_sales['Qty'].apply(clean_currency)
    df_sales['Val'] = df_sales['Val'].apply(clean_currency)
    
            # Cold Storage: 2025.12.31 (Year.Month.Day)
    # Handle Sales Dates
    if 'Date' in df_sales.columns:
        if report_type == 'SS_DRY':
            df_sales['Year'] = df_sales['Date'].astype(str).replace(r'\.0$', '', regex=True)
            df_sales['Date'] = pd.to_datetime(df_sales['Year'] + "-01-01", errors='coerce') # Dummy date
        elif report_type == 'SS' :
            # Sheng Siong: 09-12-2025 (Day-Month-Year)
            df_sales['Date'] = pd.to_datetime(df_sales['Date'], dayfirst=True, errors='coerce')
            df_sales['Year'] = df_sales['Date'].dt.year.astype(str).str.replace(r'\.0$', '', regex=True)
            df_sales['Month'] = df_sales['Date'].dt.month_name().str[:3]
            df_sales['Week'] = df_sales['Date'].dt.strftime('%Y-W%U')
        else:
            # Cold Storage: 2025.12.31 (Year.Month.Day)
            df_sales['Date'] = pd.to_datetime(df_sales['Date'], format='%Y.%m.%d', errors='coerce')
            if df_sales['Date'].isnull().all():
                 df_sales['Date'] = pd.to_datetime(df_sales['Date'], dayfirst=True, errors='coerce')
            df_sales['Year'] = df_sales['Date'].dt.year.astype(str).str.replace(r'\.0$', '', regex=True)
            df_sales['Month'] = df_sales['Date'].dt.month_name().str[:3]
            df_sales['Week'] = df_sales['Date'].dt.strftime('%Y-W%U')
    else:
        df_sales['Year'] = "2025" 
        df_sales['Month'] = "ALL"
        df_sales['Week'] = "ALL"

   
    if report_type == 'SS_DRY':
        df_sales['Month'] = "Annual"
        df_sales['Week'] = "Annual"

    # --- C. DISTRIBUTION -
    # d_map = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['Your Reference', 'key'], 'UOM': ['Unit of Measure', 'UOM'], 'Name': ['USOFT product description', 'Description', 'Name'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date','Date'], 'Chain': ['Customer']}
    df_dist = find_correct_header_row(df_dist_raw, dist_cols, "Dist Sheet")
    if df_dist is None: return None
    df_dist = strict_rename(df_dist, dist_cols)
    
    if 'Store' in df_dist.columns:
        if report_type =='CS':
            mask = df_dist['Store'].astype(str).str.upper().str.contains('CS |COLD STORAGE|CS_|COMPASS ONE|MP |NOVENA |JS |MARINA |GT |FAR ', regex=True, na=False)
            df_dist=df_dist[mask]
        elif report_type == 'SS':
            mask = df_dist['Store'].astype(str).str.upper().str.contains(r'^Sheng Siong|^SS |^SS_', regex=True, na=False)
            df_dist=df_dist[mask]
        elif report_type == 'NTUC':
            mask = df_dist['Chain'].astype(str).str.upper().str.contains(r'NTUC', regex=True, na=False)
            df_dist = df_dist[mask]
        elif report_type == 'CS_DRY':
            mask = df_dist['Store'].astype(str).str.upper().str.contains('CS |COLD STORAGE|CS_|COMPASS ONE|MP |NOVENA |JS |MARINA |GT |FAR ', regex=True, na=False)
            df_dist = df_dist[mask]
        elif report_type == 'SS_DRY':
            mask = df_dist['Chain'].astype(str).str.upper().str.contains(r'^Sheng Siong|^SS|^SS_', regex=True, na=False)
            df_dist=df_dist[mask]
        elif report_type == 'NTUC_DRY':
            mask = df_dist['Chain'].astype(str).str.upper().str.contains(r'NC', regex=True, na=False)
            df_dist = df_dist[mask]
        


       

    
    if 'Chain' in df_dist.columns and 'Store' not in df_dist.columns:
         mask_chain = df_dist['Chain'].astype(str).str.upper().str.contains('HX|', na=False)
         if mask_chain.sum() > 0: df_dist = df_dist[mask_chain]
    
    df_dist['NAV'] = df_dist['NAV'].apply(clean_id)
    if 'Name' in df_dist.columns:
        dist_names = df_dist[df_dist['NAV'] != "0"].set_index('NAV')['Name'].to_dict()
        for k, v in dist_names.items():
            if k not in master_name_map: master_name_map[k] = v

    df_dist['Store'] = df_dist['Store'].apply(lambda x: normalize_store_name(x, report_type))
    df_dist['Date'] = pd.to_datetime(df_dist['Date'], errors='coerce')
    df_dist['Year'] = df_dist['Date'].dt.year.astype(str).str.replace(r'\.0$', '', regex=True)
    df_dist['Month'] = df_dist['Date'].dt.month_name().str[:3]
    df_dist['Week'] = df_dist['Date'].dt.strftime('%Y-W%U')
    df_dist['Qty'] = df_dist['Qty'].apply(clean_currency)
    if report_type == 'SS_DRY':
        df_dist['Month'] = "Annual"
        df_dist['Week'] = "Annual"
    
    if report_type == "CS_DRY" or report_type == "SS_DRY" or report_type == "NTUC_DRY":
        pass
    else:
        if 'UOM' in df_dist.columns:
            raw_qty = pd.to_numeric(df_dist['Qty'], errors='coerce').fillna(0)
            uom_factor = df_dist['UOM'].apply(parse_uom_factor)
            df_dist['Qty'] = raw_qty * uom_factor 
        

    cost = df_dist['Cost'].apply(clean_currency) if 'Cost' in df_dist.columns else 0
    df_dist['Val'] = df_dist['Qty'] * cost

    # --- D. WASTAGE ---
    if report_type == "CS_DRY" or report_type == "SS_DRY" or report_type== "NTUC_DRY":
        # No wastage file for CS_DRY
        df_waste = pd.DataFrame(columns=["NAV", "Qty", "Val", "Store", "Date", "Year", "Month", "Week", "Weight", "Chain"])
    else:
        # w_map = {'NAV': ['NAV', 'NAV_CODE'], 'Qty': ['QTY', 'Quantity'], 'Weight': ['WEIGHT'], 'Store': ['Store', 'LONG_NAME'], 'Val': ['Amount', 'TOT_AMT'], 'Date': ['DATE', 'Date'], 'Chain': ['MAIN_CODE']}
        df_waste = find_correct_header_row(df_waste_raw, waste_cols, "Waste Sheet")
        if df_waste is None: return None
        df_waste = strict_rename(df_waste, waste_cols)
        if 'Chain' in df_waste.columns: 
            if report_type == 'CS':
                mask = df_waste['Chain'].astype(str).str.upper().str.contains('C.STORAGE|CS|COLD', regex=True, na=False)
                df_waste = df_waste[mask]
            elif report_type == 'SS':
                mask = df_waste['Chain'].astype(str).str.upper().str.contains(r'^SHENG SHIONG|^SS|^SS_|S.SIONG', regex=True, na=False)
                df_waste = df_waste[mask]
            elif report_type == 'NTUC':
                mask = df_waste['Chain'].astype(str).str.upper().str.contains('NTUC', regex=True, na=False)
                df_waste = df_waste[mask]
        df_waste['NAV'] = df_waste['NAV'].apply(clean_id)
        df_waste['Store'] = df_waste['Store'].apply(lambda x: normalize_store_name(x, report_type))
        df_waste['Date'] = pd.to_datetime(df_waste['Date'], dayfirst=True, errors='coerce')
        df_waste['Year'] = df_waste['Date'].dt.year.astype(str).replace(r'\.0$', '', regex=True)
        df_waste['Month'] = df_waste['Date'].dt.month_name().str[:3]
        df_waste['Week'] = df_waste['Date'].dt.strftime('%Y-W%U')
        qty_units = df_waste['Qty'].apply(clean_currency)
        weight_kg = df_waste['Weight'].apply(clean_currency)
        df_waste['Qty'] = qty_units * weight_kg
        df_waste['Val'] = df_waste['Val'].apply(clean_currency)

    def get_max_date(dframe):
        try:
            if not dframe.empty and 'Date' in dframe.columns:
                return dframe['Date'].max().strftime('%d %b %Y')
        except: pass
        return "N/A"

    update_info = {
        "Sales": get_max_date(df_sales),
        "Dist": get_max_date(df_dist),
        "Waste": get_max_date(df_waste)
    }

    return df_sales, df_dist, df_waste, master_name_map, nav_to_article_map, [], update_info
# --- 4. MAIN APP LOGIC ---
def main_app_interface(authenticator, name, permissions):
    with st.sidebar:
        st.write(f"👤 User: **{name}**")
        authenticator.logout('Logout', 'sidebar')
        st.divider()
        st.header("⚙️ Configuration")
        
        if 'urls' not in st.session_state: st.session_state['urls'] = None

        # Check Permissions
        my_systems = permissions.get("systems", [])
        def can_view(sys_code): return "ALL" in my_systems or sys_code in my_systems

        b1, b2 = st.sidebar.columns(2)
        with b1:
            if can_view("CS") and st.button("CS FRESH"):
                st.session_state['report_type'] = "CS"
                st.session_state['urls'] = {
                    's': make_url(st.secrets["sheet_ids"]["cs_sales"]),
                    'db': make_url(st.secrets["sheet_ids"]["cs_db"]),
                    'd': make_url(st.secrets["sheet_ids"]["cs_dist"]),
                    'w': make_url(st.secrets["sheet_ids"]["cs_waste"]),
                    'h': make_url(st.secrets["sheet_ids"]["cs_history"])
                }
                st.rerun()
        with b2:
            if can_view("SS") and st.button("SS FRESH"):
                st.session_state['report_type'] = "SS"
                st.session_state['urls'] = {
                    's': make_url(st.secrets["sheet_ids"]["ss_sales"]),
                    'db': make_url(st.secrets["sheet_ids"]["ss_db"]),
                    'd': make_url(st.secrets["sheet_ids"]["ss_dist"]),
                    'w': make_url(st.secrets["sheet_ids"]["ss_waste"]),
                    'h': make_url(st.secrets["sheet_ids"]["ss_history"])
                }
                st.rerun()
        
        b3, b4 = st.sidebar.columns(2)
        with b3:
            if can_view("NTUC") and st.button("NTUC FRESH"):
                st.session_state['report_type'] = "NTUC"
                st.session_state['urls'] = {
                    's': make_url(st.secrets["sheet_ids"]["ntuc_sales"]),
                    'db': make_url(st.secrets["sheet_ids"]["ntuc_db"]),
                    'd': make_url(st.secrets["sheet_ids"]["ntuc_dist"]),
                    'w': make_url(st.secrets["sheet_ids"]["ntuc_waste"]),
                    'h': make_url(st.secrets["sheet_ids"]["ntuc_history"])
                }
                st.rerun()
        with b4:
            if can_view("CS_DRY") and st.button("CS DRY"):
                st.session_state['report_type'] = "CS_DRY"
                st.session_state['urls'] = {
                    's': make_url(st.secrets["sheet_ids"]["cs_dry_sales"]),
                    'db': make_url(st.secrets["sheet_ids"]["cs_dry_db"]),
                    'd': make_url(st.secrets["sheet_ids"]["cs_dry_dist"]),
                    'w': make_url(st.secrets["sheet_ids"]["cs_dry_waste"]),
                    'h': make_url(st.secrets["sheet_ids"]["cs_dry_history"])
                }
                st.rerun()

        b5, b6 = st.sidebar.columns(2)
        with b5:
             if can_view("SS_DRY") and st.button("SS DRY"):
                st.session_state['report_type'] = "SS_DRY"
                st.session_state['urls'] = {
                    's': make_url(st.secrets["sheet_ids"]["ss_dry_sales"]),
                    'db': make_url(st.secrets["sheet_ids"]["ss_dry_db"]),
                    'd': make_url(st.secrets["sheet_ids"]["ss_dry_dist"]),
                    'w': make_url(st.secrets["sheet_ids"]["ss_dry_waste"]),
                    'h': make_url(st.secrets["sheet_ids"]["ss_dry_history"])
                }
                st.rerun()
        with b6:
           if can_view("NTUC_DRY") and st.button("NTUC DRY"):
                st.session_state['report_type'] = "NTUC_DRY"
                st.session_state['urls'] = {
                    's': make_url(st.secrets["sheet_ids"]["ntuc_dry_sales"]),
                    'db': make_url(st.secrets["sheet_ids"]["ntuc_dry_db"]),
                    'd': make_url(st.secrets["sheet_ids"]["ntuc_dry_dist"]),
                    'w': make_url(st.secrets["sheet_ids"]["ntuc_dry_waste"]),
                    'h': make_url(st.secrets["sheet_ids"]["ntuc_dry_history"])
                }
                st.rerun()
        
        st.markdown("---")
        app_mode = st.radio("Mode:", ["📡 Live Analysis", "🗄️ Saved Reports"])

        # with st.expander("☁️ Upload Data"):
        #     st.info("Update Dist/Waste")
        #     up_type = st.radio("Target:", ["Dist", "Waste"])
        #     up_file = st.file_uploader("File", type=['csv','xlsx'])
        #     if up_file and st.button("🚀 Push"):
        #         if st.session_state['urls'] is None: st.error("Select System first"); return
        #         t_url = st.session_state['urls']['d'] if up_type == "Dist" else st.session_state['urls']['w']
        #         if t_url:
        #             if up_file.name.endswith('.csv'): df_up = pd.read_csv(up_file, dtype=str)
        #             else: df_up = pd.read_excel(up_file, dtype=str)
        #             if write_to_sheet(t_url, "Sheet1", df_up): st.success("Updated!")
        #         else: st.error("No URL provided.")
        # with st.expander("☁️ Upload Data (NTUC Smart Update)"):
        #     st.info("Upload your NTUC Excel file. This version preserves GSheet structure, adds new products at bottom and new dates at right.")
            
        #     up_file = st.file_uploader("Upload NTUC Excel File", type=['xlsx'])
            
        #     if up_file and st.button("🚀 Update NTUC Sheets"):
        #         if "NTUC" not in st.session_state.get('report_type', ''):
        #             st.error(f"⚠️ Please select 'NTUC FRESH' or 'NTUC DRY' first.")
        #             st.stop()
                
        #         try:
        #             status_msg = st.empty()
        #             sales_url = st.session_state['urls']['s']
        #             xls = pd.ExcelFile(up_file)
        #             target_tabs = ["Sales", "Quantity"]
        #             progress_bar = st.progress(0)
                    
                   
        #             def standardize_text(val):
        #                 return " ".join(str(val).strip().upper().split())

        #             def standardize_date_col(col):
        #                 try:
        #                     return pd.to_datetime(col, dayfirst=True).strftime('%d/%m/%Y')
        #                 except:
        #                     return standardize_text(col)

        #             for i, tab_name in enumerate(target_tabs):
        #                 if tab_name in xls.sheet_names:
        #                     status_msg.info(f"Processing '{tab_name}'...")
                            
        #                     # A. Read New Data (Excel)
        #                     new_df = pd.read_excel(up_file, sheet_name=tab_name)
        #                     # Harmonize New Column Names
        #                     new_df.columns = [standardize_date_col(c) for c in new_df.columns]

        #                     # B. Read Old Data (Google Sheets)
        #                     client = get_gspread_client()
        #                     sh = client.open_by_url(sales_url)
        #                     ws = sh.worksheet(tab_name)
        #                     gsheet_raw = ws.get_all_values()
                            
        #                     if gsheet_raw:
        #                         # 1. Find Header Index
        #                         search_keys = ['1ST COLUMN', '2ND COLUMN', 'METRIC']
        #                         header_idx = -1
        #                         for r_idx, row in enumerate(gsheet_raw[:20]):
        #                             row_std = [standardize_text(x) for x in row]
        #                             if sum(1 for k in search_keys if k in row_std) >= 3:
        #                                 header_idx = r_idx
        #                                 header_row_raw = row # Save original header string
        #                                 break
                                
        #                         if header_idx == -1:
        #                             st.error(f"❌ Could not find headers in GSheet '{tab_name}'.")
        #                             st.stop()

                               
        #                         top_rows = gsheet_raw[:header_idx]
                                
        #                         old_df = pd.DataFrame(gsheet_raw[header_idx+1:], columns=header_row_raw)
        #                         old_df.columns = [standardize_date_col(c) for c in old_df.columns]
                                
        #                         # 3. Standardize Row Keys (The Matchmaker)
        #                         keys = ['1ST COLUMN', '2ND COLUMN', 'METRIC']
        #                         for k in keys:
        #                             if k in old_df.columns: old_df[k] = old_df[k].apply(standardize_text)
        #                             if k in new_df.columns: new_df[k] = new_df[k].apply(standardize_text)

        #                         # 4. SET INDEX & MERGE
                               
        #                         old_df = old_df.drop_duplicates(subset=keys).set_index(keys)
        #                         new_df = new_df.drop_duplicates(subset=keys).set_index(keys)

                                
        #                         final_row_order = old_df.index.tolist()
        #                         brand_new_products = [idx for idx in new_df.index if idx not in old_df.index]
        #                         final_row_order.extend(brand_new_products)

                               
        #                         final_col_order = old_df.columns.tolist()
        #                         brand_new_dates = [c for c in new_df.columns if c not in old_df.columns]
        #                         final_col_order.extend(brand_new_dates)

                                
        #                         old_df = old_df.replace(r'^\s*$', np.nan, regex=True)
        #                         new_df = new_df.replace(r'^\s*$', np.nan, regex=True)
                                
        #                         combined = new_df.combine_first(old_df)
                                
                                
        #                         final_df = combined.reindex(index=final_row_order, columns=final_col_order).reset_index()

        #                         # 5. RECONSTRUCT & UPLOAD
        #                         final_header = final_df.columns.tolist()
        #                         final_data = final_df.fillna("").astype(str).values.tolist()
                                
        #                         # Pad Top Metadata rows to match new width
        #                         width = len(final_header)
        #                         padded_top = [tr + [''] * (width - len(tr)) if len(tr) < width else tr[:width] for tr in top_rows]
                                
        #                         final_output = padded_top + [final_header] + final_data
                                
        #                         ws.clear()
        #                         ws.resize(rows=len(final_output), cols=len(final_output[0]))
        #                         ws.update(final_output)
                                
        #                     else:
        #                         write_to_sheet(sales_url, tab_name, new_df)
                                
        #                 progress_bar.progress((i + 1) / len(target_tabs))
                    
        #             status_msg.success("✅ Tally Successful! Existing data preserved, new items and dates added.")
        #             st.balloons()
        #             st.cache_data.clear()
                    
        #         except Exception as e:
        #             st.error(f"Error during update: {e}")
        

    
    
    if st.session_state['urls'] is None:
        st.info("👈 Please select a Report System from the sidebar to begin.")
        return

    urls = st.session_state['urls']
    rpt = st.session_state['report_type']
    st.caption(f"Active System: {rpt}")

    if app_mode == "📡 Live Analysis":
        with st.spinner("Fetching Live Data for {rpt}..."):

            r_s = load_google_sheet(urls['s'])
            r_db = load_google_sheet(urls['db'])
            r_d = load_google_sheet(urls['d'])
            # Only load wastage file if not CS_DRY
            r_w = None if rpt == "CS_DRY" or rpt == "SS_DRY" else load_google_sheet(urls['w'])

            if r_s is not None and r_d is not None:
                res = process_data(r_s, r_db, r_d, r_w, rpt)
                if res:
                    # 1. Variables are defined here
                    df_s, df_d, df_w, map_name, map_art, _, update_info = res
                    
                    my_stores = permissions.get("stores", [])
                    if "ALL" not in my_stores:
                        if not df_s.empty: df_s = df_s[df_s['Store'].isin(my_stores)]
                        if not df_d.empty: df_d = df_d[df_d['Store'].isin(my_stores)]
                        if not df_w.empty: df_w = df_w[df_w['Store'].isin(my_stores)]
                        st.warning(f"🔒 View restricted to assigned stores.")

                    
                    st.caption(f"""
                    **Last Data Updates:** 🛒 Sales: **{update_info['Sales']}** |  🚚 Dist: **{update_info['Dist']}** |  🗑️ Waste: **{update_info['Waste']}**
                    """)
                    
                    st.sidebar.markdown("---")
                    st.sidebar.header("Filters")

                    
                    all_years = sorted(list(set(df_s['Year'].dropna()) | set(df_d['Year'].dropna()) | set(df_w['Year'].dropna() if not df_w.empty else [])), reverse=True)
                    if not all_years: all_years = ["2025"] 
                    sel_year = st.sidebar.selectbox("Select Year", all_years)
                if sel_year:
                    df_s = df_s[df_s['Year'] == sel_year]
                    df_d = df_d[df_d['Year'] == sel_year]
                    if not df_w.empty:
                        df_w = df_w[df_w['Year'] == sel_year]

                    # Filter
                    ft = st.sidebar.radio("Filter:", ["Month", "Week"])
                    if ft == "Month":
                        group_col = "Month"
                        month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        opts = sorted(list(set(df_s['Month']) | set(df_d['Month']) | set(df_w['Month'] if not df_w.empty else [])), key=lambda x: month_order.index(x) if x in month_order else 99)
                        if opts:
                            default_opts = opts[-2:] if len(opts) > 1 else opts
                        else:
                            default_opts = []
                        sel = st.sidebar.multiselect("Select", opts, default=default_opts)
                        if sel:
                            df_s = df_s[df_s['Month'].isin(sel)]
                            df_d = df_d[df_d['Month'].isin(sel)]
                            if not df_w.empty:
                                df_w = df_w[df_w['Month'].isin(sel)]
                    else:
                        group_col = "Week" # Dynamic grouping variable
                        opts = sorted(list(set(df_s['Week']) | set(df_d['Week']) | set(df_w['Week'] if not df_w.empty else [])), reverse=True)
                        sel = st.sidebar.multiselect("Select", opts, default=opts[:4] if len(opts)>0 else opts) # Default to last 4 weeks
                        if sel:
                            df_s = df_s[df_s['Week'].isin(sel)]
                            df_d = df_d[df_d['Week'].isin(sel)]
                            if not df_w.empty:
                                df_w = df_w[df_w['Week'].isin(sel)]

                    # Calculation
                    s_grp = df_s.groupby([group_col,'Store', 'NAV'])[['Qty', 'Val']].sum().reset_index().rename(columns={'Qty': 'Sales_Qty', 'Val': 'Sales_Val'})
                    d_grp = df_d.groupby([group_col,'Store', 'NAV'])[['Qty', 'Val']].sum().reset_index().rename(columns={'Qty': 'Dist_Qty', 'Val': 'Dist_Val'})
                    if not df_w.empty:
                        w_grp = df_w.groupby([group_col,'Store', 'NAV'])[['Qty', 'Val']].sum().reset_index().rename(columns={'Qty': 'Waste_Qty', 'Val': 'Waste_Val'})
                    else:
                        w_grp = pd.DataFrame(columns=[group_col, 'Store', 'NAV', 'Waste_Qty', 'Waste_Val'])

                    df = pd.merge(d_grp, s_grp, on=[group_col,'Store', 'NAV'], how='outer')
                    if not w_grp.empty:
                        df = pd.merge(df, w_grp, on=[group_col,'Store', 'NAV'], how='outer').fillna(0)
                    else:
                        df['Waste_Qty'] = 0
                        df['Waste_Val'] = 0
                    
                    df['Article_Code'] = df['NAV'].map(map_art).fillna("0")
                    df.loc[df['Article_Code'] == "0", 'Article_Code'] = "Unmapped (NAV " + df['NAV'].astype(str) + ")"

                    df['Item_Name'] = df['NAV'].map(map_name).fillna("Unknown Item")
                    mask_unknown = df['Item_Name'] == "Unknown Item"
                    df.loc[mask_unknown, 'Item_Name'] = "Item " + df.loc[mask_unknown, 'NAV'].astype(str)
                    df['Profit'] = df['Sales_Val'] - df['Dist_Val']
                    # df['Balance_Qty'] = df['Dist_Qty'] - df['Sales_Qty'] - df['Waste_Qty']
                    
                   

                    # Views
                    v_s_qty = df.groupby([group_col,'Store'])[['Dist_Qty', 'Sales_Qty', 'Waste_Qty']].sum()
                    v_s_qty['STR%'] = (v_s_qty['Sales_Qty']/ v_s_qty['Dist_Qty'])*100
                    v_s_qty['STR%'] = v_s_qty['STR%'].replace([np.inf, -np.inf], 0).fillna(0)
                    v_s_val = df.groupby([group_col,'Store'])[['Dist_Val', 'Sales_Val', 'Waste_Val','Profit']].sum()
                    v_i_qty = df.groupby([group_col,'Article_Code', 'Item_Name'])[['Dist_Qty', 'Sales_Qty', 'Waste_Qty']].sum()
                    v_i_qty['STR%'] = (v_i_qty['Sales_Qty'] / v_i_qty['Dist_Qty'] * 100).replace([np.inf, -np.inf], 0).fillna(0).round(2)
                    v_i_qty = v_i_qty.sort_values('Dist_Qty', ascending=False)
                    v_i_val = df.groupby([group_col,'Article_Code', 'Item_Name'])[['Dist_Val', 'Sales_Val', 'Waste_Val']].sum().sort_values('Dist_Val', ascending=False)
                    v_top10_all = df.groupby([group_col, 'Item_Name'])['Sales_Val'].sum().reset_index()



                    st.subheader(f"📊 {rpt} Live Report ({sel_year}-{ft})")
                    t1, t2, t3, t4, t5, t6 = st.tabs(["📦 QTY (Store)", "💰 $ (Store)", "📦 QTY (Item)", "💰 $ (Item)", "🏆 Top 10", "📉 Bottom 10"])

                    def display_drilldown(tab, main_df, detail_cols, sort_col, fmt, time_col):
                        with tab:
                            if main_df.empty:
                                st.info("No data.")
                                return
                            # 1. Store Summary
                            summary = main_df.unstack(level=0, fill_value=0)
                            # Calculate Totals
                            metrics = summary.columns.get_level_values(0).unique()
                            for m in metrics:
                                m_cols = summary.loc[:, (m, slice(None))].columns
                                for c in m_cols:
                                    summary[c] = pd.to_numeric(summary[c], errors='coerce').fillna(0)
                                summary[(m, 'TOTAL')] = summary[m_cols].sum(axis=1)
                            if (sort_col, 'TOTAL') in summary.columns:
                                summary = summary.sort_values((sort_col, 'TOTAL'), ascending=False)
                            st.markdown(f"### 🏢 Store Summary")
                            st.dataframe(summary.style.format(fmt), height=400, use_container_width=True)
                            st.divider()
                            # 2. FAST DRILL-DOWN (Selectbox instead of Loop)
                            st.markdown("### 🔍 Select Store to View Details")
                            store_options = [f"{s}" for s in summary.index]

                            for store in summary.index:
                                val = summary.loc[store, (sort_col, 'TOTAL')]
                                store_options.append(f"{store} | Total {sort_col}: {val:,.2f}")
                            sel_store_str = st.selectbox(f"Select Store ({sort_col})", options=store_options, key=f"sel_{sort_col}")
                            if sel_store_str:
                                selected_store = sel_store_str.split(" | ")[0]
                                store_mask = df['Store'] == selected_store
                                # Check if time_col in df columns for groupby
                                if time_col not in df.columns:
                                    st.warning(f"Cannot drill down: '{time_col}' not found in data columns.")
                                    return
                                detail_view = df[store_mask].groupby(['Item_Name', time_col])[detail_cols].sum().unstack(level=1, fill_value=0)
                                d_metrics = detail_view.columns.get_level_values(0).unique()
                                for m in d_metrics:
                                    m_cols = detail_view.loc[:, (m, slice(None))].columns
                                    for c in m_cols:
                                        detail_view[c] = pd.to_numeric(detail_view[c], errors='coerce').fillna(0)
                                    detail_view[(m, 'TOTAL')] = detail_view[m_cols].sum(axis=1)
                                if (sort_col, 'TOTAL') in detail_view.columns:
                                    detail_view = detail_view.sort_values((sort_col, 'TOTAL'), ascending=False)
                                st.markdown(f"#### 📦 Items in {selected_store}")
                                st.dataframe(detail_view.style.format(fmt), width='stretch')
                    
                    # Tab 1: Store QTY (Drilldown shows Dist, Sales, Waste, Balance)
                    display_drilldown(
                        t1, 
                        v_s_qty, 
                        ['Dist_Qty', 'Sales_Qty', 'Waste_Qty'], # Columns to show in detail
                        'Sales_Qty', # Column to sort by
                        "{:,.2f}",group_col
                    ) 

                    # Tab 2: Store Val (Drilldown shows Dist, Sales, Waste)
                    display_drilldown(
                        t2, 
                        v_s_val, 
                        ['Dist_Val', 'Sales_Val', 'Waste_Val'], # Columns to show in detail
                        'Sales_Val', # Column to sort by
                        "{:,.2f}",group_col
                    )
                    def display_item_drilldown(tab, detail_cols, sort_col, fmt, time_col):
                        with tab:
                            
                            summary = df.groupby(['Item_Name', time_col])[detail_cols].sum().unstack(level=1, fill_value=0)
                            if summary.empty:
                                st.info("No data.")
                                return
                            # Calculate Totals
                            metrics = summary.columns.get_level_values(0).unique()
                            for m in metrics:
                                m_cols = summary.loc[:, (m, slice(None))].columns
                                for c in m_cols:
                                    summary[c] = pd.to_numeric(summary[c], errors='coerce').fillna(0)
                                summary[(m, 'TOTAL')] = summary[m_cols].sum(axis=1)

                            if 'Sales_Qty' in metrics and 'Dist_Qty' in metrics:
                                sales_total = summary[('Sales_Qty', 'TOTAL')]
                                dist_total =summary[('Dist_Qty','TOTAL')]
                                str_vals = (sales_total/dist_total * 100).replace([float('inf'), -float('inf')], 0)
                                summary[('STR%', 'TOTAL')] = str_vals.round(2)
                            if (sort_col, 'TOTAL') in summary.columns:
                                summary = summary.sort_values((sort_col, 'TOTAL'), ascending=False)
                            st.markdown(f"### 📦 Item Summary")
                            st.dataframe(summary.style.format(fmt), height=400, use_container_width=True)
                            st.divider()
                            # 2. FAST DRILL-DOWN
                            st.markdown("### 🔍 Select Item to View Stores")
                            limit_list = summary.index[:2000]
                            item_options = []
                            for item in limit_list:
                                val = summary.loc[item, (sort_col, 'TOTAL')]
                                item_options.append(f"{item} | Total {sort_col}: {val:,.2f}")
                            sel_item_str = st.selectbox(f"Select Item ({sort_col})", options=item_options, key=f"sel_item_{sort_col}")
                            if sel_item_str:
                                selected_item = sel_item_str.split(" | ")[0]
                                item_mask = df['Item_Name'] == selected_item
                                if time_col not in df.columns:
                                    st.warning(f"Cannot drill down: '{time_col}' not found in data columns.")
                                    return
                                item_view = df[item_mask].groupby(['Store', time_col])[detail_cols].sum().unstack(level=1, fill_value=0)
                                d_metrics = item_view.columns.get_level_values(0).unique()
                                for m in d_metrics:
                                    m_cols = item_view.loc[:, (m, slice(None))].columns
                                    for c in m_cols:
                                        item_view[c] = pd.to_numeric(item_view[c], errors='coerce').fillna(0)
                                    item_view[(m, 'TOTAL')] = item_view[m_cols].sum(axis=1)
                                if (sort_col, 'TOTAL') in item_view.columns:
                                    item_view = item_view.sort_values((sort_col, 'TOTAL'), ascending=False)
                                st.markdown(f"#### 📍 Stores selling {selected_item}")
                                st.dataframe(item_view.sort_index(axis=1).style.format(fmt), width='stretch')

                    # Tab 3 & 4: Item Views (Keep as simple Pivot)
                    def display_simple_pivot(tab, df_in, fmt,time_col):
                        with tab:
                            try:
                                p = df_in.unstack(level=time_col, fill_value=0)
                                p['Total'] = p.sum(axis=1)
                                p = p.sort_values('Total', ascending=False).drop(columns=['Total'])
                                st.dataframe(p.style.format(fmt))

                                st.markdown("---")
                                st.markdown("### 🔍 Store Details (Click to Expand)")
                                
                            except: st.info("No data")

                    display_item_drilldown(
                        t3, 
                        ['Dist_Qty', 'Sales_Qty', 'Waste_Qty'], 
                        'Sales_Qty', "{:,.2f}",group_col
                    )

                    # Tab 4: Item Val (Item -> Stores) - NEW LOGIC
                    display_item_drilldown(
                        t4, 
                        ['Dist_Val', 'Sales_Val', 'Waste_Val'], 
                        'Sales_Val', "{:,.2f}",group_col
                    )

                    with t5:
                        if not v_top10_all.empty:
                        
                            top10_grp = v_top10_all.groupby('Item_Name')['Sales_Val'].sum()
                            top10_items = top10_grp.nlargest(10).index.tolist()
                            top10_df = v_top10_all[v_top10_all['Item_Name'].isin(top10_items)].set_index([group_col, 'Item_Name'])
                            v_top10 = df.groupby('Item_Name')['Sales_Val'].sum().sort_values(ascending=False).head(10).reset_index()

                            
                            try:
                                t10_pivot = top10_df.unstack(level=0, fill_value=0)
                                t10_pivot[('Sales_Val', 'TOTAL')] = t10_pivot['Sales_Val'].sum(axis=1)
                                t10_pivot = t10_pivot.sort_values(('Sales_Val', 'TOTAL'), ascending=False)
                                st.dataframe(t10_pivot.style.format("{:,.2f}"))
                                chart_data = t10_pivot[('Sales_Val', 'TOTAL')].rename("Total Sales")
                                st.bar_chart(chart_data)
                                
                            except Exception as e:
                                st.error(f"Error in Top 10: {e}")
                        else:
                            st.info("No Sales Data available for Top 10.")
                    
                    with t6:
                        valid_items_df = v_top10_all[
                            (~v_top10_all['Item_Name'].str.startswith('Item ')) & 
                            (v_top10_all['Item_Name'] != 'Unknown Item')
                        ]
                        
                        if not valid_items_df.empty:
                            bottom10_grp = valid_items_df.groupby('Item_Name')['Sales_Val'].sum()
                            bottom10_items = bottom10_grp.nsmallest(10).index.tolist()
                            
                            bottom10_df = valid_items_df[valid_items_df['Item_Name'].isin(bottom10_items)].set_index([group_col, 'Item_Name'])
                            
                            try:
                                b10_pivot = bottom10_df.unstack(level=0, fill_value=0)
                                b10_pivot[('Sales_Val', 'TOTAL')] = b10_pivot['Sales_Val'].sum(axis=1)
                                b10_pivot = b10_pivot.sort_values(('Sales_Val', 'TOTAL'), ascending=True)
                                st.dataframe(b10_pivot.style.format("${:,.2f}"))
                                chart_data = b10_pivot[('Sales_Val', 'TOTAL')].rename("Total Sales")
                                st.bar_chart(chart_data)
                            except Exception as e:
                                st.error(f"Error in Bottom 10: {e}")
                        else:
                            st.info("No valid sales data for Bottom 10.")
                    st.divider()
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        # 1. Store Qty (Pivoted like visual)
                        qty_pivot = v_s_qty.unstack(level=0).fillna(0)
                        metrics = qty_pivot.columns.get_level_values(0).unique()
                        for m in metrics:
                            m_cols = qty_pivot.loc[:, (m, slice(None))].columns
                            for c in m_cols:
                                qty_pivot[c] = pd.to_numeric(qty_pivot[c], errors='coerce').fillna(0)
                            qty_pivot[(m, 'TOTAL')] = qty_pivot[m_cols].sum(axis=1)
                        if ('Sales_Qty', 'TOTAL') in qty_pivot.columns:
                            qty_pivot = qty_pivot.sort_values(('Sales_Qty', 'TOTAL'), ascending=False)
                        if ('Dist_Qty', 'TOTAL') in qty_pivot.columns:
                            qty_pivot = qty_pivot.sort_values(('Dist_Qty', 'TOTAL'), ascending=False)
                        qty_pivot.to_excel(writer, sheet_name='Store QTY Analysis')
                        
                        # 2. Store Value (Pivoted like visual)
                        val_pivot = v_s_val.unstack(level=0).fillna(0)
                        metrics = val_pivot.columns.get_level_values(0).unique()
                        for m in metrics:
                            m_cols = val_pivot.loc[:, (m, slice(None))].columns
                            for c in m_cols:
                                val_pivot[c] = pd.to_numeric(val_pivot[c], errors='coerce').fillna(0)
                            val_pivot[(m, 'TOTAL')] = val_pivot[m_cols].sum(axis=1)
                        if ('Sales_Val', 'TOTAL') in val_pivot.columns:
                            val_pivot = val_pivot.sort_values(('Sales_Val', 'TOTAL'), ascending=False)
                        if ('Dist_Val', 'TOTAL') in val_pivot.columns:
                            val_pivot = val_pivot.sort_values(('Dist_Val', 'TOTAL'), ascending=False)
                        val_pivot.to_excel(writer, sheet_name='Store Value Analysis')
                        
                        # 3. Item Qty Summary (Top Items by Qty)
                        item_qty_pivot = v_i_qty.unstack(level=0).fillna(0)
                        metrics = item_qty_pivot.columns.get_level_values(0).unique()
                        for m in metrics:
                            m_cols = item_qty_pivot.loc[:, (m, slice(None))].columns
                            for c in m_cols:
                                item_qty_pivot[c] = pd.to_numeric(item_qty_pivot[c], errors='coerce').fillna(0)
                            item_qty_pivot[(m, 'TOTAL')] = item_qty_pivot[m_cols].sum(axis=1)
                        if ('Sales_Qty', 'TOTAL') in item_qty_pivot.columns:
                            item_qty_pivot = item_qty_pivot.sort_values(('Sales_Qty', 'TOTAL'), ascending=False)
                        if ('Dist_Qty', 'TOTAL') in item_qty_pivot.columns:
                            item_qty_pivot = item_qty_pivot.sort_values(('Dist_Qty', 'TOTAL'), ascending=False)
                        item_qty_pivot.to_excel(writer, sheet_name='Item QTY Summary')

                        # 4. Item Value Summary (Top Items by Value)
                        item_val_pivot = v_i_val.unstack(level=0).fillna(0)
                        metrics = item_val_pivot.columns.get_level_values(0).unique()
                        for m in metrics:
                            m_cols = item_val_pivot.loc[:, (m, slice(None))].columns
                            for c in m_cols:
                                item_val_pivot[c] = pd.to_numeric(item_val_pivot[c], errors='coerce').fillna(0)
                            item_val_pivot[(m, 'TOTAL')] = item_val_pivot[m_cols].sum(axis=1)
                        if ('Sales_Val', 'TOTAL') in item_val_pivot.columns:
                            item_val_pivot = item_val_pivot.sort_values(('Sales_Val', 'TOTAL'), ascending=False)
                        if ('Dist_Val', 'TOTAL') in item_val_pivot.columns:
                            item_val_pivot = item_val_pivot.sort_values(('Dist_Val', 'TOTAL'), ascending=False)
                        item_val_pivot.to_excel(writer, sheet_name='Item Value Summary')

                        # 5. Top 10 Data
                        if not v_top10_all.empty:
                            # Group to get total sales per item for the list
                            top10_export = v_top10_all.groupby('Item_Name')['Sales_Val'].sum().sort_values(ascending=False).head(10).reset_index()
                            top10_export.to_excel(writer, sheet_name='Top 10 Items', index=False)

                        # 6. Master Data (Raw combined data)
                        df.to_excel(writer, sheet_name='Master Data Raw', index=False)

                    excel_data = output.getvalue()
                    
                    col_d1, col_d2 = st.columns([2,1])
                    with col_d1:
                         st.download_button(
                            label="📥 Download Full Excel Report",
                            data=excel_data,
                            file_name=f"Report_{sel_year}_{rpt}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            help="Downloads a multi-tab Excel file with all summaries."
                        )

                    c1, c2 = st.columns([3, 1])
                    rep_name = c1.text_input("Report Name (e.g. Week48)", "")
                    if c2.button("💾 Save All to History"):
                        if urls['h'] and rep_name:
                            with st.spinner("Saving..."):
                                write_to_sheet(urls['h'], f"Rep_{rep_name}_StoreQty", v_s_qty)
                                write_to_sheet(urls['h'], f"Rep_{rep_name}_StoreVal", v_s_val)
                                write_to_sheet(urls['h'], f"Rep_{rep_name}_ItemQty", v_i_qty)
                                write_to_sheet(urls['h'], f"Rep_{rep_name}_ItemVal", v_i_val)
                                write_to_sheet(urls['h'], f"Rep_{rep_name}_Top10", v_top10)
                                write_to_sheet(urls['h'], f"Rep_{rep_name}_Master", df)
                                st.success("✅ Saved!")
                        else: st.error("Need URL & Name")

    elif app_mode == "🗄️ Saved Reports":
        if urls['h']:
            reps = get_saved_reports(urls['h'])
            if reps:
                sel = st.selectbox("Select Report:", reps)
                
                if sel:
                    # 1. LOAD DATA FIRST (Inside Spinner)
                    loaded_data = {}
                    sheet_tabs = ["StoreQty", "StoreVal", "ItemQty", "ItemVal", "Top10", "Master"]
                    
                    with st.spinner("Downloading Report Data..."):
                        try:
                            client = get_gspread_client()
                            sh = client.open_by_url(urls['h'])
                            
                            # Pre-fetch all necessary tabs to avoid UI lag later
                            for tab_name in sheet_tabs:
                                try:
                                    full_data = sh.worksheet(f"Rep_{sel}_{tab_name}").get_all_values()
                                    if full_data:
                                        header = full_data[0]
                                        rows = full_data[1:]
                                        loaded_data[tab_name] = pd.DataFrame(rows, columns=header)
                                    else:
                                        loaded_data[tab_name] = pd.DataFrame()
                                except:
                                    loaded_data[tab_name] = pd.DataFrame()
                                    
                        except Exception as e:
                            st.error(f"Connection Error: {e}")
                            st.stop()

                    # 2. RENDER UI (Outside Spinner - Prevents White Screen Error)
                    if loaded_data:
                        # Create Tabs
                        t1, t2, t3, t4, t5, t6 = st.tabs([
                            "📦 Store Qty", 
                            "💰 Store Val", 
                            "📦 Item Qty", 
                            "💰 Item Val", 
                            "🏆 Top 10", 
                            "📝 Master Data"
                        ])

                        # Render Dataframes safely
                        with t1: 
                            st.dataframe(loaded_data.get("StoreQty", pd.DataFrame()), use_container_width=True)
                        
                        with t2: 
                            st.dataframe(loaded_data.get("StoreVal", pd.DataFrame()), use_container_width=True)
                        
                        with t3: 
                            st.dataframe(loaded_data.get("ItemQty", pd.DataFrame()), use_container_width=True)
                        
                        with t4: 
                            st.dataframe(loaded_data.get("ItemVal", pd.DataFrame()), use_container_width=True)
                        
                        with t5: 
                            df_top = loaded_data.get("Top10", pd.DataFrame())
                            st.dataframe(df_top, use_container_width=True)
                            # Try to render chart if data exists
                            if not df_top.empty and 'Total Sales' in df_top.columns:
                                try:
                                    # Ensure numeric for chart
                                    df_top['Total Sales'] = pd.to_numeric(df_top['Total Sales'], errors='coerce')
                                    st.bar_chart(df_top.set_index(df_top.columns[0])['Total Sales'])
                                except: pass

                        with t6: 
                            st.dataframe(loaded_data.get("Master", pd.DataFrame()), use_container_width=True)
