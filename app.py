import streamlit as st
import pandas as pd
import re
import io
import gspread
from oauth2client.service_account import ServiceAccountCredentials

st.set_page_config(page_title="Fresh PPL Report System", layout="wide",page_icon="📊")
st.title("📊 Fresh PPL Report System")
st.info("ℹ️ Cloud System: Upload Dist/Waste once. Auto-updates from Sales/DB. Save full report history.")


@st.cache_resource
def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gcp_service_account"], scope)
    return gspread.authorize(creds)

@st.cache_data(ttl=600)
def load_google_sheet(url, sheet_name=0):
    try:
        client = get_gspread_client()
        sheet = client.open_by_url(url)
        # If sheet_name is a string, open by name; else open by index
        if isinstance(sheet_name, str):
            worksheet = sheet.worksheet(sheet_name)
        else:
            worksheet = sheet.get_worksheet(sheet_name)
            
        data = worksheet.get_all_values()
        return pd.DataFrame(data)
    except Exception as e:
        # st.error(f"Error loading {sheet_name}: {e}") # Optional debug
        return None

def write_to_sheet(url, sheet_name, df):
    """Saves a dataframe to a specific tab."""
    try:
        client = get_gspread_client()
        sh = client.open_by_url(url)
        try: 
            ws = sh.worksheet(sheet_name)
            ws.clear()
        except: 
            ws = sh.add_worksheet(title=sheet_name, rows=100, cols=20)
        
        # Prepare Data
        df_str = df.fillna("").astype(str)
        if df.index.name is not None or not isinstance(df.index, pd.RangeIndex):
            df_str = df_str.reset_index()
            
        data = [df_str.columns.values.tolist()] + df_str.values.tolist()
        ws.resize(rows=len(data), cols=len(data[0]))
        ws.update(data)
        return True
    except Exception as e:
        st.error(f"Error saving {sheet_name}: {e}")
        return False

def get_saved_reports(url):
    
        client = get_gspread_client()
        sh = client.open_by_url(url)
        titles = [ws.title for ws in sh.worksheets()]
        reports = set()
        for t in titles:
            if t.startswith("Rep_"):
                parts = t.split('_') 
                if len(parts) >= 2: reports.add(parts[1])
        return sorted(list(reports), reverse=True)
   

# --- 2. HELPER FUNCTIONS (Strictly from your code) ---
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
        return name

    return name


    
def parse_uom_factor(uom_str):
    if pd.isna(uom_str): return 1.0
    s = str(uom_str).upper().strip()
    if 'KG' in s: return 1.0
    match = re.search(r'(\d+)G', s)
    if match: return float(match.group(1)) / 1000.0
    return 1.0

def clean_header(header):
    return str(header).replace('\n', ' ').replace('\r', ' ').strip().upper()

def clean_id(val):
    if pd.isna(val) or val == '': return "0"
    s = str(val).strip().upper()
    if s == 'NAN' or s == 'NONE': return "0"
    if "HCZX" in s: return "0"
    s = s.split('-')[0].strip()
    if s.endswith('.0'): s = s[:-2]
    return s

def clean_currency(val):
    """Handles both 4,54 (EU) and 4.54 (US) formats correctly"""
    if pd.isna(val) or str(val).strip() == "": return 0.0
    s = str(val).strip().replace('$', '').replace(' ', '')
    if ',' in s and '.' not in s: s = s.replace(',', '.')
    elif ',' in s and '.' in s:
        if s.rfind(',') > s.rfind('.'): s = s.replace('.', '').replace(',', '.') 
        else: s = s.replace(',', '') 
    try: return float(s)
    except: return 0.0

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

# --- 3. DATA PROCESSOR ---
@st.cache_data
def process_data(df_sales_raw, df_db_raw, df_dist_raw, df_waste_raw, report_type):
    master_name_map = {}
    nav_to_article_map = {} 

    if report_type =="CS":
        db_cols = {'Article': ['Article', 'ITEMCODE'], 'NAV': ['NAV', 'NAV_CODE', 'No.'], 'ArtDesc': ['Article Description', 'ArticleDesc'], 'NavDesc': ['NAV description', 'Description']}
        sales_cols ={'Article': ['Article', 'ITEMCODE'], 'Qty': ['Quantity','QTY','SALESQTY','Billed Quantity'], 'Val': ['Amount','SALESAMOUNT','Total Amount'], 'Store': ['STOREDESC', 'Store name'], 'Date': ['TRXDATE','Date'], 'Name': ['ITEMDESC', 'Description', 'Name']}

    elif report_type == "SS":
        db_cols = {'Article': ['ITEM CODE', 'Article'], 'NAV': ['NAV CODE', 'NAV'], 'ArtDesc': ['DESCRIPTION', 'Article Description'], 'NavDesc': ['NAV Description']}
        # Sales: ITEM CODE, OUTLET, QTY, SALES BEF GST
        sales_cols = {'Article': ['ITEM CODE', 'Article'], 'Qty': ['QTY', 'Quantity'], 'Val': ['SALES BEF GST', 'Total Amount', 'Amount'], 'Store': ['OUTLET', 'Store'], 'Date': ['Date', 'TRXDATE'], 'Name': ['DESCRIPTION', 'Name']}

    elif report_type =="NTUC":
        db_cols = {'Article': ['cno_sku'], 'NAV': ['id'], 'ArtDesc': ['name1'], 'NavDesc': ['name2']}
        sales_cols = {'Store': ['1st Column'], 'Raw_Item': ['2nd Column']}

    # Common Maps
    dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['Your Reference', 'key'], 'UOM': ['Unit of Measure', 'UOM'], 'Name': ['USOFT product description', 'Description', 'Name'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date','Date'], 'Chain': ['External Doc No.']}
    waste_cols = {'NAV': ['NAV', 'NAV_CODE'], 'Qty': ['QTY', 'Quantity'], 'Weight': ['WEIGHT'], 'Store': ['Store', 'LONG_NAME'], 'Val': ['Amount', 'TOT_AMT'], 'Date': ['DATE', 'Date'], 'Chain': ['MAIN_CODE']}



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
    if report_type == "NTUC":
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



    # df_sales = find_correct_header_row(df_sales_raw, sales_cols, "Sales Sheet")
    # if df_sales is None: return None
    # df_sales = strict_rename(df_sales, sales_cols)
    
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
        if report_type == 'SS':
            # Sheng Siong: 09-12-2025 (Day-Month-Year)
            df_sales['Date'] = pd.to_datetime(df_sales['Date'], dayfirst=True, errors='coerce')
        else:
            # Cold Storage: 2025.12.31 (Year.Month.Day)
            df_sales['Date'] = pd.to_datetime(df_sales['Date'], format='%Y.%m.%d', errors='coerce')
            
        df_sales['Year'] = df_sales['Date'].dt.year.astype(str).str.replace(r'\.0$', '', regex=True)
        df_sales['Month'] = df_sales['Date'].dt.month_name().str[:3]
        df_sales['Week'] = df_sales['Date'].dt.strftime('%Y-W%U')
    else:
        
        df_sales['Year'] = "2025" 
        df_sales['Month'] = "ALL"
        df_sales['Week'] = "ALL"

    # --- C. DISTRIBUTION ---
    # d_map = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['Your Reference', 'key'], 'UOM': ['Unit of Measure', 'UOM'], 'Name': ['USOFT product description', 'Description', 'Name'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date','Date'], 'Chain': ['Customer']}
    df_dist = find_correct_header_row(df_dist_raw, dist_cols, "Dist Sheet")
    if df_dist is None: return None
    df_dist = strict_rename(df_dist, dist_cols)
    
    if 'Store' in df_dist.columns:
        if report_type =='CS':
            mask = df_dist['Store'].astype(str).str.upper().str.contains('CS |COLD STORAGE|CS_|COMPASS ONE|MP |NOVENA |JS |MARINA |GT |FAR ', regex=True, na=False)
            df_dist=df_dist[mask]
        elif report_type == 'SS':
            mask = df_dist['Store'].astype(str).str.upper().str.contains(r'SHENG SIONG|^SS |^SS_', regex=True, na=False)
            df_dist=df_dist[mask]
        elif report_type == 'NTUC':
            mask = df_dist['Chain'].astype(str).str.upper().str.contains(r'NTUC', regex=True, na=False)
            df_dist = df_dist[mask]
       

    
    if 'Chain' in df_dist.columns and 'Store' not in df_dist.columns:
         mask_chain = df_dist['Chain'].astype(str).str.upper().str.contains('HX', na=False)
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
    
    if 'UOM' in df_dist.columns:
        raw_qty = pd.to_numeric(df_dist['Qty'], errors='coerce').fillna(0)
        uom_factor = df_dist['UOM'].apply(parse_uom_factor)
        df_dist['Qty'] = raw_qty * uom_factor 
    else:
        df_dist['Qty'] = pd.to_numeric(df_dist['Qty'], errors='coerce').fillna(0)
        
    cost = df_dist['Cost'] if 'Cost' in df_dist.columns else 0
    df_dist['Val'] = df_dist['Qty'] * pd.to_numeric(cost, errors='coerce').fillna(0)

    # --- D. WASTAGE ---
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
    df_waste['Year'] = df_sales['Date'].dt.year.astype(str).replace(r'\.0$', '', regex=True)
    df_waste['Month'] = df_waste['Date'].dt.month_name().str[:3]
    df_waste['Week'] = df_waste['Date'].dt.strftime('%Y-W%U')
    
    qty_units = df_waste['Qty'].apply(clean_currency)
    weight_kg = df_waste['Weight'].apply(clean_currency)
    df_waste['Qty'] = qty_units * weight_kg
    df_waste['Val'] = df_waste['Val'].apply(clean_currency)

    return df_sales, df_dist, df_waste, master_name_map, nav_to_article_map, []

# --- 4. APP UI ---
st.sidebar.header("⚙️ Configuration")

if 'urls' not in st.session_state:
    st.session_state['urls'] =None 

def make_url(sheet_id):
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"

button_col1, button_col2= st.sidebar.columns(2)
with button_col1:
    if st.button("CS FRESH PPL REPORT"):
        st.session_state['report_type'] = "CS"
        st.session_state['urls'] ={
            's':  make_url(st.secrets["sheet_ids"]["cs_sales"]),
            'db': make_url(st.secrets["sheet_ids"]["cs_db"]),
            'd':  make_url(st.secrets["sheet_ids"]["cs_dist"]),
            'w':  make_url(st.secrets["sheet_ids"]["cs_waste"]),
            'h':  make_url(st.secrets["sheet_ids"]["cs_history"])
        }
        st.rerun()
with button_col2:
    if st.button("SS FRESH PPL REPORT"):
        st.session_state['report_type'] = "SS"
        st.session_state['urls'] = {
            's':  make_url(st.secrets["sheet_ids"]["ss_sales"]),
            'db': make_url(st.secrets["sheet_ids"]["ss_db"]),
            'd':  make_url(st.secrets["sheet_ids"]["ss_dist"]),
            'w':  make_url(st.secrets["sheet_ids"]["ss_waste"]),
            'h':  make_url(st.secrets["sheet_ids"]["ss_history"])

        }
        st.rerun()

button_col3, button_col4= st.sidebar.columns(2)
with button_col3:
    if st.button("NTUC FRESH PPL REPORT"):
        st.session_state['report_type'] = "NTUC"
        st.session_state['urls'] ={
            's':  make_url(st.secrets["sheet_ids"]["ntuc_sales"]),
            'db': make_url(st.secrets["sheet_ids"]["ntuc_db"]),
            'd':  make_url(st.secrets["sheet_ids"]["ntuc_dist"]),
            'w':  make_url(st.secrets["sheet_ids"]["ntuc_waste"]),
            'h':  make_url(st.secrets["sheet_ids"]["ntuc_history"])
        }
        st.rerun()
        

if st.session_state['urls'] is None:
    st.info("👈 👈 Please select a Report System (CS , SS or NTUC) to start.")
    st.stop()

urls = st.session_state['urls']
rpt = st.session_state['report_type']
st.sidebar.markdown(f"**Active System:** {rpt}")


st.sidebar.markdown("---")
with st.sidebar.expander("☁️ Upload to Cloud"):
    st.info("Update the Dist/Waste Google Sheets here.")
    up_type = st.radio("Target:", ["Dist", "Waste"])
    up_file = st.file_uploader("File", type=['csv','xlsx'])
    if up_file and st.button("🚀 Push to Sheet"):
        t_url = urls['d'] if up_type == "Dist" else urls['w']
        if t_url:
            if up_file.name.endswith('.csv'): df_up = pd.read_csv(up_file, dtype=str)
            else: df_up = pd.read_excel(up_file, dtype=str)
            if write_to_sheet(t_url, "Sheet1", df_up): st.success("Updated!")
        else: st.error("No URL provided.")

st.sidebar.markdown("---")
app_mode = st.sidebar.radio("Mode:", ["📡 Live Analysis", "🗄️ Saved Reports"])

if app_mode == "📡 Live Analysis":
    with st.spinner("Fetching Live Data for {rpt}..."):
        r_s = load_google_sheet(urls['s'])
        r_db = load_google_sheet(urls['db'])
        r_d = load_google_sheet(urls['d'])
        r_w = load_google_sheet(urls['w'])

        if r_s is not None and r_d is not None:
            res = process_data(r_s, r_db, r_d, r_w,rpt)
            if res:
                df_s, df_d, df_w, map_name, map_art, _ = res

                st.sidebar.markdown("---")
                st.sidebar.header("Filters")

            
            all_years = sorted(list(set(df_s['Year'].dropna()) | set(df_d['Year'].dropna()) | set(df_w['Year'].dropna())), reverse=True)
            if not all_years: all_years = ["2025"] # Fallback
            sel_year = st.sidebar.selectbox("Select Year", all_years)
            if sel_year:
                df_s = df_s[df_s['Year'] == sel_year]
                df_d = df_d[df_d['Year'] == sel_year]
                df_w = df_w[df_w['Year'] == sel_year]

                
                # Filter
                ft = st.sidebar.radio("Filter:", ["Month", "Week"])
                if ft == "Month":
                    group_col = "Month"
                    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                    opts = sorted(list(set(df_s['Month']) | set(df_d['Month']) | set(df_w['Month'])), key=lambda x: month_order.index(x) if x in month_order else 99)
                    if opts:
                        default_opts = opts[-2:] if len(opts) > 1 else opts
                    else:
                        default_opts = []
                    sel = st.sidebar.multiselect("Select", opts, default=default_opts)
                    if sel:
                        df_s = df_s[df_s['Month'].isin(sel)]
                        df_d = df_d[df_d['Month'].isin(sel)]
                        df_w = df_w[df_w['Month'].isin(sel)]
                else:
                    group_col = "Week" # Dynamic grouping variable
                    opts = sorted(list(set(df_s['Week']) | set(df_d['Week'])), reverse=True)
                    sel = st.sidebar.multiselect("Select", opts, default=opts[:4] if len(opts)>0 else opts) # Default to last 4 weeks
                    if sel:
                        df_s = df_s[df_s['Week'].isin(sel)]
                        df_d = df_d[df_d['Week'].isin(sel)]
                        df_w = df_w[df_w['Week'].isin(sel)]

                # Calculation
                s_grp = df_s.groupby([group_col,'Store', 'NAV'])[['Qty', 'Val']].sum().reset_index().rename(columns={'Qty': 'Sales_Qty', 'Val': 'Sales_Val'})
                d_grp = df_d.groupby([group_col,'Store', 'NAV'])[['Qty', 'Val']].sum().reset_index().rename(columns={'Qty': 'Dist_Qty', 'Val': 'Dist_Val'})
                w_grp = df_w.groupby([group_col,'Store', 'NAV'])[['Qty', 'Val']].sum().reset_index().rename(columns={'Qty': 'Waste_Qty', 'Val': 'Waste_Val'})

                df = pd.merge(d_grp, s_grp, on=[group_col,'Store', 'NAV'], how='outer')
                df = pd.merge(df, w_grp, on=[group_col,'Store', 'NAV'], how='outer').fillna(0)
                
                df['Article_Code'] = df['NAV'].map(map_art).fillna("0")
                df.loc[df['Article_Code'] == "0", 'Article_Code'] = "Unmapped (NAV " + df['NAV'].astype(str) + ")"

                df['Item_Name'] = df['NAV'].map(map_name).fillna("Unknown Item")
                mask_unknown = df['Item_Name'] == "Unknown Item"
                df.loc[mask_unknown, 'Item_Name'] = "Item " + df.loc[mask_unknown, 'NAV'].astype(str)
                df['Profit'] = df['Sales_Val'] - df['Dist_Val']
                df['Balance_Qty'] = df['Dist_Qty'] - df['Sales_Qty'] - df['Waste_Qty']

                # Views
                v_s_qty = df.groupby([group_col,'Store'])[['Dist_Qty', 'Sales_Qty', 'Waste_Qty', 'Balance_Qty']].sum()
                v_s_val = df.groupby([group_col,'Store'])[['Dist_Val', 'Sales_Val', 'Waste_Val','Profit']].sum()
                v_i_qty = df.groupby([group_col,'Article_Code', 'Item_Name'])[['Dist_Qty', 'Sales_Qty', 'Waste_Qty']].sum().sort_values('Dist_Qty', ascending=False)
                v_i_val = df.groupby([group_col,'Article_Code', 'Item_Name'])[['Dist_Val', 'Sales_Val', 'Waste_Val']].sum().sort_values('Dist_Val', ascending=False)
                v_top10_all = df.groupby([group_col, 'Item_Name'])['Sales_Val'].sum().reset_index()



                st.subheader(f"📊 {rpt} Live Report ({sel_year}-{ft})")
                t1, t2, t3, t4, t5 = st.tabs(["📦 QTY (Store)", "💰 $ (Store)", "📦 QTY (Item)", "💰 $ (Item)", "🏆 Top 10"])

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
                        store_options = []
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


                # --- CALLING THE FUNCTIONS ---
                
                # Tab 1: Store QTY (Drilldown shows Dist, Sales, Waste, Balance)
                display_drilldown(
                    t1, 
                    v_s_qty, 
                    ['Dist_Qty', 'Sales_Qty', 'Waste_Qty', 'Balance_Qty'], # Columns to show in detail
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
                    ['Dist_Qty', 'Sales_Qty', 'Waste_Qty', 'Balance_Qty'], 
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
                            st.dataframe(t10_pivot.style.format("${:,.2f}"))
                            chart_data = t10_pivot[('Sales_Val', 'TOTAL')].rename("Total Sales")
                            st.bar_chart(chart_data)
                            
                        except Exception as e:
                            st.error(f"Error in Top 10: {e}")
                    else:
                        st.info("No Sales Data available for Top 10.")

                st.divider()
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
                with st.spinner("Loading..."):
                    client = get_gspread_client()
                    sh = client.open_by_url(urls['h'])
                    def get_tab(s):
                        try:
                            data = sh.worksheet(f"Rep_{sel}_{s}").get_all_values()
                            if not data:
                                return pd.DataFrame()
                            header = data [0]
                            rows= data[1:]

                            d=pd.DataFrame(rows,columns=header)
                            return d
                        except Exception as e:
                            return pd.DataFrame()
                           
                    t1, t2, t3, t4, t5, t6 = st.tabs(["Store Qty", "Store Val", "Item Qty", "Item Val", "Top 10", "Master Data"])
                    with t1: st.dataframe(get_tab("StoreQty"), use_container_width=True)
                    with t2: st.dataframe(get_tab("StoreVal"), use_container_width=True)
                    with t3: st.dataframe(get_tab("ItemQty"), use_container_width=True)
                    with t4: st.dataframe(get_tab("ItemVal"), use_container_width=True)
                    with t5: st.dataframe(get_tab("Top10"), use_container_width=True)
                    with t6: st.dataframe(get_tab("Master"), use_container_width=True)
