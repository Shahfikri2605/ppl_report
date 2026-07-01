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

@st.cache_data(ttl=300)
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

def get_rank_table(df, group_col, sort_by='Profit', top=True, n=10):
    # 1. Calculate totals per item for the sort_by metric to get the ranking
    totals = df.groupby('Item_Name')[sort_by].sum()
    
    # 2. Get the items
    if top:
        ranked_items = totals.nlargest(n).index
    else:
        ranked_items = totals.nsmallest(n).index
        
    # 3. Filter the dataframe
    subset = df[df['Item_Name'].isin(ranked_items)]
    
    # 4. Create the Pivot Table
    pivot_df = subset.pivot_table(
        index='Item_Name', 
        columns=group_col, 
        values=['Dist_Val', 'Sales_Val', 'Waste_Val', 'Profit'], 
        aggfunc='sum'
    ).fillna(0)
    
    # 5. Add the TOTAL columns for every metric so we can sort by them
    metrics = pivot_df.columns.get_level_values(0).unique()
    for m in metrics:
        pivot_df[(m, 'TOTAL')] = pivot_df[m].sum(axis=1)
    
    # 6. FIXED: Use a 3-element tuple layout to prevent int vs str crashes
    metric_order = {'Dist_Val': 0, 'Sales_Val': 1, 'Waste_Val': 2, 'Profit': 3}
    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    
    def rank_table_sort_key(col_tuple):
        m, t = col_tuple
        m_idx = metric_order.get(m, 99)
        
        if t == 'TOTAL':
            # The middle flag '1' pushes TOTAL columns to the very end of their metric block safely
            return (m_idx, 1, 0 if group_col == "Month" else '')
        else:
            # The middle flag '0' handles regular layout metrics first
            if group_col == "Month" and t in month_order:
                return (m_idx, 0, month_order.index(t))
            else:
                return (m_idx, 0, t)
    
    sorted_cols = sorted(pivot_df.columns, key=rank_table_sort_key)
    pivot_df = pivot_df.reindex(columns=sorted_cols)
    
    # 7. Sort the rows by Profit Total
    pivot_df = pivot_df.sort_values(by=('Profit', 'TOTAL'), ascending=not top)
    
    return pivot_df

def get_store_rank_table(df, group_col, sort_by='Profit', top=True, n=10):
    # 1. Calculate totals per store for the target metric to find top/bottom performers
    totals = df.groupby('Store')[sort_by].sum()
    
    # 2. Extract the target stores
    if top:
        ranked_stores = totals.nlargest(n).index
    else:
        ranked_stores = totals.nsmallest(n).index
        
    # 3. Filter data for just those stores
    subset = df[df['Store'].isin(ranked_stores)]
    
    # 4. Generate the structured Pivot Table broken down by month/week
    pivot_df = subset.pivot_table(
        index='Store', 
        columns=group_col, 
        values=['Dist_Val', 'Sales_Val', 'Waste_Val', 'Profit'], 
        aggfunc='sum'
    ).fillna(0)
    
    # 5. Inject a grand total baseline calculation column per row
    metrics = pivot_df.columns.get_level_values(0).unique()
    for m in metrics:
        pivot_df[(m, 'TOTAL')] = pivot_df[m].sum(axis=1)
        
    # 6. Apply sequence mapping weights to align blocks chronologically
    metric_order = {'Dist_Val': 0, 'Sales_Val': 1, 'Waste_Val': 2, 'Profit': 3}
    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    
    def store_table_sort_key(col_tuple):
        m, t = col_tuple
        m_idx = metric_order.get(m, 99)
        if t == 'TOTAL':
            return (m_idx, 1, 0 if group_col == "Month" else '')
        else:
            if group_col == "Month" and t in month_order:
                return (m_idx, 0, month_order.index(t))
            else:
                return (m_idx, 0, t)
                
    sorted_cols = sorted(pivot_df.columns, key=store_table_sort_key)
    pivot_df = pivot_df.reindex(columns=sorted_cols)
    
    # 7. Order rows based on overall consolidated profit performance
    pivot_df = pivot_df.sort_values(by=('Profit', 'TOTAL'), ascending=not top)
    return pivot_df
# --- 2. DATA PROCESSING HELPERS ---
def normalize_store_name(name, report_type='AEON', loc_map=None):
    if pd.isna(name) or str(name).strip() == "": return "UNKNOWN"
    
    raw_name = str(name).strip() # Keep original format for the error message
    name = re.sub(r'\s+', ' ', raw_name).upper() # Cleaned version for matching

    if report_type in ['AEON', 'AEON DF']:
        if loc_map and name in loc_map:
            return loc_map[name]
        return f"UNMAPPED - {raw_name}"
    
    elif report_type in ['CS']:
        if loc_map and name in loc_map:
            return loc_map[name]
        return f"UNMAPPED - {raw_name}"
    
    elif report_type in ['TFP', 'TFP DF']:
        if loc_map and name in loc_map:
            return loc_map[name]

        if loc_map:
            for code in loc_map.keys():
                if name.startswith(code):
                    return loc_map[code]
        return f"UNMAPPED - {raw_name}"

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
    if re.match(r'^\d+,\d{3}\.0+$', s):
        s = s.split('.')[0].replace(',', '.')
        return float(s)
    if s.endswith(",000"):
        s = s[:-4]
        if s.count('.') > 1: s = s.replace('.', '')
        return float(s)
    if ',' in s and '.' not in s:
        s = s.replace(',', '.')
        return float(s)
    if ',' in s and '.' in s:
        if s.rfind(',') < s.rfind('.'): s = s.replace(',', '')
        else: s = s.replace('.', '').replace(',', '.')
    try: return float(s)
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
def process_data(df_sales_raw, df_db_raw, df_dist_raw, df_waste_raw, report_type,df_uom_raw=None,df_dist2_raw=None,df_loc_raw=None):
    master_name_map = {}
    df_dist2 = pd.DataFrame()
    nav_to_article_map = {} 

    if report_type =="AEON" or report_type == "AEON DF":
        db_cols = {'Article': ['ITEM CODE', 'ITEMCODE'], 'NAV': ['NAV code', 'NAV_CODE', 'No.'], 'ArtDesc': ['NAV Description', 'Description'], 'NavDesc': ['Aeon Item code', 'ArticleDesc'],'UOM': ['UOM PKT/KG (NAV)', 'UOM']}
        # AEON Sales now scans for STORE CODE instead of name
        sales_cols ={'Article': ['Article', 'ITEM CODE'], 'Qty': ['SALES QTY','QTY','SALESQTY','Billed Quantity'], 'Val': ['TOTAL SALES','SALESAMOUNT','Total Amount'], 'Store': ['STORE CODE'], 'Date': ['SELLING DATE'], 'Name': ['ITEM DESCRIPTION']}
        # AEON Dist now scans for Transfer-to Code
        dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['Transfer-to Code'], 'UOM': ['Unit of Measure Code'], 'Name': ['USOFT product description'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date'], 'Chain': ['Your Reference主key']}
        # AEON Waste now scans for CNO
        waste_cols = {'NAV': ['NAV', 'NAV_CODE'], 'Qty': ['QTY', 'Quantity'], 'Weight': ['WEIGHT'], 'Store': ['CNO'], 'Val': ['Amount', 'TOT_AMT'], 'Date': ['DATE', 'Date'], 'Chain': ['MAIN_CODE']}

    elif report_type =="TFP" or report_type =="TFP DF":
        db_cols = {'Article': ['CODE SKU', 'cno_sku'], 'NAV': ['NAV CODE', 'id'], 'ArtDesc': ['Description', 'name1'], 'NavDesc': ['Item No/SKU', 'name2'], 'UOM': ['UOM']}
        # Sales looks for Location (to extract BBT)
        sales_cols = {'Article': ['SKU NO', '1st Column'], 'Qty': ['Qty Sold', 'Quantity'], 'Val': ['Net Excl Tax', 'Amount'], 'Store': ['Location'], 'Date': ['Sales Date', 'TRXDATE'], 'Name': ['Item']}
        # Dist looks for Location Code or Transfer-to Code (to extract 3003)
        dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['Transfer-to Code'], 'UOM': ['Unit of Measure Code'], 'Name': ['USOFT product description'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date'], 'Chain': ['Your Reference主key']}
        # Waste looks for CNO (to extract 3012)
        waste_cols = {'NAV': ['NAV_CODE', 'NAV'], 'Qty': ['QTY', 'Quantity'], 'Weight': ['WEIGHT'], 'Store': ['CNO'], 'Val': ['TOT_AMT', 'Amount'], 'Date': ['DATE', 'Date'], 'Chain': ['MAIN_CODE']}
    
    elif report_type == "CS" :
        db_cols = {'Article': ['Cust Itemcode'], 'NAV': ['NAV CODE'], 'ArtDesc': ['Cust Description'], 'NavDesc': ['NAV Description'], 'UOM': ['Cust UOM']}
        sales_cols ={'Article': ['ITEMCODE'], 'Qty': ['SALESQTY'], 'Val': ['SALESAMOUNT'], 'Store': ['STOREDESC'], 'Date': ['TRXDATE'], 'Name': ['ITEMDESC']}
        dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['Your Reference'], 'UOM': ['Unit of Measure Code'], 'Name': ['USOFT product description'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date']}
        waste_cols = {'NAV': ['NAV', 'NAV_CODE'], 'Qty': ['QTY', 'Quantity'], 'Weight': ['WEIGHT'], 'Store': ['LONG_NAME'], 'Val': ['Amount', 'TOT_AMT'], 'Date': ['DATE', 'Date'], 'Chain': ['MAIN_CODE']}

    elif report_type == "CS DF":
        db_cols = {'Article': ['Cust Itemcode'], 'NAV': ['NAV CODE'], 'ArtDesc': ['Cust Description'], 'NavDesc': ['NAV Description'], 'UOM': ['Cust UOM']}
        sales_cols ={'Article': ['ITEMCODE'], 'Qty': ['SALESQTY'], 'Val': ['SALESAMOUNT'], 'Store': ['STOREDESC'], 'Date': ['TRXDATE'], 'Name': ['ITEMDESC']}
        dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['External Doc No.'], 'UOM': ['Unit of Measure Code'], 'Name': ['USOFT product description'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date']}
        waste_cols = {'NAV': ['NAV', 'NAV_CODE'], 'Qty': ['QTY', 'Quantity'], 'Weight': ['WEIGHT'], 'Store': ['LONG_NAME'], 'Val': ['Amount', 'TOT_AMT'], 'Date': ['DATE', 'Date'], 'Chain': ['MAIN_CODE']}

    elif report_type == "SS":
        db_cols = {'Article': ['CUST ITEM CODE'], 'NAV': ['NAV CODE'], 'ArtDesc': ['CUST DESCRIPTION'], 'NavDesc': ['NAV Description'], 'UOM': ['CUST UOM']}
        sales_cols = {'Article': ['ITEM CODE'], 'Qty': ['QTY'], 'Val': ['SALES BEF GST'], 'Store': ['OUTLET'], 'Date': ['DATE'], 'Name': ['DESCRIPTION']}
        dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['Your Reference'], 'UOM': ['Unit of Measure Code'], 'Name': ['USOFT product description'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date']}
        waste_cols = {'NAV': ['NAV', 'NAV_CODE'], 'Qty': ['QTY', 'Quantity'], 'Weight': ['WEIGHT'], 'Store': ['LONG_NAME'], 'Val': ['Amount', 'TOT_AMT'], 'Date': ['DATE', 'Date'], 'Chain': ['MAIN_CODE']}
    
    elif report_type == "NTUC":
        db_cols = {'Article': ['Customer Item code'], 'NAV': ['Nav Code'], 'ArtDesc': ['Customer Description'], 'NavDesc': ['Nav description'], 'UOM': ['UOM']}
        sales_cols = {'Article': ['item code'], 'Qty': ['quantity'], 'Val': ['sales'], 'Store': ['location code'], 'Date': ['date'], 'Name': ['description']}
        dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['Your Reference'], 'UOM': ['Unit of Measure Code'], 'Name': ['USOFT product description'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date']}
        waste_cols = {'NAV': ['NAV_CODE'], 'Qty': ['QTY'], 'Weight': ['WEIGHT'], 'Store': ['LONG_NAME'], 'Val': ['Amount', 'TOT_AMT'], 'Date': ['DATE', 'Date'], 'Chain': ['MAIN_CODE']}
    
    elif report_type == "NTUC DF":
        db_cols = {'Article': ['Customer Item code'], 'NAV': ['Nav Code'], 'ArtDesc': ['Customer Description'], 'NavDesc': ['Nav description'], 'UOM': ['UOM']}
        sales_cols = {'Article': ['item code'], 'Qty': ['quantity'], 'Val': ['sales'], 'Store': ['location code'], 'Date': ['date'], 'Name': ['description']}
        dist_cols = {'NAV': ['No.', 'M Code'], 'Qty': ['Quantity', 'QTY'], 'Store': ['External Doc No.'], 'UOM': ['Unit of Measure Code'], 'Name': ['USOFT product description'], 'Cost': ['Price','COST','Unit Price'], 'Date': ['Posting Date']}
        waste_cols = {'NAV': ['NAV', 'NAV_CODE'], 'Qty': ['QTY', 'Quantity'], 'Weight': ['WEIGHT'], 'Store': ['LONG_NAME'], 'Val': ['Amount', 'TOT_AMT'], 'Date': ['DATE', 'Date'], 'Chain': ['MAIN_CODE']}
    # --- A. DATABASE ---
    df_db = find_correct_header_row(df_db_raw,db_cols, "DB Sheet")
    if df_db is None: return None
    df_db = strict_rename(df_db, db_cols)

    # if report_type == "NTUC":
    #     df_db['NAV'] = df_db['NAV'].astype(str).apply(lambda x: x.split('-')[0] if '-' in x else x)

    df_db['Article'] = df_db['Article'].apply(clean_id)
    df_db['NAV'] = df_db['NAV'].apply(clean_id)
    df_db = df_db[df_db['NAV'] != "0"]
    
    db_mapping_forward = df_db.set_index('Article')['NAV'].to_dict()
    df_valid_db = df_db[df_db['NAV'] != "0"]
    nav_to_article_map = df_valid_db.drop_duplicates('NAV').set_index('NAV')['Article'].to_dict()


    if 'ArtDesc' in df_db.columns:
        df_db['Final_Name'] = df_db['ArtDesc']
        if report_type == "CS"  or "SS" or "NTUC":
        # For CS, prefer NAV Description over Cust Description
            df_db['Final_Name'] = df_db['NavDesc'].fillna(df_db['ArtDesc'])
        else:
        # For other systems, keep preferring Cust Description
            df_db['Final_Name'] = df_db['ArtDesc']
        if 'NavDesc' in df_db.columns:
            df_db['Final_Name'] = df_db['Final_Name'].fillna(df_db['NavDesc'])
        df_db['Final_Name'] = df_db['Final_Name'].fillna("Unknown DB Item")
        master_name_map.update(df_db.set_index('NAV')['Final_Name'].to_dict())
    uom_mapping = {}
    if 'UOM' in df_db.columns:
        uom_mapping = df_db.set_index('NAV')['UOM'].to_dict()
    
    # --- DUAL-DICTIONARY LOCATION BUILDER ---
    loc_map_aeon_sales = {}
    loc_map_tfp_sales = {}
    loc_map_cs_sales ={}
    loc_map_ss_sales={}
    loc_map_nt_sales={}
    loc_map_nav = {}

    if report_type in ["AEON", "AEON DF", "TFP", "TFP DF","CS","CS DF","SS","SS DF","NTUC","NTUC DF"] and df_loc_raw is not None:
        if "AEON" in report_type:
            loc_sheet_cols = {'AeonCode': ['AEON CODE'], 'NavCode': ['NAV LOC CODE'], 'NavLoc': ['NAV LOC NAME']}
            sheet_title = "Loc"
        elif "CS" in report_type:
            loc_sheet_cols = {'CsCode': ['Customer Location'], 'NavCode': ['Usoft Location Code'], 'NavLoc': ['Usoft Location Name']}
            sheet_title = "Location DB"
        elif "SS" in report_type:
            loc_sheet_cols = {'SsCode': ['Customer Location Code'], 'NavCode': ['Usoft Location Code'], 'NavLoc': ['Usoft Location Name']}
            sheet_title = "DB LOCATION"
        elif "NTUC" or "NTUC DF" in report_type:
            loc_sheet_cols = {'NtCode': ['Customer Location Code'], 'NavCode': ['Usoft Location Code'], 'NavLoc': ['Usoft Location Name']}
            sheet_title = "Location DB"
        else:
            # TFP pulls Loc (BBT), Code (3001), and Name
            loc_sheet_cols = {'TfpLoc': ['Loc'], 'TfpCode': ['Code'], 'NavLoc': ['Name']}
            sheet_title = "3 - DATABASE LOCATION"
            
        df_loc = find_correct_header_row(df_loc_raw, loc_sheet_cols, sheet_title)
        
        if df_loc is not None:
            df_loc = strict_rename(df_loc, loc_sheet_cols)
            df_loc = df_loc.dropna(subset=['NavLoc'])
            for _, row in df_loc.iterrows():
                nav_loc = str(row['NavLoc']).strip()
                
                if "AEON" in report_type:
                    ac = str(row.get('AeonCode', '')).replace('.0', '').strip()
                    nc = str(row.get('NavCode', '')).replace('.0', '').strip()
                    if ac and ac not in ["NAN", "NONE", ""]: loc_map_aeon_sales[ac] = nav_loc
                    if nc and nc not in ["NAN", "NONE", ""]: loc_map_nav[nc] = nav_loc
                elif "CS" in report_type:
                    cc = str(row.get('CsCode', '')).replace('.0', '').strip()
                    nc = str(row.get('NavCode', '')).replace('.0', '').strip()
                    if cc and cc not in ["NAN", "NONE", ""]: loc_map_cs_sales[cc] = nav_loc
                    if nc and nc not in ["NAN", "NONE", ""]: loc_map_nav[nc] = nav_loc
                elif "SS" in report_type:
                    ss = str(row.get('SsCode', '')).replace('.0', '').strip()
                    nc = str(row.get('NavCode', '')).replace('.0', '').strip()
                    if ss and ss not in ["NAN", "NONE", ""]: loc_map_ss_sales[ss] = nav_loc
                    if nc and nc not in ["NAN", "NONE", ""]: loc_map_nav[nc] = nav_loc
                elif "NTUC" or "NTUC DF" in report_type:
                    nt = str(row.get('NtCode', '')).replace('.0', '').strip()
                    nc = str(row.get('NavCode', '')).replace('.0', '').strip()
                    if nt and nt not in ["NAN", "NONE", ""]: loc_map_nt_sales[nt] = nav_loc
                    if nc and nc not in ["NAN", "NONE", ""]: loc_map_nav[nc] = nav_loc
                else:
                    tc = str(row.get('TfpLoc', '')).strip().upper()
                    nc = str(row.get('TfpCode', '')).replace('.0', '').strip()
                    if tc and tc not in ["NAN", "NONE", ""]: loc_map_tfp_sales[tc] = nav_loc
                    if nc and nc not in ["NAN", "NONE", ""]: loc_map_nav[nc] = nav_loc
    
    rsp_mapping = {}  
    if (report_type == "AEON" or report_type == "AEON DF") and df_uom_raw is not None:
        uom_sheet_cols = {'Desc': ['Item Description'], 'RSP': ['RSP']}
        df_uom = find_correct_header_row(df_uom_raw, uom_sheet_cols, "UOM Sheet")
        if df_uom is not None:
            df_uom = strict_rename(df_uom, uom_sheet_cols)
            df_uom = df_uom.dropna(subset=['Desc', 'RSP'])
            rsp_mapping = df_uom.set_index('Desc')['RSP'].apply(clean_currency).to_dict()
    elif (report_type == "CS" or report_type == "CS DF" ) and df_uom_raw is not None:
        # FIX: Build CS price map indexing directly by NAV code ('M Code') to 'RSP CS'
        cs_sheet_cols = {'NAV': ['M Code'], 'RSP': ['RSP CS']}
        df_cs_rsp = find_correct_header_row(df_uom_raw, cs_sheet_cols, "RSP CS")
        if df_cs_rsp is not None:
            df_cs_rsp = strict_rename(df_cs_rsp, cs_sheet_cols)
            df_cs_rsp['NAV'] = df_cs_rsp['NAV'].apply(clean_id)
            df_cs_rsp = df_cs_rsp.dropna(subset=['NAV', 'RSP'])
            rsp_mapping = df_cs_rsp.set_index('NAV')['RSP'].apply(clean_currency).to_dict()
    elif (report_type == "NTUC" or report_type == "NTUC DF" ) and df_uom_raw is not None:
        # FIX: Build CS price map indexing directly by NAV code ('M Code') to 'RSP CS'
        ntuc_sheet_cols = {'NAV': ['M Code'], 'RSP': ['RSP NTUC']}
        df_ntuc_rsp = find_correct_header_row(df_uom_raw, ntuc_sheet_cols, "RSP NTUC")
        if df_ntuc_rsp is not None:
            df_ntuc_rsp = strict_rename(df_ntuc_rsp, ntuc_sheet_cols)
            df_ntuc_rsp['NAV'] = df_ntuc_rsp['NAV'].apply(clean_id)
            df_ntuc_rsp = df_ntuc_rsp.dropna(subset=['NAV', 'RSP'])
            rsp_mapping = df_ntuc_rsp.set_index('NAV')['RSP'].apply(clean_currency).to_dict()
    elif (report_type == "SS" or report_type == "SS DF" ) and df_uom_raw is not None:
        # FIX: Build CS price map indexing directly by NAV code ('M Code') to 'RSP CS'
        ss_sheet_cols = {'NAV': ['M Code'], 'RSP': ['RSP SS']}
        df_ss_rsp = find_correct_header_row(df_uom_raw, ss_sheet_cols, "RSP SS")
        if df_ss_rsp is not None:
            df_ss_rsp = strict_rename(df_ss_rsp, ss_sheet_cols)
            df_ss_rsp['NAV'] = df_ss_rsp['NAV'].apply(clean_id)
            df_ss_rsp = df_ss_rsp.dropna(subset=['NAV', 'RSP'])
            rsp_mapping = df_ss_rsp.set_index('NAV')['RSP'].apply(clean_currency).to_dict()
    
    if  report_type == "NTUC_DRY":
        id_vars = ['Store', 'Raw_Item']
        melt_val = pd.DataFrame()
        melt_qty = pd.DataFrame()

        try:
            client = get_gspread_client()
            sales_url = st.session_state['urls']['s'] 
            sh = client.open_by_url(sales_url)

            try:
                ws_qty = sh.worksheet("Quantity")
                df_qty_raw = pd.DataFrame(ws_qty.get_all_values())
                df_qty_clean = find_correct_header_row(df_qty_raw, sales_cols, "Qty Sheet")
                df_qty_clean = strict_rename(df_qty_clean, sales_cols)
                date_cols_q = [c for c in df_qty_clean.columns if c not in id_vars and 'METRIC' not in str(c).upper()]
                melt_qty = df_qty_clean.melt(id_vars=id_vars, value_vars=date_cols_q, var_name='Date', value_name='Qty')
            except Exception as e: st.warning(f"Error loading Quantity tab: {e}")

            try:
                try: ws_val = sh.worksheet("Sales") 
                except: ws_val = sh.get_worksheet(0)
                df_val_raw = pd.DataFrame(ws_val.get_all_values())
                df_val_clean = find_correct_header_row(df_val_raw, sales_cols, "Sales Sheet")
                df_val_clean = strict_rename(df_val_clean, sales_cols)
                date_cols_v = [c for c in df_val_clean.columns if c not in id_vars and 'METRIC' not in str(c).upper()]
                melt_val = df_val_clean.melt(id_vars=id_vars, value_vars=date_cols_v, var_name='Date', value_name='Val')
            except Exception as e: st.warning(f"Error loading Sales tab: {e}")

        except Exception as e:
            st.error(f"Critical GSheet Error: {e}")
            return None

        if melt_val.empty: return None
        if melt_qty.empty:
            melt_qty = melt_val.copy()[id_vars + ['Date']]
            melt_qty['Qty'] = 0

        melt_val['Val'] = melt_val['Val'].apply(clean_currency)
        melt_qty['Qty'] = pd.to_numeric(melt_qty['Qty'], errors='coerce').fillna(0)
        melt_val['Date'] = pd.to_datetime(melt_val['Date'], dayfirst=True, errors='coerce')
        melt_qty['Date'] = pd.to_datetime(melt_qty['Date'], dayfirst=True, errors='coerce')
        melt_val = melt_val.dropna(subset=['Date'])
        melt_qty = melt_qty.dropna(subset=['Date'])

        df_sales = pd.merge(melt_val, melt_qty, on=['Store', 'Raw_Item', 'Date'], how='outer').fillna(0)
        df_sales['Article'] = df_sales['Raw_Item'].astype(str).str.extract(r'(\d+)\s*$')
        df_sales['Name'] = df_sales['Raw_Item'].astype(str).str.rsplit('-', n=1).str[0].str.strip()

    else:
        df_sales = find_correct_header_row(df_sales_raw, sales_cols, "Sales Sheet")
        if df_sales is None: return None
        df_sales = strict_rename(df_sales, sales_cols)

    if report_type == "SS_DRY" and 'Store' in df_sales.columns:
        mask_total = df_sales['Store'].astype(str).str.upper() == 'TOTAL'
        df_sales.loc[mask_total, 'Qty'] = 0.0
        df_sales.loc[mask_total, 'Val'] = 0.0
    
    df_sales['Article'] = df_sales['Article'].apply(clean_id)
    df_sales['NAV'] = df_sales['Article'].map(db_mapping_forward).fillna(df_sales['Article'])

    if 'Name' in df_sales.columns:
        sales_names = df_sales.set_index('NAV')['Name'].to_dict()
        for k, v in sales_names.items():
            if k not in master_name_map: master_name_map[k] = v
     
    # APPLY STORE MAPPINGS
    if "AEON" in report_type:
        def map_aeon_sales(x):
            code = str(x).replace('.0', '').strip()
            if code == "" or code == "0": return "UNKNOWN"
            return loc_map_aeon_sales.get(code, f"UNMAPPED - {code}")
        df_sales['Store'] = df_sales['Store'].apply(map_aeon_sales)
    elif "CS" in report_type:
        def map_cs_sales(x):
            code = str(x).replace('.0', '').strip()
            if code == "" or code == "0": return "UNKNOWN"
            return loc_map_cs_sales.get(code, f"UNMAPPED - {code}")
        df_sales['Store'] = df_sales['Store'].apply(map_cs_sales)
    elif  "SS" in report_type:
        def map_ss_sales(x):
            code = str(x).replace('.0', '').strip()
            if code == "" or code == "0": return "UNKNOWN"
            return loc_map_ss_sales.get(code, f"UNMAPPED - {code}")
        df_sales['Store'] = df_sales['Store'].apply(map_ss_sales)
    elif "NTUC" or "NTUC DF" in report_type:
        def map_nt_sales(x):
            code = str(x).replace('.0', '').strip()
            if code == "" or code == "0": return "UNKNOWN"
            return loc_map_nt_sales.get(code, f"UNMAPPED - {code}")
        df_sales['Store'] = df_sales['Store'].apply(map_nt_sales)
    elif "TFP" in report_type:
        def map_tfp_sales(x):
            # Split "BBT - BIG BATAI" and grab the first part "BBT"
            code = str(x).split('-')[0].strip().upper()
            if code == "" or code == "0": return "UNKNOWN"
            return loc_map_tfp_sales.get(code, f"UNMAPPED - {code}")
        df_sales['Store'] = df_sales['Store'].apply(map_tfp_sales)

    df_sales['Qty'] = df_sales['Qty'].apply(clean_currency)
    if report_type == 'AEON':
        df_sales['Val'] = df_sales['Val'].apply(clean_currency)*0.77
    elif report_type =='AEON DF':
        df_sales['Val'] = df_sales['Val'].apply(clean_currency)*0.8
    elif report_type in ['TFP','TFP DF']:
        df_sales['Val'] =df_sales['Val'].apply(clean_currency)*0.75
    elif report_type =='CS':
        df_sales['Val'] =df_sales['Val'].apply(clean_currency)*0.73
    elif report_type == 'CS DF':
        df_sales['Val'] =df_sales['Val'].apply(clean_currency)*0.7
    elif report_type =='NTUC':
        df_sales['Val'] =df_sales['Val'].apply(clean_currency)*0.685
    elif report_type=='NTUC DF':
        df_sales['Val'] =df_sales['Val'].apply(clean_currency)*0.63
    elif report_type == 'SS':
        df_sales['Val'] =df_sales['Val'].apply(clean_currency)*0.76
    elif report_type =='SS_DF':
        df_sales['Val'] =df_sales['Val'].apply(clean_currency)*0.74
    else:
        df_sales['Val'] = df_sales['Val'].apply(clean_currency)
    
    if report_type in ['AEON', 'AEON DF', 'TFP', 'TFP DF', 'CS', 'CS DF','NTUC','NTUC DF','SS','SS DF']:
        df_sales['UOM_Str'] = df_sales['NAV'].map(uom_mapping).fillna('KG')
        
        # FIX: Pull price matching rules dynamically (CS checks by direct NAV code, AEON checks by item name description)
        if report_type in ['CS', 'CS DF','NTUC','NTUC DF','SS','SS DF']:
            df_sales['RSP_Val'] = df_sales['NAV'].map(rsp_mapping).fillna(0.0)
        else:
            df_sales['DB_Item_Name'] = df_sales['NAV'].map(df_db.set_index('NAV')['Final_Name'].to_dict())
            df_sales['RSP_Val'] = df_sales['DB_Item_Name'].map(rsp_mapping).fillna(0.0)
            
        def calc_calculated_qty(row):
            # If the item metric is marked as a KG Weighted produce element and has valid price data
            if str(row['UOM_Str']).upper().strip() == 'KG' and row['RSP_Val'] > 0:
                return row['Val'] / row['RSP_Val']
            else:
                # If it's a piece/packet element (e.g. 150GEA), multiply by its parsed factor decimal ratio
                factor = parse_uom_factor(row['UOM_Str'])
                return row['Qty'] * factor
                
        df_sales['Qty'] = df_sales.apply(calc_calculated_qty, axis=1)
        df_sales = df_sales.drop(columns=['UOM_Str', 'RSP_Val', 'DB_Item_Name'], errors='ignore')
    
        

    if 'Date' in df_sales.columns:
        if report_type == 'SS_DRY':
            df_sales['Year'] = df_sales['Date'].astype(str).replace(r'\.0$', '', regex=True)
            df_sales['Date'] = pd.to_datetime(df_sales['Year'] + "-01-01", errors='coerce')
        elif report_type in ['AEON', 'AEON DF', 'TFP', 'TFP DF','NTUC','NTUC DF']:
            df_sales['Date'] = pd.to_datetime(df_sales['Date'], format='%d/%m/%Y', errors='coerce')
            df_sales['Year'] = df_sales['Date'].dt.year.astype('Int64').astype(str)
            df_sales['Month'] = df_sales['Date'].dt.month_name().str[:3]
            df_sales['Week'] = df_sales['Date'].apply(lambda x: f"{x.strftime('%Y')}-W{(int(x.strftime('%U')) + 1):02d}" if pd.notnull(x) else None)
        elif report_type in ['SS']:
            df_sales['Date'] = pd.to_datetime(df_sales['Date'], format='%d-%m-%Y', errors='coerce')
            df_sales['Year'] = df_sales['Date'].dt.year.astype(str).str.replace(r'\.0$', '', regex=True)
            df_sales['Month'] = df_sales['Date'].dt.month_name().str[:3]
            df_sales['Week'] = df_sales['Date'].dt.strftime('%Y-W%U')
        elif report_type in ['CS','CS DF']:
            df_sales['Date'] = pd.to_datetime(df_sales['Date'], format='%Y.%m.%d', errors='coerce')
            df_sales['Year'] = df_sales['Date'].dt.year.astype('Int64').astype(str)
            df_sales['Month'] = df_sales['Date'].dt.month_name().str[:3]
            df_sales['Week'] = df_sales['Date'].apply(lambda x: f"{x.strftime('%Y')}-W{(int(x.strftime('%U')) + 1):02d}" if pd.notnull(x) else None)
        else:
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

    # --- C. DISTRIBUTION ---
    df_dist = find_correct_header_row(df_dist_raw, dist_cols, "Dist Sheet")
    if df_dist is None: return None
    df_dist = strict_rename(df_dist, dist_cols)

    if 'Date' in df_dist.columns:
        df_dist['Date'] = pd.to_datetime(df_dist['Date'], errors='coerce', dayfirst=False)
        # SAVE THIS EXCLUSIVELY FOR THE SIDEBAR CAPTION LOGIC Later:
        df_dist_raw_date_max = df_dist['Date'].max().strftime('%d %b %Y') if not df_dist['Date'].dropna().empty else "N/A"

    if df_dist2_raw is not None and not df_dist2_raw.empty:
        dist2_cols = {
            'NAV': ['Item No.'], 'Qty': ['Quantity'], 'Store': ['Location Name'], 
            'UOM': ['Unit of Measure Code'], 'Name': ['Item Description'], 
            'Cost': ['Cost Amount (Actual)'], 'Date': ['Posting Date']
        }
        df_dist2 = find_correct_header_row(df_dist2_raw, dist2_cols, "Dist Sheet 2")
        
        if df_dist2 is not None:
            df_dist2 = strict_rename(df_dist2, dist2_cols)
            
            # --- NEW: FILTER DIST2 SO WE DON'T GET UNMAPPED ERRORS FOR OTHER STORES ---
            if report_type in ['AEON', 'AEON DF']:
                mask = df_dist2['Store'].astype(str).str.upper().str.contains('AEON|JUSCO|MAXVALU', regex=True, na=False)
                df_dist2 = df_dist2[mask]
            elif report_type in ['TFP', 'TFP DF']:
                mask = df_dist2['Store'].astype(str).str.upper().str.contains('VG|BIP|BBT|BSC', regex=True, na=False)
                df_dist2 = df_dist2[mask]
            # --------------------------------------------------------------------------

            if 'Date' in df_dist2.columns:
                df_dist2['Date'] = pd.to_datetime(df_dist2['Date'], format='%m/%d/%Y', errors='coerce')
            if 'Qty' in df_dist2.columns:
                df_dist2['Qty'] = pd.to_numeric(df_dist2['Qty'], errors='coerce').abs()
            if 'Cost' in df_dist2.columns:
                df_dist2['Cost'] = df_dist2['Cost'].apply(clean_currency).astype(float) / df_dist2['Qty'].replace(0, 1)
            
            df_dist = pd.concat([df_dist, df_dist2], ignore_index=True)
    if 'Store' in df_dist.columns:
        if report_type in ['AEON', 'AEON DF', 'TFP', 'TFP DF','CS','CS DF','SS','NTUC','NTUC DF']:
            # Aeon now uses numeric codes, so we relax the text filtering here to avoid wiping data before mapping
            pass 
        # elif report_type == 'TFP' or report_type == 'TFP DF':
        #     mask = df_dist['Store'].astype(str).str.upper().str.contains('VG|BIP|BBT|BSC', regex=True, na=False)
        #     df_dist=df_dist[mask]
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

    # APPLY STORE MAPPINGS
    if report_type in ["AEON", "AEON DF", "TFP", "TFP DF","CS","CS DF","NTUC","SS","NTUC DF"]:
        def map_nav(x):
            val = str(x).replace('.0', '').strip()
            if val == "" or val == "0" or val.upper() == "TRANSFER": return "UNKNOWN"
            if val in loc_map_nav.values(): return val # Preserves names from Dist2
            return loc_map_nav.get(val, f"UNMAPPED - {val}")
        df_dist['Store'] = df_dist['Store'].apply(map_nav)

    df_dist['Date'] = pd.to_datetime(df_dist['Date'], errors='coerce')
    df_dist['Year'] = df_dist['Date'].dt.year.astype(str).str.replace(r'\.0$', '', regex=True)
    df_dist['Month'] = df_dist['Date'].dt.month_name().str[:3]
    df_dist['Week'] = df_dist['Date'].apply(lambda x: f"{x.strftime('%Y')}-W{(int(x.strftime('%U')) + 1):02d}" if pd.notnull(x) else None)
    df_dist['Qty'] = df_dist['Qty'].apply(clean_currency)
    df_dist['Qty_1'] = df_dist['Qty'].apply(clean_currency)
    
    cost = df_dist['Cost'].apply(clean_currency) if 'Cost' in df_dist.columns else 0.0
    if report_type == 'SS_DRY':
        df_dist['Month'] = "Annual"
        df_dist['Week'] = "Annual"
        
    if 'UOM' in df_dist.columns:
        raw_qty = pd.to_numeric(df_dist['Qty'], errors='coerce').fillna(0)
        uom_factor = df_dist['UOM'].apply(parse_uom_factor)
        df_dist['Qty'] = raw_qty * uom_factor 
        cost = df_dist['Cost'].apply(clean_currency) if 'Cost' in df_dist.columns else 0
        df_dist['Val'] = df_dist['Qty_1'] * (cost/2)
        
    

    # --- D. WASTAGE ---
    if report_type == "CS_DRY" or report_type == "SS_DRY" or report_type== "NTUC_DRY":
        df_waste = pd.DataFrame(columns=["NAV", "Qty", "Val", "Store", "Date", "Year", "Month", "Week", "Weight", "Chain"])
    else:
        df_waste = find_correct_header_row(df_waste_raw, waste_cols, "Waste Sheet")
        if df_waste is None: return None
        df_waste = strict_rename(df_waste, waste_cols)
        if 'Chain' in df_waste.columns: 
            if report_type == 'AEON' or report_type == 'AEON DF':
                mask = df_waste['Chain'].astype(str).str.upper().str.contains('HC000020|AEON|JUSCO|MAXVALU', regex=True, na=False)
                df_waste = df_waste[mask]
            elif report_type == 'SS':
                mask = df_waste['Chain'].astype(str).str.upper().str.contains(r'^SHENG SHIONG|^SS|^SS_|S.SIONG', regex=True, na=False)
                df_waste = df_waste[mask]
            elif report_type == 'NTUC' :
                mask = df_waste['Chain'].astype(str).str.upper().str.contains('NTUC', regex=True, na=False)
                df_waste = df_waste[mask]
        df_waste['NAV'] = df_waste['NAV'].apply(clean_id)

        # APPLY STORE MAPPINGS
        # if report_type in ["AEON", "AEON DF", "TFP","CS", "TFP DF","SS","NTUC"]:
        #     def map_waste(x):
        #         # 1. Split by '-' to get all parts
        #         parts = str(x).split('-')
                
        #         # 2. Find the part that is numeric and exactly 4 digits long
        #         # This ignores 'HC001500' (too long) and '3' (too short)
        #         val = next((p for p in parts if p.isdigit() and len(p) == 4), None)
                
        #         # 3. Fallback: If no 4-digit code found, use the original string
        #         if not val:
        #             val = str(x).strip()
                
        #         if val == "" or val == "0": return "UNKNOWN"
        #         if val in loc_map_nav.values(): return val 
        #         return loc_map_nav.get(val, f"UNMAPPED - {val}")
                
            # df_waste['Store'] = df_waste['Store'].apply(map_waste)
        if report_type in ["AEON", "AEON DF", "TFP", "TFP DF","CS","CS DF","NTUC","SS","NTUC DF"]:
            def map_nav(x):
                val = str(x).replace('.0', '').strip()
                if val == "" or val == "0" or val.upper() == "TRANSFER": return "UNKNOWN"
                if val in loc_map_nav.values(): return val # Preserves names from Dist2
                return loc_map_nav.get(val, f"UNMAPPED - {val}")
            df_waste['Store'] = df_waste['Store'].apply(map_nav)
            
        df_waste['Date'] = pd.to_datetime(df_waste['Date'], dayfirst=True, errors='coerce')
        df_waste['Year'] = df_waste['Date'].dt.year.astype(str).replace(r'\.0$', '', regex=True)
        df_waste['Month'] = df_waste['Date'].dt.month_name().str[:3]
        df_waste['Week'] = df_waste['Date'].apply(lambda x: f"{x.strftime('%Y')}-W{(int(x.strftime('%U')) + 1):02d}" if pd.notnull(x) else None)
        qty_units = df_waste['Qty'].apply(clean_currency)
        weight_kg = df_waste['Weight'].apply(clean_currency)
        df_waste['Qty'] = qty_units * weight_kg
        df_waste['Val'] = df_waste['Val'].apply(clean_currency)

    def get_max_date(dframe):
        try:
            if dframe is not None and not dframe.empty and 'Date' in dframe.columns:
                return dframe['Date'].max().strftime('%d %b %Y')
        except: pass
        return "N/A"

    # Separate tracking logic before returning
    # We calculate 'Dist' tracking by filtering out rows that came from df_dist2
    # Standard distribution rows won't match df_dist2's unique 'Item Description' or structural columns if handled cleanly,
    # but a simpler way is to calculate standard distribution max date directly from the df_dist rows before concat or by checking rows where 'Store' wasn't modified by map_nav's ledger rules.
    
    # Let's find the true max date of standard distribution rows safely:
    standard_dist_rows = df_dist[~df_dist.index.isin(df_dist2.index)] if not df_dist2.empty else df_dist

    update_info = {
        "Sales": get_max_date(df_sales),
        # If df_dist2 added rows, extract max date from the original distribution dates safely
        "Dist": df_dist_raw_date_max if 'df_dist_raw_date_max' in locals() else get_max_date(standard_dist_rows),
        "Dist2": get_max_date(df_dist2) if not df_dist2.empty else "N/A",
        "Waste": get_max_date(df_waste)
    }

    return df_sales, df_dist, df_waste, master_name_map, nav_to_article_map, [], update_info

# --- 4. MAIN APP LOGIC ---
def main_app_interface(authenticator, name, permissions):
    st.title("PPL Report")
    with st.sidebar:
        st.write(f"👤 User: **{name}**")
        authenticator.logout('Logout', 'sidebar')
        st.divider()
        st.header("⚙️ Configuration")
        
        if 'urls' not in st.session_state: st.session_state['urls'] = None

        my_systems = permissions.get("systems", [])
        def can_view(sys_code): return "ALL" in my_systems or sys_code in my_systems

        b1, b2 = st.sidebar.columns(2)
        with b1:
            if can_view("CS") and st.button("CS Vege"):
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
            if can_view("CS") and st.button("CS DF"):
                st.session_state['report_type'] = "CS DF"
                st.session_state['urls'] = {
                    's': make_url(st.secrets["sheet_ids"]["cs_dry_sales"]),
                    'db': make_url(st.secrets["sheet_ids"]["cs_dry_db"]),
                    'd': make_url(st.secrets["sheet_ids"]["cs_dry_dist"]),
                    # 'd2': make_url(st.secrets["sheet_ids"]["cs_dry_dist_2"]),
                    'w': make_url(st.secrets["sheet_ids"]["cs_dry_waste"]),
                    'h': make_url(st.secrets["sheet_ids"]["cs_dry_history"])
                }
                st.rerun()
        
        b3, b4 = st.sidebar.columns(2)
        with b3:
            if can_view("NTUC") and st.button("NTUC Vege"):
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
            if can_view("NTUC") and st.button("NTUC DF"):
                st.session_state['report_type'] = "NTUC DF"
                st.session_state['urls'] = {
                    's': make_url(st.secrets["sheet_ids"]["ntuc_dry_sales"]),
                    'db': make_url(st.secrets["sheet_ids"]["ntuc_dry_db"]),
                    'd': make_url(st.secrets["sheet_ids"]["ntuc_dry_dist"]),
                    #'d2': make_url(st.secrets["sheet_ids"]["ntuc_dry_dist_2"]),
                    'w': make_url(st.secrets["sheet_ids"]["ntuc_dry_waste"]),
                    'h': make_url(st.secrets["sheet_ids"]["ntuc_dry_history"])                
                    }
                st.rerun()
        b5,b6 = st.sidebar.columns(2)
        with b5:
            if can_view("SS") and st.button("SS Vege"):
                st.session_state['report_type'] = "SS"
                st.session_state['urls'] = {
                    's': make_url(st.secrets["sheet_ids"]["ss_sales"]),
                    'db': make_url(st.secrets["sheet_ids"]["ss_db"]),
                    'd': make_url(st.secrets["sheet_ids"]["ss_dist"]),
                    'w': make_url(st.secrets["sheet_ids"]["ss_waste"]),
                    'h': make_url(st.secrets["sheet_ids"]["ss_history"])
                }
                st.rerun()
        with b6:
            if can_view("SS") and st.button("SS DF"):
                st.session_state['report_type'] = "SS DF"
                st.session_state['urls'] = {
                    's': make_url(st.secrets["sheet_ids"]["ss_dry_sales"]),
                    'db': make_url(st.secrets["sheet_ids"]["ss_dry_db"]),
                    'd': make_url(st.secrets["sheet_ids"]["ss_dry_dist"]),
                    'd2': make_url(st.secrets["sheet_ids"]["ss_dry_dist_2"]),
                    'w': make_url(st.secrets["sheet_ids"]["ss_dry_waste"]),
                    'h': make_url(st.secrets["sheet_ids"]["ss_dry_history"])  
                }
                st.rerun()
        
        st.markdown("---")
        app_mode = st.radio("Mode:", ["📡 Live Analysis", "🗄️ Saved Reports"])
    
    if st.session_state['urls'] is None:
        st.info("👈 Please select a Report System from the sidebar to begin.")
        return

    urls = st.session_state['urls']
    rpt = st.session_state['report_type']
    st.caption(f"Active System: {rpt}")

    if app_mode == "📡 Live Analysis":
        with st.spinner(f"Fetching Live Data for {rpt}..."):

            r_s = load_google_sheet(urls['s'])
            r_db = load_google_sheet(urls['db'])
            r_d = load_google_sheet(urls['d'])
            r_d2 = load_google_sheet(urls['d2']) if 'd2' in urls and urls['d2'] else None
            if rpt in ["AEON", "AEON DF"]:
                r_uom = load_google_sheet(urls['db'], "UOM")
            elif rpt in ["CS", "CS DF"]:
                r_uom = load_google_sheet(urls['db'], "RSP CS")
            elif rpt in ["NTUC","NTUC DF"]:
                r_uom = load_google_sheet(urls['db'], "RSP NTUC")
            elif rpt in ["SS","SS DF"]:
                r_uom =load_google_sheet(urls['db'], "RSP SS")
            else:
                r_uom = None
            
            if rpt in ["AEON", "AEON DF"]:
                r_loc = load_google_sheet(urls['db'], "Loc")
            elif rpt in ["TFP", "TFP DF"]:
                r_loc = load_google_sheet(urls['db'], "3 - DATABASE LOCATION")
            elif rpt in ["CS","CS DF"]:
                r_loc = load_google_sheet(urls['db'], "Location DB")
            elif rpt in ["SS"]:
                r_loc = load_google_sheet(urls['db'], "DB LOCATION")
            elif rpt in ["NTUC","NTUC DF"]:
                r_loc = load_google_sheet(urls['db'], "Location DB")
            else:
                r_loc = None
                
            r_w = None if rpt == "CS_DF" or rpt == "SS_DRY" else load_google_sheet(urls['w'])

            if r_s is not None and r_d is not None:
                res = process_data(r_s, r_db, r_d, r_w, rpt, r_uom, r_d2, r_loc)
                if res:
                    df_s, df_d, df_w, map_name, map_art, _, update_info = res
                    
                    if not df_s.empty: df_s = df_s[df_s['Store'] != "UNKNOWN"]
                    if not df_d.empty: df_d = df_d[df_d['Store'] != "UNKNOWN"]
                    if not df_w.empty: df_w = df_w[df_w['Store'] != "UNKNOWN"]

                    my_stores = permissions.get("stores", [])
                    if "ALL" not in my_stores:
                        if not df_s.empty: df_s = df_s[df_s['Store'].isin(my_stores)]
                        if not df_d.empty: df_d = df_d[df_d['Store'].isin(my_stores)]
                        if not df_w.empty: df_w = df_w[df_w['Store'].isin(my_stores)]
                        st.warning(f"🔒 View restricted to assigned stores.")

                    st.caption(f"""
                    **Last Data Updates:** 🛒 Sales: **{update_info['Sales']}** | 🚚 Dist: **{update_info['Dist']}** | 📝 Ledger: **{update_info.get('Dist2', 'N/A')}** | 🗑️ Waste: **{update_info['Waste']}**
                    """)
                    
                    # --- NEW UNMAPPED STORE ALERT ---
                    # unmapped_stores = set()
                    # if not df_s.empty: unmapped_stores.update(df_s[df_s['Store'].astype(str).str.startswith('UNMAPPED')]['Store'].unique())
                    # if not df_d.empty: unmapped_stores.update(df_d[df_d['Store'].astype(str).str.startswith('UNMAPPED')]['Store'].unique())
                    # if not df_w.empty: unmapped_stores.update(df_w[df_w['Store'].astype(str).str.startswith('UNMAPPED')]['Store'].unique())
                    
                    # if unmapped_stores:
                    #     st.error(f"⚠️ **ACTION REQUIRED:** The following store codes/names were not found in your Google Sheet Database and could not be mapped:\n\n**{', '.join(unmapped_stores)}**")
                    
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
                        group_col = "Week" 
                        opts = sorted(list(set(df_s['Week']) | set(df_d['Week']) | set(df_w['Week'] if not df_w.empty else [])), reverse=True)
                        sel = st.sidebar.multiselect("Select", opts, default=opts[:4] if len(opts)>0 else opts) 
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

                    df = pd.merge(d_grp, s_grp, on=[group_col,'Store', 'NAV'], how='outer').fillna(0)
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
                    if rpt == 'AEON' or rpt == 'TFP':
                        df = df[~df['Item_Name'].astype(str).str.upper().str.startswith(('SN ','SNBG '))]
                    elif rpt == 'CS' or rpt =='SS' or rpt == 'NTUC':
                        df = df[~df['Item_Name'].astype(str).str.upper().str.startswith(('SN ','SNBG ','SIMPLY ','BETTER ','* ORGANIC DRIED DATES 250G','* ORGANIC DRIED GOJIBERRIES 200G','TRULY ','* ORGANIC DRIED CRANBERRIES 220G',"FAIRCHILD'S ORG APP CIDER VINEGAR 946ML"))]
                    elif rpt == 'AEON DF' or rpt == 'TFP DF' or 'CS DF' or 'NTUC DF':
                        mask_is_sn = df['Item_Name'].astype(str).str.upper().str.startswith(('SN ','SNBG ','SIMPLY ','BETTER ','* ORGANIC DRIED DATES 250G','* ORGANIC DRIED GOJIBERRIES 200G','TRULY ','* ORGANIC DRIED CRANBERRIES 220G',"FAIRCHILD'S ORG APP CIDER VINEGAR 946ML"))
                        mask_not_egg = ~df['Item_Name'].astype(str).str.upper().str.contains('SELENIUM EGG MYS PAPER TRAY', na=False)
                        df = df[mask_is_sn & mask_not_egg]
                    
                    df['Item_Name'] = df['NAV'].map(map_name).fillna("Unknown Item")
                    mask_unknown = df['Item_Name'] == "Unknown Item"
                    df.loc[mask_unknown, 'Item_Name'] = "Item " + df.loc[mask_unknown, 'NAV'].astype(str)
                    if rpt == 'CS' or 'SS':
                        df['Sales_Val'] = pd.to_numeric(df['Sales_Val'], errors='coerce').fillna(0.0)
                        df['Dist_Val'] = pd.to_numeric(df['Dist_Val'], errors='coerce').fillna(0.0)
                    df['Profit'] = df['Sales_Val'] - df['Dist_Val']
                    df['Balance Stock'] = df['Dist_Qty'] - df['Sales_Qty']
                    
                    is_dry = rpt in ["CS_DRY","SS_DRY", "NTUC_DRY"]

                    if is_dry:
                        qty_display_list = ['Dist_Qty','Sales_Qty','Balance Stock']
                        val_display_list = ['Dist_Val','Sales_Val','Profit']
                    else:
                        qty_display_list =['Dist_Qty','Sales_Qty','Waste_Qty']
                        val_display_list =['Dist_Val', 'Sales_Val', 'Waste_Val', 'Profit']

                    v_s_qty = df.groupby([group_col,'Store'])[qty_display_list].sum()
                    v_s_qty['STR%'] = (v_s_qty['Sales_Qty']/ v_s_qty['Dist_Qty'])*100
                    v_s_qty['STR%'] = v_s_qty['STR%'].replace([np.inf, -np.inf], 0).fillna(0).round(0)
                    v_s_val = df.groupby([group_col,'Store'])[val_display_list].sum()
                    v_i_qty = df.groupby([group_col,'Article_Code', 'Item_Name'])[qty_display_list].sum()
                    v_i_qty['STR%'] = (v_i_qty['Sales_Qty'] / v_i_qty['Dist_Qty'] * 100).replace([np.inf, -np.inf], 0).fillna(0).round(0)
                    v_i_qty = v_i_qty.sort_values('Dist_Qty', ascending=False)
                    v_i_val = df.groupby([group_col,'Article_Code', 'Item_Name'])[['Dist_Val', 'Sales_Val', 'Waste_Val', 'Profit']].sum().sort_values('Dist_Val', ascending=False)
                    v_top10_all = df.groupby('Item_Name')[['Dist_Val', 'Sales_Val', 'Waste_Val', 'Profit']].sum().reset_index()

                    st.subheader(f"📊 {rpt} Live Report ({sel_year}-{ft})")
                    t1, t2, t3, t4, t5, t6,t7,t8 = st.tabs(["📦 QTY (Store)", "💰 $ (Store)", "📦 QTY (Item)", "💰 $ (Item)", "🏆 Top 10", "📉 Bottom 10","🏪 Top 10 Stores", "🏪 Bottom 10 Stores"])

                    def display_drilldown(tab, main_df, detail_cols, sort_col, fmt, time_col):
                        with tab:
                            if main_df.empty:
                                st.info("No data.")
                                return
                            summary = main_df.unstack(level=0, fill_value=0)
                            metrics = summary.columns.get_level_values(0).unique()
                            for m in metrics:
                                m_cols = summary.loc[:, (m, slice(None))].columns
                                for c in m_cols:
                                    summary[c] = pd.to_numeric(summary[c], errors='coerce').fillna(0)
                                summary[(m, 'TOTAL')] = summary[m_cols].sum(axis=1)
                            
                            month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                            def screen_sort_key(col_tuple):
                                m, t = col_tuple
                                m_idx = list(metrics).index(m) if m in metrics else 99
                                if t == 'TOTAL': t_idx = 100
                                else: t_idx = month_order.index(t) if t in month_order else 99
                                return (m_idx, t_idx)
                            summary = summary.reindex(columns=sorted(summary.columns, key=screen_sort_key))
                            if (sort_col, 'TOTAL') in summary.columns:
                                summary = summary.sort_values((sort_col, 'TOTAL'), ascending=False)
                            st.markdown(f"### 🏢 Store Summary")
                            f_dict = {c: "{:,.0f}" if 'STR%' in str(c) else fmt for c in summary.columns}
                            st.dataframe(summary.style.format(f_dict), height=400, use_container_width=True)
                            st.divider()
                            st.markdown("### 🔍 Select Store to View Details")
                            store_options = [f"{s}" for s in summary.index]

                            for store in summary.index:
                                val = summary.loc[store, (sort_col, 'TOTAL')]
                                store_options.append(f"{store} | Total {sort_col}: {val:,.2f}")
                            
                        sel_store_str = st.selectbox(f"Select Store ({sort_col})", options=store_options, key=f"sel_selectbox_{tab}_{sort_col}")
                        if sel_store_str:
                            if sel_store_str:
                                selected_store = sel_store_str.split(" | ")[0]
                                store_mask = df['Store'] == selected_store
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
                                
                                if group_col == "Month":
                                    detail_view = detail_view.reindex(columns=sorted(detail_view.columns, key=screen_sort_key))
                                if (sort_col, 'TOTAL') in detail_view.columns:
                                    detail_view = detail_view.sort_values((sort_col, 'TOTAL'), ascending=False)
                                st.markdown(f"#### 📦 Items in {selected_store}")
                                f_det = {c: "{:,.0f}" if 'STR%' in str(c) else fmt for c in detail_view.columns}
                                st.dataframe(detail_view.style.format(f_det), width='stretch')
                    
                    display_drilldown(t1, v_s_qty, qty_display_list, 'Sales_Qty', "{:,.2f}",group_col) 
                    display_drilldown(t2, v_s_val, val_display_list, 'Sales_Val', "{:,.2f}",group_col)
                    
                    def display_item_drilldown(tab, detail_cols, sort_col, fmt, time_col):
                        with tab:
                            summary = df.groupby(['Item_Name', time_col])[detail_cols].sum().unstack(level=1, fill_value=0)
                            if summary.empty:
                                st.info("No data.")
                                return
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
                                summary[('STR%', 'TOTAL')] = str_vals.round(0)
                            if group_col == "Month":
                                month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                                actual_metrics = list(metrics)
                                if 'Sales_Qty' in actual_metrics and 'Dist_Qty' in actual_metrics: actual_metrics.append('STR%')
                                def item_sort_key(col_tuple):
                                    m, t = col_tuple
                                    m_idx = actual_metrics.index(m) if m in actual_metrics else 99
                                    if t == 'TOTAL': t_idx = 100
                                    else: t_idx = month_order.index(t) if t in month_order else 99
                                    return (m_idx, t_idx)
                                summary = summary.reindex(columns=sorted(summary.columns, key=item_sort_key))
                            if (sort_col, 'TOTAL') in summary.columns:
                                summary = summary.sort_values((sort_col, 'TOTAL'), ascending=False)
                            st.markdown(f"### 📦 Item Summary")
                            f_dict = {c: "{:,.0f}" if 'STR%' in str(c) else fmt for c in summary.columns}
                            st.dataframe(summary.style.format(f_dict), height=400, use_container_width=True)
                            st.divider()
                            st.markdown("### 🔍 Select Item to View Stores")
                            limit_list = summary.index[:2000]
                            item_options = []
                            for item in limit_list:
                                val = summary.loc[item, (sort_col, 'TOTAL')]
                                item_options.append(f"{item} | Total {sort_col}: {val:,.2f}")
                            sel_item_str = st.selectbox(f"Select Item ({sort_col})", options=item_options, key=f"sel_item_{tab}_{sort_col}")
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
                                f_det = {c: "{:,.0f}" if 'STR%' in str(c) else fmt for c in item_view.columns}
                                st.dataframe(item_view.style.format(f_det), width='stretch')

                    display_item_drilldown(t3, qty_display_list, 'Sales_Qty', "{:,.2f}",group_col)
                    display_item_drilldown(t4, val_display_list, 'Sales_Val', "{:,.2f}",group_col)

                    with t5:
                        st.subheader("🏆 TOP 10 Items by Profit")
                        if not df.empty:
                            # Sorts by Profit, Top 10 (Highest to Lowest)
                            top10_table = get_rank_table(df, group_col, sort_by='Profit', top=True, n=10)
                            st.dataframe(top10_table.style.format("{:,.2f}"), use_container_width=True)
                        else:
                            st.info("No data available.")

                    with t6:
                        st.subheader("📉 BOTTOM 10 Items by Profit")
                        if not df.empty:
                            # Sorts by Profit, Bottom 10 (Lowest to Highest)
                            bot10_table = get_rank_table(df, group_col, sort_by='Profit', top=False, n=10)
                            st.dataframe(bot10_table.style.format("{:,.2f}"), use_container_width=True)
                        else:
                            st.info("No data available.")
                    with t7:
                        st.subheader("🏆 TOP 10 Stores by Profit")
                        if not df.empty:
                            top10_stores = get_store_rank_table(df, group_col, sort_by='Profit', top=True, n=10)
                            st.dataframe(top10_stores.style.format("{:,.2f}"), use_container_width=True)
                        else:
                            st.info("No data available.")

                    with t8:
                        st.subheader("📉 BOTTOM 10 Stores by Profit")
                        if not df.empty:
                            bot10_stores = get_store_rank_table(df, group_col, sort_by='Profit', top=False, n=10)
                            st.dataframe(bot10_stores.style.format("{:,.2f}"), use_container_width=True)
                        else:
                            st.info("No data available.")
                    
                    st.divider()
                    
                    # --- DUAL FILE DATA SEGREGATION ---
                    # Separate clean data from unmapped data
                    is_unmapped_store = df['Store'].astype(str).str.startswith('UNMAPPED')
                    is_unmapped_item = df['Article_Code'].astype(str).str.startswith('Unmapped')
                    
                    # 2. Check metrics to see where the activity is coming from
                    has_sales_data = (df['Sales_Qty'] > 0) | (df['Sales_Val'] > 0)
                    has_distribution_data = (df['Dist_Qty'] > 0) | (df['Dist_Val'] > 0)
                    no_distribution_data = (df['Dist_Qty'] == 0) & (df['Dist_Val'] == 0)
                    has_wastage_data = (df['Waste_Qty']>0) | (df['Waste_Val'] >0)
                    
                    # 3. CLEAN REPORT RULE: 
                    # Exclude an unmapped row ONLY IF it has no distribution data.
                    # If it HAS distribution data, allow it to stay in the clean report!
                    df_clean = df[~((is_unmapped_store) & ~has_distribution_data)]
                    
                    # 4. UNMAPPED REPORT LOG RULE:
                    # Only catch rows that are unmapped, have sales impact, but are completely missing from distribution sheets
                    df_unmapped_raw = df[(is_unmapped_store | is_unmapped_item ) & has_sales_data & no_distribution_data]

                    # Regenerate summaries exclusively for the Clean Excel Report
                    def create_hierarchical_qty(df_source, primary_col, secondary_col, time_col):
                        # 1. Master Rows (Store Totals)
                        p = df_source.groupby([primary_col, time_col])[qty_display_list].sum()
                        p['STR%'] = (p['Sales_Qty'] / p['Dist_Qty'].replace(0, 1) * 100).replace([np.inf, -np.inf], 0).fillna(0).round(0)
                        p = p.reset_index()
                        p['Detail'] = " SUMMARY" # Space forces it to sort to the top
                        
                        # 2. Detail Rows (Items inside Store)
                        c = df_source.groupby([primary_col, secondary_col, time_col])[qty_display_list].sum()
                        c['STR%'] = (c['Sales_Qty'] / c['Dist_Qty'].replace(0, 1) * 100).replace([np.inf, -np.inf], 0).fillna(0).round(0)
                        c = c.reset_index().rename(columns={secondary_col: 'Detail'})
                        
                        # 3. Combine and Pivot (Keep as MultiIndex for precise grouping later)
                        combined = pd.concat([p, c]).set_index([primary_col, 'Detail', time_col])
                        unstacked = combined.unstack(level=2).fillna(0).sort_index(level=[0, 1])
                        return unstacked

                    def create_hierarchical_val(df_source, primary_col, secondary_col, time_col):
                        p = df_source.groupby([primary_col, time_col])[val_display_list].sum().reset_index()
                        p['Detail'] = " SUMMARY"
                        
                        c = df_source.groupby([primary_col, secondary_col, time_col])[val_display_list].sum().reset_index().rename(columns={secondary_col: 'Detail'})
                        
                        combined = pd.concat([p, c]).set_index([primary_col, 'Detail', time_col])
                        unstacked = combined.unstack(level=2).fillna(0).sort_index(level=[0, 1])
                        return unstacked

                    qty_pivot = create_hierarchical_qty(df_clean, 'Store', 'Item_Name', group_col)
                    val_pivot = create_hierarchical_val(df_clean, 'Store', 'Item_Name', group_col)
                    item_qty_pivot = create_hierarchical_qty(df_clean, 'Item_Name', 'Store', group_col)
                    item_val_pivot = create_hierarchical_val(df_clean, 'Item_Name', 'Store', group_col)
                    if group_col == "Month":
                        month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                        
                        # Explicitly assigns sorting weights to force metrics to the far right
                        metric_order_weights = {
                            'Dist_Qty': 0, 'Sales_Qty': 1, 'Waste_Qty': 2, 'Balance Stock': 3, 'STR%': 4,
                            'Dist_Val': 0, 'Sales_Val': 1, 'Waste_Val': 2, 'Profit': 3
                        }
                        
                        # Unpacks 3 levels safely (Metric, Year, Month) and orders by custom weight
                        def chronological_column_key(col_tuple):
                            metric_name, month_name = col_tuple
                            
                            m_weight = metric_order_weights.get(metric_name, 99)
                            m_idx = month_order.index(month_name) if month_name in month_order else 99
                            return (m_weight, m_idx)
                        
                        # Reindex all 4 pivots safely using the custom priority rules
                        if not qty_pivot.empty:
                            qty_pivot = qty_pivot.reindex(columns=sorted(qty_pivot.columns, key=chronological_column_key))
                        if not val_pivot.empty:
                            val_pivot = val_pivot.reindex(columns=sorted(val_pivot.columns, key=chronological_column_key))
                        if not item_qty_pivot.empty:
                            item_qty_pivot = item_qty_pivot.reindex(columns=sorted(item_qty_pivot.columns, key=chronological_column_key))
                        if not item_val_pivot.empty:
                            item_val_pivot = item_val_pivot.reindex(columns=sorted(item_val_pivot.columns, key=chronological_column_key))
                    # ----------------------------------------------------
                    # FILE 1: BUILD CLEAN FULL REPORT
                    # ----------------------------------------------------
                    output_clean = io.BytesIO()
                    with pd.ExcelWriter(output_clean, engine='xlsxwriter') as writer:
                        workbook = writer.book
                        title_fmt = workbook.add_format({'bold': True, 'font_size': 14, 'color': '#1F497D'})
                        cell_fmt = workbook.add_format({'border': 1, 'valign': 'vcenter'})
                        num_fmt = workbook.add_format({'num_format': '#,##0.00', 'border': 1, 'valign': 'vcenter'})
                        total_fmt = workbook.add_format({'bold': True, 'bg_color': '#D9D9D9', 'border': 1, 'valign': 'vcenter'})
                        int_fmt = workbook.add_format({'num_format': '#,##0', 'border': 1, 'valign': 'vcenter'})
                        total_int_fmt = workbook.add_format({'num_format': '#,##0', 'bold': True, 'bg_color': '#D9D9D9', 'border': 1, 'valign': 'vcenter'})
                        total_num_fmt = workbook.add_format({'num_format': '#,##0.00', 'bold': True, 'bg_color': '#D9D9D9', 'border': 1, 'valign': 'vcenter'})
                        header_base = workbook.add_format({'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#D9D9D9', 'font_color': 'black'})
                        fmt_dist = workbook.add_format({'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#B4C6E7', 'font_color': 'black'}) 
                        fmt_sales = workbook.add_format({'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#F8CBAD', 'font_color': 'black'}) 
                        fmt_waste = workbook.add_format({'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#C6E0B4', 'font_color': 'black'}) 
                        fmt_calc = workbook.add_format({'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#FFE699', 'font_color': 'black'}) 

                        def get_fmt(metric_name):
                            m = str(metric_name).upper()
                            if 'DIST' in m: return fmt_dist
                            if 'SALES' in m: return fmt_sales
                            if 'WASTE' in m: return fmt_waste
                            if 'STR' in m or 'PROFIT' in m or 'BALANCE' in m: return fmt_calc
                            return header_base

                        def format_pivot(df_to_write, sheet_name, title, col_w=20):
                            if df_to_write.empty: return
                            
                            # 1. Use the unflattened MultiIndex to safely calculate Grand Totals 
                            master_mask = df_to_write.index.get_level_values(1) == " SUMMARY"
                            totals = df_to_write[master_mask].sum(numeric_only=True)
                            
                            # 2. Build the 1-Column visual list and strictly map out outline levels
                            flat_index = []
                            outline_levels = []
                            for p, d in df_to_write.index:
                                if d == " SUMMARY":
                                    flat_index.append(str(p))
                                    outline_levels.append(0) # Store row (Master)
                                else:
                                    flat_index.append(f"      ↳ {d}")
                                    outline_levels.append(1) # Item row (Detail)
                                    
                            # 3. Replace the 2-column MultiIndex with the beautiful 1-column list
                            df_to_write.index = flat_index
                            df_to_write.index.name = "Store" if "Store" in sheet_name else "Item Name"
                            
                            # 4. Write to Excel
                            df_to_write.to_excel(writer, sheet_name=sheet_name, startrow=2)
                            ws = writer.sheets[sheet_name]
                            ws.write(0, 0, title, title_fmt)
                            
                            # Enable Outline Symbols (+ / -)
                            ws.outline_settings(visible=True, symbols_below=False, symbols_right=True, auto_style=False)
                            
                            idx_cols = 1 # Keep 1 column layout
                            num_cols = len(df_to_write.columns)
                            hdr_rows = df_to_write.columns.nlevels
                            
                            # FIX: Pandas automatically inserts an extra row for the index name when dealing with multi-columns. 
                            # We must offset the data_start_row by +1 so it targets the actual Store names!
                            data_start_row = 2 + hdr_rows + 1 
                            total_row = data_start_row + len(df_to_write.index) 
                            
                            ws.set_column(0, 0, col_w, cell_fmt)
                            for c_idx, col_tuple in enumerate(df_to_write.columns):
                                excel_c = idx_cols + c_idx
                                metric = col_tuple[0] if isinstance(col_tuple, tuple) else col_tuple
                                c_fmt = int_fmt if 'STR%' in str(metric).upper() else num_fmt
                                ws.set_column(excel_c, excel_c, 14, c_fmt)
                            
                            for i, idx_name in enumerate(df_to_write.index.names):
                                name = str(idx_name) if idx_name else ""
                                # Format all the header rows on the left, including the new index name row
                                for r in range(2, data_start_row):
                                    val = name if r == data_start_row - 1 else ""
                                    ws.write(r, i, val, header_base)

                            for c_idx, col_tuple in enumerate(df_to_write.columns):
                                excel_c = idx_cols + c_idx
                                metric = col_tuple[0] if isinstance(col_tuple, tuple) else col_tuple
                                c_fmt = get_fmt(metric)
                                if isinstance(col_tuple, tuple):
                                    for r_idx, val in enumerate(col_tuple):
                                        ws.write(2 + r_idx, excel_c, str(val), c_fmt)
                                else:
                                    ws.write(2, excel_c, str(col_tuple), c_fmt)
                                    
                            # --- APPLY COLLAPSIBLE ROW GROUPS (+/-) DIRECTLY FROM STRICT MAP ---
                            for row_idx, level_id in enumerate(outline_levels):
                                actual_excel_row = data_start_row + row_idx
                                
                                if level_id == 0:
                                    # Master Row (Store names) -> Visible by default, gets the [+]
                                    ws.set_row(actual_excel_row, None, None, {'level': 0, 'collapsed': True})
                                else:
                                    # Indented Detail Row (Items) -> Hidden cleanly underneath the Master
                                    ws.set_row(actual_excel_row, None, None, {'level': 1, 'hidden': True})
                            
                            ws.set_row(total_row, 20, total_fmt)
                            ws.write_string(total_row, 0, "GRAND TOTAL", total_fmt)
                            
                            for col in range(idx_cols, idx_cols + num_cols):
                                col_tuple = df_to_write.columns[col - idx_cols]
                                metric = col_tuple[0] if isinstance(col_tuple, tuple) else col_tuple
                                time_key = col_tuple[1] if isinstance(col_tuple, tuple) else None
                                
                                if 'STR%' in str(metric).upper():
                                    if time_key is not None:
                                        s_tot = totals.get(('Sales_Qty', time_key), 0)
                                        d_tot = totals.get(('Dist_Qty', time_key), 0)
                                    else:
                                        s_tot = totals.get('Sales_Qty', 0)
                                        d_tot = totals.get('Dist_Qty', 0)
                                    val = (s_tot / d_tot * 100) if d_tot > 0 else 0.0
                                    val = round(val, 0)
                                    t_fmt = total_int_fmt 
                                else:
                                    val = totals.iloc[col - idx_cols]
                                    t_fmt = total_num_fmt
                                ws.write_number(total_row, col, val, t_fmt)

                        # Create the 4 clean sheets
                        format_pivot(qty_pivot, 'Store Qty', "📊 STORE QUANTITY ANALYSIS (CLEAN)", col_w=35)
                        format_pivot(val_pivot, 'Store $', "💰 STORE VALUE ANALYSIS (CLEAN)", col_w=35)
                        format_pivot(item_qty_pivot, 'Item Qty', "📦 ITEM QUANTITY SUMMARY (CLEAN)", col_w=40)
                        format_pivot(item_val_pivot, 'Item $', "💵 ITEM VALUE SUMMARY (CLEAN)", col_w=40)

                        if not df_clean.empty:
                            ws5 = workbook.add_worksheet('TOP&BTM 10')
                            
                            # Filter out unassigned item strings from raw clean records first
                            valid_items_df = df_clean[(~df_clean['Item_Name'].str.startswith('Item ', na=False)) & (df_clean['Item_Name'] != 'Unknown Item')]
                            
                            # Generate dynamic 2D multi-index timeline rank tables matching active group_col tokens
                            top10_df = get_rank_table(valid_items_df, group_col, sort_by='Profit', top=True, n=10)
                            bottom10_df = get_rank_table(valid_items_df, group_col, sort_by='Profit', top=False, n=10)
                            
                            # --- 1. RENDER TOP 10 CHRONOLOGICAL TIMELINE ---
                            ws5.write(0, 0, "🏆 TOP 10 ITEMS BY PROFIT", title_fmt)
                            top10_df.to_excel(writer, sheet_name='TOP&BTM 10', startrow=2, index=True)
                            
                            # Color headers to match standard layout sheets
                            ws5.write(2, 0, "Item Name", header_base)
                            ws5.write(3, 0, "", header_base)
                            for c_idx, col_tuple in enumerate(top10_df.columns):
                                excel_c = 1 + c_idx
                                ws5.write(2, excel_c, str(col_tuple[0]), get_fmt(col_tuple[0]))
                                ws5.write(3, excel_c, str(col_tuple[1]), get_fmt(col_tuple[0]))
                                
                            # Write grand summary bottom row
                            total_row_top = 4 + len(top10_df)
                            ws5.write(total_row_top, 0, "GRAND TOTAL", total_fmt)
                            for c_idx, col_tuple in enumerate(top10_df.columns):
                                val = top10_df[col_tuple].sum()
                                ws5.write_number(total_row_top, 1 + c_idx, val, total_num_fmt)
                                
                            # --- 2. RENDER BOTTOM 10 CHRONOLOGICAL TIMELINE ---
                            start_btm_row = total_row_top + 3
                            ws5.write(start_btm_row, 0, "📉 BOTTOM 10 ITEMS BY PROFIT", title_fmt)
                            bottom10_df.to_excel(writer, sheet_name='TOP&BTM 10', startrow=start_btm_row + 2, index=True)
                            
                            ws5.write(start_btm_row + 2, 0, "Item Name", header_base)
                            ws5.write(start_btm_row + 3, 0, "", header_base)
                            for c_idx, col_tuple in enumerate(bottom10_df.columns):
                                excel_c = 1 + c_idx
                                ws5.write(start_btm_row + 2, excel_c, str(col_tuple[0]), get_fmt(col_tuple[0]))
                                ws5.write(start_btm_row + 3, excel_c, str(col_tuple[1]), get_fmt(col_tuple[0]))
                                
                            total_row_btm = start_btm_row + 4 + len(bottom10_df)
                            ws5.write(total_row_btm, 0, "GRAND TOTAL", total_fmt)
                            for c_idx, col_tuple in enumerate(bottom10_df.columns):
                                val = bottom10_df[col_tuple].sum()
                                ws5.write_number(total_row_btm, 1 + c_idx, val, total_num_fmt)
                                
                            # Global formatting widths for sheet columns
                            ws5.set_column(0, 0, 40, cell_fmt)
                            ws5.set_column(1, len(top10_df.columns) + 1, 14, num_fmt)
                            
                            df.to_excel(writer, sheet_name='Master Data Raw', index=False)

                    # ----------------------------------------------------
                    # FILE 2: BUILD UNMAPPED STORES & ITEMS REPORT
                    # ----------------------------------------------------
                    output_unmapped = io.BytesIO()
                    with pd.ExcelWriter(output_unmapped, engine='xlsxwriter') as writer_unmapped:
                        wb_un = writer_unmapped.book
                        title_fmt_un = wb_un.add_format({'bold': True, 'font_size': 14, 'color': '#C00000'})
                        header_un = wb_un.add_format({'bold': True, 'border': 1, 'bg_color': '#FCE4D6', 'align': 'center'})
                        cell_un = wb_un.add_format({'border': 1})
                        num_un = wb_un.add_format({'num_format': '#,##0.00', 'border': 1})

                        # Tab 1: Unmapped Rows
                        if not df_unmapped_raw.empty:
                            df_unmapped_raw.to_excel(writer_unmapped, sheet_name='Unmapped Data Rows', index=False, startrow=2)
                            ws_un1 = writer_unmapped.sheets['Unmapped Data Rows']
                            ws_un1.write(0, 0, "⚠️ ALL UNMAPPED ENTRIES TRANSACTION LOG", title_fmt_un)
                        else:
                            # Fallback if empty
                            empty_df = pd.DataFrame([["Perfect match! No unmapped items or stores found."]], columns=["Status"])
                            empty_df.to_excel(writer_unmapped, sheet_name='Unmapped Data Rows', index=False)

                    excel_data_clean = output_clean.getvalue()
                    excel_data_unmapped = output_unmapped.getvalue()

                    # Render Download Buttons Side by Side
                    col_d1, col_d2 = st.columns(2)
                    with col_d1:
                        st.download_button(
                            label="📥 Download Clean Full Excel Report", 
                            data=excel_data_clean, 
                            file_name=f"Clean_Report_{sel_year}_{rpt}.xlsx", 
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    with col_d2:
                        if not df_unmapped_raw.empty:
                            st.download_button(
                                label="⚠️ Download Unmapped Stores & Items Report", 
                                data=excel_data_unmapped, 
                                file_name=f"UNMAPPED_Log_{sel_year}_{rpt}.xlsx", 
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        else:
                            st.button("✅ No Unmapped Data Found", disabled=True)

                    c1, c2 = st.columns([3, 1])
    elif app_mode == "🗄️ Saved Reports":
        if urls['h']:
            reps = get_saved_reports(urls['h'])
            if reps:
                sel = st.selectbox("Select Report:", reps)
                if sel:
                    loaded_data = {}
                    sheet_tabs = ["StoreQty", "StoreVal", "ItemQty", "ItemVal", "Top10", "Master"]
                    with st.spinner("Downloading Report Data..."):
                        try:
                            client = get_gspread_client()
                            sh = client.open_by_url(urls['h'])
                            for tab_name in sheet_tabs:
                                try:
                                    full_data = sh.worksheet(f"Rep_{sel}_{tab_name}").get_all_values()
                                    if full_data:
                                        header = full_data[0]
                                        rows = full_data[1:]
                                        loaded_data[tab_name] = pd.DataFrame(rows, columns=header)
                                    else: loaded_data[tab_name] = pd.DataFrame()
                                except: loaded_data[tab_name] = pd.DataFrame()
                        except Exception as e:
                            st.error(f"Connection Error: {e}")
                            st.stop()

                    if loaded_data:
                        t1, t2, t3, t4, t5, t6 = st.tabs(["📦 Store Qty", "💰 Store Val", "📦 Item Qty", "💰 Item Val", "🏆 Top 10", "📝 Master Data"])
                        with t1: st.dataframe(loaded_data.get("StoreQty", pd.DataFrame()), use_container_width=True)
                        with t2: st.dataframe(loaded_data.get("StoreVal", pd.DataFrame()), use_container_width=True)
                        with t3: st.dataframe(loaded_data.get("ItemQty", pd.DataFrame()), use_container_width=True)
                        with t4: st.dataframe(loaded_data.get("ItemVal", pd.DataFrame()), use_container_width=True)
                        with t5: 
                            df_top = loaded_data.get("Top10", pd.DataFrame())
                            st.dataframe(df_top, use_container_width=True)
                            if not df_top.empty and 'Total Sales' in df_top.columns:
                                try:
                                    df_top['Total Sales'] = pd.to_numeric(df_top['Total Sales'], errors='coerce')
                                    st.bar_chart(df_top.set_index(df_top.columns[0])['Total Sales'])
                                except: pass
                        with t6: st.dataframe(loaded_data.get("Master", pd.DataFrame()), use_container_width=True)
