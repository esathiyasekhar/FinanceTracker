import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import time
import re
import pdfplumber 

# --- 1. VISUAL STYLING (CSS) ---
def inject_custom_css():
    st.markdown("""
        <style>
        @keyframes flashFast { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
        .emi-box-paid { background-color: #d4edda; color: #155724; padding: 15px; border-radius: 10px; border-left: 5px solid #28a745; margin-bottom: 10px; }
        .emi-box-due { background-color: #f8d7da; color: #721c24; padding: 15px; border-radius: 10px; border-left: 5px solid #dc3545; margin-bottom: 10px; }
        .text-rem { color: #dc3545; font-weight: bold; }
        .text-ok { color: #28a745; font-weight: bold; }
        .stTabs [data-baseweb="tab-list"] { gap: 10px; }
        .stTabs [data-baseweb="tab"] { height: 50px; background-color: #f0f2f6; border-radius: 4px 4px 0 0; gap: 1px; padding: 10px; }
        .stTabs [aria-selected="true"] { background-color: #ffffff; border-top: 2px solid #ff4b4b; }
        </style>
    """, unsafe_allow_html=True)

# --- 2. DATA HELPERS ---
def safe_float(val):
    if pd.isna(val) or val == "": return 0.0
    if isinstance(val, (int, float)): return float(val)
    clean = re.sub(r'[^\d.-]', '', str(val))
    try: return float(clean)
    except: return 0.0

def safe_date(val):
    if not val or pd.isna(val): return None
    val = str(val).strip()
    # Supports DD-MM-YYYY, YYYY-MM-DD, and DD-Mon (e.g. 12-Jan)
    formats = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", "%Y/%m/%d", "%d-%b-%y", "%d-%b"]
    for fmt in formats:
        try: 
            dt = datetime.strptime(val, fmt)
            # If date format has no year (e.g. 12-Jan), use current year
            if "%Y" not in fmt and "%y" not in fmt:
                dt = dt.replace(year=datetime.now().year)
            return dt.date()
        except: continue
    return None

def check_duplicate(df, col_name, value, label="Entry", exclude_id=None):
    if df.empty or col_name not in df.columns: return False
    if exclude_id: df = df[df['ID'].astype(str) != str(exclude_id)]
    existing = df[col_name].astype(str).str.strip().str.lower().tolist()
    new_val = str(value).strip().lower()
    if new_val in existing:
        st.error(f"❌ Duplicate: A {label} with {col_name} '{value}' already exists!")
        return True
    return False

# --- 3. GOOGLE SHEETS CONNECTION ---
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

@st.cache_resource
def connect_gsheets():
    try:
        # Tries Streamlit Secrets first (for Cloud), then local file (for PC)
        if "gcp_service_account" in st.secrets:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gcp_service_account"], SCOPE)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", SCOPE)
        client = gspread.authorize(creds)
        return client.open("MyFinanceDB") 
    except Exception as e:
        st.error(f"❌ Auth Error: {e}")
        st.stop()

# --- RETRY & CACHE LOGIC ---
def api_retry(func, *args, **kwargs):
    """Retries API call if we hit the Google Quota limit (429 Error)."""
    for i in range(5):
        try: return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e): time.sleep((i+1)*2); continue
            raise e
    return func(*args, **kwargs)

@st.cache_data(ttl=60) 
def fetch_sheet_data_cached(_sh, sheet_name):
    return api_retry(_sh.worksheet, sheet_name).get_all_records()

def clear_cache(): st.cache_data.clear()

def get_df(sh, name):
    try:
        data = fetch_sheet_data_cached(sh, name)
        df = pd.DataFrame(data)
        required_cols = {
            "Cards": ["ID", "Name", "Limit", "GraceDays", "MatchCode"],
            "Statements": ["CardID", "Year", "Month", "Billed", "Paid", "Unbilled", "UnbilledDate", "StmtDate", "DueDate"],
            "Card_Payments": ["ID", "CardID", "Year", "Month", "Date", "Amount", "Note"],
            "Loans": ["ID", "Source", "Type", "Category", "Principal", "EMI", "Tenure", "StartDate", "Outstanding", "Status", "DueDay", "MatchCode"],
            "Loan_Repayments": ["ID", "LoanID", "PaymentDate", "Amount"],
            "Active_EMIs": ["ID", "CardID", "Item", "Beneficiary", "TotalVal", "MonthlyEMI", "Start", "Tenure", "Status"],
            "Banks": ["ID", "Name", "Type", "AccNo", "MatchCode"],
            "Bank_Balances": ["BankID", "Year", "Month", "Balance"],
            "Transactions": ["ID", "Date", "Year", "Month", "Type", "Category", "Amount", "Notes", "SourceAccount"]
        }
        if df.empty and name in required_cols: return pd.DataFrame(columns=required_cols[name])
        if name in required_cols:
            for c in required_cols[name]:
                if c not in df.columns: df[c] = ""
        return df
    except: return pd.DataFrame()

def update_full_sheet(sh, name, df):
    ws = api_retry(sh.worksheet, name)
    ws.clear()
    ws.append_row(df.columns.tolist())
    ws.append_rows(df.values.tolist())
    clear_cache()

def get_next_id(df):
    if df.empty or 'ID' not in df.columns: return 1
    ids = pd.to_numeric(df['ID'], errors='coerce').fillna(0)
    return int(ids.max()) + 1 if not ids.empty else 1

def add_row(sh, name, row):
    ws = api_retry(sh.worksheet, name)
    ws.append_row(row)
    clear_cache()

def update_row_by_id(sh, name, id_val, updated_dict, df_current):
    idx_list = df_current.index[df_current['ID'].astype(str) == str(id_val)].tolist()
    if not idx_list: return False
    idx = idx_list[0]
    for col, val in updated_dict.items(): df_current.at[idx, col] = val
    update_full_sheet(sh, name, df_current)
    return True

def delete_row_by_id(sh, sheet_name, id_val):
    try:
        ws = api_retry(sh.worksheet, sheet_name)
        data = ws.get_all_records()
        row_idx = None
        for i, row in enumerate(data):
            if str(row.get('ID')) == str(id_val):
                row_idx = i + 2 
                break
        if row_idx:
            ws.delete_rows(row_idx)
            clear_cache()
            return True
        return False
    except: return False

def init_sheets(sh):
    # Initializes sheets if they don't exist
    schema = {
        "Config": ["Key", "Value"],
        "Cards": ["ID", "Name", "First4", "Last4", "Limit", "GraceDays", "MatchCode"], 
        "Banks": ["ID", "Name", "Type", "AccNo", "MatchCode"],
        "Loans": ["ID", "Source", "Type", "Category", "Collateral", "Principal", "Rate", "EMI", "Tenure", "StartDate", "Outstanding", "Status", "DueDay", "MatchCode"],
        "Active_EMIs": ["ID", "CardID", "Item", "Beneficiary", "TotalVal", "MonthlyEMI", "Start", "Tenure", "Status"],
        "Transactions": ["ID", "Date", "Year", "Month", "Type", "Category", "Amount", "Notes", "SourceAccount"],
        "Statements": ["CardID", "Year", "Month", "StmtDate", "Billed", "Unbilled", "UnbilledDate", "Paid", "DueDate"], 
        "Bank_Balances": ["BankID", "Year", "Month", "Balance"],
        "Owings": ["Year", "Month", "Person", "Type", "Amount", "Status"],
        "Loan_Repayments": ["ID", "LoanID", "PaymentDate", "Amount", "Type"],
        "Card_Payments": ["ID", "CardID", "Year", "Month", "Date", "Amount", "Note"]
    }
    try: existing = [w.title for w in api_retry(sh.worksheets)]
    except: existing = []
    for name, cols in schema.items():
        if name not in existing:
            ws = api_retry(sh.add_worksheet, title=name, rows=100, cols=20)
            ws.append_row(cols)
        else:
            # Auto-Add Missing Columns if needed
            ws = api_retry(sh.worksheet, name)
            headers = ws.row_values(1)
            new_headers = [c for c in cols if c not in headers]
            for i, h in enumerate(new_headers): ws.update_cell(1, len(headers) + i + 1, h)

# --- 4. MAIN APP ---
def main():
    st.set_page_config(page_title="Finance Hub Pro", layout="wide")
    inject_custom_css()
    sh = connect_gsheets()
    
    if 'init_final' not in st.session_state:
        init_sheets(sh)
        st.session_state['init_final'] = True
    
    st.sidebar.title("☁️ Finance Hub")
    c1, c2 = st.sidebar.columns(2)
    year = c1.selectbox("Year", list(range(2025, 2035)))
    month = c2.selectbox("Month", ["January", "February", "March", "April", "May", "June", 
                                  "July", "August", "September", "October", "November", "December"])
    
    menu = ["Dashboard", "Credit Cards", "Loans", "Credit Card EMIs", "Bank Accounts", "Income/Exp"]
    choice = st.sidebar.radio("Go To", menu)

    # ==========================
    # DASHBOARD
    # ==========================
    if choice == "Dashboard":
        st.title(f"📊 Dashboard - {month} {year}")
        try:
            stmts = get_df(sh, "Statements")
            bk = get_df(sh, "Bank_Balances")
            tx = get_df(sh, "Transactions")
            emis = get_df(sh, "Active_EMIs")
        except: st.stop()
        
        liq = 0.0
        if not bk.empty: 
            curr_bk = bk[(bk['Year'] == year) & (bk['Month'] == month)]
            liq = curr_bk['Balance'].apply(safe_float).sum()

        bill = 0; paid = 0; unbilled = 0
        curr_stmts = stmts[(stmts['Year'] == year) & (stmts['Month'] == month)].copy()
        if not curr_stmts.empty:
            bill = curr_stmts['Billed'].apply(safe_float).sum()
            paid = curr_stmts['Paid'].apply(safe_float).sum()
            unbilled = curr_stmts['Unbilled'].apply(safe_float).sum()
        
        my_liab = (bill - paid) + unbilled
        
        k1, k2, k3 = st.columns(3)
        k1.metric("Net Liquidity", f"₹{liq:,.0f}")
        k2.metric("Pending Bills", f"₹{bill-paid:,.0f}", delta_color="inverse")
        k3.metric("Total Liability", f"₹{my_liab:,.0f}")

    # ==========================
    # CREDIT CARDS
    # ==========================
    elif choice == "Credit Cards":
        st.title("💳 Credit Cards")
        cards = get_df(sh, "Cards")
        
        tab_view, tab_manage = st.tabs(["Overview & Payments", "Manage Cards"])
        
        with tab_view:
            if cards.empty: st.warning("No cards found.")
            stmts = get_df(sh, "Statements")
            cpays = get_df(sh, "Card_Payments")
            
            for _, row in cards.iterrows():
                match = stmts[(stmts['CardID'] == row['ID']) & (stmts['Year'] == year) & (stmts['Month'] == month)]
                curr_b=0.0; curr_p=0.0; curr_d=""; curr_stmt_dt=""; curr_unb=0.0; curr_unb_dt=""
                if not match.empty:
                    r = match.iloc[0]
                    curr_b = safe_float(r['Billed'])
                    hist_df = cpays[(cpays['CardID'] == row['ID']) & (cpays['Year'] == year) & (cpays['Month'] == month)]
                    calc_paid = hist_df['Amount'].apply(safe_float).sum()
                    curr_p = calc_paid if not hist_df.empty else safe_float(r['Paid'])
                    curr_d = str(r['DueDate'])
                    curr_stmt_dt = str(r.get('StmtDate', ''))
                    curr_unb = safe_float(r.get('Unbilled', 0))
                    curr_unb_dt = str(r.get('UnbilledDate', ''))
                
                rem = max(0, curr_b - curr_p)
                bg = "#e9ecef"
                if curr_b > 0:
                    if rem == 0: bg = "#d4edda"
                    elif safe_date(curr_d):
                        days = (safe_date(curr_d) - datetime.now().date()).days
                        if days <= 5: bg = "#fff3cd"
                        if days <= 0: bg = "#f8d7da"

                with st.container():
                    match_label = f"(ID: {row.get('MatchCode', 'N/A')})"
                    st.markdown(f"""
                    <div style="background-color:{bg}; padding:10px; border-radius:10px; border:1px solid #ccc; margin-bottom:10px;">
                        <div style="display:flex; justify-content:space-between;">
                            <h3 style="margin:0;">{row['Name']} {match_label}</h3>
                            <span>Bill: <b>₹{curr_b:,.0f}</b></span>
                        </div>
                        <div style="display:flex; justify-content:space-between; margin-top:5px;">
                            <small>Unbilled: ₹{curr_unb:,.0f}</small>
                            <span class="{'text-ok' if rem==0 else 'text-rem'}">To Pay: ₹{rem:,.0f}</span>
                        </div>
                    </div>""", unsafe_allow_html=True)

                with st.expander(f"Update {row['Name']}", expanded=(rem>0)):
                    with st.form(f"st_{row['ID']}"):
                        c1,c2,c3 = st.columns(3)
                        s_dt = c1.date_input("Stmt Date", value=safe_date(curr_stmt_dt))
                        d_dt_obj = safe_date(curr_d)
                        if s_dt and not d_dt_obj:
                            grace = int(safe_float(row.get('GraceDays', 20)))
                            d_dt_obj = s_dt + timedelta(days=grace)
                        d_dt = c2.date_input("Due Date", value=d_dt_obj)
                        b_amt = c3.number_input("Bill Amt", value=curr_b)
                        st.markdown("---")
                        u1, u2 = st.columns(2)
                        u_amt = u1.number_input("Unbilled Amt", value=curr_unb)
                        u_date = u2.date_input("As of", value=safe_date(curr_unb_dt) or datetime.now())
                        if st.form_submit_button("Update Info"):
                            if not stmts.empty: stmts = stmts[~((stmts['CardID'] == row['ID']) & (stmts['Year'] == year) & (stmts['Month'] == month))]
                            new_row = {"CardID": row['ID'], "Year": year, "Month": month, "StmtDate": str(s_dt) if s_dt else "", "Billed": b_amt, "Unbilled": u_amt, "UnbilledDate": str(u_date), "Paid": curr_p, "DueDate": str(d_dt) if d_dt else ""}
                            stmts = pd.concat([stmts, pd.DataFrame([new_row])], ignore_index=True)
                            update_full_sheet(sh, "Statements", stmts)
                            st.success("Updated"); st.rerun()

                    with st.form(f"p_{row['ID']}"):
                        pc1, pc2 = st.columns([1, 2])
                        amt = pc1.number_input("Amount", min_value=0.0, value=float(rem))
                        nt = pc2.text_input("Note")
                        if st.form_submit_button("Add Payment"):
                            if amt <= 0: st.error("Amount must be > 0"); st.stop()
                            pid = get_next_id(cpays)
                            add_row(sh, "Card_Payments", [pid, row['ID'], year, month, str(date.today()), amt, nt])
                            st.success("Paid"); time.sleep(1); st.rerun()

        with tab_manage:
            action = st.radio("Action", ["Add New", "Edit Existing", "Delete"], horizontal=True)
            if action == "Add New":
                with st.form("add_c"):
                    n = st.text_input("Name (e.g. HDFC Regalia)")
                    m_code = st.text_input("Match Code (Unique ID found in filename)", max_chars=10, help="e.g. 0639 or SBI1 0639")
                    l = st.number_input("Limit", step=1000); g = st.number_input("Grace Days", value=20)
                    if st.form_submit_button("Create"):
                        if not n.strip() or not m_code.strip(): st.error("Name & Code Required"); st.stop()
                        if check_duplicate(cards, "Name", n, "Card"): st.stop()
                        if check_duplicate(cards, "MatchCode", m_code, "Card Identifier"): st.stop()
                        cid = get_next_id(cards)
                        add_row(sh, "Cards", [cid, n, "", "", l, g, m_code])
                        st.success("Added"); time.sleep(1); st.rerun()
            elif action == "Edit Existing" and not cards.empty:
                sel_c = st.selectbox("Select Card", cards['Name'].unique())
                card_row = cards[cards['Name'] == sel_c].iloc[0]
                with st.form("edit_c"):
                    n = st.text_input("Name", value=card_row['Name'])
                    m_code = st.text_input("Match Code", value=str(card_row.get('MatchCode','')))
                    l = st.number_input("Limit", value=float(safe_float(card_row['Limit'])))
                    g = st.number_input("Grace Days", value=int(safe_float(card_row['GraceDays'])))
                    if st.form_submit_button("Update Card"):
                        update_row_by_id(sh, "Cards", card_row['ID'], {"Name": n, "MatchCode": m_code, "Limit": l, "GraceDays": g}, cards)
                        st.success("Updated"); time.sleep(1); st.rerun()
            elif action == "Delete" and not cards.empty:
                del_n = st.selectbox("Select Card", cards['Name'].unique())
                if st.button("Confirm Delete"):
                    cid = cards[cards['Name'] == del_n].iloc[0]['ID']
                    delete_row_by_id(sh, "Cards", cid)
                    st.success("Deleted"); time.sleep(1); st.rerun()

    # ==========================
    # LOANS
    # ==========================
    elif choice == "Loans":
        st.title("🏠 Loan Portfolio")
        loans = get_df(sh, "Loans")
        repay = get_df(sh, "Loan_Repayments")
        tab_view, tab_manage = st.tabs(["Overview", "Manage Loans"])
        
        with tab_view:
            active = loans[loans['Status'] == 'Active']
            if active.empty: st.info("No active loans.")
            for _, row in active.iterrows():
                is_paid = False
                if not repay.empty:
                    matches = repay[repay['LoanID'] == row['ID']]
                    for _, r in matches.iterrows():
                        if safe_date(r['PaymentDate']).strftime("%Y-%B") == f"{year}-{month}": is_paid = True
                
                start = safe_date(row.get('StartDate'))
                tenure = int(safe_float(row.get('Tenure', 0)))
                closure_str = "Unknown"
                if start and tenure > 0:
                    closure_date = start + relativedelta(months=tenure)
                    closure_str = closure_date.strftime("%b %Y")

                style = "emi-box-paid" if is_paid else "emi-box-due"
                icon = "✅ PAID" if is_paid else "❌ UNPAID"
                st.markdown(f"""
                <div class="{style}">
                    <div style="display:flex; justify-content:space-between;">
                        <b>{row['Source']} ({row['Type']})</b> <span>{icon}</span>
                    </div>
                    <div style="font-size:0.9em; margin-top:5px;">
                        Code: {row.get('MatchCode', 'N/A')} <br>
                        EMI: ₹{safe_float(row['EMI']):,.0f} | Bal: ₹{safe_float(row['Outstanding']):,.0f}<br>
                        Ends: {closure_str}
                    </div>
                </div>""", unsafe_allow_html=True)
                
                if not is_paid:
                    with st.expander(f"Pay {row['Source']}"):
                        with st.form(f"lp_{row['ID']}"):
                            p_amt = st.number_input("Amount", value=float(safe_float(row['EMI'])))
                            p_dt = st.date_input("Date")
                            if st.form_submit_button("Pay"):
                                rid = get_next_id(repay)
                                add_row(sh, "Loan_Repayments", [rid, int(row['ID']), str(p_dt), p_amt, "EMI"])
                                loans.loc[loans['ID'] == row['ID'], 'Outstanding'] = safe_float(row['Outstanding']) - p_amt
                                update_full_sheet(sh, "Loans", loans)
                                st.success("Paid"); time.sleep(1); st.rerun()

        with tab_manage:
            action = st.radio("Action", ["Add New", "Edit Existing", "Delete"], horizontal=True)
            if action == "Add New":
                with st.form("add_l"):
                    src = st.text_input("Source"); typ = st.text_input("Type")
                    m_code = st.text_input("Match Code (Unique ID)", help="e.g. Loan Acc Number Last 4")
                    amt = st.number_input("Principal"); emi = st.number_input("EMI")
                    start = st.date_input("Start"); ten = st.number_input("Tenure (M)", min_value=1)
                    if st.form_submit_button("Add"):
                        if not src.strip() or not m_code.strip(): st.error("Fields Required"); st.stop()
                        if check_duplicate(loans, "MatchCode", m_code, "Loan ID"): st.stop()
                        lid = get_next_id(loans)
                        add_row(sh, "Loans", [lid, src, typ, "Std", "", amt, 0, emi, ten, str(start), amt, "Active", 5, m_code])
                        st.success("Added"); time.sleep(1); st.rerun()
            elif action == "Edit Existing" and not loans.empty:
                loans['Label'] = loans['Source'] + " (" + loans['MatchCode'].astype(str) + ")"
                sel_l = st.selectbox("Select Loan", loans['Label'].unique())
                l_row = loans[loans['Label'] == sel_l].iloc[0]
                with st.form("edit_l"):
                    src = st.text_input("Source", value=l_row['Source'])
                    m_code = st.text_input("Match Code", value=str(l_row.get('MatchCode','')))
                    emi = st.number_input("EMI", value=float(safe_float(l_row['EMI'])))
                    status = st.selectbox("Status", ["Active", "Closed"], index=0 if l_row['Status']=="Active" else 1)
                    if st.form_submit_button("Update"):
                        update_row_by_id(sh, "Loans", l_row['ID'], {"Source": src, "MatchCode": m_code, "EMI": emi, "Status": status}, loans)
                        st.success("Updated"); time.sleep(1); st.rerun()
            elif action == "Delete" and not loans.empty:
                loans['Label'] = loans['Source'] + " (" + loans['MatchCode'].astype(str) + ")"
                del_l = st.selectbox("Select Loan", loans['Label'].unique())
                if st.button("Confirm Delete"):
                    lid = loans[loans['Label'] == del_l].iloc[0]['ID']
                    delete_row_by_id(sh, "Loans", lid)
                    st.success("Deleted"); time.sleep(1); st.rerun()

    # ==========================
    # CREDIT CARD EMIS
    # ==========================
    elif choice == "Credit Card EMIs":
        st.title("📉 Credit Card EMIs")
        emis = get_df(sh, "Active_EMIs")
        cards = get_df(sh, "Cards")
        tab_view, tab_manage = st.tabs(["Active", "Manage"])
        
        with tab_view:
            if not emis.empty: st.dataframe(emis[emis['Status']=='Active'])
            else: st.info("No active EMIs")
        
        with tab_manage:
            action = st.radio("Action", ["Add New", "Edit Existing", "Delete"], horizontal=True)
            if action == "Add New":
                with st.form("add_e"):
                    if cards.empty: st.error("No Cards"); st.stop()
                    cn = st.selectbox("Card", cards['Name'].unique())
                    it = st.text_input("Item"); who = st.radio("Who", ["Self", "Other"])
                    bn = st.text_input("Name") if who == "Other" else "Self"
                    val = st.number_input("Total"); mon = st.number_input("EMI")
                    if st.form_submit_button("Add"):
                        cid = cards[cards['Name']==cn].iloc[0]['ID']
                        eid = get_next_id(emis)
                        add_row(sh, "Active_EMIs", [eid, int(cid), it, bn, val, mon, str(date.today()), 12, "Active"])
                        st.success("Added"); time.sleep(1); st.rerun()
            elif action == "Edit Existing" and not emis.empty:
                sel_e = st.selectbox("Select Item", emis['Item'].unique())
                e_row = emis[emis['Item'] == sel_e].iloc[0]
                with st.form("edit_e"):
                    it = st.text_input("Item", value=e_row['Item'])
                    mon = st.number_input("EMI", value=float(safe_float(e_row['MonthlyEMI'])))
                    status = st.selectbox("Status", ["Active", "Closed"], index=0 if e_row['Status']=="Active" else 1)
                    if st.form_submit_button("Update"):
                        update_row_by_id(sh, "Active_EMIs", e_row['ID'], {"Item": it, "MonthlyEMI": mon, "Status": status}, emis)
                        st.success("Updated"); time.sleep(1); st.rerun()
            elif action == "Delete" and not emis.empty:
                del_e = st.selectbox("Select Item", emis['Item'].unique())
                if st.button("Delete"):
                    eid = emis[emis['Item']==del_e].iloc[0]['ID']
                    delete_row_by_id(sh, "Active_EMIs", eid)
                    st.success("Deleted"); time.sleep(1); st.rerun()

    # ==========================
    # BANK ACCOUNTS
    # ==========================
    elif choice == "Bank Accounts":
        st.title("🏦 Bank Accounts")
        banks = get_df(sh, "Banks")
        tab_view, tab_manage = st.tabs(["Balances", "Manage"])
        
        with tab_view:
            with st.form("bal_f"):
                updates = {}
                for _, r in banks.iterrows():
                    bal_df = get_df(sh, "Bank_Balances")
                    curr = 0.0
                    if not bal_df.empty:
                        match = bal_df[(bal_df['BankID']==r['ID'])&(bal_df['Year']==year)&(bal_df['Month']==month)]
                        if not match.empty: curr = safe_float(match.iloc[0]['Balance'])
                    updates[r['ID']] = st.number_input(f"{r['Name']} ({r.get('MatchCode','')})", value=curr)
                if st.form_submit_button("Save"):
                    df = get_df(sh, "Bank_Balances")
                    if not df.empty: df = df[~((df['Year']==year)&(df['Month']==month))]
                    new_rows = []
                    for bid, val in updates.items(): new_rows.append({"BankID": bid, "Year": year, "Month": month, "Balance": val})
                    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
                    update_full_sheet(sh, "Bank_Balances", df)
                    st.success("Synced")

        with tab_manage:
            action = st.radio("Action", ["Add New", "Edit Existing", "Delete"], horizontal=True)
            if action == "Add New":
                with st.form("add_b"):
                    bn = st.text_input("Name"); bt = st.selectbox("Type", ["Savings", "Current"])
                    acc_no = st.text_input("Full Account Number")
                    m_code = st.text_input("Match Code (Last 4 digits)", max_chars=10)
                    if st.form_submit_button("Add"):
                        if not bn.strip() or not m_code.strip(): st.error("Fields Required"); st.stop()
                        if check_duplicate(banks, "MatchCode", m_code, "Bank"): st.stop()
                        bid = get_next_id(banks)
                        add_row(sh, "Banks", [bid, bn, bt, acc_no, m_code])
                        st.success("Added"); time.sleep(1); st.rerun()
            elif action == "Edit Existing" and not banks.empty:
                banks['Label'] = banks['Name'] + " (" + banks['MatchCode'].astype(str) + ")"
                sel_b = st.selectbox("Select Bank", banks['Label'].unique())
                b_row = banks[banks['Label'] == sel_b].iloc[0]
                with st.form("edit_b"):
                    bn = st.text_input("Name", value=b_row['Name'])
                    acc_no = st.text_input("Full Account No", value=str(b_row.get('AccNo','')))
                    m_code = st.text_input("Match Code", value=str(b_row.get('MatchCode','')))
                    if st.form_submit_button("Update"):
                        update_row_by_id(sh, "Banks", b_row['ID'], {"Name": bn, "AccNo": acc_no, "MatchCode": m_code}, banks)
                        st.success("Updated"); time.sleep(1); st.rerun()
            elif action == "Delete" and not banks.empty:
                banks['Label'] = banks['Name'] + " (" + banks['MatchCode'].astype(str) + ")"
                del_b = st.selectbox("Select", banks['Label'].unique())
                if st.button("Delete"):
                    bid = banks[banks['Label']==del_b].iloc[0]['ID']
                    delete_row_by_id(sh, "Banks", bid)
                    st.success("Deleted"); time.sleep(1); st.rerun()

    # ==========================
    # INCOME / EXP (AUTO IDENTIFIER MATCH)
    # ==========================
    elif choice == "Income/Exp":
        st.title("💸 Income & Expenses")
        tab_view, tab_manage, tab_upload = st.tabs(["History", "Manage", "Statement Upload (Auto-Match)"])
        
        with tab_view:
            tx = get_df(sh, "Transactions")
            if not tx.empty: st.dataframe(tx[(tx['Year']==year)&(tx['Month']==month)])
            else: st.info("No entries.")

        with tab_manage:
            action = st.radio("Action", ["Add New", "Edit Existing", "Delete"], horizontal=True)
            if action == "Add New":
                with st.form("new_tx"):
                    tt = st.selectbox("Type", ["Expense", "Income"])
                    cat = st.text_input("Category"); amt = st.number_input("Amount"); nt = st.text_input("Notes")
                    if st.form_submit_button("Add"):
                        tid = get_next_id(get_df(sh, "Transactions"))
                        add_row(sh, "Transactions", [tid, str(date.today()), year, month, tt, cat, amt, nt, "Manual"])
                        st.success("Added"); time.sleep(1); st.rerun()
            elif action == "Edit Existing":
                tx = get_df(sh, "Transactions")
                curr_tx = tx[(tx['Year']==year)&(tx['Month']==month)]
                if not curr_tx.empty:
                    curr_tx['Label'] = curr_tx['Date'] + " | " + curr_tx['Category'] + " | " + curr_tx['Amount'].astype(str)
                    sel_tx = st.selectbox("Select Entry", curr_tx['Label'].unique())
                    tx_row = curr_tx[curr_tx['Label'] == sel_tx].iloc[0]
                    with st.form("edit_tx"):
                        cat = st.text_input("Category", value=tx_row['Category'])
                        amt = st.number_input("Amount", value=float(safe_float(tx_row['Amount'])))
                        nt = st.text_input("Notes", value=str(tx_row['Notes']))
                        if st.form_submit_button("Update"):
                            update_row_by_id(sh, "Transactions", tx_row['ID'], {"Category": cat, "Amount": amt, "Notes": nt}, tx)
                            st.success("Updated"); time.sleep(1); st.rerun()
            elif action == "Delete":
                tx = get_df(sh, "Transactions")
                curr_tx = tx[(tx['Year']==year)&(tx['Month']==month)]
                if not curr_tx.empty:
                    curr_tx['Label'] = curr_tx['Date'] + " | " + curr_tx['Category'] + " | " + curr_tx['Amount'].astype(str)
                    del_lbl = st.selectbox("Select Entry", curr_tx['Label'].unique())
                    if st.button("Delete"):
                        tid = curr_tx[curr_tx['Label'] == del_lbl].iloc[0]['ID']
                        delete_row_by_id(sh, "Transactions", tid)
                        st.success("Deleted"); time.sleep(1); st.rerun()

        with tab_upload:
            st.subheader("📥 Smart Upload")
            
            # 1. Fetch all Identifiers
            cards = get_df(sh, "Cards")
            banks = get_df(sh, "Banks")
            
            # Create a lookup map: "0639" -> "HDFC Regalia"
            match_map = {}
            if not cards.empty:
                for _, r in cards.iterrows():
                    if str(r.get('MatchCode')).strip(): match_map[str(r['MatchCode']).strip()] = f"Card: {r['Name']}"
            if not banks.empty:
                for _, r in banks.iterrows():
                    if str(r.get('MatchCode')).strip(): match_map[str(r['MatchCode']).strip()] = f"Bank: {r['Name']}"

            # 2. File Upload
            uploaded_file = st.file_uploader("Upload Excel/CSV Statement", type=['xlsx', 'csv'])
            if uploaded_file:
                # AUTO MATCH LOGIC
                fname = uploaded_file.name
                detected_source = "Unknown Source"
                
                # Check filename against match codes
                for code, name in match_map.items():
                    if code in fname:
                        detected_source = name
                        st.success(f"✅ Auto-Detected Source: {name} (Matched '{code}' in filename)")
                        break
                
                # Manual Override
                final_source = st.text_input("Confirm Source Account", value=detected_source)
                
                if st.button("Process & Import"):
                    try:
                        if uploaded_file.name.endswith('.csv'): df = pd.read_csv(uploaded_file)
                        else: df = pd.read_excel(uploaded_file)
                        
                        df.columns = df.columns.str.lower()
                        d_col = next((c for c in df.columns if 'date' in c), None)
                        a_col = next((c for c in df.columns if 'amount' in c or 'debit' in c), None)
                        n_col = next((c for c in df.columns if 'narration' in c or 'description' in c), None)
                        
                        if d_col and a_col:
                            entries = []
                            for _, row in df.iterrows():
                                dt_val = safe_date(row[d_col]) or date.today()
                                amt_val = safe_float(row[a_col])
                                narr_val = str(row[n_col]) if n_col else "Upload"
                                if amt_val > 0:
                                    entries.append([get_next_id(get_df(sh,"Transactions")), str(dt_val), year, month, "Expense", "Statement", amt_val, narr_val, final_source])
                            
                            if entries:
                                ws = api_retry(sh.worksheet, "Transactions"); ws.append_rows(entries); clear_cache()
                                st.success(f"Imported {len(entries)} transactions linked to {final_source}!")
                            else: st.warning("No valid transactions found in file.")
                        else: st.error("Could not find Date/Amount columns.")
                    except Exception as e: st.error(str(e))

if __name__ == "__main__":
    main()