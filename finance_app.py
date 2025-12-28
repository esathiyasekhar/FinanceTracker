import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import time
import re

# --- 1. VISUAL STYLING (CSS) ---
def inject_custom_css():
    st.markdown("""
        <style>
        @keyframes flashFast { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
        .emi-box-paid { background-color: #d4edda; color: #155724; padding: 15px; border-radius: 10px; border-left: 5px solid #28a745; margin-bottom: 10px; }
        .emi-box-due { background-color: #f8d7da; color: #721c24; padding: 15px; border-radius: 10px; border-left: 5px solid #dc3545; margin-bottom: 10px; }
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
    formats = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", "%Y/%m/%d", "%d-%b-%y", "%d-%b"]
    for fmt in formats:
        try: 
            dt = datetime.strptime(val, fmt)
            if "%Y" not in fmt and "%y" not in fmt: dt = dt.replace(year=datetime.now().year)
            return dt.date()
        except: continue
    return None

def check_duplicate(df, col_name, value, label="Entry", exclude_id=None):
    if df.empty or col_name not in df.columns: return False
    if exclude_id: df = df[df['ID'].astype(str) != str(exclude_id)]
    existing = df[col_name].astype(str).str.strip().str.lower().tolist()
    new_val = str(value).strip().lower()
    if new_val in existing:
        st.error(f"❌ Duplicate: {label} '{value}' exists!"); return True
    return False

# --- 3. GOOGLE SHEETS CONNECTION ---
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

@st.cache_resource
def connect_gsheets():
    try:
        if "gcp_service_account" in st.secrets:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gcp_service_account"], SCOPE)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", SCOPE)
        client = gspread.authorize(creds)
        return client.open("MyFinanceDB") 
    except Exception as e:
        st.error(f"❌ Auth Error: {e}")
        st.stop()

def api_retry(func, *args, **kwargs):
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
            "EMI_Log": ["ID", "EMI_ID", "Date", "Month", "Year", "Amount"], # NEW SHEET FOR EMI HISTORY
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
    schema = {
        "Config": ["Key", "Value"],
        "Cards": ["ID", "Name", "First4", "Last4", "Limit", "GraceDays", "MatchCode"], 
        "Banks": ["ID", "Name", "Type", "AccNo", "MatchCode"],
        "Loans": ["ID", "Source", "Type", "Category", "Collateral", "Principal", "Rate", "EMI", "Tenure", "StartDate", "Outstanding", "Status", "DueDay", "MatchCode"],
        "Active_EMIs": ["ID", "CardID", "Item", "Beneficiary", "TotalVal", "MonthlyEMI", "Start", "Tenure", "Status"],
        "EMI_Log": ["ID", "EMI_ID", "Date", "Month", "Year", "Amount"], # NEW
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
            ws = api_retry(sh.worksheet, name)
            headers = ws.row_values(1)
            new_headers = [c for c in cols if c not in headers]
            for i, h in enumerate(new_headers): ws.update_cell(1, len(headers) + i + 1, h)

# --- 4. GRID EDITOR HELPER (NEW) ---
def render_editable_grid(sh, df, sheet_name, key_prefix, hidden_cols=[]):
    """Renders a dataframe as an editable UI with Delete capability."""
    if df.empty:
        st.info("No records found.")
        return

    # Add a 'Delete' checkbox column locally
    df_display = df.copy()
    df_display["Delete"] = False
    
    # Configure columns
    col_config = {"Delete": st.column_config.CheckboxColumn(required=True)}
    for h in hidden_cols: col_config[h] = None
    
    # Render Editor
    edited_df = st.data_editor(
        df_display,
        key=f"{key_prefix}_editor",
        column_config=col_config,
        hide_index=True,
        use_container_width=True,
        num_rows="fixed" # We don't want adding rows here, only edit/delete
    )

    if st.button(f"Save Changes ({key_prefix})"):
        changes_made = False
        
        # 1. Process Deletions
        to_delete = edited_df[edited_df["Delete"] == True]
        for _, row in to_delete.iterrows():
            delete_row_by_id(sh, sheet_name, row['ID'])
            changes_made = True
            
        # 2. Process Edits (Rows that are NOT marked for delete)
        if not to_delete.empty:
            st.success("Deleted rows. Refreshing...")
            time.sleep(1)
            st.rerun()
        else:
            # Check for edits by dropping Delete col
            final_df = edited_df.drop(columns=["Delete"])
            
            # If dataframe values changed, update full sheet
            if not final_df.equals(df):
                update_full_sheet(sh, sheet_name, final_df)
                st.success("Changes Saved!")
                time.sleep(1)
                st.rerun()
            else:
                st.info("No changes detected.")

# --- 5. MAIN APP ---
def main():
    st.set_page_config(page_title="Finance Hub Pro", layout="wide")
    inject_custom_css()
    sh = connect_gsheets()
    
    if 'init_v14' not in st.session_state:
        init_sheets(sh)
        st.session_state['init_v14'] = True
    
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
                    # Sync Paid with History
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

                st.markdown(f"""
                <div style="background-color:{bg}; padding:10px; border-radius:10px; border:1px solid #ccc; margin-bottom:10px;">
                    <div style="display:flex; justify-content:space-between;">
                        <h3 style="margin:0;">{row['Name']} ({row.get('MatchCode','N/A')})</h3>
                        <span>Bill: <b>₹{curr_b:,.0f}</b></span>
                    </div>
                    <div style="display:flex; justify-content:space-between; margin-top:5px;">
                        <small>Unbilled: ₹{curr_unb:,.0f}</small>
                        <span class="{'text-ok' if rem==0 else 'text-rem'}">To Pay: ₹{rem:,.0f}</span>
                    </div>
                </div>""", unsafe_allow_html=True)

                with st.expander(f"Manage Payments & History - {row['Name']}", expanded=(rem>0)):
                    # 1. Statement Update Form
                    st.caption("Update Bill Details")
                    with st.form(f"st_{row['ID']}"):
                        c1,c2,c3 = st.columns(3)
                        s_dt = c1.date_input("Stmt Date", value=safe_date(curr_stmt_dt))
                        d_dt_obj = safe_date(curr_d)
                        if s_dt and not d_dt_obj: d_dt_obj = s_dt + timedelta(days=int(safe_float(row.get('GraceDays', 20))))
                        d_dt = c2.date_input("Due Date", value=d_dt_obj)
                        b_amt = c3.number_input("Bill Amt", value=curr_b)
                        st.markdown("---")
                        u1, u2 = st.columns(2)
                        u_amt = u1.number_input("Unbilled Amt", value=curr_unb)
                        u_date = u2.date_input("As of", value=safe_date(curr_unb_dt) or datetime.now())
                        if st.form_submit_button("Update Statement"):
                            if not stmts.empty: stmts = stmts[~((stmts['CardID'] == row['ID']) & (stmts['Year'] == year) & (stmts['Month'] == month))]
                            new_row = {"CardID": row['ID'], "Year": year, "Month": month, "StmtDate": str(s_dt) if s_dt else "", "Billed": b_amt, "Unbilled": u_amt, "UnbilledDate": str(u_date), "Paid": curr_p, "DueDate": str(d_dt) if d_dt else ""}
                            stmts = pd.concat([stmts, pd.DataFrame([new_row])], ignore_index=True)
                            update_full_sheet(sh, "Statements", stmts)
                            st.success("Updated"); st.rerun()

                    # 2. Add New Payment
                    st.caption("Record New Payment")
                    with st.form(f"p_{row['ID']}"):
                        pc1, pc2 = st.columns([1, 2])
                        amt = pc1.number_input("Amount", min_value=0.0, value=float(rem))
                        nt = pc2.text_input("Note")
                        if st.form_submit_button("Pay Now"):
                            if amt <= 0: st.error("Amount must be > 0"); st.stop()
                            pid = get_next_id(cpays)
                            add_row(sh, "Card_Payments", [pid, row['ID'], year, month, str(date.today()), amt, nt])
                            st.success("Paid"); time.sleep(1); st.rerun()
                    
                    # 3. EDIT/DELETE PAYMENTS
                    st.divider()
                    st.write("📝 **Edit/Delete Past Payments**")
                    if not hist_df.empty:
                        render_editable_grid(sh, hist_df, "Card_Payments", f"grid_cp_{row['ID']}", hidden_cols=["CardID", "Year", "Month"])
                    else:
                        st.info("No payments recorded for this month.")

        with tab_manage:
            action = st.radio("Action", ["Add New", "Edit Existing", "Delete"], horizontal=True)
            if action == "Add New":
                with st.form("add_c"):
                    n = st.text_input("Name"); m_code = st.text_input("Match Code"); l = st.number_input("Limit"); g = st.number_input("Grace Days")
                    if st.form_submit_button("Create"):
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
                    if st.form_submit_button("Update"):
                        update_row_by_id(sh, "Cards", card_row['ID'], {"Name": n, "MatchCode": m_code, "Limit": l}, cards)
                        st.success("Updated"); st.rerun()
            elif action == "Delete":
                del_n = st.selectbox("Select", cards['Name'].unique())
                if st.button("Delete"):
                    cid = cards[cards['Name'] == del_n].iloc[0]['ID']
                    delete_row_by_id(sh, "Cards", cid)
                    st.success("Deleted"); st.rerun()

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
            for _, row in active.iterrows():
                matches = repay[(repay['LoanID'] == row['ID'])]
                is_paid = False
                month_payment = None
                
                # Filter matches for current month
                for _, r in matches.iterrows():
                    pd_date = safe_date(r['PaymentDate'])
                    if pd_date and pd_date.year == year and pd_date.strftime("%B") == month:
                        is_paid = True
                        month_payment = r
                
                style = "emi-box-paid" if is_paid else "emi-box-due"
                icon = "✅ PAID" if is_paid else "❌ UNPAID"
                
                st.markdown(f"""
                <div class="{style}">
                    <div style="display:flex; justify-content:space-between;">
                        <b>{row['Source']} ({row['Type']})</b> <span>{icon}</span>
                    </div>
                    <div style="font-size:0.9em; margin-top:5px;">
                        EMI: ₹{safe_float(row['EMI']):,.0f} | Bal: ₹{safe_float(row['Outstanding']):,.0f}
                    </div>
                </div>""", unsafe_allow_html=True)
                
                with st.expander(f"Details & Payments - {row['Source']}"):
                    # 1. Pay Button
                    if not is_paid:
                        with st.form(f"lp_{row['ID']}"):
                            p_amt = st.number_input("Amount", value=float(safe_float(row['EMI'])))
                            p_dt = st.date_input("Date")
                            if st.form_submit_button("Record Payment"):
                                rid = get_next_id(repay)
                                add_row(sh, "Loan_Repayments", [rid, int(row['ID']), str(p_dt), p_amt, "EMI"])
                                loans.loc[loans['ID'] == row['ID'], 'Outstanding'] = safe_float(row['Outstanding']) - p_amt
                                update_full_sheet(sh, "Loans", loans)
                                st.success("Paid"); time.sleep(1); st.rerun()
                    
                    # 2. EDIT/DELETE LOAN PAYMENTS
                    st.divider()
                    st.write("📝 **History & Edits (This Month)**")
                    if not matches.empty:
                        curr_matches = matches[matches['PaymentDate'].apply(lambda x: safe_date(x).strftime("%Y-%B") == f"{year}-{month}" if safe_date(x) else False)]
                        if not curr_matches.empty:
                            render_editable_grid(sh, curr_matches, "Loan_Repayments", f"grid_lp_{row['ID']}", hidden_cols=["LoanID"])
                        else:
                            st.info("No payments this month.")
        
        with tab_manage:
            action = st.radio("Action", ["Add New", "Edit Existing", "Delete"], horizontal=True)
            if action == "Add New":
                with st.form("add_l"):
                    src = st.text_input("Source"); typ = st.text_input("Type")
                    m_code = st.text_input("Match Code"); amt = st.number_input("Principal"); emi = st.number_input("EMI")
                    start = st.date_input("Start"); ten = st.number_input("Tenure (M)")
                    if st.form_submit_button("Add"):
                        lid = get_next_id(loans)
                        add_row(sh, "Loans", [lid, src, typ, "Std", "", amt, 0, emi, ten, str(start), amt, "Active", 5, m_code])
                        st.success("Added"); st.rerun()
            elif action == "Edit Existing":
                loans['Label'] = loans['Source'] + " (" + loans['MatchCode'].astype(str) + ")"
                sel_l = st.selectbox("Select", loans['Label'].unique())
                l_row = loans[loans['Label'] == sel_l].iloc[0]
                with st.form("edit_l"):
                    src = st.text_input("Source", value=l_row['Source'])
                    emi = st.number_input("EMI", value=float(safe_float(l_row['EMI'])))
                    if st.form_submit_button("Update"):
                        update_row_by_id(sh, "Loans", l_row['ID'], {"Source": src, "EMI": emi}, loans)
                        st.success("Updated"); st.rerun()
            elif action == "Delete":
                del_l = st.selectbox("Select", loans['Label'].unique())
                if st.button("Delete"):
                    lid = loans[loans['Label'] == del_l].iloc[0]['ID']
                    delete_row_by_id(sh, "Loans", lid); st.success("Deleted"); st.rerun()

    # ==========================
    # CREDIT CARD EMIS (WITH HISTORY)
    # ==========================
    elif choice == "Credit Card EMIs":
        st.title("📉 Credit Card EMIs")
        emis = get_df(sh, "Active_EMIs")
        emi_log = get_df(sh, "EMI_Log") # Load History
        cards = get_df(sh, "Cards")
        
        tab_view, tab_manage = st.tabs(["Active", "Manage"])
        
        with tab_view:
            if emis.empty: st.info("No active EMIs")
            active = emis[emis['Status']=='Active']
            
            for _, row in active.iterrows():
                is_paid = False
                if not emi_log.empty:
                    log_match = emi_log[
                        (emi_log['EMI_ID'] == row['ID']) & 
                        (emi_log['Year'] == year) & 
                        (emi_log['Month'] == month)
                    ]
                    if not log_match.empty: is_paid = True

                style = "emi-box-paid" if is_paid else "emi-box-due"
                icon = "✅ PAID" if is_paid else "❌ UNPAID"
                
                st.markdown(f"""
                <div class="{style}">
                    <div style="display:flex; justify-content:space-between;">
                        <b>{row['Item']} ({row['Beneficiary']})</b> <span>{icon}</span>
                    </div>
                    <div style="font-size:0.9em; margin-top:5px;">
                        EMI: ₹{safe_float(row['MonthlyEMI']):,.0f} | Total: ₹{safe_float(row['TotalVal']):,.0f}
                    </div>
                </div>""", unsafe_allow_html=True)
                
                with st.expander(f"Manage Payment - {row['Item']}"):
                    if not is_paid:
                        if st.button(f"Mark Paid (₹{row['MonthlyEMI']})", key=f"pay_emi_{row['ID']}"):
                            log_id = get_next_id(emi_log)
                            add_row(sh, "EMI_Log", [log_id, int(row['ID']), str(date.today()), month, year, float(row['MonthlyEMI'])])
                            st.success("Marked Paid!"); time.sleep(1); st.rerun()
                    
                    st.write("📝 **History (This Month)**")
                    if not emi_log.empty:
                        curr_logs = emi_log[
                            (emi_log['EMI_ID'] == row['ID']) & 
                            (emi_log['Year'] == year) & 
                            (emi_log['Month'] == month)
                        ]
                        if not curr_logs.empty:
                            render_editable_grid(sh, curr_logs, "EMI_Log", f"grid_elog_{row['ID']}", hidden_cols=["EMI_ID", "Month", "Year"])
                        else:
                            st.info("No payment log found.")

        with tab_manage:
            action = st.radio("Action", ["Add New", "Delete"], horizontal=True)
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
                        st.success("Added"); st.rerun()
            elif action == "Delete":
                del_e = st.selectbox("Select Item", emis['Item'].unique())
                if st.button("Delete"):
                    eid = emis[emis['Item']==del_e].iloc[0]['ID']
                    delete_row_by_id(sh, "Active_EMIs", eid); st.success("Deleted"); st.rerun()

    # ==========================
    # BANK ACCOUNTS & INCOME/EXP
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
                    updates[r['ID']] = st.number_input(f"{r['Name']}", value=curr)
                if st.form_submit_button("Save"):
                    df = get_df(sh, "Bank_Balances")
                    if not df.empty: df = df[~((df['Year']==year)&(df['Month']==month))]
                    new_rows = []
                    for bid, val in updates.items(): new_rows.append({"BankID": bid, "Year": year, "Month": month, "Balance": val})
                    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
                    update_full_sheet(sh, "Bank_Balances", df)
                    st.success("Synced")
        with tab_manage:
            action = st.radio("Action", ["Add New", "Delete"], horizontal=True)
            if action == "Add New":
                with st.form("add_b"):
                    bn = st.text_input("Name"); bt = st.selectbox("Type", ["Savings", "Current"])
                    m_code = st.text_input("Match Code")
                    if st.form_submit_button("Add"):
                        bid = get_next_id(banks)
                        add_row(sh, "Banks", [bid, bn, bt, "", m_code])
                        st.success("Added"); st.rerun()
            elif action == "Delete":
                del_b = st.selectbox("Select", banks['Name'].unique())
                if st.button("Delete"):
                    bid = banks[banks['Name']==del_b].iloc[0]['ID']
                    delete_row_by_id(sh, "Banks", bid); st.success("Deleted"); st.rerun()

    elif choice == "Income/Exp":
        st.title("💸 Income & Expenses")
        tab_view, tab_manage, tab_upload = st.tabs(["History (Editable)", "Add Manual", "Upload"])
        
        with tab_view:
            tx = get_df(sh, "Transactions")
            curr_tx = tx[(tx['Year']==year)&(tx['Month']==month)]
            st.write("📝 **Edit/Delete Transactions**")
            render_editable_grid(sh, curr_tx, "Transactions", "grid_tx", hidden_cols=["Year", "Month"])

        with tab_manage:
            with st.form("new_tx"):
                tt = st.selectbox("Type", ["Expense", "Income"])
                cat = st.text_input("Category"); amt = st.number_input("Amount"); nt = st.text_input("Notes")
                if st.form_submit_button("Add"):
                    tid = get_next_id(get_df(sh, "Transactions"))
                    add_row(sh, "Transactions", [tid, str(date.today()), year, month, tt, cat, amt, nt, "Manual"])
                    st.success("Added"); st.rerun()
        
        with tab_upload:
            st.subheader("Smart Upload")
            cards = get_df(sh, "Cards"); banks = get_df(sh, "Banks")
            match_map = {}
            for _, r in cards.iterrows():
                if str(r.get('MatchCode')).strip(): match_map[str(r['MatchCode']).strip()] = f"Card: {r['Name']}"
            for _, r in banks.iterrows():
                if str(r.get('MatchCode')).strip(): match_map[str(r['MatchCode']).strip()] = f"Bank: {r['Name']}"
            
            uploaded_file = st.file_uploader("Upload Excel/CSV", type=['xlsx', 'csv'])
            if uploaded_file:
                fname = uploaded_file.name
                detected_source = "Unknown"
                for code, name in match_map.items():
                    if code in fname: detected_source = name; st.success(f"Matched: {name}"); break
                final_src = st.text_input("Source", value=detected_source)
                
                if st.button("Process"):
                    try:
                        if uploaded_file.name.endswith('.csv'): df = pd.read_csv(uploaded_file)
                        else: df = pd.read_excel(uploaded_file, engine='openpyxl')
                        df.columns = df.columns.str.lower()
                        d_col = next((c for c in df.columns if 'date' in c), None)
                        a_col = next((c for c in df.columns if 'amount' in c or 'debit' in c), None)
                        if d_col and a_col:
                            entries = []
                            for _, row in df.iterrows():
                                dt_val = safe_date(row[d_col]) or date.today()
                                amt_val = safe_float(row[a_col])
                                if amt_val > 0: entries.append([get_next_id(get_df(sh,"Transactions")), str(dt_val), year, month, "Expense", "Statement", amt_val, "Upload", final_src])
                            if entries:
                                ws = api_retry(sh.worksheet, "Transactions"); ws.append_rows(entries); clear_cache()
                                st.success(f"Added {len(entries)}"); st.rerun()
                    except Exception as e: st.error(str(e))

if __name__ == "__main__":

    main()
