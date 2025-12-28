import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date, timedelta
import time
import re
import pdfplumber

# ==========================================
# 1. VISUAL STYLING & CSS
# ==========================================

def inject_custom_css():
    st.markdown("""
        <style>
        /* Card Containers */
        .card-container {
            padding: 15px; 
            border-radius: 10px; 
            border: 1px solid #e0e0e0; 
            margin-bottom: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        /* Status Colors */
        .paid-bg { background-color: #d4edda; border-left: 5px solid #28a745; color: #155724; }
        .due-bg { background-color: #fff3cd; border-left: 5px solid #ffc107; color: #856404; }
        .overdue-bg { background-color: #f8d7da; border-left: 5px solid #dc3545; color: #721c24; }
        .neutral-bg { background-color: #f8f9fa; border-left: 5px solid #6c757d; }
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] { gap: 8px; }
        .stTabs [data-baseweb="tab"] { height: 45px; background-color: #ffffff; border-radius: 4px; border: 1px solid #ddd; }
        .stTabs [aria-selected="true"] { background-color: #f0f2f6; border-bottom: 2px solid #ff4b4b; font-weight: bold; }
        
        /* Message Boxes */
        .success-box { padding: 15px; background-color: #d1e7dd; color: #0f5132; border-radius: 5px; margin-bottom: 10px; }
        </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. ROBUST DATA UTILITIES
# ==========================================

def safe_float(val):
    """Converts to float with 2-decimal precision, handling currency symbols."""
    if pd.isna(val) or str(val).strip() == "": return 0.0
    if isinstance(val, (int, float)): return round(float(val), 2)
    clean = re.sub(r'[^\d.-]', '', str(val))
    try: return round(float(clean), 2)
    except ValueError: return 0.0

def safe_date(val):
    """Robust date parsing for multiple formats."""
    if not val or pd.isna(val) or str(val).strip() == "": return None
    val = str(val).strip()
    formats = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", "%Y/%m/%d", "%d-%b-%y", "%d-%m-%y", "%d-%b"]
    for fmt in formats:
        try: 
            dt = datetime.strptime(val, fmt)
            if "%Y" not in fmt and "%y" not in fmt: dt = dt.replace(year=datetime.now().year)
            return dt.date()
        except ValueError: continue
    return None

def get_next_id(df):
    """Auto-increments ID."""
    if df.empty or 'ID' not in df.columns: return 1
    ids = pd.to_numeric(df['ID'], errors='coerce').fillna(0)
    return int(ids.max()) + 1 if not ids.empty else 1

def check_duplicate(df, col_name, value, label="Entry", exclude_id=None):
    """Prevents duplicate entries."""
    if df.empty or col_name not in df.columns: return False
    if exclude_id: df = df[df['ID'].astype(str) != str(exclude_id)]
    existing = df[col_name].astype(str).str.strip().str.lower().tolist()
    if str(value).strip().lower() in existing:
        st.error(f"❌ Duplicate Error: {label} '{value}' already exists.")
        return True
    return False

# ==========================================
# 3. GOOGLE SHEETS CONNECTION & I/O
# ==========================================

SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

@st.cache_resource
def connect_gsheets():
    try:
        if "gcp_service_account" in st.secrets:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gcp_service_account"], SCOPE)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", SCOPE)
        return gspread.authorize(creds).open("MyFinanceDB") 
    except Exception as e:
        st.error(f"❌ Connection Failed: {e}")
        st.stop()

def api_retry(func, *args, **kwargs):
    for i in range(5):
        try: return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e): time.sleep((i+1)*1.5); continue
            raise e
    return func(*args, **kwargs)

@st.cache_data(ttl=60) 
def fetch_sheet_data_cached(_sh, sheet_name):
    return api_retry(_sh.worksheet, sheet_name).get_all_records()

def clear_cache(): st.cache_data.clear()

def get_df(sh, name):
    required_cols = {
        "Cards": ["ID", "Name", "Limit", "GraceDays", "MatchCode"],
        "Statements": ["CardID", "Year", "Month", "Billed", "Paid", "Unbilled", "UnbilledDate", "StmtDate", "DueDate"],
        "Card_Payments": ["ID", "CardID", "Year", "Month", "Date", "Amount", "Note"],
        "Loans": ["ID", "Source", "Type", "Category", "Principal", "EMI", "Tenure", "StartDate", "Outstanding", "Status", "DueDay", "MatchCode"],
        "Loan_Repayments": ["ID", "LoanID", "PaymentDate", "Amount", "Type"],
        "Active_EMIs": ["ID", "CardID", "Item", "Beneficiary", "TotalVal", "MonthlyEMI", "Start", "Tenure", "Status"],
        "EMI_Log": ["ID", "EMI_ID", "Date", "Month", "Year", "Amount"],
        "Banks": ["ID", "Name", "Type", "AccNo", "MatchCode"],
        "Bank_Balances": ["BankID", "Year", "Month", "Balance"],
        "Transactions": ["ID", "Date", "Year", "Month", "Type", "Category", "Amount", "Notes", "SourceAccount"]
    }
    try:
        data = fetch_sheet_data_cached(sh, name)
        df = pd.DataFrame(data)
        if name in required_cols:
            if df.empty: return pd.DataFrame(columns=required_cols[name])
            for c in required_cols[name]: 
                if c not in df.columns: df[c] = ""
        return df
    except gspread.WorksheetNotFound: return pd.DataFrame(columns=required_cols.get(name, []))
    except Exception: return pd.DataFrame()

def update_full_sheet(sh, name, df):
    ws = api_retry(sh.worksheet, name)
    ws.clear()
    ws.append_row(df.columns.tolist())
    ws.append_rows(df.values.tolist())
    clear_cache()

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
        row_idx = next((i + 2 for i, row in enumerate(data) if str(row.get('ID')) == str(id_val)), None)
        if row_idx: ws.delete_rows(row_idx); clear_cache(); return True
        return False
    except: return False

def init_sheets(sh):
    schema = {
        "Config": ["Key", "Value"],
        "Cards": ["ID", "Name", "First4", "Last4", "Limit", "GraceDays", "MatchCode"], 
        "Banks": ["ID", "Name", "Type", "AccNo", "MatchCode"],
        "Loans": ["ID", "Source", "Type", "Category", "Collateral", "Principal", "Rate", "EMI", "Tenure", "StartDate", "Outstanding", "Status", "DueDay", "MatchCode"],
        "Active_EMIs": ["ID", "CardID", "Item", "Beneficiary", "TotalVal", "MonthlyEMI", "Start", "Tenure", "Status"],
        "EMI_Log": ["ID", "EMI_ID", "Date", "Month", "Year", "Amount"],
        "Transactions": ["ID", "Date", "Year", "Month", "Type", "Category", "Amount", "Notes", "SourceAccount"],
        "Statements": ["CardID", "Year", "Month", "StmtDate", "Billed", "Unbilled", "UnbilledDate", "Paid", "DueDate"], 
        "Bank_Balances": ["BankID", "Year", "Month", "Balance"],
        "Loan_Repayments": ["ID", "LoanID", "PaymentDate", "Amount", "Type"],
        "Card_Payments": ["ID", "CardID", "Year", "Month", "Date", "Amount", "Note"]
    }
    try: 
        existing = [w.title for w in api_retry(sh.worksheets)]
    except: existing = []

    for name, cols in schema.items():
        if name not in existing:
            ws = api_retry(sh.add_worksheet, title=name, rows=100, cols=20)
            api_retry(ws.append_row, cols); time.sleep(0.5)
        else:
            ws = api_retry(sh.worksheet, name)
            try: headers = api_retry(ws.row_values, 1)
            except: headers = []
            new_headers = [c for c in cols if c not in headers]
            for i, h in enumerate(new_headers):
                api_retry(ws.update_cell, 1, len(headers) + i + 1, h); time.sleep(0.5)

# ==========================================
# 4. COMPONENT: EDITABLE GRID
# ==========================================

def render_editable_grid(sh, df, sheet_name, key_prefix, hidden_cols=[]):
    if df.empty:
        st.info("No records to display.")
        return

    df_display = df.copy()
    df_display["Delete"] = False
    
    col_config = {"Delete": st.column_config.CheckboxColumn(required=True)}
    for h in hidden_cols: col_config[h] = None
    
    edited_df = st.data_editor(
        df_display,
        key=f"{key_prefix}_editor",
        column_config=col_config,
        hide_index=True,
        use_container_width=True,
        num_rows="fixed"
    )

    if st.button(f"💾 Save Changes", key=f"btn_{key_prefix}"):
        to_delete = edited_df[edited_df["Delete"] == True]
        
        # 1. Process Deletions with Warning if Bulk
        if not to_delete.empty:
            for _, row in to_delete.iterrows():
                delete_row_by_id(sh, sheet_name, row['ID'])
            st.toast("🗑️ Rows deleted!", icon="✅")
            
        # 2. Process Edits
        final_df = edited_df.drop(columns=["Delete"])
        original_cmp = df.copy().reset_index(drop=True)
        final_cmp = final_df.reset_index(drop=True)
        
        if not final_cmp.equals(original_cmp):
            update_full_sheet(sh, sheet_name, final_df)
            st.toast("💾 Changes synced!", icon="✅")
            time.sleep(1); st.rerun()
        elif not to_delete.empty:
             time.sleep(1); st.rerun()
        else:
            st.info("No changes detected.")

# ==========================================
# 5. PAGE MODULES
# ==========================================

def render_dashboard(sh, year, month):
    st.title(f"📊 Dashboard - {month} {year}")
    with st.spinner("Crunching numbers..."):
        stmts = get_df(sh, "Statements")
        bk = get_df(sh, "Bank_Balances")
        
        liq = 0.0
        if not bk.empty: 
            curr_bk = bk[(bk['Year'] == year) & (bk['Month'] == month)]
            liq = curr_bk['Balance'].apply(safe_float).sum()
        
        bill = 0; paid = 0; unbilled = 0
        if not stmts.empty:
            curr_stmts = stmts[(stmts['Year'] == year) & (stmts['Month'] == month)].copy()
            if not curr_stmts.empty:
                bill = curr_stmts['Billed'].apply(safe_float).sum()
                paid = curr_stmts['Paid'].apply(safe_float).sum()
                unbilled = curr_stmts['Unbilled'].apply(safe_float).sum()
        
        pending = max(0, bill - paid)
        liability = pending + unbilled

    c1, c2, c3 = st.columns(3)
    c1.metric("💰 Net Liquidity", f"₹{liq:,.0f}")
    c2.metric("🧾 Pending Bills", f"₹{pending:,.0f}", delta_color="inverse")
    c3.metric("📉 Total Liability", f"₹{liability:,.0f}")

def render_credit_cards(sh, year, month):
    st.title("💳 Credit Cards")
    cards = get_df(sh, "Cards")
    tab_view, tab_manage = st.tabs(["Overview", "Manage Cards"])
    
    with tab_view:
        if cards.empty: st.warning("No cards found."); return
        stmts = get_df(sh, "Statements")
        cpays = get_df(sh, "Card_Payments")
        
        for _, row in cards.iterrows():
            hist_df = cpays[(cpays['CardID'] == row['ID']) & (cpays['Year'] == year) & (cpays['Month'] == month)]
            match = stmts[(stmts['CardID'] == row['ID']) & (stmts['Year'] == year) & (stmts['Month'] == month)]
            
            curr_b=0.0; curr_p=0.0; curr_d=""; curr_stmt_dt=""; curr_unb=0.0; curr_unb_dt=""
            if not match.empty:
                r = match.iloc[0]
                curr_b = safe_float(r['Billed'])
                calc_paid = hist_df['Amount'].apply(safe_float).sum()
                curr_p = calc_paid if not hist_df.empty else safe_float(r['Paid'])
                curr_d = str(r['DueDate'])
                curr_stmt_dt = str(r.get('StmtDate', ''))
                curr_unb = safe_float(r.get('Unbilled', 0))
                curr_unb_dt = str(r.get('UnbilledDate', ''))
            
            rem = max(0, curr_b - curr_p)
            status_cls = "neutral-bg"
            if curr_b > 0:
                if rem <= 1: status_cls = "paid-bg"
                elif safe_date(curr_d) and (safe_date(curr_d) - date.today()).days < 0: status_cls = "overdue-bg"
                else: status_cls = "due-bg"
            
            st.markdown(f"""
            <div class="card-container {status_cls}">
                <div style="display:flex; justify-content:space-between;">
                    <h3>{row['Name']}</h3> <span>Due: ₹{rem:,.2f}</span>
                </div>
                <div>Billed: ₹{curr_b:,.2f} | Paid: ₹{curr_p:,.2f} | Unbilled: ₹{curr_unb:,.2f}</div>
            </div>""", unsafe_allow_html=True)

            with st.expander(f"Manage {row['Name']}", expanded=(rem > 0)):
                with st.form(f"st_{row['ID']}"):
                    c1,c2,c3 = st.columns(3)
                    s_dt = c1.date_input("Stmt Date", value=safe_date(curr_stmt_dt))
                    d_dt = c2.date_input("Due Date", value=safe_date(curr_d) or date.today())
                    b_amt = c3.number_input("Bill Amt", value=curr_b)
                    
                    st.markdown("---")
                    u1, u2 = st.columns(2)
                    u_amt = u1.number_input("Unbilled Amt", value=curr_unb)
                    u_date = u2.date_input("Unbilled As Of", value=safe_date(curr_unb_dt) or date.today())
                    
                    if st.form_submit_button("💾 Update Statement"):
                        if not stmts.empty: stmts = stmts[~((stmts['CardID'] == row['ID']) & (stmts['Year'] == year) & (stmts['Month'] == month))]
                        new_row = {"CardID": row['ID'], "Year": year, "Month": month, "StmtDate": str(s_dt), "Billed": b_amt, "Unbilled": u_amt, "UnbilledDate": str(u_date), "Paid": curr_p, "DueDate": str(d_dt)}
                        stmts = pd.concat([stmts, pd.DataFrame([new_row])], ignore_index=True)
                        update_full_sheet(sh, "Statements", stmts)
                        st.toast("Statement updated!", icon="✅"); time.sleep(1); st.rerun()

                with st.form(f"p_{row['ID']}"):
                    c1, c2 = st.columns([1,2])
                    p_amt = c1.number_input("Pay Amount", value=float(rem))
                    nt = c2.text_input("Notes")
                    if st.form_submit_button("💸 Record Payment"):
                        add_row(sh, "Card_Payments", [get_next_id(cpays), row['ID'], year, month, str(date.today()), p_amt, nt])
                        st.toast("Payment recorded!", icon="✅"); st.success(f"Recorded ₹{p_amt}"); time.sleep(1); st.rerun()
                
                render_editable_grid(sh, hist_df, "Card_Payments", f"cpgrid_{row['ID']}", hidden_cols=["CardID", "Year", "Month"])

    with tab_manage:
        if st.radio("Action", ["Add", "Delete"], horizontal=True) == "Add":
            with st.form("add_c"):
                n = st.text_input("Name"); mc = st.text_input("Match Code"); l = st.number_input("Limit")
                if st.form_submit_button("Add Card"):
                    if not check_duplicate(cards, "Name", n):
                        add_row(sh, "Cards", [get_next_id(cards), n, "", "", l, 20, mc])
                        st.toast("Card Added!", icon="🎉"); st.success(f"Added {n}"); time.sleep(1); st.rerun()
        else:
            del_n = st.selectbox("Select Card", cards['Name'].unique() if not cards.empty else [])
            if st.button("Delete"):
                delete_row_by_id(sh, "Cards", cards[cards['Name']==del_n].iloc[0]['ID'])
                st.toast("Deleted!", icon="🗑️"); st.rerun()

def render_loans(sh, year, month):
    st.title("🏠 Loans")
    loans = get_df(sh, "Loans")
    repay = get_df(sh, "Loan_Repayments")
    tab_view, tab_manage = st.tabs(["Active", "Manage"])
    
    with tab_view:
        active = loans[loans['Status'] == 'Active']
        for _, row in active.iterrows():
            matches = repay[repay['LoanID'] == row['ID']]
            is_paid = False
            curr_matches = pd.DataFrame()
            if not matches.empty:
                curr_matches = matches[matches['PaymentDate'].apply(lambda x: safe_date(x).year == year and safe_date(x).strftime("%B") == month if safe_date(x) else False)]
                if not curr_matches.empty: is_paid = True
            
            style = "paid-bg" if is_paid else "overdue-bg"
            st.markdown(f"""<div class="card-container {style}"><b>{row['Source']} ({row['Type']})</b><br>EMI: ₹{safe_float(row['EMI']):,.2f} | Bal: ₹{safe_float(row['Outstanding']):,.2f}</div>""", unsafe_allow_html=True)
            
            with st.expander(f"Repay {row['Source']}"):
                if st.button(f"Pay EMI (₹{row['EMI']})", key=f"emi_{row['ID']}", disabled=is_paid):
                    add_row(sh, "Loan_Repayments", [get_next_id(repay), int(row['ID']), str(date.today()), float(row['EMI']), "EMI"])
                    update_row_by_id(sh, "Loans", row['ID'], {"Outstanding": max(0, safe_float(row['Outstanding']) - safe_float(row['EMI']))}, loans)
                    st.toast("Paid!", icon="✅"); st.rerun()
                render_editable_grid(sh, curr_matches, "Loan_Repayments", f"lp_{row['ID']}", hidden_cols=["LoanID"])

    with tab_manage:
        with st.form("add_l"):
            src = st.text_input("Source"); typ = st.text_input("Type"); amt = st.number_input("Principal"); emi = st.number_input("EMI")
            if st.form_submit_button("Add Loan"):
                add_row(sh, "Loans", [get_next_id(loans), src, typ, "", "", amt, 0, emi, 12, str(date.today()), amt, "Active", 5, ""])
                st.toast("Loan Created!", icon="🎉"); st.rerun()

def render_active_emis(sh, year, month):
    st.title("📉 Active EMIs")
    emis = get_df(sh, "Active_EMIs")
    emi_log = get_df(sh, "EMI_Log")
    cards = get_df(sh, "Cards")
    
    tab_view, tab_manage = st.tabs(["Active", "Manage"])
    with tab_view:
        active = emis[emis['Status']=='Active']
        if active.empty: st.info("No Active EMIs")
        for _, row in active.iterrows():
            is_paid = not emi_log[(emi_log['EMI_ID']==row['ID'])&(emi_log['Year']==year)&(emi_log['Month']==month)].empty
            style = "paid-bg" if is_paid else "due-bg"
            st.markdown(f"""<div class="card-container {style}"><b>{row['Item']}</b>: ₹{safe_float(row['MonthlyEMI']):,.2f}</div>""", unsafe_allow_html=True)
            if not is_paid and st.button(f"Mark Paid", key=f"me_{row['ID']}"):
                add_row(sh, "EMI_Log", [get_next_id(emi_log), int(row['ID']), str(date.today()), month, year, float(row['MonthlyEMI'])])
                st.toast("Paid!", icon="✅"); st.rerun()

    with tab_manage:
        if st.radio("Mode", ["Add", "Delete"], horizontal=True) == "Add":
            if cards.empty: st.warning("Add a Credit Card first.")
            else:
                with st.form("add_e"):
                    cn = st.selectbox("Card", cards['Name'].unique())
                    it = st.text_input("Item"); val = st.number_input("Total"); mon = st.number_input("Monthly")
                    if st.form_submit_button("Add Plan"):
                        cid = cards[cards['Name']==cn].iloc[0]['ID']
                        add_row(sh, "Active_EMIs", [get_next_id(emis), int(cid), it, "Self", val, mon, str(date.today()), 12, "Active"])
                        st.toast("EMI Added!", icon="🎉"); st.rerun()
        else:
            del_e = st.selectbox("Select", emis['Item'].unique() if not emis.empty else [])
            if st.button("Delete"):
                delete_row_by_id(sh, "Active_EMIs", emis[emis['Item']==del_e].iloc[0]['ID']); st.toast("Deleted!"); st.rerun()

def render_bank_accounts(sh, year, month):
    st.title("🏦 Bank Accounts")
    banks = get_df(sh, "Banks")
    tab_view, tab_manage = st.tabs(["Balances", "Manage"])
    
    with tab_view:
        with st.form("bal_up"):
            updates = {}
            for _, r in banks.iterrows():
                bal_df = get_df(sh, "Bank_Balances")
                curr = 0.0
                if not bal_df.empty:
                    match = bal_df[(bal_df['BankID']==r['ID'])&(bal_df['Year']==year)&(bal_df['Month']==month)]
                    if not match.empty: curr = safe_float(match.iloc[0]['Balance'])
                updates[r['ID']] = st.number_input(f"{r['Name']}", value=curr)
            if st.form_submit_button("💾 Save Balances"):
                df = get_df(sh, "Bank_Balances")
                if not df.empty: df = df[~((df['Year']==year)&(df['Month']==month))]
                new_rows = [{"BankID": bid, "Year": year, "Month": month, "Balance": val} for bid, val in updates.items()]
                df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
                update_full_sheet(sh, "Bank_Balances", df)
                st.toast("Synced!", icon="✅"); st.success("Balances updated.")

    with tab_manage:
        with st.form("add_b"):
            bn = st.text_input("Name"); mc = st.text_input("Match Code")
            if st.form_submit_button("Add Bank"):
                add_row(sh, "Banks", [get_next_id(banks), bn, "Savings", "", mc])
                st.toast("Bank Added!", icon="🎉"); st.rerun()

def render_transactions(sh, year, month):
    st.title("💸 Income & Expenses")
    tab_hist, tab_man, tab_up = st.tabs(["History", "Manual Add", "Smart Upload"])
    
    with tab_hist:
        tx = get_df(sh, "Transactions")
        if not tx.empty:
            curr_tx = tx[(tx['Year'] == year) & (tx['Month'] == month)]
            
            # Editable Grid with Delete Checkbox
            edited_df = st.data_editor(
                curr_tx, key="tx_grid",
                column_config={"Delete": st.column_config.CheckboxColumn(required=True), "Amount": st.column_config.NumberColumn(format="₹%.2f")},
                hide_index=True, use_container_width=True, disabled=["ID"]
            )
            
            if st.button("💾 Apply Grid Changes"):
                to_delete = edited_df[edited_df["Delete"] == True]
                
                # Deletion Confirmation Logic
                if not to_delete.empty:
                    st.warning(f"⚠️ You are about to delete {len(to_delete)} transactions.")
                    if st.button("🔴 Confirm Permanent Deletion"):
                        for _, row in to_delete.iterrows(): delete_row_by_id(sh, "Transactions", row['ID'])
                        st.toast("🗑️ Deleted successfully!", icon="✅")
                        time.sleep(1); st.rerun()
                else:
                    # Update Non-Deleted Rows
                    update_full_sheet(sh, "Transactions", edited_df.drop(columns=["Delete"]))
                    st.toast("💾 Updates Saved!", icon="✅"); time.sleep(1); st.rerun()
        else: st.info("No transactions.")

    with tab_man:
        with st.form("new_tx"):
            c1, c2 = st.columns(2)
            tt = c1.selectbox("Type", ["Expense", "Income"])
            cat = c2.text_input("Category", placeholder="Food, Salary...")
            c3, c4 = st.columns(2)
            amt = c3.number_input("Amount", min_value=0.0)
            dt = c4.date_input("Date", value=date.today())
            nt = st.text_input("Notes")
            
            if st.form_submit_button("➕ Add Transaction"):
                add_row(sh, "Transactions", [get_next_id(get_df(sh, "Transactions")), str(dt), dt.year, dt.strftime("%B"), tt, cat, amt, nt, "Manual"])
                st.toast(f"✅ Saved ₹{amt} for {cat}")
                st.success(f"**Added Successfully:** {tt} - {cat} - ₹{amt}")
                time.sleep(1.5); st.rerun()

    with tab_up:
        st.subheader("Smart Import")
        uploaded_file = st.file_uploader("Upload PDF/XLSX/CSV", type=['pdf', 'xlsx', 'xls', 'csv'])
        
        cards = get_df(sh, "Cards"); banks = get_df(sh, "Banks")
        match_map = {str(r.get('MatchCode')).strip(): f"Card: {r['Name']}" for _, r in cards.iterrows() if str(r.get('MatchCode')).strip()}
        match_map.update({str(r.get('MatchCode')).strip(): f"Bank: {r['Name']}" for _, r in banks.iterrows() if str(r.get('MatchCode')).strip()})
        
        if uploaded_file:
            final_src = next((name for code, name in match_map.items() if code.lower() in uploaded_file.name.lower()), "Unknown")
            final_src = st.text_input("Source", value=final_src)
            
            if st.button("Process File"):
                try:
                    entries = []; df = None
                    ext = uploaded_file.name.split('.')[-1].lower()
                    
                    if ext == 'pdf':
                        with pdfplumber.open(uploaded_file) as pdf:
                            text = "\n".join([p.extract_text() for p in pdf.pages if p.extract_text()])
                            matches = re.findall(r"(\d{2}[-/]\d{2}[-/]\d{2,4}).*?([\d,]+\.?\d{0,2})", text)
                            for d_str, a_str in matches:
                                dv = safe_date(d_str); av = safe_float(a_str)
                                if dv and av > 0: entries.append([get_next_id(get_df(sh,"Transactions")), str(dv), year, month, "Expense", "PDF Import", av, "Imported", final_src])
                    else:
                        if ext == 'csv': df = pd.read_csv(uploaded_file)
                        elif ext == 'xlsx': df = pd.read_excel(uploaded_file, engine='openpyxl')
                        elif ext == 'xls': df = pd.read_excel(uploaded_file, engine='xlrd')
                        
                        if df is not None:
                            df.columns = df.columns.astype(str).str.lower()
                            d_col = next((c for c in df.columns if any(x in c for x in ['date', 'txn'])), None)
                            a_col = next((c for c in df.columns if any(x in c for x in ['amount', 'debit', 'with'])), None)
                            n_col = next((c for c in df.columns if any(x in c for x in ['desc', 'narration'])), None)
                            
                            if d_col and a_col:
                                for _, r in df.iterrows():
                                    dv = safe_date(r[d_col]); av = safe_float(r[a_col])
                                    if dv and av > 0: entries.append([get_next_id(get_df(sh,"Transactions")), str(dv), year, month, "Expense", "Statement", av, str(r[n_col]) if n_col else "Import", final_src])
                    
                    if entries:
                        api_retry(sh.worksheet("Transactions").append_rows, entries)
                        clear_cache()
                        st.toast(f"Imported {len(entries)} rows!", icon="🚀")
                        st.success(f"**Success:** Imported {len(entries)} transactions from {uploaded_file.name}")
                        time.sleep(2); st.rerun()
                    else: st.error("No valid transactions found.")
                except Exception as e: st.error(f"Error: {e}")

# ==========================================
# 6. MAIN APP LOOP
# ==========================================

def main():
    st.set_page_config(page_title="Finance Hub", layout="wide", page_icon="📈")
    inject_custom_css()
    
    with st.status("🚀 System Check...", expanded=True) as status:
        sh = connect_gsheets()
        if 'init_db' not in st.session_state: init_sheets(sh); st.session_state['init_db']=True
        status.update(label="System Online", state="complete", expanded=False)

    st.sidebar.title("☁️ Finance Hub")
    curr_y = datetime.now().year
    year = st.sidebar.selectbox("Year", range(curr_y-1, curr_y+5), index=1)
    month = st.sidebar.selectbox("Month", ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], index=datetime.now().month-1)
    
    choice = st.sidebar.radio("Go To", ["Dashboard", "Credit Cards", "Loans", "Active EMIs", "Bank Accounts", "Income/Expenses"])
    
    if st.sidebar.button("🔄 Refresh Data"): clear_cache(); st.rerun()

    if choice == "Dashboard": render_dashboard(sh, year, month)
    elif choice == "Credit Cards": render_credit_cards(sh, year, month)
    elif choice == "Loans": render_loans(sh, year, month)
    elif choice == "Active EMIs": render_active_emis(sh, year, month)
    elif choice == "Bank Accounts": render_bank_accounts(sh, year, month)
    elif choice == "Income/Expenses": render_transactions(sh, year, month)

if __name__ == "__main__":
    main()
