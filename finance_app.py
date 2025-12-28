import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date, timedelta
import time
import re
import pdfplumber

# ==========================================
# 1. CONFIGURATION & STYLING
# ==========================================

def inject_custom_css():
    st.markdown("""
        <style>
        /* Modern Card Styling */
        .card-container {
            padding: 15px; 
            border-radius: 10px; 
            border: 1px solid #e0e0e0; 
            margin-bottom: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        .paid-bg { background-color: #d4edda; border-left: 5px solid #28a745; color: #155724; }
        .due-bg { background-color: #fff3cd; border-left: 5px solid #ffc107; color: #856404; }
        .overdue-bg { background-color: #f8d7da; border-left: 5px solid #dc3545; color: #721c24; }
        .neutral-bg { background-color: #f8f9fa; border-left: 5px solid #6c757d; }
        
        /* Tab Styling */
        .stTabs [data-baseweb="tab-list"] { gap: 8px; }
        .stTabs [data-baseweb="tab"] { height: 45px; background-color: #ffffff; border-radius: 4px; border: 1px solid #ddd; }
        .stTabs [aria-selected="true"] { background-color: #f0f2f6; border-bottom: 2px solid #ff4b4b; font-weight: bold; }
        </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. DATA UTILITIES
# ==========================================

def safe_float(val):
    """Robust conversion to float with 2-decimal rounding for currency."""
    if pd.isna(val) or str(val).strip() == "": return 0.0
    if isinstance(val, (int, float)): return round(float(val), 2)
    
    # Remove everything except digits, dots, and minus signs
    clean = re.sub(r'[^\d.-]', '', str(val))
    try:
        return round(float(clean), 2)
    except ValueError:
        return 0.0

def safe_date(val):
    """Robust date parsing for various formats."""
    if not val or pd.isna(val) or str(val).strip() == "": return None
    val = str(val).strip()
    # Common formats
    formats = [
        "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", 
        "%Y/%m/%d", "%d-%b-%y", "%d-%m-%y", "%d-%b"
    ]
    for fmt in formats:
        try: 
            dt = datetime.strptime(val, fmt)
            # If year is missing (e.g. 12-Oct), default to current year
            if "%Y" not in fmt and "%y" not in fmt: 
                dt = dt.replace(year=datetime.now().year)
            return dt.date()
        except ValueError: 
            continue
    return None

def get_next_id(df):
    """Generates the next incremental Integer ID."""
    if df.empty or 'ID' not in df.columns: return 1
    # Force conversion to numeric to avoid string sorting issues (e.g., "10" < "2")
    ids = pd.to_numeric(df['ID'], errors='coerce').fillna(0)
    return int(ids.max()) + 1 if not ids.empty else 1

def check_duplicate(df, col_name, value, label="Entry", exclude_id=None):
    """Checks if a value already exists in a column (case-insensitive)."""
    if df.empty or col_name not in df.columns: return False
    if exclude_id: 
        df = df[df['ID'].astype(str) != str(exclude_id)]
    
    existing = df[col_name].astype(str).str.strip().str.lower().tolist()
    if str(value).strip().lower() in existing:
        st.error(f"❌ Duplicate Prevention: {label} '{value}' already exists.")
        return True
    return False

# ==========================================
# 3. GOOGLE SHEETS ENGINE
# ==========================================

SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

@st.cache_resource
def connect_gsheets():
    try:
        if "gcp_service_account" in st.secrets:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gcp_service_account"], SCOPE)
        else:
            # Fallback for local testing
            creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", SCOPE)
        return gspread.authorize(creds).open("MyFinanceDB") 
    except Exception as e:
        st.error(f"❌ Critical Auth Error: {e}")
        st.stop()

def api_retry(func, *args, **kwargs):
    """Decorator to handle Google API rate limits."""
    for i in range(5):
        try: return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e): # Rate limit error
                time.sleep((i+1)*1.5)
                continue
            raise e
    return func(*args, **kwargs)

@st.cache_data(ttl=60) 
def fetch_sheet_data_cached(_sh, sheet_name):
    return api_retry(_sh.worksheet, sheet_name).get_all_records()

def clear_cache():
    st.cache_data.clear()

def get_df(sh, name):
    """Fetches data and ensures all required columns exist."""
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
        
        # Enforce schema if empty or missing columns
        if name in required_cols:
            if df.empty:
                return pd.DataFrame(columns=required_cols[name])
            for c in required_cols[name]: 
                if c not in df.columns: df[c] = ""
        return df
    except gspread.WorksheetNotFound:
        # Graceful handling if sheet doesn't exist yet
        return pd.DataFrame(columns=required_cols.get(name, []))
    except Exception as e:
        st.error(f"Error fetching {name}: {e}")
        return pd.DataFrame()

def update_full_sheet(sh, name, df):
    """Overwrites a sheet with the dataframe content."""
    try:
        ws = api_retry(sh.worksheet, name)
        ws.clear()
        ws.append_row(df.columns.tolist())
        ws.append_rows(df.values.tolist())
        clear_cache()
    except Exception as e:
        st.error(f"Failed to update {name}: {e}")

def add_row(sh, name, row):
    ws = api_retry(sh.worksheet, name)
    ws.append_row(row)
    clear_cache()

def update_row_by_id(sh, name, id_val, updated_dict, df_current):
    idx_list = df_current.index[df_current['ID'].astype(str) == str(id_val)].tolist()
    if not idx_list: return False
    
    idx = idx_list[0]
    for col, val in updated_dict.items():
        df_current.at[idx, col] = val
    
    update_full_sheet(sh, name, df_current)
    return True

def delete_row_by_id(sh, sheet_name, id_val):
    try:
        ws = api_retry(sh.worksheet, sheet_name)
        data = ws.get_all_records()
        row_idx = None
        # Find row index (data starts at row 2 in sheets because of headers)
        for i, row in enumerate(data):
            if str(row.get('ID')) == str(id_val):
                row_idx = i + 2 
                break
        
        if row_idx:
            ws.delete_rows(row_idx)
            clear_cache()
            return True
        return False
    except Exception as e:
        st.error(f"Delete failed: {e}")
        return False

def init_sheets(sh):
    """Ensures all sheets and headers exist on startup."""
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
        ws_list = api_retry(sh.worksheets)
        existing = [w.title for w in ws_list]
    except: existing = []

    for name, cols in schema.items():
        if name not in existing:
            ws = api_retry(sh.add_worksheet, title=name, rows=100, cols=20)
            api_retry(ws.append_row, cols)
            time.sleep(0.5) # Prevent rate limiting during creation
        else:
            # Sync headers if columns added in code
            ws = api_retry(sh.worksheet, name)
            try: headers = api_retry(ws.row_values, 1)
            except: headers = []
            
            new_headers = [c for c in cols if c not in headers]
            if new_headers:
                for i, h in enumerate(new_headers):
                    api_retry(ws.update_cell, 1, len(headers) + i + 1, h)
                    time.sleep(0.5)

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
        
        # Process Deletions
        for _, row in to_delete.iterrows():
            delete_row_by_id(sh, sheet_name, row['ID'])
            
        # Process Edits (if no deletion happened or alongside it)
        final_df = edited_df.drop(columns=["Delete"])
        
        # Check if data actually changed to avoid unnecessary API calls
        # Note: We need to drop 'Delete' from original df to compare
        original_cmp = df.copy().reset_index(drop=True)
        final_cmp = final_df.reset_index(drop=True)
        
        # Simple equality check might fail on types, so we check broadly
        if not final_cmp.equals(original_cmp):
            update_full_sheet(sh, sheet_name, final_df)
            st.success("✅ Changes synced successfully!")
            time.sleep(1)
            st.rerun()
        elif not to_delete.empty:
             st.success("✅ Rows deleted.")
             time.sleep(1)
             st.rerun()
        else:
            st.info("No changes detected.")

# ==========================================
# 5. MODULES (PAGES)
# ==========================================

def render_dashboard(sh, year, month):
    st.title(f"📊 Financial Dashboard - {month} {year}")
    
    col1, col2, col3 = st.columns(3)
    
    with st.spinner("Crunching numbers..."):
        stmts = get_df(sh, "Statements")
        bk = get_df(sh, "Bank_Balances")
        
        # 1. Liquidity
        liq = 0.0
        if not bk.empty: 
            curr_bk = bk[(bk['Year'] == year) & (bk['Month'] == month)]
            liq = curr_bk['Balance'].apply(safe_float).sum()
        
        # 2. Liabilities
        bill = 0; paid = 0; unbilled = 0
        if not stmts.empty:
            curr_stmts = stmts[(stmts['Year'] == year) & (stmts['Month'] == month)].copy()
            if not curr_stmts.empty:
                bill = curr_stmts['Billed'].apply(safe_float).sum()
                paid = curr_stmts['Paid'].apply(safe_float).sum()
                unbilled = curr_stmts['Unbilled'].apply(safe_float).sum()
        
        pending_bills = max(0, bill - paid)
        total_liability = pending_bills + unbilled

    col1.metric("💰 Net Liquidity", f"₹{liq:,.0f}")
    col2.metric("🧾 Pending Bills", f"₹{pending_bills:,.0f}", delta_color="inverse")
    col3.metric("📉 Total Liability", f"₹{total_liability:,.0f}", help="Pending Bills + Unbilled usage")
    
    st.divider()
    st.caption("Detailed analytics can be added here (Charts, Trends, etc.)")

def render_credit_cards(sh, year, month):
    st.title("💳 Credit Card Management")
    cards = get_df(sh, "Cards")
    tab_view, tab_manage = st.tabs(["Overview & Payments", "Manage Cards"])
    
    # --- TAB 1: OVERVIEW ---
    with tab_view:
        if cards.empty: 
            st.warning("No cards found. Please add one in the 'Manage Cards' tab.")
            return

        stmts = get_df(sh, "Statements")
        cpays = get_df(sh, "Card_Payments")
        
        for _, row in cards.iterrows():
            # SCOPE FIX: Define hist_df here to ensure it exists for both calculation and grid
            hist_df = cpays[(cpays['CardID'] == row['ID']) & (cpays['Year'] == year) & (cpays['Month'] == month)]
            
            # Fetch Statement Data
            match = stmts[(stmts['CardID'] == row['ID']) & (stmts['Year'] == year) & (stmts['Month'] == month)]
            
            curr_b=0.0; curr_p=0.0; curr_d=""; curr_stmt_dt=""; curr_unb=0.0; curr_unb_dt=""
            
            if not match.empty:
                r = match.iloc[0]
                curr_b = safe_float(r['Billed'])
                # Calculate total paid from actual transaction history vs static field
                calc_paid = hist_df['Amount'].apply(safe_float).sum()
                curr_p = calc_paid if not hist_df.empty else safe_float(r['Paid'])
                curr_d = str(r['DueDate'])
                curr_stmt_dt = str(r.get('StmtDate', ''))
                curr_unb = safe_float(r.get('Unbilled', 0))
                curr_unb_dt = str(r.get('UnbilledDate', ''))
            
            rem = max(0, curr_b - curr_p)
            
            # Determine Status Color
            status_class = "neutral-bg"
            status_text = "No Bill"
            if curr_b > 0:
                if rem <= 1: 
                    status_class = "paid-bg"; status_text = "Fully Paid"
                elif safe_date(curr_d):
                    days = (safe_date(curr_d) - date.today()).days
                    if days < 0: status_class = "overdue-bg"; status_text = f"Overdue by {abs(days)} days"
                    elif days <= 5: status_class = "due-bg"; status_text = f"Due in {days} days"
                    else: status_class = "neutral-bg"; status_text = f"Due: {curr_d}"
            
            # Card UI
            st.markdown(f"""
            <div class="card-container {status_class}">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <h3 style="margin:0;">{row['Name']} <span style="font-size:0.6em; color:#666;">({row.get('MatchCode','N/A')})</span></h3>
                    <div style="text-align:right;">
                        <span style="font-size:1.2em; font-weight:bold;">Due: ₹{rem:,.2f}</span><br>
                        <small>{status_text}</small>
                    </div>
                </div>
                <div style="margin-top:10px; font-size:0.9em; display:flex; gap:15px;">
                    <span>📜 Billed: <b>₹{curr_b:,.2f}</b></span>
                    <span>✅ Paid: <b>₹{curr_p:,.2f}</b></span>
                    <span>⏳ Unbilled: <b>₹{curr_unb:,.2f}</b></span>
                </div>
            </div>""", unsafe_allow_html=True)

            with st.expander(f"Update Bill or Pay - {row['Name']}", expanded=(rem > 0)):
                # 1. Update Statement Form
                st.markdown("##### 1. Update Statement Details")
                with st.form(f"st_{row['ID']}"):
                    c1,c2,c3 = st.columns(3)
                    s_dt = c1.date_input("Stmt Date", value=safe_date(curr_stmt_dt))
                    
                    # Auto-calculate Due Date based on Grace Days
                    default_due = s_dt + timedelta(days=int(safe_float(row.get('GraceDays', 20)))) if s_dt else date.today()
                    d_dt = c2.date_input("Due Date", value=safe_date(curr_d) or default_due)
                    
                    b_amt = c3.number_input("Total Billed Amount", value=curr_b)
                    
                    st.markdown("---")
                    u1, u2 = st.columns(2)
                    u_amt = u1.number_input("Current Unbilled Amount", value=curr_unb)
                    u_date = u2.date_input("Unbilled As Of", value=safe_date(curr_unb_dt) or date.today())
                    
                    if st.form_submit_button("💾 Save Statement Info"):
                        # Remove old entry for this month/year/card to avoid duplicates
                        if not stmts.empty: 
                            stmts = stmts[~((stmts['CardID'] == row['ID']) & (stmts['Year'] == year) & (stmts['Month'] == month))]
                        
                        new_row = {
                            "CardID": row['ID'], "Year": year, "Month": month, 
                            "StmtDate": str(s_dt), "Billed": b_amt, 
                            "Unbilled": u_amt, "UnbilledDate": str(u_date), 
                            "Paid": curr_p, "DueDate": str(d_dt)
                        }
                        stmts = pd.concat([stmts, pd.DataFrame([new_row])], ignore_index=True)
                        update_full_sheet(sh, "Statements", stmts)
                        st.success("Statement Updated"); time.sleep(0.5); st.rerun()

                # 2. Make Payment Form
                st.markdown("##### 2. Record Payment")
                with st.form(f"p_{row['ID']}"):
                    pc1, pc2 = st.columns([1, 2])
                    amt = pc1.number_input("Amount", min_value=0.0, value=float(rem))
                    nt = pc2.text_input("Note (e.g. UPI Ref)")
                    if st.form_submit_button("💸 Pay Now"):
                        if amt <= 0: st.error("Amount must be > 0"); st.stop()
                        pid = get_next_id(cpays)
                        add_row(sh, "Card_Payments", [pid, row['ID'], year, month, str(date.today()), amt, nt])
                        st.success("Payment Recorded"); time.sleep(1); st.rerun()
                
                st.divider()
                st.write("**Payment History (This Month)**")
                render_editable_grid(sh, hist_df, "Card_Payments", f"grid_cp_{row['ID']}", hidden_cols=["CardID", "Year", "Month"])

    # --- TAB 2: MANAGE CARDS ---
    with tab_manage:
        action = st.radio("Choose Action", ["Add New Card", "Edit Card", "Delete Card"], horizontal=True)
        
        if action == "Add New Card":
            with st.form("add_c"):
                n = st.text_input("Card Name (e.g. HDFC Regalia)")
                m_code = st.text_input("Match Code (Text in SMS, e.g. HDFC Bank)")
                c1, c2 = st.columns(2)
                l = c1.number_input("Credit Limit", step=1000)
                g = c2.number_input("Grace Days", value=20)
                
                if st.form_submit_button("Add Card"):
                    if not n: st.error("Name is required"); st.stop()
                    if check_duplicate(cards, "Name", n, "Card Name"): st.stop()
                    
                    cid = get_next_id(cards)
                    add_row(sh, "Cards", [cid, n, "", "", l, g, m_code])
                    st.success(f"Card '{n}' Added!"); time.sleep(1); st.rerun()

        elif action == "Edit Card" and not cards.empty:
            sel_c = st.selectbox("Select Card to Edit", cards['Name'].unique())
            card_row = cards[cards['Name'] == sel_c].iloc[0]
            with st.form("edit_c"):
                n = st.text_input("Name", value=card_row['Name'])
                m_code = st.text_input("Match Code", value=str(card_row.get('MatchCode','')))
                l = st.number_input("Limit", value=float(safe_float(card_row['Limit'])))
                
                if st.form_submit_button("Update Card"):
                    update_row_by_id(sh, "Cards", card_row['ID'], {"Name": n, "MatchCode": m_code, "Limit": l}, cards)
                    st.success("Updated"); st.rerun()

        elif action == "Delete Card":
            del_n = st.selectbox("Select Card to Delete", cards['Name'].unique())
            st.error("⚠️ Warning: Deleting a card will NOT delete its history logs (payments/statements).")
            if st.button("Confirm Delete"):
                cid = cards[cards['Name'] == del_n].iloc[0]['ID']
                delete_row_by_id(sh, "Cards", cid); st.success("Deleted"); st.rerun()

def render_loans(sh, year, month):
    st.title("🏠 Loan Portfolio")
    loans = get_df(sh, "Loans")
    repay = get_df(sh, "Loan_Repayments")
    tab_view, tab_manage = st.tabs(["Overview", "Manage Loans"])
    
    with tab_view:
        active = loans[loans['Status'] == 'Active']
        if active.empty: st.info("No active loans.")
        
        for _, row in active.iterrows():
            # Check if paid this month
            matches = repay[(repay['LoanID'] == row['ID'])]
            is_paid = False
            
            # Filter matches for current month safely
            curr_matches = pd.DataFrame()
            if not matches.empty:
                curr_matches = matches[matches['PaymentDate'].apply(
                    lambda x: safe_date(x).year == year and safe_date(x).strftime("%B") == month if safe_date(x) else False
                )]
                if not curr_matches.empty: is_paid = True
            
            style = "paid-bg" if is_paid else "overdue-bg"
            icon = "✅ PAID" if is_paid else "⏳ DUE"
            
            st.markdown(f"""
            <div class="card-container {style}">
                <div style="display:flex; justify-content:space-between;">
                    <b>{row['Source']} <small>({row['Type']})</small></b> <span>{icon}</span>
                </div>
                <div style="margin-top:5px; font-size:0.9em;">
                    EMI: <b>₹{safe_float(row['EMI']):,.2f}</b> | Outstanding: ₹{safe_float(row['Outstanding']):,.2f}
                </div>
            </div>""", unsafe_allow_html=True)
            
            with st.expander(f"Repayment Options - {row['Source']}"):
                c1, c2 = st.columns(2)
                with c1:
                    st.caption("Quick Pay (EMI)")
                    if st.button(f"Pay ₹{row['EMI']}", key=f"qk_l_{row['ID']}", disabled=is_paid):
                        rid = get_next_id(repay)
                        add_row(sh, "Loan_Repayments", [rid, int(row['ID']), str(date.today()), float(row['EMI']), "EMI"])
                        
                        # Update outstanding
                        new_bal = max(0, safe_float(row['Outstanding']) - safe_float(row['EMI']))
                        update_row_by_id(sh, "Loans", row['ID'], {"Outstanding": new_bal}, loans)
                        st.success("EMI Paid!"); time.sleep(1); st.rerun()
                
                with c2:
                    st.caption("Custom Payment / Prepayment")
                    with st.form(f"cp_l_{row['ID']}"):
                        amt = st.number_input("Amount", value=0.0)
                        typ = st.selectbox("Type", ["EMI", "Prepayment"])
                        if st.form_submit_button("Record"):
                            rid = get_next_id(repay)
                            add_row(sh, "Loan_Repayments", [rid, int(row['ID']), str(date.today()), amt, typ])
                            new_bal = max(0, safe_float(row['Outstanding']) - amt)
                            update_row_by_id(sh, "Loans", row['ID'], {"Outstanding": new_bal}, loans)
                            st.success("Recorded!"); st.rerun()
                
                st.markdown("---")
                st.write("**Transaction History (This Month)**")
                render_editable_grid(sh, curr_matches, "Loan_Repayments", f"grid_lp_{row['ID']}", hidden_cols=["LoanID"])

    with tab_manage:
        with st.form("add_loan"):
            st.write("Add New Loan")
            c1, c2 = st.columns(2)
            src = c1.text_input("Source (Bank Name)")
            typ = c2.text_input("Type (Home, Car, Personal)")
            amt = c1.number_input("Principal Amount", min_value=0.0)
            emi = c2.number_input("Monthly EMI", min_value=0.0)
            start = c1.date_input("Start Date")
            ten = c2.number_input("Tenure (Months)", min_value=1)
            
            if st.form_submit_button("Create Loan"):
                lid = get_next_id(loans)
                add_row(sh, "Loans", [lid, src, typ, "Standard", "", amt, 0, emi, ten, str(start), amt, "Active", 5, ""])
                st.success("Loan Added"); st.rerun()

def render_active_emis(sh, year, month):
    st.title("📉 Active EMI Plans (Credit Cards)")
    emis = get_df(sh, "Active_EMIs")
    emi_log = get_df(sh, "EMI_Log")
    
    if emis.empty: st.info("No Active Cost EMIs (e.g. Amazon No Cost EMI)."); return
    
    active = emis[emis['Status']=='Active']
    
    for _, row in active.iterrows():
        # Check status in log
        is_paid = False
        if not emi_log.empty:
            log_match = emi_log[(emi_log['EMI_ID'] == row['ID']) & (emi_log['Year'] == year) & (emi_log['Month'] == month)]
            if not log_match.empty: is_paid = True

        style = "paid-bg" if is_paid else "due-bg"
        
        st.markdown(f"""
        <div class="card-container {style}">
             <div style="display:flex; justify-content:space-between;">
                <b>{row['Item']}</b> <span>₹{safe_float(row['MonthlyEMI']):,.2f}/mo</span>
            </div>
            <small>Beneficiary: {row['Beneficiary']} | Total: {row['TotalVal']}</small>
        </div>""", unsafe_allow_html=True)
        
        if not is_paid:
            if st.button(f"Mark Paid", key=f"pay_emi_{row['ID']}"):
                log_id = get_next_id(emi_log)
                add_row(sh, "EMI_Log", [log_id, int(row['ID']), str(date.today()), month, year, float(row['MonthlyEMI'])])
                st.success("Updated!"); st.rerun()

def render_bank_accounts(sh, year, month):
    st.title("🏦 Bank Balance Tracker")
    banks = get_df(sh, "Banks")
    
    tab_view, tab_manage = st.tabs(["Update Balances", "Add Bank"])
    
    with tab_view:
        with st.form("bal_f"):
            st.write(f"Closing Balance for **{month} {year}**")
            updates = {}
            for _, r in banks.iterrows():
                bal_df = get_df(sh, "Bank_Balances")
                curr = 0.0
                if not bal_df.empty:
                    match = bal_df[(bal_df['BankID']==r['ID'])&(bal_df['Year']==year)&(bal_df['Month']==month)]
                    if not match.empty: curr = safe_float(match.iloc[0]['Balance'])
                
                updates[r['ID']] = st.number_input(f"{r['Name']} ({r['Type']})", value=curr)
            
            if st.form_submit_button("💾 Save All Balances"):
                df = get_df(sh, "Bank_Balances")
                # Remove existing entries for this month to overwrite
                if not df.empty: df = df[~((df['Year']==year)&(df['Month']==month))]
                
                new_rows = []
                for bid, val in updates.items(): 
                    new_rows.append({"BankID": bid, "Year": year, "Month": month, "Balance": val})
                
                df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
                update_full_sheet(sh, "Bank_Balances", df)
                st.success("Balances Synced Successfully")

    with tab_manage:
        with st.form("add_b"):
            bn = st.text_input("Bank Name")
            bt = st.selectbox("Type", ["Savings", "Current", "Salary"])
            mc = st.text_input("Match Code (for upload mapping)")
            
            if st.form_submit_button("Add Bank"):
                if check_duplicate(banks, "Name", bn): st.stop()
                bid = get_next_id(banks)
                add_row(sh, "Banks", [bid, bn, bt, "", mc])
                st.success("Bank Added"); st.rerun()

def render_transactions(sh, year, month):
    st.title("💸 Income & Expenses")
    tab_hist, tab_man, tab_up = st.tabs(["History", "Manual Add", "Smart Upload"])
    
    with tab_hist:
        tx = get_df(sh, "Transactions")
        if not tx.empty:
            curr_tx = tx[(tx['Year'] == year) & (tx['Month'] == month)]
            st.caption(f"Showing {len(curr_tx)} transactions for {month} {year}")
            render_editable_grid(sh, curr_tx, "Transactions", "grid_tx", hidden_cols=["Year", "Month"])
        else:
            st.info("No transaction history found.")

    with tab_man:
        with st.form("new_tx"):
            c1, c2 = st.columns(2)
            tt = c1.selectbox("Type", ["Expense", "Income"])
            cat = c2.text_input("Category (e.g. Food, Fuel)")
            amt = c1.number_input("Amount", min_value=0.0)
            nt = c2.text_input("Description / Notes")
            
            if st.form_submit_button("Add Entry"):
                tid = get_next_id(get_df(sh, "Transactions"))
                add_row(sh, "Transactions", [tid, str(date.today()), year, month, tt, cat, amt, nt, "Manual"])
                st.success("Entry Added"); st.rerun()

    with tab_up:
        st.subheader("Import Statement (PDF/CSV/Excel)")
        
        # Build Map for Auto-Detection
        cards = get_df(sh, "Cards"); banks = get_df(sh, "Banks")
        match_map = {}
        for _, r in cards.iterrows():
            if str(r.get('MatchCode')).strip(): match_map[str(r['MatchCode']).strip()] = f"Card: {r['Name']}"
        for _, r in banks.iterrows():
            if str(r.get('MatchCode')).strip(): match_map[str(r['MatchCode']).strip()] = f"Bank: {r['Name']}"
        
        uploaded_file = st.file_uploader("Drop file here", type=['xlsx', 'csv', 'xls', 'pdf'])
        
        if uploaded_file:
            final_src = "Unknown"
            # Attempt to guess source from filename
            for code, name in match_map.items():
                if code.lower() in uploaded_file.name.lower(): 
                    final_src = name
            
            final_src = st.text_input("Source Account", value=final_src)
            
            if st.button("Process File"):
                try:
                    df = None
                    entries = []
                    
                    # 1. HANDLE PDF
                    if uploaded_file.name.lower().endswith('.pdf'):
                        with pdfplumber.open(uploaded_file) as pdf:
                            text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
                            # Improved regex: allows comma or dot for decimals, looks for dates
                            pat = r"(\d{2}[-/]\d{2}[-/]\d{2,4}).*?([\d,]+\.?\d{0,2})"
                            matches = re.findall(pat, text)
                            for dt_str, amt_str in matches:
                                try:
                                    dt_val = safe_date(dt_str)
                                    if not dt_val: continue
                                    # clean amount
                                    amt_val = safe_float(amt_str)
                                    if amt_val > 0:
                                        entries.append([get_next_id(get_df(sh,"Transactions")), str(dt_val), year, month, "Expense", "PDF Import", amt_val, "Imported", final_src])
                                except: continue
                    
                    # 2. HANDLE EXCEL/CSV
                    else:
                        if uploaded_file.name.lower().endswith('.csv'):
                            df = pd.read_csv(uploaded_file)
                        else:
                            try: df = pd.read_excel(uploaded_file, engine='openpyxl')
                            except: 
                                try: df = pd.read_excel(uploaded_file, engine='xlrd') # Fallback for old .xls
                                except: st.error("Could not read Excel file. Check format."); st.stop()
                        
                        if df is not None:
                            df.columns = df.columns.astype(str).str.lower()
                            # Heuristic column finding
                            d_col = next((c for c in df.columns if 'date' in c), None)
                            a_col = next((c for c in df.columns if any(x in c for x in ['amount', 'debit', 'withdraw', 'inr'])), None)
                            desc_col = next((c for c in df.columns if any(x in c for x in ['desc', 'narration', 'particulars'])), None)

                            if d_col and a_col:
                                for _, row in df.iterrows():
                                    dt_val = safe_date(row[d_col])
                                    if not dt_val: continue
                                    amt_val = safe_float(row[a_col])
                                    narr = str(row[desc_col]) if desc_col else "Bulk Import"
                                    if amt_val > 0:
                                        entries.append([get_next_id(get_df(sh,"Transactions")), str(dt_val), year, month, "Expense", "Statement", amt_val, narr, final_src])
                            else:
                                st.error(f"Could not identify Date/Amount columns. Found: {list(df.columns)}")

                    if entries:
                        ws = api_retry(sh.worksheet, "Transactions")
                        ws.append_rows(entries)
                        clear_cache()
                        st.success(f"✅ Successfully Imported {len(entries)} transactions!")
                    else:
                        st.warning("No valid transactions parsed. Check format.")

                except Exception as e:
                    st.error(f"Parsing Error: {str(e)}")

# ==========================================
# 6. MAIN APP LOOP
# ==========================================

def main():
    st.set_page_config(page_title="Finance Hub Pro", layout="wide", page_icon="📈")
    inject_custom_css()
    
    # Initialization Phase
    with st.status("🚀 Booting Finance Hub...", expanded=True) as status:
        st.write("Connecting to Google Cloud...")
        sh = connect_gsheets()
        st.write("Verifying Database Schema...")
        if 'init_v15' not in st.session_state:
            init_sheets(sh)
            st.session_state['init_v15'] = True
        status.update(label="System Ready", state="complete", expanded=False)

    # Sidebar Navigation
    st.sidebar.title("☁️ Finance Hub")
    
    # Time Selection
    c1, c2 = st.sidebar.columns(2)
    current_year = datetime.now().year
    year = c1.selectbox("Year", list(range(current_year-1, current_year+5)), index=1)
    
    months = ["January", "February", "March", "April", "May", "June", 
              "July", "August", "September", "October", "November", "December"]
    curr_month_idx = datetime.now().month - 1
    month = c2.selectbox("Month", months, index=curr_month_idx)
    
    menu = ["Dashboard", "Credit Cards", "Loans", "Credit Card EMIs", "Bank Accounts", "Income/Exp"]
    choice = st.sidebar.radio("Go To", menu)
    
    st.sidebar.divider()
    if st.sidebar.button("🔄 Force Refresh Data"):
        clear_cache()
        st.rerun()

    # Router
    if choice == "Dashboard": render_dashboard(sh, year, month)
    elif choice == "Credit Cards": render_credit_cards(sh, year, month)
    elif choice == "Loans": render_loans(sh, year, month)
    elif choice == "Credit Card EMIs": render_active_emis(sh, year, month)
    elif choice == "Bank Accounts": render_bank_accounts(sh, year, month)
    elif choice == "Income/Exp": render_transactions(sh, year, month)

if __name__ == "__main__":
    main()
