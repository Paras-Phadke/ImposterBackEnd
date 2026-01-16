import os
import json
import psycopg2
import gspread
from google.oauth2.service_account import Credentials # Switched to modern google-auth
from datetime import datetime
from psycopg2.extras import RealDictCursor

# ======== CONFIG ========
DATABASE_URL = os.environ.get("DATABASE_URL")
# Render Env Var: Paste the content of your JSON key file here
GOOGLE_CREDS_JSON = os.environ.get("SHEETS_SERVICE_ACCOUNT_FILE") 

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def get_google_sheet_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    
    # Use modern google.oauth2.service_account.Credentials
    if os.path.exists("service_account.json"):
        creds = Credentials.from_service_account_file(GOOGLE_CREDS_JSON, scopes=scopes)
    elif GOOGLE_CREDS_JSON:
        creds_dict = json.loads(GOOGLE_CREDS_JSON)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    else:
        raise Exception(f"No Google Credentials found in Environment or service_account.json, invalid path:{GOOGLE_CREDS_JSON}")
    
    return gspread.authorize(creds)

# ======== SYNC LOGIC ========

def sync_table(sheet, table_name, columns):
    conn = get_db_connection()
    # Use RealDictCursor so we can access columns by name: row['id']
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 1. Fetch DB Data
    if table_name == "words":
        cur.execute(f"SELECT * FROM {table_name} WHERE deleted = FALSE")
    else:
        cur.execute(f"SELECT * FROM {table_name}")
    
    db_rows = cur.fetchall()
    db_map = {str(row['id']): row for row in db_rows}

    # 2. Fetch Sheet Data
    try:
        worksheet = sheet.worksheet(table_name.capitalize()) 
    except gspread.exceptions.WorksheetNotFound:
        conn.close()
        return {"error": f"Tab {table_name.capitalize()} not found"}

    all_sheet_records = worksheet.get_all_records()
    sheet_ids_found = set()
    
    for i, row in enumerate(all_sheet_records):
        row_num = i + 2 
        row_id = str(row.get('id', '')).strip()
        
        # --- CASE A: NEW ROW (Added in Sheet) ---
        if not row_id or row_id == "None" or row_id == "":
            cols = [c for c in columns if c != 'id' and c != 'updated_at']
            vals = [row[c] for c in cols]
            
            query = f"""
                INSERT INTO {table_name} ({', '.join(cols)}, updated_at) 
                VALUES ({', '.join(['%s']*len(cols))}, NOW()) 
                RETURNING id, updated_at
            """
            cur.execute(query, tuple(vals))
            new_data = cur.fetchone()
            conn.commit()
            
            # Type-safe check: Ensure new_data exists before subscripting
            if new_data:
                worksheet.update_cell(row_num, 1, new_data['id'])
                worksheet.update_cell(row_num, len(columns), str(new_data['updated_at']))
            continue

        sheet_ids_found.add(row_id)
        
        # --- CASE B: EXISTING ROW (Compare & Sync) ---
        if row_id in db_map:
            db_record = db_map[row_id]
            db_content = [str(db_record[c]) for c in columns if c not in ['updated_at', 'id']]
            sheet_content = [str(row[c]) for c in columns if c not in ['updated_at', 'id']]
            
            if db_content != sheet_content:
                update_cols = [c for c in columns if c != 'id' and c != 'updated_at']
                update_vals = [row[c] for c in update_cols]
                update_vals.append(row_id)
                
                query = f"""
                    UPDATE {table_name} 
                    SET ({', '.join(update_cols)}, updated_at) = ({', '.join(['%s']*len(update_cols))}, NOW()) 
                    WHERE id = %s
                """
                cur.execute(query, tuple(update_vals))
                conn.commit()
                worksheet.update_cell(row_num, len(columns), str(datetime.now()))

    # 3. Handle Deletions (In DB but missing from Sheet)
    for db_id in db_map:
        if db_id not in sheet_ids_found:
            if table_name == "words":
                cur.execute("UPDATE words SET deleted = TRUE WHERE id = %s", (db_id,))
            else:
                cur.execute(f"DELETE FROM {table_name} WHERE id = %s", (db_id,))
            conn.commit()

    # 4. Handle Missing in Sheet (In DB but not in Sheet)
    if table_name == "words":
        cur.execute(f"SELECT * FROM {table_name} WHERE deleted = FALSE")
    else:
        cur.execute(f"SELECT * FROM {table_name}")
    
    current_db_rows = cur.fetchall()
    rows_to_append = []
    for row in current_db_rows:
        if str(row['id']) not in sheet_ids_found:
            sheet_row = [row[c] for c in columns]
            sheet_row[-1] = str(sheet_row[-1]) # Convert timestamp to string
            rows_to_append.append(sheet_row)

    if rows_to_append:
        worksheet.append_rows(rows_to_append)

    conn.close()
    return {"status": "synced", "table": table_name}

def run_sync(sheet_id):
    gc = get_google_sheet_client()
    sh = gc.open_by_key(sheet_id)
    cat_res = sync_table(sh, "categories", ["id", "name", "updated_at"])
    word_res = sync_table(sh, "words", ["id", "category_id", "word", "updated_at"])
    return {"categories": cat_res, "words": word_res}