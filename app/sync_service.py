import os
import json
import logging
import psycopg2
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
CREDS_PATH = os.environ.get("SHEETS_SERVICE_ACCOUNT_FILE")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def get_google_sheet_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if CREDS_PATH and os.path.exists(CREDS_PATH):
        try:
            # CORRECT WAY: Load from the file path provided by Render Secret Files
            creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scopes)
            logger.info("Successfully loaded credentials from Secret File path.")
        except Exception as e:
            logger.error(f"Failed to parse JSON from Secret File: {e}")
            raise
    elif os.path.exists("service_account.json"):
        creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
        logger.info("Loaded credentials from local service_account.json")
    else:
        logger.error("No valid credentials file found! Check SHEETS_SERVICE_ACCOUNT_FILE env var.")
        raise FileNotFoundError("Google Credentials file not found.")
    
    return gspread.authorize(creds)

def sync_table(sheet, table_name, columns):
    logger.info(f"Starting sync for table: {table_name}")
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # --- DELTA: CASE-INSENSITIVE SHEET SEARCH ---
        titles = [s.title for s in sheet.worksheets()]
        # This finds 'categories' even if the code says 'Categories' or 'CATEGORIES'
        target_title = next((t for t in titles if t.lower().strip() == table_name.lower()), None)
        
        if not target_title:
            logger.error(f"Tab matching '{table_name}' not found. Found: {titles}")
            raise Exception(f"Worksheet for {table_name} missing.")

        worksheet = sheet.worksheet(target_title)

        # 1. DB Fetch
        query = f"SELECT * FROM {table_name} WHERE deleted = FALSE" if table_name == "words" else f"SELECT * FROM {table_name}"
        cur.execute(query)
        db_map = {str(row['id']): row for row in cur.fetchall()}

        # 2. Sheet Fetch
        all_sheet_records = worksheet.get_all_records()
        sheet_ids_found = set()
        
        for i, row in enumerate(all_sheet_records):
            row_num = i + 2 
            row_id = str(row.get('id', '')).strip()
            
            # --- DELTA: ROBUST EMPTY ID CHECK ---
            if not row_id or row_id.lower() in ["none", "", "0"]:
                cols = [c for c in columns if c != 'id' and c != 'updated_at']
                vals = [row[c] for c in cols]
                cur.execute(f"INSERT INTO {table_name} ({', '.join(cols)}, updated_at) VALUES ({', '.join(['%s']*len(cols))}, NOW()) RETURNING id, updated_at")
                new_data = cur.fetchone()
                conn.commit()
                if new_data:
                    worksheet.update_cell(row_num, 1, new_data['id'])
                    worksheet.update_cell(row_num, len(columns), str(new_data['updated_at']))
                continue

            sheet_ids_found.add(row_id)
            
            # 3. Update Check
            if row_id in db_map:
                db_record = db_map[row_id]
                db_content = [str(db_record[c]) for c in columns if c not in ['updated_at', 'id']]
                sheet_content = [str(row[c]) for c in columns if c not in ['updated_at', 'id']]
                
                if db_content != sheet_content:
                    update_cols = [c for c in columns if c != 'id' and c != 'updated_at']
                    update_vals = [row[c] for c in update_cols] + [row_id]
                    cur.execute(f"UPDATE {table_name} SET ({', '.join(update_cols)}, updated_at) = ({', '.join(['%s']*len(update_cols))}, NOW()) WHERE id = %s", tuple(update_vals))
                    conn.commit()
                    worksheet.update_cell(row_num, len(columns), str(datetime.now()))

        # 4. --Deletions--
        for db_id in db_map:
            if db_id not in sheet_ids_found:
                if table_name == "words":
                    logger.info(f"Soft-deleting word ID {db_id}")
                    cur.execute("UPDATE words SET deleted = TRUE WHERE id = %s", (db_id,))
                else:
                    # Use 'as count' alias and verify fetchone result isn't None
                    cur.execute("SELECT count(*) as count FROM words WHERE category_id = %s AND deleted = FALSE", (db_id,))
                    result = cur.fetchone()
                    
                    # Safety check: if result is None, assume count is 0 to avoid crash
                    count = result['count'] if result else 0
                    
                    if count > 0:
                        logger.warning(f"Cannot delete category {db_id}: {count} active words still reference it.")
                    else:
                        logger.info(f"Hard-deleting empty category ID {db_id}")
                        cur.execute(f"DELETE FROM {table_name} WHERE id = %s", (db_id,))
                conn.commit()

# ... (rest of the file remains the same)
        # 5. Appends (New in DB -> Sheet)
        cur.execute(query)
        rows_to_append = [[row[c] for c in columns] for row in cur.fetchall() if str(row['id']) not in sheet_ids_found]
        if rows_to_append:
            for r in rows_to_append: r[-1] = str(r[-1])
            worksheet.append_rows(rows_to_append)

    finally:
        conn.close()
    return {"status": "success"}

def run_sync(sheet_id):
    gc = get_google_sheet_client()
    sh = gc.open_by_key(sheet_id)
    
    word_res = sync_table(sh, "words", ["id", "category_id", "word", "updated_at"])
    cat_res = sync_table(sh, "categories", ["id", "name", "updated_at"])
    
    return {"categories": cat_res, "words": word_res}