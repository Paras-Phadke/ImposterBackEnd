import os
import json
import logging
import psycopg2
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from psycopg2.extras import RealDictCursor

# ======== LOGGING SETUP ========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
# This should be the PATH to the secret file (e.g., /etc/secrets/google_creds.json)
CREDS_PATH = os.environ.get("SHEETS_SERVICE_ACCOUNT_FILE")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def get_google_sheet_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    
    logger.info(f"Attempting to load Google Credentials from path: {CREDS_PATH}")
    
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
        # 1. Fetch DB Data
        if table_name == "words":
            cur.execute(f"SELECT * FROM {table_name} WHERE deleted = FALSE")
        else:
            cur.execute(f"SELECT * FROM {table_name}")
        
        db_rows = cur.fetchall()
        db_map = {str(row['id']): row for row in db_rows}
        logger.info(f"Fetched {len(db_rows)} active rows from DB table '{table_name}'")

        # 2. Fetch Sheet Data
        worksheet = sheet.worksheet(table_name.capitalize())
        all_sheet_records = worksheet.get_all_records()
        logger.info(f"Fetched {len(all_sheet_records)} rows from Google Sheet tab '{table_name.capitalize()}'")

        sheet_ids_found = set()
        
        for i, row in enumerate(all_sheet_records):
            row_num = i + 2 
            row_id = str(row.get('id', '')).strip()
            
            # --- CASE A: NEW ROW ---
            if not row_id or row_id.lower() == "none" or row_id == "":
                cols = [c for c in columns if c != 'id' and c != 'updated_at']
                vals = [row[c] for c in cols]
                
                logger.info(f"Inserting new row into {table_name}: {vals}")
                query = f"INSERT INTO {table_name} ({', '.join(cols)}, updated_at) VALUES ({', '.join(['%s']*len(cols))}, NOW()) RETURNING id, updated_at"
                cur.execute(query, tuple(vals))
                new_data = cur.fetchone()
                conn.commit()
                
                if new_data:
                    worksheet.update_cell(row_num, 1, new_data['id'])
                    worksheet.update_cell(row_num, len(columns), str(new_data['updated_at']))
                continue

            sheet_ids_found.add(row_id)
            
            # --- CASE B: EXISTING ROW (Update Check) ---
            if row_id in db_map:
                db_record = db_map[row_id]
                db_content = [str(db_record[c]) for c in columns if c not in ['updated_at', 'id']]
                sheet_content = [str(row[c]) for c in columns if c not in ['updated_at', 'id']]
                
                if db_content != sheet_content:
                    logger.info(f"Updating ID {row_id} in DB (Sheet content changed)")
                    update_cols = [c for c in columns if c != 'id' and c != 'updated_at']
                    update_vals = [row[c] for c in update_cols]
                    update_vals.append(row_id)
                    
                    query = f"UPDATE {table_name} SET ({', '.join(update_cols)}, updated_at) = ({', '.join(['%s']*len(update_cols))}, NOW()) WHERE id = %s"
                    cur.execute(query, tuple(update_vals))
                    conn.commit()
                    worksheet.update_cell(row_num, len(columns), str(datetime.now()))

        # 3. Handle Deletions (In DB but missing from Sheet)
        for db_id in db_map:
            if db_id not in sheet_ids_found:
                logger.info(f"ID {db_id} missing from sheet. Deleting/Soft-deleting in DB.")
                if table_name == "words":
                    cur.execute("UPDATE words SET deleted = TRUE WHERE id = %s", (db_id,))
                else:
                    cur.execute(f"DELETE FROM {table_name} WHERE id = %s", (db_id,))
                conn.commit()

        # 4. Handle Additions to Sheet (In DB but not in Sheet)
        # We re-query to get the state after current sync updates
        if table_name == "words":
            cur.execute(f"SELECT * FROM {table_name} WHERE deleted = FALSE")
        else:
            cur.execute(f"SELECT * FROM {table_name}")
        
        final_db_rows = cur.fetchall()
        rows_to_append = []
        for row in final_db_rows:
            if str(row['id']) not in sheet_ids_found:
                sheet_row = [row[c] for c in columns]
                sheet_row[-1] = str(sheet_row[-1])
                rows_to_append.append(sheet_row)

        if rows_to_append:
            logger.info(f"Appending {len(rows_to_append)} new DB rows to Sheet.")
            worksheet.append_rows(rows_to_append)

    except Exception as e:
        logger.error(f"Error during sync of {table_name}: {e}")
        raise
    finally:
        conn.close()

    return {"status": "success", "table": table_name}

def run_sync(sheet_id):
    try:
        gc = get_google_sheet_client()
        sh = gc.open_by_key(sheet_id)
        
        logger.info("Starting Full Two-Way Sync...")
        cat_res = sync_table(sh, "categories", ["id", "name", "updated_at"])
        word_res = sync_table(sh, "words", ["id", "category_id", "word", "updated_at"])
        
        logger.info("Sync Completed Successfully.")
        return {"categories": cat_res, "words": word_res}
    except Exception as e:
        logger.error(f"Global Sync Failure: {e}")
        raise