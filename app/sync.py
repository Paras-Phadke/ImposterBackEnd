import pandas as pd

def resolve_conflicts(sheet_df, db_df, key='id'):
    sheet_df = sheet_df.copy()
    db_df = db_df.copy()

    # convert timestamps
    sheet_df['updated_at'] = pd.to_datetime(sheet_df['updated_at'])
    db_df['updated_at'] = pd.to_datetime(db_df['updated_at'])

    merged = sheet_df.merge(db_df, on=key, how='outer', suffixes=('_sheet', '_db'))

    to_db = []
    to_sheet = []

    for _, row in merged.iterrows():
        s = row.filter(like='_sheet')
        d = row.filter(like='_db')

        if pd.isna(s['updated_at_sheet']):
            to_sheet.append(row)
        elif pd.isna(d['updated_at_db']):
            to_db.append(row)
        else:
            if s['updated_at_sheet'] > d['updated_at_db']:
                to_db.append(row)
            elif d['updated_at_db'] > s['updated_at_sheet']:
                to_sheet.append(row)

    return to_db, to_sheet

import pandas as pd

def merge_back_to_sheet(sheet_df, db_df, db_updates):
    sheet_df = sheet_df.copy()
    db_df = db_df.copy()

    # ensure all types align
    for col in db_df.columns:
        if col not in sheet_df.columns:
            sheet_df[col] = None

    # convert id to int for merge/index
    sheet_df['id'] = sheet_df['id'].astype(int)
    db_df['id'] = db_df['id'].astype(int)

    # set indexes for updates
    sheet_df = sheet_df.set_index('id')
    db_df = db_df.set_index('id')

    # apply db updates into sheet_df
    for _, row in db_updates.iterrows() :
        rid = row['id']
        sheet_df.loc[rid] = db_df.loc[rid]

    # add any new DB rows not in sheet
    missing_ids = db_df.index.difference(sheet_df.index)
    if len(missing_ids) > 0:
        sheet_df = pd.concat([sheet_df, db_df.loc[missing_ids]])

    sheet_df = sheet_df.reset_index()

    # return with sorted by id
    return sheet_df.sort_values('id')
