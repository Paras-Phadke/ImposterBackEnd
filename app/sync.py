import pandas as pd


def normalize_ids(df):
    if 'id' in df.columns:
        df['id'] = df['id'].replace('', pd.NA)
    return df


def resolve_conflicts(sheet_df, db_df, key='id'):
    sheet_df = normalize_ids(sheet_df.copy())
    db_df = normalize_ids(db_df.copy())

    # only rows WITH ids participate in conflict resolution
    sheet_df = sheet_df[sheet_df[key].notna()]
    db_df = db_df[db_df[key].notna()]

    sheet_df[key] = sheet_df[key].astype(int)
    db_df[key] = db_df[key].astype(int)

    sheet_df['updated_at'] = pd.to_datetime(sheet_df['updated_at'])
    db_df['updated_at'] = pd.to_datetime(db_df['updated_at'])

    merged = sheet_df.merge(
        db_df,
        on=key,
        how='outer',
        suffixes=('_sheet', '_db')
    )

    to_db = []
    to_sheet = []

    for _, row in merged.iterrows():
        s = row.filter(like='_sheet')
        d = row.filter(like='_db')

        if pd.isna(s['updated_at_sheet']):
            to_sheet.append(row)
        elif pd.isna(d['updated_at_db']):
            to_db.append(row)
        elif s['updated_at_sheet'] > d['updated_at_db']:
            to_db.append(row)
        elif d['updated_at_db'] > s['updated_at_sheet']:
            to_sheet.append(row)

    return pd.DataFrame(to_db), pd.DataFrame(to_sheet)


def merge_back_to_sheet(sheet_df, db_df, db_updates):
    sheet_df = sheet_df.copy()
    db_df = db_df.copy()

    sheet_df['id'] = sheet_df['id'].replace('', pd.NA)

    sheet_df = sheet_df[sheet_df['id'].notna()]

    sheet_df['id'] = sheet_df['id'].astype(int)
    db_df['id'] = db_df['id'].astype(int)

    sheet_df = sheet_df.set_index('id')
    db_df = db_df.set_index('id')

    # apply DB-updated rows
    if isinstance(db_updates, pd.DataFrame) and not db_updates.empty:
        for _, row in db_updates.iterrows():
            rid = int(row['id'])
            if rid in db_df.index:
                sheet_df.loc[rid] = db_df.loc[rid]

    # add any DB rows missing from sheet
    missing_ids = db_df.index.difference(sheet_df.index)
    if len(missing_ids) > 0:
        sheet_df = pd.concat([sheet_df, db_df.loc[missing_ids]])

    return sheet_df.reset_index().sort_values('id')

