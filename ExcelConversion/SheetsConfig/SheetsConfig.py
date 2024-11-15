# SheetsConfig/SheetsConfig.py
import unicodedata
from datetime import datetime


# This will solve text problems with the sheet(remove special characters and make uppercase)
def normalize_text(jf):
    if isinstance(jf, str):  # Apply normalization only if it's a string
        jf = jf.upper()  # Transform into uppercase
        # Remove especial char and Mn
        jf = ''.join(
            c for c in unicodedata.normalize('NFD', jf)
            if unicodedata.category(c) != 'Mn'
        )
    return jf


# Use this to remove calculation error in ROI column and remove duplicated data
def fix_sheets_values(fc):
    fc = fc.drop_duplicates()
    if 'ROI' in fc.columns:
        fc.loc[fc['ROI'] == -0.01, 'ROI'] = 0
    return fc


def date_type(dt):
    # Specify what columns the code will alter to datetype
    date_columns = ['DATA_DE_INICIO_DO_CONTRATO', 'DATA_FINAL_DO_CONTRATO']
    for item in dt:
        for col in date_columns:
            if col in item and isinstance(item[col], int):  # Check if the field is a timestamp (bigint)
                # Convert bigint numbers into a datetype number
                item[col] = datetime.fromtimestamp(item[col] / 1000).strftime('%Y-%m-%d')  # Specify what type of date
    return dt
