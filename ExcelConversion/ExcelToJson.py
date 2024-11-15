import pandas as pd
from pathlib import Path
from ExcelConversion.SheetsConfig.SheetsConfig import normalize_text, fix_sheets_values

# Specify what folder you want to use
path = Path("/home/rafael-vieira/Desktop/CPV_Server/ExcelConversion/JsonFiles/JsonFiles.json")
# Make sure if the directory exist
path.parent.mkdir(parents=True, exist_ok=True)


# Use this to read the xlsx file and transform into a Json
def run_task():
    try:
        print("Running task: Converting Excel to JSON")
        # Convert the Excel file into json
        excel_file = pd.ExcelFile('/home/rafael-vieira/Desktop/CPV_Server/DataBase_CPV.xlsx')
        df = pd.read_excel(excel_file)
        # Fix calculations results
        df = fix_sheets_values(df)
        # Normalize all the letters to UpperCase
        df = df.map(normalize_text)
        # Save Json in file as a records type
        json_data = df.to_json(orient='records')
        # Print Json as an example of the archive
        print(type(df))  # Should be <class 'list'>
        print(df[:5])  # Print first 5 items to inspect
        # Saves the converted file into the directory
        with path.open('w') as file:
            file.write(json_data)

    except FileNotFoundError:
        print(f"Error: {FileNotFoundError}")
    except Exception as error:
        print(f"Error: {error}")
