"""
Google Sheets Export Module
Exports processed leads to Google Sheets by vertical
"""

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
from pathlib import Path
import glob
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from config import (
    PROCESSED_DATA_DIR,
    VERTICAL_RULES,
    GOOGLE_SHEETS_CREDENTIALS_PATH
)


def authenticate_google_sheets():
    """
    Authenticate with Google Sheets API
    
    Returns:
        gspread.Client: Authenticated client
    """
    
    logger.info("Authenticating with Google Sheets API")
    
    if not GOOGLE_SHEETS_CREDENTIALS_PATH.exists():
        raise FileNotFoundError(
            f"Google Sheets credentials not found at {GOOGLE_SHEETS_CREDENTIALS_PATH}. "
            "Please place your service account JSON file as 'credentials.json' in the project root."
        )
    
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        str(GOOGLE_SHEETS_CREDENTIALS_PATH),
        scope
    )
    
    client = gspread.authorize(credentials)
    
    logger.info("Authentication successful")
    
    return client


def load_latest_processed_data():
    """
    Load the most recent processed data file
    
    Returns:
        pandas.DataFrame: Processed leads data
    """
    
    pattern = str(PROCESSED_DATA_DIR / 'scored_leads_*.csv')
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"No processed data files found in {PROCESSED_DATA_DIR}")
    
    latest_file = max(files)
    logger.info(f"Loading data from: {latest_file}")
    
    df = pd.read_csv(latest_file)
    logger.info(f"Loaded {len(df)} scored leads")
    
    return df


def apply_vertical_filters(df):
    """
    Apply vertical-specific business rules and filters
    
    Args:
        df: pandas.DataFrame with scored leads
        
    Returns:
        dict: Dictionary with DataFrames for each vertical
    """
    
    logger.info("Applying vertical-specific filters")
    
    vertical_dfs = {}
    
    for vertical, rules in VERTICAL_RULES.items():
        df_vertical = df[df['vertical'] == vertical].copy()
        
        if len(df_vertical) == 0:
            logger.warning(f"  {vertical}: No leads found")
            continue
        
        df_filtered = df_vertical[df_vertical['lead_score'] >= rules['min_score']]
        df_filtered = df_filtered.sort_values('lead_score', ascending=False)
        df_filtered = df_filtered.head(rules['target_count'])
        
        vertical_dfs[vertical] = df_filtered
        
        logger.info(f"  {vertical}: {len(df_filtered)} leads")
    
    return vertical_dfs


def create_or_open_spreadsheet(client, sheet_name):
    """
    Create a new spreadsheet or open existing one
    
    Args:
        client: gspread.Client
        sheet_name: str, name of the spreadsheet
        
    Returns:
        gspread.Spreadsheet: Spreadsheet object
    """
    
    try:
        spreadsheet = client.open(sheet_name)
        logger.info(f"Opened existing spreadsheet: {sheet_name}")
    except gspread.SpreadsheetNotFound:
        spreadsheet = client.create(sheet_name)
        logger.info(f"Created new spreadsheet: {sheet_name}")
    
    return spreadsheet


def format_worksheet_header(worksheet):
    """
    Format the header row of a worksheet
    
    Args:
        worksheet: gspread.Worksheet
    """
    
    worksheet.format('A1:Q1', {
        'textFormat': {'bold': True, 'fontSize': 11},
        'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
        'horizontalAlignment': 'CENTER'
    })
    
    worksheet.freeze(rows=1)


def export_vertical_to_sheet(client, vertical, df, week_number, year):
    """
    Export a vertical's leads to Google Sheet

    Args:
        client: gspread.Client
        vertical: str, vertical name
        df: pandas.DataFrame, leads data
        week_number: int, week number (1-52)
        year: int, year

    Returns:
        str: URL of the created/updated sheet
    """

    sheet_name = f"Week{week_number}_{year}_{vertical.capitalize()}_Leads"

    logger.info(f"Exporting {vertical} to Google Sheet: {sheet_name}")

    spreadsheet = create_or_open_spreadsheet(client, sheet_name)

    try:
        worksheet = spreadsheet.sheet1
    except Exception:
        worksheet = spreadsheet.add_worksheet(title='Leads', rows=1000, cols=20)

    worksheet.clear()

    export_columns = [
        'name', 'address', 'city', 'state', 'phone', 'website',
        'lead_score', 'priority', 'review_count', 'rating',
        'on_ubereats', 'on_grubhub', 'contact_by_date'
    ]
    available_columns = [col for col in export_columns if col in df.columns]
    df_export = df[available_columns].copy()

    # Convert to list of lists for gspread
    header = available_columns
    rows = df_export.fillna('').astype(str).values.tolist()
    data = [header] + rows

    worksheet.update('A1', data)

    format_worksheet_header(worksheet)

    logger.info(f"  Exported {len(df_export)} leads to {sheet_name}")

    return spreadsheet.url
    


def share_spreadsheet(client, sheet_name, email_addresses):
    """
    Share a spreadsheet with specific email addresses
    
    Args:
        client: gspread.Client
        sheet_name: str, name of the spreadsheet
        email_addresses: list of str, email addresses to share with
    """
    
    spreadsheet = client.open(sheet_name)
    
    for email in email_addresses:
        try:
            spreadsheet.share(email, perm_type='user', role='writer', notify=True)
            logger.info(f"  Shared with {email}")
        except Exception as e:
            logger.warning(f"  Failed to share with {email}: {e}")


def create_summary_sheet(client, vertical_urls, week_number, year):
    """
    Create a summary sheet with links to all vertical sheets

    Args:
        client: gspread.Client
        vertical_urls: dict, vertical name to sheet URL mapping
        week_number: int, week number (1-52)
        year: int, year

    Returns:
        str: URL of summary sheet
    """

    sheet_name = f"Week{week_number}_{year}_Leads_Summary"

    logger.info(f"Creating summary sheet: {sheet_name}")

    spreadsheet = create_or_open_spreadsheet(client, sheet_name)

    try:
        worksheet = spreadsheet.worksheet('Summary')
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title='Summary', rows=100, cols=10)

    worksheet.clear()

    summary_data = [
        ['DoorDash GTM - Weekly Leads Summary'],
        [f'Week: {week_number}, {year}'],
        [f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'],
        [],
        ['Vertical', 'Sheet Link', 'Lead Count'],
    ]
    
    for vertical, url in vertical_urls.items():
        summary_data.append([
            vertical.capitalize(),
            url,
            ''
        ])
    
    worksheet.update('A1', summary_data)
    
    worksheet.format('A1', {
        'textFormat': {'bold': True, 'fontSize': 14},
        'horizontalAlignment': 'CENTER'
    })
    
    worksheet.format('A5:C5', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2}
    })
    
    worksheet.columns_auto_resize(0, 3)
    
    logger.info(f"  Summary sheet created: {spreadsheet.url}")
    
    return spreadsheet.url


def export_to_google_sheets(share_with_emails=None):
    """
    Export all verticals to Google Sheets
    
    Args:
        share_with_emails: list of str, optional email addresses to share sheets with
        
    Returns:
        dict: Vertical name to sheet URL mapping
    """
    
    logger.info("="*60)
    logger.info("GOOGLE SHEETS EXPORT")
    logger.info("="*60)
    
    try:
        client = authenticate_google_sheets()
        
        df = load_latest_processed_data()
        
        vertical_dfs = apply_vertical_filters(df)
        
        if not vertical_dfs:
            logger.error("No leads to export after applying filters")
            return {}
        
        week_number = datetime.now().isocalendar()[1]
        year = datetime.now().year

        vertical_urls = {}

        for vertical, df_vertical in vertical_dfs.items():
            url = export_vertical_to_sheet(client, vertical, df_vertical, week_number, year)
            vertical_urls[vertical] = url

            if share_with_emails:
                sheet_name = f"Week{week_number}_{year}_{vertical.capitalize()}_Leads"
                share_spreadsheet(client, sheet_name, share_with_emails)
        
        summary_url = create_summary_sheet(client, vertical_urls, week_number, year)
        
        logger.info("="*60)
        logger.info("EXPORT SUMMARY")
        logger.info("="*60)
        logger.info(f"Total Sheets Created: {len(vertical_urls)}")
        logger.info(f"\nSheet URLs:")
        for vertical, url in vertical_urls.items():
            logger.info(f"  {vertical.capitalize()}: {url}")
        logger.info(f"\nSummary Sheet: {summary_url}")
        logger.info("="*60)
        
        return vertical_urls
        
    except Exception as e:
        logger.error(f"Google Sheets export failed: {e}")
        raise


def main():
    """Main execution function"""
    
    try:
        vertical_urls = export_to_google_sheets()
        
        logger.info("Google Sheets export completed successfully")
        
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise


if __name__ == "__main__":
    main()