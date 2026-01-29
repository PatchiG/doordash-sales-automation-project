"""
Test Google Sheets authentication and basic operations
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_authentication():
    """Test Google Sheets authentication"""
    
    logger.info("Testing Google Sheets authentication")
    
    credentials_path = Path('credentials.json')
    
    if not credentials_path.exists():
        logger.error("credentials.json not found in project root")
        return False
    
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            str(credentials_path),
            scope
        )
        
        client = gspread.authorize(credentials)
        
        logger.info("Authentication successful")
        
        test_sheet_name = "DoorDash_Test_Sheet"
        
        try:
            spreadsheet = client.create(test_sheet_name)
            logger.info(f"Created test spreadsheet: {spreadsheet.url}")
            
            worksheet = spreadsheet.sheet1
            worksheet.update('A1', [['Name', 'Score', 'City']])
            worksheet.update('A2', [['Test Business', '85', 'San Francisco']])
            
            logger.info("Test data written successfully")
            
            client.del_spreadsheet(spreadsheet.id)
            logger.info("Test spreadsheet deleted")
            
        except Exception as e:
            logger.error(f"Test operations failed: {e}")
            return False
        
        logger.info("All tests passed")
        return True
        
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        return False


if __name__ == "__main__":
    success = test_authentication()
    if success:
        print("\nGoogle Sheets integration is ready")
    else:
        print("\nGoogle Sheets integration failed - check errors above")