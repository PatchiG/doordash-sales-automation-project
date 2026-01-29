"""
Export Module
Exports processed leads to CSV files by vertical with business logic filters
"""

import pandas as pd
import logging
from pathlib import Path
import glob
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from config import PROCESSED_DATA_DIR, OUTPUT_DIR, VERTICAL_RULES


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
        # Filter by vertical
        df_vertical = df[df['vertical'] == vertical].copy()
        
        if len(df_vertical) == 0:
            logger.warning(f"  {vertical}: No leads found")
            continue
        
        # Apply minimum score filter
        df_filtered = df_vertical[df_vertical['lead_score'] >= rules['min_score']]
        
        # Sort by lead score (descending)
        df_filtered = df_filtered.sort_values('lead_score', ascending=False)
        
        # Limit to target count
        df_filtered = df_filtered.head(rules['target_count'])
        
        vertical_dfs[vertical] = df_filtered
        
        logger.info(f"  {vertical}: {len(df_filtered)} leads (filtered from {len(df_vertical)}, min score: {rules['min_score']}, target: {rules['target_count']})")
    
    return vertical_dfs


def export_to_csv(vertical_dfs):
    """
    Export each vertical to separate CSV files
    
    Args:
        vertical_dfs: dict of DataFrames by vertical
        
    Returns:
        dict: Paths to exported files
    """
    
    logger.info("Exporting to CSV files")
    
    # Changed: Include week number in filename
    week_number = datetime.now().isocalendar()[1]
    year = datetime.now().year
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    exported_files = {}
    
    export_columns = [
        'name',
        'address',
        'city',
        'state',
        'phone',
        'website',
        'types',
        'lead_score',
        'priority',
        'review_count',
        'rating',
        'on_ubereats',
        'on_grubhub',
        'contact_by_date',
        'latitude',
        'longitude',
        'place_id'
    ]
    
    for vertical, df in vertical_dfs.items():
        available_columns = [col for col in export_columns if col in df.columns]
        df_export = df[available_columns].copy()
        
        # Changed: Include week in filename
        filename = f'{vertical}_leads_week{week_number}_{year}_{timestamp}.csv'
        filepath = OUTPUT_DIR / filename
        
        df_export.to_csv(filepath, index=False)
        
        exported_files[vertical] = str(filepath)
        logger.info(f"  Exported {vertical}: {filepath} ({len(df_export)} leads)")
    
    return exported_files


def export_summary_report(vertical_dfs, exported_files):
    """
    Create and log a summary report of the export
    
    Args:
        vertical_dfs: dict of DataFrames by vertical
        exported_files: dict of file paths by vertical
    """
    
    logger.info("="*60)
    logger.info("EXPORT SUMMARY REPORT")
    logger.info("="*60)
    
    total_leads = sum(len(df) for df in vertical_dfs.values())
    logger.info(f"Total Leads Exported: {total_leads}")
    
    logger.info("\nBreakdown by Vertical:")
    for vertical, df in vertical_dfs.items():
        avg_score = df['lead_score'].mean()
        high_priority = len(df[df['priority'].isin(['High', 'Critical'])])
        on_competitor = len(df[(df['on_ubereats']) | (df['on_grubhub'])])
        avg_rating = df['rating'].mean()
        total_reviews = df['review_count'].sum()
        
        logger.info(f"\n  {vertical.upper()}:")
        logger.info(f"    Total Leads: {len(df)}")
        logger.info(f"    Average Score: {avg_score:.1f}")
        logger.info(f"    High/Critical Priority: {high_priority} ({high_priority/len(df)*100:.1f}%)")
        logger.info(f"    On Competitor Platform: {on_competitor} ({on_competitor/len(df)*100:.1f}%)")
        logger.info(f"    Average Rating: {avg_rating:.2f}")
        logger.info(f"    Total Reviews: {total_reviews:,.0f}")
        logger.info(f"    File: {exported_files[vertical]}")
    
    logger.info("="*60)


def create_combined_export(vertical_dfs):
    """
    Create a single combined CSV with all verticals
    
    Args:
        vertical_dfs: dict of DataFrames by vertical
        
    Returns:
        str: Path to combined file
    """
    
    logger.info("Creating combined export file")
    
    # Combine all vertical DataFrames
    all_dfs = []
    for vertical, df in vertical_dfs.items():
        df_copy = df.copy()
        all_dfs.append(df_copy)
    
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Sort by lead score
    combined_df = combined_df.sort_values('lead_score', ascending=False)
    
    # Export
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'all_leads_combined_{timestamp}.csv'
    filepath = OUTPUT_DIR / filename
    
    export_columns = [
        'name',
        'vertical',
        'address',
        'city',
        'state',
        'phone',
        'website',
        'types',
        'lead_score',
        'priority',
        'review_count',
        'rating',
        'on_ubereats',
        'on_grubhub',
        'contact_by_date',
        'latitude',
        'longitude',
        'place_id'
    ]
    
    available_columns = [col for col in export_columns if col in combined_df.columns]
    combined_df[available_columns].to_csv(filepath, index=False)
    
    logger.info(f"Combined export created: {filepath} ({len(combined_df)} leads)")
    
    return str(filepath)


def generate_sales_summary(vertical_dfs):
    """
    Generate a sales team summary document

    Args:
        vertical_dfs: dict of DataFrames by vertical

    Returns:
        str: Path to summary file
    """

    logger.info("Generating sales team summary")

    # Changed: Include week number
    week_number = datetime.now().isocalendar()[1]
    year = datetime.now().year
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'sales_summary_week{week_number}_{year}_{timestamp}.txt'
    filepath = OUTPUT_DIR / filename

    with open(filepath, 'w') as f:
        f.write("="*70 + "\n")
        f.write("DOORDASH GTM - WEEKLY LEADS GENERATION SUMMARY\n")
        f.write("="*70 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Week: {week_number}, {year}\n")
        f.write("="*70 + "\n\n")

        total_leads = sum(len(df) for df in vertical_dfs.values())
        f.write(f"Total Leads: {total_leads}\n\n")

        for vertical, df in vertical_dfs.items():
            avg_score = df['lead_score'].mean()
            high_priority = len(df[df['priority'].isin(['High', 'Critical'])])
            f.write(f"{vertical.upper()}\n")
            f.write(f"  Leads: {len(df)}\n")
            f.write(f"  Avg Score: {avg_score:.1f}\n")
            f.write(f"  High/Critical Priority: {high_priority}\n\n")

    logger.info(f"Sales summary saved to {filepath}")

    return str(filepath)


def main():
    """Main execution function"""
    
    logger.info("="*60)
    logger.info("EXPORT MODULE")
    logger.info("="*60)
    
    try:
        # Load processed data
        df = load_latest_processed_data()
        
        # Apply vertical filters
        vertical_dfs = apply_vertical_filters(df)
        
        if not vertical_dfs:
            logger.error("No leads to export after applying filters")
            return
        
        # Export to CSV by vertical
        exported_files = export_to_csv(vertical_dfs)
        
        # Create combined export
        combined_file = create_combined_export(vertical_dfs)
        
        # Generate sales summary
        summary_file = generate_sales_summary(vertical_dfs)
        
        # Print summary report
        export_summary_report(vertical_dfs, exported_files)
        
        logger.info("="*60)
        logger.info("Export completed successfully")
        logger.info(f"Files created:")
        for vertical, filepath in exported_files.items():
            logger.info(f"  - {filepath}")
        logger.info(f"  - {combined_file}")
        logger.info(f"  - {summary_file}")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise


if __name__ == "__main__":
    main()