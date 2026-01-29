"""
Feature Engineering Module
Transforms raw business data into scored leads with features
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from pathlib import Path
import glob

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from config import (
    HIGH_DEMAND_CATEGORIES,
    SCORING_WEIGHTS,
    VERTICAL_RULES,
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR
)


def load_latest_raw_data():
    """
    Load the most recent raw data file
    
    Returns:
        pandas.DataFrame: Raw business data
    """
    
    pattern = str(RAW_DATA_DIR / 'businesses_*.csv')
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"No raw data files found in {RAW_DATA_DIR}")
    
    latest_file = max(files)
    logger.info(f"Loading data from: {latest_file}")
    
    df = pd.read_csv(latest_file)
    logger.info(f"Loaded {len(df)} records")
    
    return df


def clean_data(df):
    """
    Clean and standardize raw data
    
    Args:
        df: pandas.DataFrame with raw data
        
    Returns:
        pandas.DataFrame: Cleaned data
    """
    
    logger.info("Cleaning data")
    
    df = df.copy()
    
    # Fill missing numeric values
    df['review_count'] = df['review_count'].fillna(0).astype(int)
    df['rating'] = df['rating'].fillna(0).astype(float)
    df['price_level'] = df['price_level'].fillna(0).astype(int)
    
    # Extract city and state from source_location
    location_split = df['source_location'].str.split(',', expand=True)
    df['city'] = location_split[0].str.strip()
    df['state'] = location_split[1].str.strip() if len(location_split.columns) > 1 else ''
    
    # Clean phone numbers (remove formatting)
    df['phone'] = df['phone'].fillna('')
    
    # Clean website URLs
    df['website'] = df['website'].fillna('')
    
    logger.info(f"Data cleaned: {len(df)} records")
    
    return df


def engineer_features(df):
    """
    Create features for lead scoring
    
    Args:
        df: pandas.DataFrame with cleaned data
        
    Returns:
        pandas.DataFrame: Data with engineered features
    """
    
    logger.info("Engineering features")
    
    df = df.copy()
    
    # Feature 1: High-demand category flag
    df['high_demand_category'] = df['types'].apply(
        lambda x: any(cat in str(x).lower() for cat in HIGH_DEMAND_CATEGORIES)
    )
    
    # Feature 2: Review volume buckets
    df['review_volume'] = pd.cut(
        df['review_count'],
        bins=[0, 50, 200, 500, np.inf],
        labels=['Very Low', 'Low', 'Medium', 'High']
    )
    
    # Feature 3: Price accessibility (lower price = more accessible)
    df['is_affordable'] = df['price_level'].isin([0, 1, 2])
    
    # Feature 4: Rating quality
    df['high_rating'] = df['rating'] >= 4.0
    
    # Feature 5: Urban location indicator
    urban_cities = ['San Francisco', 'New York', 'Chicago', 'Los Angeles', 'Seattle']
    df['urban_location'] = df['city'].isin(urban_cities)
    
    # Feature 6: Simulated competitor presence (for demo purposes)
    # In production, this would come from web scraping or competitor API
    np.random.seed(42)
    df['on_ubereats'] = np.random.choice([True, False], size=len(df), p=[0.60, 0.40])
    df['on_grubhub'] = np.random.choice([True, False], size=len(df), p=[0.50, 0.50])
    
    # Feature 7: Vertical classification
    def classify_vertical(types_str):
        types_lower = str(types_str).lower()
        if any(term in types_lower for term in ['restaurant', 'food', 'meal_takeaway', 'meal_delivery', 'cafe']):
            return 'restaurants'
        elif any(term in types_lower for term in ['grocery', 'supermarket', 'store']):
            return 'grocery'
        elif any(term in types_lower for term in ['shopping', 'store', 'clothing_store', 'convenience_store']):
            return 'retail'
        else:
            return 'other'
    
    df['vertical'] = df['types'].apply(classify_vertical)
    
    logger.info(f"Features engineered for {len(df)} records")
    logger.info(f"  High demand category: {df['high_demand_category'].sum()} ({df['high_demand_category'].sum()/len(df)*100:.1f}%)")
    logger.info(f"  Urban location: {df['urban_location'].sum()} ({df['urban_location'].sum()/len(df)*100:.1f}%)")
    logger.info(f"  High rating: {df['high_rating'].sum()} ({df['high_rating'].sum()/len(df)*100:.1f}%)")
    
    return df


def calculate_lead_score(df):
    """
    Calculate 0-100 lead score for each business
    
    Args:
        df: pandas.DataFrame with engineered features
        
    Returns:
        pandas.DataFrame: Data with lead scores
    """
    
    logger.info("Calculating lead scores")
    
    scores = []
    breakdowns = []
    
    for idx, row in df.iterrows():
        score = 0
        breakdown = {}
        
        # Factor 1: Competitor Presence (25 points)
        if row['on_ubereats'] or row['on_grubhub']:
            points = SCORING_WEIGHTS['competitor_platform']
            score += points
            breakdown['Competitor Platform'] = points
        
        # Factor 2: Review Count (20 points max)
        if row['review_count'] > 500:
            points = SCORING_WEIGHTS['review_count_high']
            breakdown['Review Volume'] = points
        elif row['review_count'] > 200:
            points = SCORING_WEIGHTS['review_count_medium']
            breakdown['Review Volume'] = points
        elif row['review_count'] > 50:
            points = SCORING_WEIGHTS['review_count_low']
            breakdown['Review Volume'] = points
        else:
            points = 5
            breakdown['Review Volume'] = points
        score += points
        
        # Factor 3: High-Demand Category (20 points)
        if row['high_demand_category']:
            points = SCORING_WEIGHTS['high_demand_category']
            score += points
            breakdown['High Demand Category'] = points
        
        # Factor 4: Urban Location (15 points)
        if row['urban_location']:
            points = SCORING_WEIGHTS['urban_location']
            score += points
            breakdown['Urban Location'] = points
        
        # Factor 5: Rating (10 points)
        if row['high_rating']:
            points = SCORING_WEIGHTS['high_rating']
            score += points
            breakdown['High Rating'] = points
        
        # Factor 6: Price Accessibility (10 points)
        if row['is_affordable']:
            points = SCORING_WEIGHTS['affordable_price']
            score += points
            breakdown['Affordable Price'] = points
        
        scores.append(score)
        breakdowns.append(str(breakdown))
    
    df['lead_score'] = scores
    df['score_breakdown'] = breakdowns
    
    # Priority classification based on score
    df['priority'] = pd.cut(
        df['lead_score'],
        bins=[0, 50, 70, 85, 100],
        labels=['Low', 'Medium', 'High', 'Critical']
    )
    
    # Calculate contact by date based on priority
    def get_contact_date(priority):
        sla_days = {
            'Critical': 3,
            'High': 7,
            'Medium': 14,
            'Low': 30
        }
        days = sla_days.get(str(priority), 30)
        return (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d')
    
    df['contact_by_date'] = df['priority'].apply(get_contact_date)
    
    # Simulated conversion probability (for model validation)
    df['expected_conversion_prob'] = (
        0.12 +
        0.15 * df['on_ubereats'].astype(int) +
        0.10 * df['high_demand_category'].astype(int) +
        0.08 * (df['review_count'] > 200).astype(int) +
        0.05 * df['high_rating'].astype(int)
    ).clip(0, 1)
    
    logger.info(f"Scored {len(df)} leads")
    logger.info("Score Distribution:")
    for priority, count in df['priority'].value_counts().sort_index().items():
        logger.info(f"  {priority}: {count} ({count/len(df)*100:.1f}%)")
    
    return df


def validate_scoring_model(df):
    """
    Validate that scoring model makes business sense
    
    Args:
        df: pandas.DataFrame with lead scores
    """
    
    logger.info("="*60)
    logger.info("SCORING MODEL VALIDATION")
    logger.info("="*60)
    
    # Score distribution statistics
    logger.info("Score Distribution:")
    logger.info(f"  Mean: {df['lead_score'].mean():.1f}")
    logger.info(f"  Median: {df['lead_score'].median():.1f}")
    logger.info(f"  Std Dev: {df['lead_score'].std():.1f}")
    logger.info(f"  Min: {df['lead_score'].min():.0f}")
    logger.info(f"  Max: {df['lead_score'].max():.0f}")
    
    # Top leads
    logger.info("\nTop 10 Leads:")
    top_10 = df.nlargest(10, 'lead_score')[['name', 'city', 'lead_score', 'priority', 'review_count']]
    for idx, row in top_10.iterrows():
        name_truncated = row['name'][:40] if len(row['name']) > 40 else row['name']
        logger.info(f"  {name_truncated:40s} | {row['city']:15s} | Score: {row['lead_score']:3.0f} | {row['priority']}")
    
    # Correlation with expected conversion
    correlation = df['lead_score'].corr(df['expected_conversion_prob'])
    logger.info(f"\nCorrelation with Expected Conversion: {correlation:.3f}")
    if correlation > 0.7:
        logger.info("  Model shows strong predictive power")
    else:
        logger.warning("  Model correlation is lower than expected")
    
    # Score by vertical
    logger.info("\nAverage Score by Vertical:")
    for vertical, score in df.groupby('vertical')['lead_score'].mean().sort_values(ascending=False).items():
        count = len(df[df['vertical'] == vertical])
        logger.info(f"  {vertical:15s}: {score:.1f} (n={count})")
    
    # Priority distribution by vertical
    logger.info("\nPriority Distribution by Vertical:")
    priority_dist = pd.crosstab(df['vertical'], df['priority'], normalize='index') * 100
    for vertical in priority_dist.index:
        logger.info(f"  {vertical:15s}:")
        for priority in ['Low', 'Medium', 'High', 'Critical']:
            if priority in priority_dist.columns:
                pct = priority_dist.loc[vertical, priority]
                logger.info(f" {priority}: {pct:5.1f}%")
        logger.info("")
    
    logger.info("="*60)


def save_processed_data(df):
    """
    Save processed data to CSV
    
    Args:
        df: pandas.DataFrame with processed data
        
    Returns:
        str: Path to saved file
    """
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = PROCESSED_DATA_DIR / f'scored_leads_{timestamp}.csv'
    
    df.to_csv(output_path, index=False)
    logger.info(f"Saved processed data to {output_path}")
    
    return str(output_path)


def main():
    """Main execution function"""
    
    logger.info("="*60)
    logger.info("FEATURE ENGINEERING MODULE")
    logger.info("="*60)
    
    try:
        # Load raw data
        df = load_latest_raw_data()
        
        # Clean data
        df = clean_data(df)
        
        # Engineer features
        df = engineer_features(df)
        
        # Calculate scores
        df = calculate_lead_score(df)
        
        # Validate
        validate_scoring_model(df)
        
        # Save
        output_path = save_processed_data(df)
        
        logger.info("="*60)
        logger.info("Feature engineering completed successfully")
        logger.info(f"Output: {output_path}")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Feature engineering failed: {e}")
        raise


if __name__ == "__main__":
    main()