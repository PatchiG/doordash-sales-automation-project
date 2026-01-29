"""
Quarterly Leads Generation DAG
Automates the complete pipeline: data collection, feature engineering, export, and RAG update
"""

from airflow import DAG # type: ignore
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path
import logging

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'gtm_team',
    'depends_on_past': False,
    'email': ['gtm-team@doordash.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weekly_leads_generation',
    default_args=default_args,
    description='Automated weekly leads list generation and RAG system update',
    schedule='0 0 * * 1',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['sales', 'leads', 'gtm', 'automation', 'weekly']
)


def task_collect_data(**context):
    """
    Task 1: Collect business data from Google Places API
    """
    from data_collection import collect_multi_city_data
    
    logger.info("Starting data collection task")
    
    try:
        df = collect_multi_city_data()
        
        row_count = len(df)
        
        logger.info(f"Data collection completed: {row_count} businesses")
        
        context['ti'].xcom_push(key='businesses_collected', value=row_count)
        
        return row_count
        
    except Exception as e:
        logger.error(f"Data collection failed: {e}")
        raise


def task_engineer_features(**context):
    """
    Task 2: Engineer features and calculate lead scores
    """
    from feature_engineering import (
        load_latest_raw_data,
        clean_data,
        engineer_features,
        calculate_lead_score,
        validate_scoring_model,
        save_processed_data
    )
    
    logger.info("Starting feature engineering task")
    
    try:
        df = load_latest_raw_data()
        
        df = clean_data(df)
        
        df = engineer_features(df)
        
        df = calculate_lead_score(df)
        
        validate_scoring_model(df)
        
        output_path = save_processed_data(df)
        
        row_count = len(df)
        avg_score = df['lead_score'].mean()
        
        logger.info(f"Feature engineering completed: {row_count} leads, avg score: {avg_score:.1f}")
        
        context['ti'].xcom_push(key='leads_scored', value=row_count)
        context['ti'].xcom_push(key='avg_lead_score', value=float(avg_score))
        context['ti'].xcom_push(key='output_path', value=output_path)
        
        return output_path
        
    except Exception as e:
        logger.error(f"Feature engineering failed: {e}")
        raise


def task_export_csv(**context):
    """
    Task 3: Export leads to CSV files by vertical
    """
    from export_leads import (
        load_latest_processed_data,
        apply_vertical_filters,
        export_to_csv,
        create_combined_export,
        generate_sales_summary
    )
    
    logger.info("Starting CSV export task")
    
    try:
        df = load_latest_processed_data()
        
        vertical_dfs = apply_vertical_filters(df)
        
        exported_files = export_to_csv(vertical_dfs)
        
        combined_file = create_combined_export(vertical_dfs)
        
        summary_file = generate_sales_summary(vertical_dfs)
        
        total_exported = sum(len(df) for df in vertical_dfs.values())
        
        logger.info(f"CSV export completed: {total_exported} leads")
        
        context['ti'].xcom_push(key='total_exported', value=total_exported)
        context['ti'].xcom_push(key='exported_files', value=exported_files)
        context['ti'].xcom_push(key='combined_file', value=combined_file)
        context['ti'].xcom_push(key='summary_file', value=summary_file)
        
        return exported_files
        
    except Exception as e:
        logger.error(f"CSV export failed: {e}")
        raise


def task_update_vector_store(**context):
    """
    Task 5: Update RAG vector store with new leads
    """
    import sys
    from pathlib import Path
    
    rag_path = Path(__file__).parent.parent.parent / 'rag_system'
    sys.path.insert(0, str(rag_path))
    
    from create_vectorstore import (
        load_latest_scored_leads,
        create_documents_from_leads,
        create_vector_store
    )
    
    logger.info("Starting vector store update task")
    
    try:
        df = load_latest_scored_leads()
        
        documents = create_documents_from_leads(df)
        
        vectorstore = create_vector_store(documents)
        
        collection_size = vectorstore._collection.count()
        
        logger.info(f"Vector store updated: {collection_size} documents")
        
        context['ti'].xcom_push(key='vector_store_size', value=collection_size)
        
        return collection_size
        
    except Exception as e:
        logger.error(f"Vector store update failed: {e}")
        raise


def task_generate_notification(**context):
    """
    Task 6: Generate completion notification message
    """
    logger.info("Generating completion notification")
    
    ti = context['ti']
    
    businesses_collected = ti.xcom_pull(key='businesses_collected', task_ids='collect_data')
    leads_scored = ti.xcom_pull(key='leads_scored', task_ids='engineer_features')
    avg_score = ti.xcom_pull(key='avg_lead_score', task_ids='engineer_features')
    total_exported = ti.xcom_pull(key='total_exported', task_ids='export_csv')
    vector_store_size = ti.xcom_pull(key='vector_store_size', task_ids='update_vector_store')

    # Changed: Get week number instead of quarter
    week_number = datetime.now().isocalendar()[1]
    year = datetime.now().year

    notification_html = f"""
    <h2>Weekly Leads Generation Complete - Week {week_number}, {year}</h2>

    <h3>Pipeline Summary:</h3>
    <ul>
        <li>Businesses Collected: {businesses_collected}</li>
        <li>Leads Scored: {leads_scored}</li>
        <li>Average Lead Score: {avg_score:.1f}</li>
        <li>Total Exported: {total_exported}</li>
        <li>Vector Store Size: {vector_store_size}</li>
    </ul>

    <h3>Next Steps:</h3>
    <ol>
        <li>Review exported CSV files in data/output/</li>
        <li>Distribute to sales teams</li>
        <li>Prioritize High/Critical priority leads</li>
        <li>RAG system updated and ready for queries</li>
    </ol>

    <p><strong>System Status:</strong> All tasks completed successfully</p>
    """
    
    context['ti'].xcom_push(key='notification_html', value=notification_html)
    
    logger.info("Notification generated")
    
    return notification_html


collect_data = PythonOperator(
    task_id='collect_data',
    python_callable=task_collect_data,
    dag=dag,
)

engineer_features_task = PythonOperator(
    task_id='engineer_features',
    python_callable=task_engineer_features,
    dag=dag,
)

export_csv_task = PythonOperator(
    task_id='export_csv',
    python_callable=task_export_csv,
    dag=dag,
)

update_vector_store_task = PythonOperator(
    task_id='update_vector_store',
    python_callable=task_update_vector_store,
    dag=dag,
)

generate_notification_task = PythonOperator(
    task_id='generate_notification',
    python_callable=task_generate_notification,
    dag=dag,
)

collect_data >> engineer_features_task >> export_csv_task >> update_vector_store_task >> generate_notification_task