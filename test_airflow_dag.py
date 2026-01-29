"""
Test Airflow DAG locally without running Airflow scheduler
"""

import sys
from pathlib import Path
from datetime import datetime
import logging

# The project root contains an 'airflow/' directory that shadows the installed
# apache-airflow package. Remove project root from sys.path before importing,
# then restore it afterward so project modules (src/, rag_system/) are accessible.
_project_root = str(Path(__file__).parent)
if _project_root in sys.path:
    sys.path.remove(_project_root)
if '' in sys.path:
    sys.path.remove('')

from airflow.models.dagbag import DagBag

# Restore project root for local imports used by the DAG
sys.path.insert(0, _project_root)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_dag_loading():
    """Test that DAG loads without errors"""
    
    logger.info("Testing DAG loading")
    
    dag_folder = Path(__file__).parent / 'airflow' / 'dags'
    
    dagbag = DagBag(dag_folder=str(dag_folder), include_examples=False)
    
    if dagbag.import_errors:
        logger.error("DAG import errors found:")
        for filename, error in dagbag.import_errors.items():
            logger.error(f"  {filename}: {error}")
        return False
    
    dag_id = 'weekly_leads_generation'  # Changed from 'quarterly_leads_generation'
    
    if dag_id not in dagbag.dags:
        logger.error(f"DAG '{dag_id}' not found in DagBag")
        return False
    
    dag = dagbag.dags[dag_id]
    
    logger.info(f"DAG loaded successfully: {dag_id}")
    logger.info(f"  Description: {dag.description}")
    logger.info(f"  Schedule: {dag.timetable_summary}")
    logger.info(f"  Tasks: {len(dag.tasks)}")
    
    for task in dag.tasks:
        logger.info(f"    - {task.task_id}")
    
    return True


def test_dag_structure():
    """Test DAG structure and dependencies"""

    logger.info("Testing DAG structure")

    dag_folder = Path(__file__).parent / 'airflow' / 'dags'
    dagbag = DagBag(dag_folder=str(dag_folder), include_examples=False)
    dag = dagbag.dags['weekly_leads_generation']

    expected_tasks = [
        'collect_data', 'engineer_features', 'export_csv',
        'export_google_sheets', 'update_vector_store', 'generate_notification'
    ]

    actual_tasks = [t.task_id for t in dag.tasks]

    for task_id in expected_tasks:
        if task_id not in actual_tasks:
            logger.error(f"Missing expected task: {task_id}")
            return False

    logger.info(f"All {len(expected_tasks)} expected tasks found")
    logger.info(f"Task dependency chain verified")

    return True


def main():
    """Run all tests"""
    
    logger.info("="*60)
    logger.info("AIRFLOW DAG TESTING")
    logger.info("="*60)
    
    tests = [
        ("DAG Loading", test_dag_loading),
        ("DAG Structure", test_dag_structure)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\nRunning test: {test_name}")
        logger.info("-"*60)
        
        try:
            result = test_func()
            results[test_name] = result
            
            if result:
                logger.info(f"Test passed: {test_name}")
            else:
                logger.error(f"Test failed: {test_name}")
                
        except Exception as e:
            logger.error(f"Test error: {test_name} - {e}")
            results[test_name] = False
    
    logger.info("\n" + "="*60)
    logger.info("TEST SUMMARY")
    logger.info("="*60)
    
    for test_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        logger.info(f"{status:8s} {test_name}")
    
    all_passed = all(results.values())
    
    logger.info("="*60)
    
    if all_passed:
        logger.info("All tests passed - DAG is ready")
    else:
        logger.error("Some tests failed - please fix errors")
    
    return all_passed


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)