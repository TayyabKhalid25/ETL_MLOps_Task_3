"""DAG integrity and task dependency validation."""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors(dag_bag):
    assert not dag_bag.import_errors, f"DAG import errors detected: {dag_bag.import_errors}"


def test_currency_exchange_etl_structure(dag_bag):
    dag = dag_bag.get_dag("currency_exchange_etl")
    assert dag is not None, "currency_exchange_etl DAG was not found in DagBag"

    task_ids = set(dag.task_dict.keys())
    expected_tasks = {
        "extract_currency_rates",
        "transform_exchange_rates",
        "load_to_mongodb",
    }
    assert task_ids == expected_tasks, f"Expected {expected_tasks}, found {task_ids}"

    extract_task = dag.get_task("extract_currency_rates")
    transform_task = dag.get_task("transform_exchange_rates")
    load_task = dag.get_task("load_to_mongodb")

    assert transform_task in extract_task.downstream_list
    assert load_task in transform_task.downstream_list
