"""DAG integrity tests"""
from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_FOLDER = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(dag_folder=DAGS_FOLDER)


def test_no_import_errors(dag_bag):
    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"


def test_every_dag_has_tags_and_retries(dag_bag):
    assert dag_bag.dags, "no DAGs found"
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
        assert dag.default_args.get("retries", 0) >= 2, f"{dag_id} needs retries >= 2"


def test_steam_daily_contract(dag_bag):
    dag = dag_bag.dags["steam_daily"]
    assert dag.schedule == "0 12 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1

    order = ["collection", "upload_raw", "s3_to_bq", "dbt_build"]
    assert set(dag.task_ids) == set(order)
    for upstream, downstream in zip(order, order[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids, f"{upstream} must run before {downstream}"