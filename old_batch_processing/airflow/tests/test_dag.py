from airflow.models import DagBag
import unittest
import os


class TestBatchProcessingDag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag()

    def assertDagDictEqual(self, source, dag):
        self.assertEqual(dag.task_dict.keys(), source.keys())
        for task_id, downstream_list in source.items():
            self.assertTrue(
                dag.has_task(task_id), msg="Missing task_id: {} in dag".format(task_id)
            )
            task = dag.get_task(task_id)
            self.assertEqual(
                task.downstream_task_ids,
                set(downstream_list),
                msg="unexpected downstream link in {}".format(task_id),
            )

    def test_dag_loaded(self):
        dag = self.dagbag.get_dag(dag_id="batch_processing")
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 2)

    def test_dag_has_correct_tasks(self):
        dag = self.dagbag.get_dag(dag_id="batch_processing")
        self.assertDagDictEqual(
            {"create_job_flow": ["check_job_flow"], "check_job_flow": []}, dag
        )
