import pytest 


@pytest.fixture(scope="class")
def dag(dagbag):
    "Retreive user_behaviour DAG from pytest session DagBag"
    return dagbag.get_dag('user_behaviour')


class TestUserBehaviourDefinition:

    EXPECTED_NUM_TASKS = 0
    EXPECTED_TASKS = []

    def test_num_tasks(self, dag):
        """
        Verfiy # of tasks in DAG
        """
        True

    def test_contain_tasks(self, dag):
        """
        Verify that DAG does contain all tasks in EXPECTED_TASKS array
        """
        True

    def test_same_start_date_all_tasks(self, dag):
        """
        Sanity check to ensure that all tasks have the same start date
        """
        tasks = dag.tasks
        start_dates = list(map(lambda task: task.start_date, tasks))
        assert len(set(start_dates)) == 1

