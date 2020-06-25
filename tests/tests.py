import unittest
import json
from freezegun import freeze_time
import datetime
import pathlib

testdb = pathlib.Path(__file__).parent / 'tests.db'

job_runner = {
    "server2": [
        {"job": "imports", "job-status": "starting", "date": "2030-01-14", "time": "2030-01-14T12:00:00z"},
        {"job": "imports_do_thing", "job-status": "starting", "date": "2030-01-14", "time": "2030-01-14T12:00:00z"},
        {"job": "imports_do_thing", "job-status": "success", "date": "2030-01-14", "time": "2030-01-14T12:10:00z"},
        {"job": "imports_do_other", "job-status": "starting", "date": "2030-01-14", "time": "2030-01-14T12:10:00z"},
        {"job": "imports_do_other", "job-status": "success", "date": "2030-01-14", "time": "2030-01-14T12:20:00z"},
        {"job": "imports", "job-status": "success", "date": "2030-01-14", "time": "2030-01-14T12:20:00z"},
    ],
    "server1": [
        {"job": "imports", "job-status": "starting", "date": "2030-01-14", "time": "2030-01-14T10:00:00z"},
        {"job": "imports_do_thing", "job-status": "starting", "date": "2030-01-14", "time": "2030-01-14T10:00:00z"},
        {"job": "imports_do_thing", "job-status": "success", "date": "2030-01-14", "time": "2030-01-14T10:10:00z"},
        {"job": "imports_do_other", "job-status": "starting", "date": "2030-01-14", "time": "2030-01-14T10:10:00z"},
        {"job": "imports_do_other", "job-status": "failed", "date": "2030-01-14", "time": "2030-01-14T10:20:00z"},
        {"job": "imports", "job-status": "failed", "date": "2030-01-14", "time": "2030-01-14T10:20:00z"},
    ]
}



class Tests(unittest.TestCase):

    def setUp(self):
        self.db.drop_all()
        self.db.create_all()

    @classmethod
    def setUpClass(cls):
        from finishline.settings import settings
        settings.update(
            {
                "finishline.sqlalchemy.engine.uri": f"sqlite:///{testdb}",
                "finishline.job.timestamp.format": "%Y-%m-%dT%H:%M:%Sz",
                "finishline.sqlalchemy.engine.echo": True,
            }
        )
        from finishline.server import app, db, get_state, insert_jobs
        cls.db = db
        cls.get_state = staticmethod(get_state)
        cls.insert_jobs = staticmethod(insert_jobs)
        cls.app = app.test_client()
        cls.settings = settings

    def insert_all(self):
        for server, joblines in job_runner.items():
            for jobsonl in joblines:
                response = self.app.get(f"/report/", data={"server": server, "jobsonl": json.dumps(jobsonl)})
                self.assertEqual(response.status_code, 200)

    @freeze_time("2030-01-14")
    def test1(self):
        self.insert_all()

        # vi får to serverstates
        serverstates = self.get_state(date=datetime.date.today())
        self.assertEqual(serverstates[0].status, self.settings["finishline.job.status.success"])
        self.assertEqual(serverstates[1].status, self.settings["finishline.job.status.failure"])

        # fejlibesked kommer fra laveste nieveau
        self.assertEqual(serverstates[1].statustxt, "imports_do_other afsluttet med failed")

        # der er kun en sectionstate, imports
        sectionstates = self.get_state(serverstates[1].id)
        self.assertEqual(sectionstates[0].name, "imports")

        # imports skal have fået både status og statustext
        self.assertEqual(sectionstates[0].status, self.settings["finishline.job.status.failure"]) 
        self.assertEqual(sectionstates[0].statustxt, "imports_do_other afsluttet med failed") 

        # der er to jobs, der er kørt under imports
        importstates = self.get_state(sectionstates[0].id)
        self.assertEqual(len(importstates), 2)

        # det sidste er fejlet
        self.assertEqual(importstates[1].status, self.settings["finishline.job.status.failure"])

        # hent siden
        response = self.app.get("/?date=2030-01-14")
        print(response.data)

    def test_insert_jobs(self):
        self.insert_jobs()





