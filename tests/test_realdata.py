import unittest
import json
from freezegun import freeze_time
import datetime
import pathlib

jobsonlines = [
    {"time":"2020-03-20T11:32:25 +0100","date":"2020-03-20","batch":"17","job":"job-runner total-status","job-status":"success","imports-ok":"true","exports-ok":"true","reports-ok":"true","backup-ok":"true"},
    {"time":"2020-03-20T11:34:01 +0100","date":"2020-03-20","batch":"18","job":"imports","job-status":"starting"},
    {"time":"2020-03-20T11:34:01 +0100","date":"2020-03-20","batch":"18","job":"imports_opus_diff_import","job-status":"starting"},
    {"time":"2020-03-20T11:34:01 +0100","date":"2020-03-20","batch":"18","job":"imports_opus_diff_import","job-status":"failed"},
    {"time":"2020-03-20T11:34:01 +0100","date":"2020-03-20","batch":"18","job":"imports","job-status":"failed"},
    {"time":"2020-03-20T11:34:01 +0100","date":"2020-03-20","batch":"18","job":"exports","job-status":"starting"},
    {"time":"2020-03-20T11:34:01 +0100","date":"2020-03-20","batch":"18","job":"exports_cpr_uuid","job-status":"starting"},
    {"time":"2020-03-20T11:37:22 +0100","date":"2020-03-20","batch":"18","job":"exports_cpr_uuid","job-status":"success"},
    {"time":"2020-03-20T11:37:22 +0100","date":"2020-03-20","batch":"18","job":"exports","job-status":"success"},
    {"time":"2020-03-20T11:37:22 +0100","date":"2020-03-20","batch":"18","job":"reports","job-status":"starting"},
    {"time":"2020-03-20T11:37:22 +0100","date":"2020-03-20","batch":"18","job":"reports","job-status":"success"},
    {"time":"2020-03-20T11:37:22 +0100","date":"2020-03-20","batch":"18","job":"job-runner enabled-jobs","job-status":"info","crontab.RUN_OPUS_DIFF_IMPORT":"true","Xcrontab.RUN_CHECK_AD_CONNECTIVITY":"true","crontab.RUN_CPR_UUID":"true"},
    {"time":"2020-03-20T11:37:22 +0100","date":"2020-03-20","batch":"18","job":"job-runner version-info","job-status":"info","git-commit":"b98754e8f9f6ac29dc821f0786868cf941b99f84"},
    {"time":"2020-03-20T11:37:23 +0100","date":"2020-03-20","batch":"18","job":"job-runner total-status","job-status":"success","imports-ok":"true","exports-ok":"true","reports-ok":"true","backup-ok":"true"},
]

testdb = pathlib.Path(__file__).parent / 'tests.db'
alldata = pathlib.Path(__file__).parent.parent / 'data'


class Tests(unittest.TestCase):

    def insert_one(self, server, jobsonl):
        response = self.app.get(f"/report/", data={"server": server, "jobsonl": json.dumps(jobsonl)})
        return response 


    def setUp(self):
        self.db.drop_all()
        self.db.create_all()
        self.insert_jobs()

    @classmethod
    def setUpClass(cls):
        from finishline.settings import settings
        settings.update({
            #"finishline.sqlalchemy.engine.uri": f"sqlite:///{testdb}",
            "finishline.sqlalchemy.engine.echo": True,
            "finishline.job.names.ignored":["job-runner total-status", ],
            "finishline.job.names.meta":["job-runner enabled-jobs", "job-runner version-info"],
            "finishline.job.name": "job",
            "finishline.job.status": "job-status",
            "finishline.job.status.info": "info",
            "finishline.job.timestamp": "time",
            "finishline.job.date": "date",
        })
        from finishline.server import app, db, get_state, insert_jobs, ServerJob
        cls.get_state = staticmethod(get_state)
        cls.insert_jobs = staticmethod(insert_jobs)
        cls.app = app.test_client()
        cls.db = db
        cls.ServerJob = ServerJob


    @freeze_time("2020-03-20")
    def test_01(self):
        # job-runner total-status is ignored
        response = self.insert_one("test-server", jobsonlines[0])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["info"], "ignored")

    @freeze_time("2020-03-20")
    def test_02_imports(self):
        # job-runner total-status is ignored
        response = self.insert_one("test-server", jobsonlines[1])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["info"], "jobstatus")

    @freeze_time("2020-03-20")
    def test_03_enabled_jobs(self):
        # job-runner total-status is ignored
        response = self.insert_one("test-server", jobsonlines[11])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["info"], "meta")
        serverjobs = self.db.session.query(self.ServerJob).all()
        self.assertEqual(len(serverjobs),3)

    @freeze_time("2020-03-20")
    def test_04(self):
        for i in jobsonlines:
            response = self.insert_one("test-server", i)

    def test_05_all_data(self):
        for i in alldata.iterdir():
            servername = i.stem
            print(servername)
            for i in i.read_text().split("\n"):
                print(i)
                self.insert_one(server=str(servername), jobsonl=json.loads(i))


