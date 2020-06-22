settings = {
    "finishline.expects.check.every.x.seconds": 10,
    "finishline.expects.report.every.seconds": 3600,
    "finishline.sqlalchemy.engine.uri": "sqlite:///finishline.db",
    "finishline.sqlalchemy.engine.echo": False,
    "finishline.job.names.ignored":["job-runner total-status", ],
    "finishline.job.names.meta": ["job-runner version-info", "job-runner enabled-jobs"],
    "finishline.job.name": "job",
    "finishline.job.status": "job-status",
    "finishline.job.status.starting": "starting",
    "finishline.job.status.success": "success",
    "finishline.job.status.failure": "failed",
    "finishline.job.timestamp": "time",
    "finishline.job.timestamp.format": "%Y-%m-%dT%H:%M:%S %z"
}
