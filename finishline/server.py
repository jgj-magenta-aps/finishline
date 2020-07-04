import flask
import json
import pathlib
from sqlalchemy import and_, or_
from finishline.settings import settings
from finishline.app import app, db, Jobsonl, State, Stack, Job, Server, ServerJob
import collections
import datetime

libpath = pathlib.Path(__file__).parent

document="""<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="refresh" content="10; url=/">
    <title>%(title)s</title>
    <link rel="stylesheet" href="/static/bulma.css">
  </head>
  <body>
  <section class="section">
    <div class="container">
      <h1 class="title">
        %(title)s
      </h1>
      %(content)s
    </div>
  </section>
  </body>
</html>
"""



def store_jobsonl(jsonl):

    # find/create server if it does not exist:
    server = db.session.query(Server).filter(
        Server.name == jsonl["server"]
    ).one_or_none()
    if not server:
        server = Server(name=jsonl["server"])
        db.session.add(server)

    # find/create job if it does not exist:
    job = db.session.query(Job).filter(
        Job.name == jsonl[settings["finishline.job.name"]],
    ).one_or_none()
    if not job:
        job = Job(name=jsonl[settings["finishline.job.name"]])
        db.session.add(job)

    # make the line, as it does most probably not exist
    incoming = Jobsonl(
        server = server,
        job = job,
        status=jsonl[settings["finishline.job.status"]],
        date=datetime.datetime.strptime(
            jsonl[settings["finishline.job.date"]], #"%Y-%m-%dT%H:%M:%Sz"
            settings["finishline.job.date.format"]
        ),
        timestamp=datetime.datetime.strptime(
            jsonl[settings["finishline.job.timestamp"]], #"%Y-%m-%d
            settings["finishline.job.timestamp.format"]
        ),
        raw = json.dumps(jsonl)
    )

    # look for an original 
    original = db.session.query(Jobsonl).filter(and_(
        Jobsonl.job == job,
        Jobsonl.server == incoming.server,
        Jobsonl.date == incoming.date,
        or_(
            Jobsonl.timestamp == incoming.timestamp,
            Jobsonl.timestamp == None
        )
    )).one_or_none()

    if original is not None:
        return original
    else:
        db.session.add(incoming)
        db.session.commit()
        return incoming

def calc_expected(what, state):
    if what == "start":
        return state.actual_start + datetime.timedelta(days=1)
    else:
        return state.actual_end + datetime.timedelta(days=1)

def handle_meta(jobsonl):
    server = db.session.query(Server).filter(
        Server.name == jobsonl.server
    ).one()

    if jobsonl.name == "job-runner enabled-jobs":
        serverjobs = {
            k.split("crontab.")[1]: True
            for k in json.loads(jobsonl.raw)
            if k.startswith("crontab.RUN")
        }
        serverjobs.update({
            k.split("crontab.")[1]: False
            for k in json.loads(jobsonl.raw)
            if "crontab.RUN" in k and not k in serverjobs
        })
        for indicator, enabled in serverjobs.items():
            job = db.session.query(Job).filter(
                Job.indicator == indicator
            ).one_or_none()
            if job is None:
                job = upsert_job(indicator)

            serverjob = db.session.query(ServerJob).filter(and_(
                ServerJob.job == job,
                ServerJob.server == server
            )).one_or_none()
            if serverjob is None:
                serverjob = ServerJob(
                    job=job,
                    server = server
                )
                db.session.add(serverjob)
            if serverjob.enabled != enabled:
                if serverjob.enabled == None:
                    serverjob.text = "first seen"
                else:
                    serverjob.text = "enabled change"
                serverjob.enabled = enabled
                serverjob.timestamp = jobsonl.timestamp

        db.session.commit()

def get_server_state(server, date):
    # if it is there return it
    server_state = db.session.query(State).filter(and_(
        State.jobdate == date,
        State.name == "Job-Runner",
        State.server == server
    )).one_or_none()

    # if it is not there, calc job states first
    job_states = get_job_states(server, date)
    if not server_state:
        server_state = State(
            jobdate=date,
            expected_start=None,
            expected_end=None,
            name="Job-Runner",
            server=server,
            servername=server.name,
        )
        db.session.add(server_state)
    if len(job_states):
        server_state.expected_start = job_states[0].expected_start
        server_state.expected_end = job_states[-1].expected_end
        server_state.status = job_states[-1].status

    for job_state in job_states:
        job_state.parent = server_state
        server_state.status = job_state.status
    db.session.commit()
    return server_state

def get_server_states(date):
    states = [
        get_server_state(server, date)
        for server in db.session.query(Server).all()
    ]
    return states

def get_job_state(job, server, date):
    import pdb; pdb.set_trace()
    job_state = db.session.query(State).filter(and_(
        State.jobdate == date,
        State.name == job.name,
        State.server == server
    )).one_or_none()
    if job_state is not None:
        if job_state.actual_end is not None:
            #finished
            return job_state

    actual_start_job = db.session.query(Jobsonl).filter(and_(
        Jobsonl.server == server,
        Jobsonl.job == job,
        Jobsonl.date == date,
        Jobsonl.status == settings["finishline.job.status.starting"],
    )).first()
    if job_state and actual_start_job:
        job_state.actual_start = actual_start_job.timestamp

    actual_end_job = db.session.query(Jobsonl).filter(and_(
        Jobsonl.server == server,
        Jobsonl.job == job,
        Jobsonl.date == date,
        Jobsonl.status.in_ [
            settings["finishline.job.status.success"],
            settings["finishline.job.status.failure"],
        ]
    )).first()
    if job_state and actual_end_job:
        job_state.actual_start = actual_end_job.timestamp
        job_state.status = actual_end_job.status

    job_state = State(
        jobdate=date,
        expected_start=None,
        expected_end=None,
        name=job.name,
        server=server,
        servername=server.name,
    )
    if actual_start_job:
        job_state.actual_start = actual_start_job.timestamp
    if actual_end_job:
        job_state.actual_end = actual_end_job.timestamp
        job_state.status = actual_end_job.status
        db.session.add(job.state)

    # set expected times based on sliding window of max 5
    sw=5

    counter = collections.Counter()
    jobs = db.session.query(Jobsonl).filter(and_(
        Jobsonl.server == server,
        Jobsonl.job == job,
        Jobsonl.status == settings["finishline.job.status.success"],
    )).order_by(id.desc()).limit(sw).all()

    for job in jobs:
        counter["start"] += (job.actual_start - job.date).total_seconds()
        counter["end"] += (job.actual_end - job.date).total_seconds()

    if len(jobs):
        job_state.expected_start = (job_state.jobdate
            ) + datetime.timedelta(seconds = counter["start"] / len(jobs))
        job_state.expected_end = (job_state.jobdate
            ) + datetime.timedelta(seconds = counter["end"] / len(jobs))

    db.session.commit()
    return server_state



def get_job_states(server, date):
    job_states = [
        get_job_state(job, server, date)
        for job in db.session.query(ServerJob).filter(and_(
            ServerJob.server == server,
            ServerJob.enabled == True
        )).all()
    ]
    return job_states


def get_state(stateid=None, date=datetime.date.today()):
    # date only used when no stateid is given
    if stateid is None:
        return get_server_states(date)
    else:
        parentstate = db.session.query(State).get(stateid)
        return parentstate.children


@app.route("/report/", defaults={"server": None})
def report(server):
    incoming = json.loads(flask.request.form.get("jobsonl"))
    incoming["server"] = flask.request.form["server"]
    jobsonl = store_jobsonl(incoming)
    if jobsonl is None:
        return flask.jsonify({"info":"duplicate"})
    elif jobsonl.name in settings["finishline.job.name.ignored"]:
        return flask.jsonify({"info":"ignored"})
    elif jobsonl.name in settings["finishline.job.name.meta"]:
        handle_meta(jobsonl)
        return flask.jsonify({"info":"meta"})
    else:
        return flask.jsonify({"info":"jobstatus"})


@app.route("/")
def page():
    # serve the status view
    stateid = flask.request.args.get("stateid")
    date = flask.request.args.get("date", datetime.date.today())

    class Header:
        id = ""
        name = "Jobnavn"
        servername = "Servernavn"
        expected_start = "Forventet start"
        expected_end = "Forventet slut"
        actual_start = "Aktuel Start"
        actual_end = "Aktuel Slut"
        status = "Jobstatus"
        statustxt = "Text"

    def paintstate(state, td): #th/td
        classes = ""
        onclick = ""
        if td == "td":
            onclick=f"onclick='window.location=\"/?stateid={state.id}\"'"
        if state.status == settings["finishline.job.status.success"]:
            classes = "has-background-success"
        elif state.status == settings["finishline.job.status.failure"]:
            classes = "has-background-danger"
        return "".join([
            f"<tr class=\"{classes}\" {onclick}>",
            f"<{td}>{state.servername}</{td}>",
            f"<{td}>{state.name}</{td}>",
            f"<{td}>{state.expected_start or ''}</{td}>",
            f"<{td}>{state.expected_end or ''}</{td}>",
            f"<{td}>{state.actual_start or ''}</{td}>",
            f"<{td}>{state.actual_end or ''}</{td}>",
            f"<{td}>{state.status or ''}</{td}>",
            f"<{td}>{state.statustxt or ''}</{td}>",
            f"</tr>",
        ])

    states = get_state(stateid, date)
    legend = paintstate(Header, 'th')
    rows = "\n".join([paintstate(s, 'td') for s in states])
    content = (
        f"<table class=\"table is-fullwidth\">\n<thead>{legend}</thead>" + 
        f"\n<tfoot>{legend}</tfoot>\n<tbody>{rows}</tbody></table>"
    )
    title = "Finishline - for job-runners"
    if stateid is not None:
        title += " - drill down, resets after 10"
    return document % locals()

def upsert_job(indicator, name=None, showname=None):
    job = db.session.query(Job).filter(
        Job.indicator == indicator
    ).one_or_none()
    if job is None:
        job = Job(
            indicator=indicator,
        )
        db.session.add(job)
    if name is not None:
        job.name = name
    if showname is not None:
        job.showname = showname
    return job

def insert_jobs():
    # find jobs and indicators in source code
    jr = "https://raw.githubusercontent.com/OS2mo/os2mo-data-import-and-export/development/tools/job-runner.sh"
    import re
    import urllib.request
    from urllib.error import URLError
    jrsh = pathlib.Path(settings["finishline.job-runner.sh"])
    try:
        jr = urllib.request.urlopen(jr).read().decode()
        jrsh.write_text(jr)
    except URLError:
        jr = jrsh.read_text()
    for indicator, name in re.findall(
        "(RUN[_A-Z0-9]*).*\n *run-job ([_a-z0-9]*)", jr, re.MULTILINE
    ):
        upsert_job(indicator, name)

    db.session.commit()



if __name__ == "__main__":
    db.create_all()
    try:
        insert_jobs()
    except:
        pass
    app.run()

