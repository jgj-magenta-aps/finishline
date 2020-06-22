import flask
import json
import pathlib
from sqlalchemy import and_, or_
from finishline.settings import settings
from finishline.app import app, db, Jobsonl, State, Stack, Job, Server, ServerJob
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

    # find/create jobsonl if does not exist
    jobsonl = db.session.query(Jobsonl).filter(and_(
        Jobsonl.name == jsonl[settings["finishline.job.name"]],
        Jobsonl.server == jsonl["server"],
        Jobsonl.timestamp == jsonl[settings["finishline.job.timestamp"]],
    )).one_or_none()
    if jobsonl is None:
        jobsonl = Jobsonl(
            server = jsonl["server"],
            name=jsonl[settings["finishline.job.name"]],
            status=jsonl[settings["finishline.job.status"]],
            timestamp=datetime.datetime.strptime(
                jsonl[settings["finishline.job.timestamp"]], #"%Y-%m-%dT%H:%M:%Sz"
                settings["finishline.job.timestamp.format"]
            ),
            raw = json.dumps(jsonl)
        )
        db.session.add(jobsonl)
    db.session.commit()
    return jobsonl


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

            
def handle_jobstatus(jobsonl):
    #today
    stateservertoday = db.session.query(State).filter(and_(
        State.jobdate == jobsonl.timestamp.date(),
        State.name == "job-runner",
        State.server == jobsonl.server
    )).one_or_none()

    if stateservertoday is None:
        stateservertoday = State(
            server = jobsonl.server,
            name = "job-runner",
            jobdate = jobsonl.timestamp.date(), 
            status = "starting",
        )
        db.session.add(stateservertoday)

    statetoday = db.session.query(State).filter(and_(
        State.jobdate == jobsonl.timestamp.date(),
        State.name == jobsonl.name
    )).one_or_none()

    if statetoday is None:
        statetoday = State(
            server = jobsonl.server,
            name = jobsonl.name,
            jobdate = jobsonl.timestamp.date(), 
            parent = stateservertoday
        )
        db.session.add(statetoday)

    # tomorrow
    stateservertomorrow = db.session.query(State).filter(and_(
        State.jobdate == (jobsonl.timestamp.date() + datetime.timedelta(days=1)),
        State.name == "job-runner",
        State.server == jobsonl.server
    )).one_or_none()

    if stateservertomorrow is None:
        stateservertomorrow = State(
            server = jobsonl.server,
            name = "job-runner",
            jobdate = jobsonl.timestamp.date() + datetime.timedelta(days=1),
        )
        db.session.add(stateservertomorrow)

    statetomorrow = db.session.query(State).filter(and_(
        State.jobdate == (jobsonl.timestamp.date() + datetime.timedelta(days=1)),
        State.name == jobsonl.name
    )).one_or_none()

    if statetomorrow is None:
        statetomorrow = State(
            name = jobsonl.name,
            jobdate = jobsonl.timestamp.date() + datetime.timedelta(days=1),
            status = "await",
            parent = stateservertomorrow
        )
        db.session.add(statetomorrow)
  
    statetoday.status = jobsonl.status

    stackparent = db.session.query(Stack).filter(and_(
        Stack.server == jobsonl.server,
    )).order_by(Stack.id.desc()).first()

    if not stackparent:
        stackparent = Stack(
            server = jobsonl.server,
            name = "job-runner",
            state = stateservertoday
        )
        db.session.add(stackparent)

    if jobsonl.status == settings["finishline.job.status.starting"]:
        statetoday.parent = stackparent.state # add my parent
        stackme = Stack( # make myself a parent
            server = jobsonl.server,
            name = jobsonl.name,
            state = statetoday
        )
        statetoday.statustxt = "kører"
        statetoday.parent.statustxt = f"kører {jobsonl.name}"
        db.session.add(stackme)
        statetoday.actual_start = jobsonl.timestamp
        statetomorrow.expected_start = calc_expected("start", statetoday)
    else:
        db.session.delete(stackparent) # stackparent should be me
        statetoday.actual_end = jobsonl.timestamp
        statetoday.status = jobsonl.status
        if statetoday.parent.status != settings["finishline.job.status.failure"]:
            statetoday.parent.status = jobsonl.status 

        # only propagate up leaf status
        if not len(statetoday.children):
            statetoday.statustxt = f"afsluttet med {jobsonl.status}"
            statetoday.parent.statustxt = f"{jobsonl.name} afsluttet med {jobsonl.status}"
        else:
            statetoday.parent.statustxt = statetoday.statustxt
        statetomorrow.expected_end = calc_expected("end", statetoday)

    db.session.commit()
    
def get_state(stateid=None, date=datetime.date.today()):
    # date only used when no stateid is given
    if stateid is None:
        return db.session.query(State).filter(and_(
            State.jobdate == date,
            State.name == "job-runner",
        )).all()
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
    elif jobsonl.name in settings["finishline.job.names.ignored"]:
        return flask.jsonify({"info":"ignored"})
    elif jobsonl.name in settings["finishline.job.names.meta"]:
        handle_meta(jobsonl)
        return flask.jsonify({"info":"meta"})
    else:
        handle_jobstatus(jobsonl)
        return flask.jsonify({"info":"jobstatus"})


@app.route("/")
def page():
    # serve the status view
    stateid = flask.request.args.get("stateid")
    date = flask.request.args.get("date", datetime.date.today())

    class Header:
        id = ""
        name = "Jobnavn" 
        server = "Servernavn"
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
            f"<{td}>{state.server}</{td}>",
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
    jr = urllib.request.urlopen(jr).read().decode()
    for indicator, name in re.findall(
        "(RUN[_A-Z0-9]*).*\n *run-job ([_a-z0-9]*)", jr, re.MULTILINE
    ):
        upsert_job(indicator, name)

    db.session.commit()



if __name__ == "__main__":
    db.create_all()
    insert_jobs()
    app.run()

