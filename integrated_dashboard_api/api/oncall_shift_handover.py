from integrated_dashboard_api.core.config import config_db_details
from typing import Dict, Optional
from integrated_dashboard_api.db.DatabaseSession import DatabaseSession
from integrated_dashboard_api.db.DatabaseOperations import DatabaseOperations
from integrated_dashboard_api.api.send_mail import Mail, Usermodel
from integrated_dashboard_api.core import cache


# from integrated_dashboard_api.api.domains import send_mail
from integrated_dashboard_api.db.DatabaseTables import (
    INTEGRATEDDASHBOARDASSIGNMENT,
    INTDASHDATUM,
)
from datetime import datetime, timedelta
import time


class OncallShiftHandover:
    """Class handles all Oncall Shift Handover related functionality"""

    def __init__(self):
        self.team_shift = {
            "DFS_SUPPORT_SHIFT1": ["06:00:00", "14:00:00", 0],
            "DFS_SUPPORT_SHIFT2": ["14:00:00", "22:00:00", 0],
            "DFS_SUPPORT_SHIFT3": ["22:00:00", "06:00:00", -1],
            "DFS_ENG_IDC": ["07:00:00", "19:00:00", 0],
            "DFS_ENG_US": ["19:00:00", "07:00:00", -1],
        }
        self.report_columns = [
            "PARENT_JOB_NAME",
            "JOB_NAME",
            "SLA_TYPE",
            "DOMAIN_NAME",
            "ERROR",
            "ROOT_CAUSE",
            "JIRA",
            "SERVICE_NOW",
            "STATUS",
            "RUN_HIST_URL",
        ]
        self.dbOper = DatabaseOperations(**config_db_details)

    @cache.memcache
    def get_handover_shift_note(self, team_shift, SHIFT_DATE: Optional[str] = ""):
        notes = []
        columns = ["TEAM", "SHIFT", "SHIFT_DATE", "ONCALL_SUMMARY"]

        if not SHIFT_DATE:
            SHIFT_DATE = datetime.utcnow() + timedelta(days=int(self.team_shift[team_shift][2]))
            date = str(SHIFT_DATE.strftime("%Y%m%d"))
        else:
            # SHIFT_DATE = datetime.utcnow() + timedelta(days=int(self.team_shift[team_shift][2]))
            date = SHIFT_DATE
        result = self.dbOper.select(
            tablename="INT_DASH_SHIFT_HANDOVER",
            conditions={"SHIFT": team_shift, "SHIFT_DATE": date},
            required_columns=columns,
        )
        for row in result:
            notes.append(dict(row))
        return notes

    def getOnCallDate(self, team_shift):
        ini_time_for_now = datetime.now()  # system current time
        startTime, endTime, dayDelta = self.team_shift[team_shift]
        if dayDelta != 0:
            startDate = ini_time_for_now + timedelta(days=int(dayDelta))
            return startDate
        else:
            startDate = ini_time_for_now
            return startDate

    def send_handoff_mail(self, mail_para):
        team_shift = getattr(mail_para, "team_shift")
        to_list = getattr(mail_para, "to_list")
        from_list = getattr(mail_para, "from_list")
        sender_name = getattr(mail_para, "sender_name")
        if team_shift and to_list and from_list and sender_name:
            final_res = []
            m1 = Mail()
            notes = self.get_handover_shift_note(team_shift)

            if not notes:
                notes = [{"TEAM": "NA", "SHIFT": "NA", "SHIFT_DATE": "NA", "ONCALL_SUMMARY": "NA"}]

            finalResult = self.get_oncall_report(team_shift)
            for d in finalResult["pending_isses"]:
                html01 = f"""
                    <tr>
                    <td>{d['PARENT_JOB_NAME']}</td>
                    <td>{d['JOB_NAME']}</td>
                    <td>{d['SLA_TYPE']}</td>
                    <td>{d['DOMAIN_NAME']}</td>
                    <td>{d['ERROR']}</td>
                    <td>{d['ROOT_CAUSE']}</td>
                    <td>{d['JIRA']}</td>
                    <td>{d['SERVICE_NOW']}</td>
                    <td>{d['STATUS']}</td>
                    <td><a href= "{d['RUN_HIST_URL']}">Link</a></td>
                    </tr>
                    """
                final_res.append(html01)
            fr_output = "".join(final_res)

            # team = notes[0].get("TEAM")
            # shift = notes[0].get("SHIFT")
            shift = team_shift.rsplit("_", 1)[-1]
            team = team_shift.rsplit("_", 1)[0]
            # date = notes[0].get("SHIFT_DATE")
            note = notes[0].get("ONCALL_SUMMARY")
            oncall_date = self.getOnCallDate(team_shift)
            startDate = (str(oncall_date).split())[0]
            header = """
            <tr><td colspan=10, bgcolor=grey><b>PENDING ISSUES :- </b></td></tr>
            <tr style="text-align:center;background-color:LightGreen;">
            <td >PARENT_JOB_NAME</td>
            <td>JOB_NAME</td>
            <td>SLA_TYPE</td>
            <td>DOMAIN_NAME</td>
            <td>ERROR</td>
            <td>ROOT_CAUSE</td>
            <td>JIRA</td>
            <td>SERVICE_NOW</td>
            <td>STATUS</td>
            <td>RUN_HIST_URL</td>
            </tr>
            """
            pen_out_final_list = f"""<table border = 1>{header}{fr_output}</table>"""

            template = f"""
            <Table>
            <tr><td><b>Team</b></td><td>{team}</td></tr>
            <tr><td><b>Shift</b></td><td>{shift}</td></tr>
            <tr><td><b>OnCall Date</b></td><td>{startDate}</td></tr>
            <tr><td><b>Handover Notes</b></td><td>{note}</td></tr>
            <tr><td></td><td></td></tr>
            </Table>
            """
            signature = f"""
                        <Table>
                        <tr><td  colspan=2></td></tr>
                        <tr><td  colspan=2>Regards,</td></tr>
                        <tr><td colspan=2>{sender_name}</td></tr>
                        </Table>"""

            msg = template + pen_out_final_list + signature

            fmsg = f"<HTML>{msg}</HTML>"
            m1.send_mail(
                Usermodel(
                    **{
                        "to": [f"{','.join(to_list)}"],
                        "sender": f"{','.join(from_list)}",
                        "subject": f"Shift Handover : {startDate} - {team}_{shift}",
                        "message": fmsg,
                    }
                )
            )
            return {"msg": "Successfully sent mail"}

    def set_handover_shift_note(self, notes):
        TEAM = getattr(notes, "TEAM")
        SHIFT = getattr(notes, "SHIFT")
        SHIFT_DATE = getattr(notes, "SHIFT_DATE")
        USER = getattr(notes, "USER")
        ONCALL_SUMMARY = getattr(notes, "ONCALL_SUMMARY")
        UPDATED_USER = getattr(notes, "UPDATED_USER")
        if TEAM and SHIFT and SHIFT_DATE and USER and UPDATED_USER and ONCALL_SUMMARY:
            entry = [
                {
                    "TEAM": TEAM,
                    "SHIFT": SHIFT,
                    "SHIFT_DATE": SHIFT_DATE,
                    "ONCALL_SUMMARY": ONCALL_SUMMARY,
                    "CREATED_USER": USER,
                    "UPDATED_USER": UPDATED_USER,
                }
            ]
            self.dbOper.insert_on_duplicate_key_update(
                records=entry, tablename="INT_DASH_SHIFT_HANDOVER"
            )

    def get_oncall_report(
        self, teamShift, startTime=None, endTime=None, closedIssueBool=True
    ) -> Dict:
        """Given teamShift, start time , end time  and closed Issue bool return the
        pending issue ( closed issue if closedIssueBool is true),
        total issue count, pending issue count and closed issue count

        Args:
            teamShift (str) : ( dfs_support_mrng,dfs_support_aftrnoon,
                dfs_support_evng,dfs_eng_mrng,dfs_eng_evng ) or (custom)
                if teamShift is custom it is compulsory to specify startTime
                and endTime
            startTime (str): start Date time in string
                ( Eg: 2022-07-03 20:24:23)
            endTime (str): end Date time in string
                ( Eg: 2022-08-21 08:40:18)

            closedIssueBool (str): True or False
        sample query :
            # http://127.0.0.1:8000/oncallHandover/report/{team_shift}
            # http://127.0.0.1:8000/oncallHandover/report/dfs_support_mrng
            # oncallHandover/report/dfs_eng_evng?closedIssue=True
            # ...custom?closedIssue=True&startTime=2022-07-03 20:24:23
            # &endTime=2022-08-21 08:40:18

        Returns:
            pending issue , closed issue (optional) ,
        total issue count, pending issue count and closed issue count
        """
        pendingIssueList = []
        closedIssueList = []
        totalCount = 0
        closedCount = 0
        wipCount = 0
        finalResult = {}
        # date time delta based on the given shift
        if teamShift and teamShift != "custom":
            ini_time_for_now = datetime.now()  # system current time
            startTime, endTime, dayDelta = self.team_shift[teamShift]
            if dayDelta != 0:
                endDate = ini_time_for_now
                startDate = ini_time_for_now + timedelta(days=int(dayDelta))
            else:
                startDate = endDate = ini_time_for_now
            # changing start time to utc time
            startTime = startDate.strftime("%Y-%m-%d ") + startTime
            dt_obj = datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S")
            utc_struct_time = time.gmtime(time.mktime(dt_obj.timetuple()))
            startTime = datetime.fromtimestamp(time.mktime(utc_struct_time))
            # changing end time to utc time
            endTime = endDate.strftime("%Y-%m-%d ") + endTime
            dt_obj = datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S")
            utc_struct_time = time.gmtime(time.mktime(dt_obj.timetuple()))
            endTime = datetime.fromtimestamp(time.mktime(utc_struct_time))
        # custom time handler
        elif teamShift == "custom":
            if not (startTime and endTime):
                return "Enter valid time for custom team shift"
        # main logic
        with DatabaseSession(**config_db_details) as session:
            result = (
                session.query(INTDASHDATUM, INTEGRATEDDASHBOARDASSIGNMENT)
                .join(
                    INTDASHDATUM,
                    INTDASHDATUM.PARENT_RUN_ID == INTEGRATEDDASHBOARDASSIGNMENT.PARENT_RUN_ID,
                )
                .filter(
                    INTEGRATEDDASHBOARDASSIGNMENT.UPDATED_DT >= startTime,
                    INTEGRATEDDASHBOARDASSIGNMENT.UPDATED_DT <= endTime,
                )
            )

        if result:
            for databaseTable in result:
                if databaseTable:
                    record = {}
                    record.update(databaseTable[0].__dict__)
                    record.update(databaseTable[1].__dict__)
                    if record["STATUS"] == "CLOSED":
                        closedCount += 1
                        if closedIssueBool == "True":
                            closedIssueList.append(
                                {key: record[key] for key in self.report_columns}
                            )
                    elif record["STATUS"] == "WIP":
                        wipCount += 1
                        pendingIssueList.append({key: record[key] for key in self.report_columns})
                    totalCount += 1
            finalResult["pending_isses"] = pendingIssueList
            finalResult["total_issue_count"] = totalCount
            finalResult["pending_issue_count"] = wipCount
            finalResult["closed_issue_count"] = closedCount
            if closedIssueBool == "True":
                finalResult["closed_issues"] = closedIssueList
        return finalResult


if __name__ == "__main__":
    print("hello")
