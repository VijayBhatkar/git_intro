from integrated_dashboard_api.core.config import config_db_details
from integrated_dashboard_api.db.DatabaseSession import DatabaseSession
from integrated_dashboard_api.db.DatabaseTables import INTDASHDATUM as IDD
from typing import Dict, List
from pydantic import Field
from integrated_dashboard_api.api.constants import (
    domain_status_count_sql,
    ecomm_domain_status_count_sql,
    sla_job_info_sql,
    workflow_primary_sql,
    actionable_wf_sql,
    issues_pending_closure_sql,
    get_sla_health,
    get_pot_sla_miss,
)
from integrated_dashboard_api.core import cache
import time

def check_alert_category(sla_status: str='NA', failure_ind: str = 'N', ert_ind: str='N') -> str:
    """Given workflow and job name this function returns
    which all different alerts are triggered that are actionable to team.

    Args:
        sla_status (str, optional): SLA status. Defaults to 'NA'.
        failure_ind (str, optional): Failure status. Defaults to 'N'.
        ert_ind (str, optional): ERT status. Defaults to 'N'.

    Returns:
        str: type of alert
    """
    alert_category: str = "NA"
    if sla_status == "Missed":
        if failure_ind == "Y":
            if  ert_ind == "Y":
                alert_category = "SLAMISSED_FAILED_ERT"
            else:
                alert_category = "SLAMISSED_FAILED"
        elif ert_ind == "Y":
            alert_category = "SLAMISSED_ERT"
        else:
            alert_category = "SLAMISSED"
    elif failure_ind == "Y":
        if  ert_ind == "Y":
            alert_category = "FAILED_ERT"
        else:
            alert_category = "FAILED"
    elif ert_ind == "Y":
        alert_category = "ERT"
    return alert_category


class Domain:
    """Class to handle Domain level operations"""

    def __init__(self):
        self.team_shift = {
            "DFS_SUPPORT_SHIFT1": ["06:00:00", "14:00:00", 0],
            "DFS_SUPPORT_SHIFT2": ["14:00:00", "22:00:00", 0],
            "DFS_SUPPORT_SHIFT3": ["22:00:00", "06:00:00", -1],
            "DFS_ENG_IDC": ["07:00:00", "19:00:00", 0],
            "DFS_ENG_US": ["19:00:00", "07:00:00", -1],
        }
        self.domain_list = []
        self.domain_list = []
        self.required_wf_cols: List = [
            "PARENT_RUN_ID",
            "PARENT_JOB_NAME",
            "AH_CLIENT",
            "LAST_REFRESH_DT",
            "DOMAIN_NAME",
            "PRODUCT_NAME",
            "APPLICATION_NAME",
            "RETURN_CODE_TEXT",
            "JOB_FLOW_PRIORITY",
            "ASSIGNMENT_STATUS",
            "ASSIGNED_USER",
            "ACTIVATION_TIME",
            "START_TIME",
            "END_TIME",
            "CONV_END_TIME",
            "ORD_KEY",
            "AH_RUNTIME",
            "AH_ERT",
            "TOTAL_JOBS",
            "RUNNING_JOBS",
            "FAILED_JOBS",
            "ERT_EXCEEDED_JOBS",
            "RUN_HIST_URL",
            "BSNS_SLA_TIME",
            "BSNS_SLA_TIME_DATE",
            "BSNS_SLA_DIFF_MINS",
            "BSNS_SLA_DIFF",
            "BSNS_SLA_STATUS",
            "SLA_TYPE",
            "SOURCE_NM",
            "CREATED_DT",
            "UPDATED_DT",
            "JIRA_PROJECT_KEY",
            "JIRA_COMPONENT",
            "XMATTER_GRP",
        ]
        self.required_job_cols: List = [
            "JOB_NAME",
            "JOB_RUN_ID",
            "JOB_STATUS",
            "ASSIGNMENT_STATUS",
            "ASSIGNED_USER",
            "USER_ROLE",
            "JOB_START_TIME",
            "JOB_END_TIME",
            "JOB_AH_RUNTIME",
            "JOB_AH_ERT",
            "JOB_RUN_URL",
            "JOB_DURATION",
            "JOB_LVL",
            "JOB_ERT_EXCEEDED_IND",
            "JOB_FAIL_IND",
            "JOB_SLA_IND",
            "JIRA",
            "SERVICE_NOW",
            "ZOOM_LINK",
            "CLOSE_CODE",
            "ROOT_CAUSE",
            "ERROR",
        ]
        self.key_miss_msg = "KEY_NOT_FOUND"

    @cache.memcache
    def get_pot_sla_miss(self) -> List[str]:
        """
        gives list of potiential SLA miss details

        """
        potsla = []
        with DatabaseSession(**config_db_details) as session:
            result = session.execute(get_pot_sla_miss)
        for row in result:
            potsla.append(row)

        return potsla

    @cache.memcache
    def get_domain_list(self) -> List[str]:
        """Gives array of distinct domain names

        Returns:
            List[str]: Array of domain names
        """
        with DatabaseSession(**config_db_details) as session:
            result = session.query(IDD.DOMAIN_NAME).distinct().all()
        return [row.DOMAIN_NAME for row in result]

    @cache.memcache
    def get_domain_wf_counts_by_status(self, domain: List[str] = Field(default=["All"])) -> Dict:
        """For the list of domains as the input argument,
        method gives count of workflows grouped by 'status' across the
        given domains. If "all" is passed as a domain, returns the count
        of wflws across all the domains grouped by their status.

        Args:
            domain (List[str]): List of Domain names

        Returns:
            Dict: Dictionary object whose keys are 'status and the value is count
            of wflws in that status
        """
        counts: Dict = {
            "SUCCESS": 0,
            "SLA_MISS": 0,
            "RUNNING": 0,
            "FAILED": 0,
            "ERT": 0,
            "CANCELLED": 0,
        }
        if "Ecomm" in domain:
            sql = ecomm_domain_status_count_sql
        else:
            sql = domain_status_count_sql
        with DatabaseSession(**config_db_details) as session:
            result = session.execute(sql)
        if "Ecomm" in domain:
            for row in result:
                counts[row["RSTATUS"]] = counts.get(row["RSTATUS"], 0) + row["TOTAL_COUNT"]
        else:
            for row in result:
                if domain[0] in ["all", "All", "ALL"] or row["DOMAIN_NAME"] in domain:
                    counts[row["RSTATUS"]] = counts.get(row["RSTATUS"], 0) + row["TOTAL_COUNT"]
        return counts

    @cache.memcache
    def get_sla_workflows(self, domain: List[str] = Field(default=["All"])) -> List[Dict]:
        """Returns list of all the SLA jobs and
        their current run details

        Returns:
            List[Dict]: Array of dictionary where each row corresponds to
            individual SLA job and its latest run statistics
        """
        selected_domain: str = ""
        domain_filter: str = ""
        tempDict: Dict = {}
        output: List = []

        # if domain:
        #    for val in domain:
        #        selected_domain = f"{selected_domain}'{val}',"
        #    selected_domain = selected_domain.rstrip(",")
        #    domain_filter = f"and DOMAIN_NAME in ({selected_domain})"
        if domain is None or "All" in domain:  # If domain is None or "All" domains are needed
            pass
        elif "Ecomm" in domain:
            domain_filter = 'AND (idd.PARENT_JOB_NAME REGEXP ".GEC|CEXP" OR idd.PRODUCT_NAME LIKE "%ECOMM%" OR idd.APPLICATION_NAME LIKE "%ECOMM%")'
        else:
            for val in domain:
                selected_domain = f"{selected_domain}'{val}',"
            selected_domain = selected_domain.rstrip(",")
            domain_filter = f"and DOMAIN_NAME in ({selected_domain})"
        sql = sla_job_info_sql.format(filter_condition=domain_filter)

        with DatabaseSession(**config_db_details) as session:
            data = session.execute(sql)

        for row in data:
            row = dict(row)
            PARENT_RUN_ID = row["PARENT_RUN_ID"]
            if PARENT_RUN_ID not in tempDict.keys():
                tempDict[PARENT_RUN_ID] = {
                    key: row.get(key, self.key_miss_msg) for key in self.required_wf_cols
                }
                tempDict[PARENT_RUN_ID]["JOBS"] = [
                    {key: row.get(key, self.key_miss_msg) for key in self.required_job_cols}
                ]
            else:
                tempDict[PARENT_RUN_ID]["JOBS"].append(
                    {key: row.get(key, self.key_miss_msg) for key in self.required_job_cols}
                )
        for key, value in tempDict.items():
            output.append(value)
        return output

    @cache.memcache
    def get_actionable_workflows(
        self, domain: List[str] = Field(default=["All"])
    ) -> Dict[str, List]:
        """Given a list of domains as input, returns all the actionable workflows
        that are either SLA missed, FAILED, or ERT exceeded and the wf status is anything
        other then SUCCEEDED and CANCELLED. Results are joined with `INT_DASH_DTL` table
        to provide job level details of failed & ert missed
        jobs for the workflow, along with the assignment details from
        INTEGRATED_DASHBOARD_ASSIGNMENT table.

        Args:
            domain (List): List of domains

        Returns:
            Dict: Returns a dictionary where the workflows are grouped into
            3 categories. 'SLAMISSED', 'FAILED' and 'ERT' respectively.
        """
        output: Dict = {"SLAMISSED": [], "FAILED": [], "ERT": []}
        tempDict: Dict = {"SLAMISSED": {}, "FAILED": {}, "ERT": {}}
        sql: str = ""
        selected_domain: str = ""
        domain_filter: str = ""
        alert_type: str = ""
        if domain is None or "All" in domain:  # If domain is None or "All" domains are needed
            pass
        elif "Ecomm" in domain:
            domain_filter = 'AND (idd.PARENT_JOB_NAME REGEXP ".GEC|CEXP" OR idd.PRODUCT_NAME LIKE "%ECOMM%" OR idd.APPLICATION_NAME LIKE "%ECOMM%")'
        else:
            for val in domain:
                selected_domain = f"{selected_domain}'{val}',"
            selected_domain = selected_domain.rstrip(",")
            domain_filter = f"and DOMAIN_NAME in ({selected_domain})"
        sql = actionable_wf_sql.format(filter_condition=domain_filter)

        with DatabaseSession(**config_db_details) as session:
            res = session.execute(sql)
        for row in res:
            row = dict(row)
            PARENT_RUN_ID = row["PARENT_RUN_ID"]
            if row["BSNS_SLA_STATUS"] == "Missed":
                alert_type = "SLAMISSED"
            elif row["FAILED_JOBS"] > 0:
                alert_type = "FAILED"
            elif row["ERT_EXCEEDED_JOBS"] > 0:
                alert_type = "ERT"
            else:
                alert_type = "NA"
            if not tempDict[alert_type].get(PARENT_RUN_ID, ""):
                tempDict[alert_type][PARENT_RUN_ID] = {
                    key: row.get(key, self.key_miss_msg) for key in self.required_wf_cols
                }
                tempDict[alert_type][PARENT_RUN_ID].update({"ALERT_TYPE": alert_type})
                tempDict[alert_type][PARENT_RUN_ID]["JOBS"] = [
                    {key: row.get(key, self.key_miss_msg) for key in self.required_job_cols}
                ]
            else:
                tempDict[alert_type][PARENT_RUN_ID]["JOBS"].append(
                    {key: row.get(key, self.key_miss_msg) for key in self.required_job_cols}
                )

        for k, v in tempDict.items():
            for wf_run_id, data in v.items():
                output[k].append(data)

        return output

    @cache.memcache
    def get_all_workflows(self, domain: List[str] = Field(default=["All"])) -> List:
        """Given a list of domains as input, returns all the workflows
        which are part of the provided domains doesnt filter on status and the
        results are joined with `INT_DASH_DTL` table to also provide failed & ert missed
        jobs for the workflows, their assignment status along with details from
        INTEGRATED_DASHBOARD_ASSIGNMENT table.

        Args:
            domain (List): List of domains

        Returns:
            List: Array objects where each object corresponds to a unique wf parent id.
        """
        tempDict: Dict = {}
        output: List = []
        selected_domain: str = ""
        domain_filter: str = ""

        if domain is None or "All" in domain:  # If domain is None or "All" domains are needed
            pass
        elif "Ecomm" in domain:
            domain_filter = 'WHERE (idd.PARENT_JOB_NAME REGEXP ".GEC|CEXP" OR idd.PRODUCT_NAME LIKE "%ECOMM%" OR idd.APPLICATION_NAME LIKE "%ECOMM%")'
        else:
            for val in domain:
                selected_domain = f"{selected_domain}'{val}',"
            selected_domain = selected_domain.rstrip(",")
            domain_filter = f"WHERE DOMAIN_NAME in ({selected_domain})"
        sql = workflow_primary_sql.format(filter_condition=domain_filter)

        with DatabaseSession(**config_db_details) as session:
            data = session.execute(sql)

        for row in data:
            row = dict(row)
            PARENT_RUN_ID = row["PARENT_RUN_ID"]
            if PARENT_RUN_ID not in tempDict.keys():
                tempDict[PARENT_RUN_ID] = {
                    key: row.get(key, self.key_miss_msg) for key in self.required_wf_cols
                }
                tempDict[PARENT_RUN_ID]["JOBS"] = [
                    {key: row.get(key, self.key_miss_msg) for key in self.required_job_cols}
                ]
            else:
                tempDict[PARENT_RUN_ID]["JOBS"].append(
                    {key: row.get(key, self.key_miss_msg) for key in self.required_job_cols}
                )
        for key, value in tempDict.items():
            output.append(value)

        return output

    @cache.memcache
    def get_issues_pending_closure(self, domain: List[str] = Field(default=["All"])) -> List:
        """Given a list of domains as input, returns all the workflows
        that are currently assigned and assignment status is 'WIP'.
        This output is useful to keep track of previously assigned issues
        that are not closed with relevant root cause and close code details.

        Args:
            domain (List): _description_

        Returns:
            List: _description_
        """
        selected_domain: str = ""
        tempDict: Dict = {}
        output: List = []
        domain_filter: str = ""
        alert_type: str = ""
        if domain is None or "All" in domain:  # If domain is None or "All" domains are needed
            pass
        elif "Ecomm" in domain:
            domain_filter = 'AND (PARENT_JOB_NAME REGEXP ".GEC|CEXP" OR PRODUCT_NAME LIKE "%ECOMM%" OR APPLICATION_NAME LIKE "%ECOMM%")'
        else:
            for val in domain:
                selected_domain = f"{selected_domain}'{val}',"
            selected_domain = selected_domain.rstrip(",")
            domain_filter = f"and DOMAIN_NAME in ({selected_domain})"

        sql: str = issues_pending_closure_sql.format(filter_condition=domain_filter)

        with DatabaseSession(**config_db_details) as session:
            data = session.execute(sql)

        for row in data:
            row = dict(row)
            PARENT_RUN_ID = row["PARENT_RUN_ID"]
            if row["BSNS_SLA_STATUS"] == "Missed":
                alert_type = "SLAMISSED"
            elif row["FAILED_JOBS"] > 0:
                alert_type = "FAILED"
            elif row["ERT_EXCEEDED_JOBS"] > 0:
                alert_type = "ERT"
            else:
                alert_type = "NA"
            if PARENT_RUN_ID not in tempDict.keys():
                tempDict[PARENT_RUN_ID] = {
                    key: row.get(key, self.key_miss_msg) for key in self.required_wf_cols
                }
                tempDict[PARENT_RUN_ID].update({"ALERT_TYPE": alert_type})
                tempDict[PARENT_RUN_ID]["JOBS"] = [
                    {key: row.get(key, self.key_miss_msg) for key in self.required_job_cols}
                ]
            else:
                tempDict[PARENT_RUN_ID]["JOBS"].append(
                    {key: row.get(key, self.key_miss_msg) for key in self.required_job_cols}
                )
        for key, value in tempDict.items():
            output.append(value)

        return output

    @cache.memcache
    def get_slahealth(self) -> List[str]:
        """
        Returns the status of SLA accross all the domains
        """
        SLA_health = {"SLA_HEALTH": {}}
        with DatabaseSession(**config_db_details) as session:
            result = session.execute(get_sla_health)

        for row in result:
            if row["DOMAIN_NAME"] not in SLA_health["SLA_HEALTH"].keys():
                SLA_health["SLA_HEALTH"][row["DOMAIN_NAME"]] = {}
            SLA_health["SLA_HEALTH"][row["DOMAIN_NAME"]][row["STATUS"]] = row["STATUS_CNT"]
        return SLA_health


if __name__ == "__main__":
    d1 = Domain()
    start_time = time.time()
    print(d1.get_domain_list())
    print(d1.get_pot_sla_miss())
    print(d1.get_domain_wf_counts_by_status(["Merch Stores", "Marketing", "Supply Chain"]))
    d1.get_actionable_workflows(["Merchandising"])

    print(d1.get_all_workflows(["CUSTOMER"]))

    print(d1.get_issues_pending_closure(["Supply Chain"]))
    print(d1.get_slahealth())
    print("--- %s seconds ---" % (time.time() - start_time))
