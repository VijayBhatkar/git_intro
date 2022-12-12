#!/usr/bin/python
# -*- coding: utf-8 -*-
sla_job_info_sql = """
select `wjrn`.*,
    `ida`.`JIRA` as `JIRA`,
    `ida`.`SERVICE_NOW` as `SERVICE_NOW`,
    case
        when `ida`.`STATUS` IS NOT NULL then `ida`.`STATUS`
        when (
          `wjrn`.`FAILED_JOBS` > 0
          or `wjrn`.`ERT_EXCEEDED_JOBS` > 0
          or `wjrn`.`BSNS_SLA_STATUS` = "Missed"
        )  then "OPEN"
        else "NA"
    end as `ASSIGNMENT_STATUS`,
    `ida`.`ZOOM_LINK` as `ZOOM_LINK`,
    `ida`.`CLOSE_CODE` as `CLOSE_CODE`,
    `ida`.`ROOT_CAUSE` as `ROOT_CAUSE`,
    `ida`.`ERROR` as `ERROR`,
    case
        when `rn`.`ASSIGNED_USER` IS NOT NULL then `rn`.`ASSIGNED_USER`
        when (
          `wjrn`.FAILED_JOBS > 0
          or `wjrn`.`ERT_EXCEEDED_JOBS` > 0
          or `wjrn`.`BSNS_SLA_STATUS` = "Missed"
        )  then "Not Assigned"
        else "NA"
    end as `ASSIGNED_USER`,
    `rn`.`USER_ROLE` as `USER_ROLE`
from (
  select `temp2`.*
    from (
    select `idd2`.*,
        `iddtl`.`JOB_NAME` as `JOB_NAME`,
        `iddtl`.`JOB_RUN_ID` as `JOB_RUN_ID`,
        `iddtl`.`RETURN_CODE_TEXT` as `JOB_STATUS`,
        `iddtl`.`START_TIME` as `JOB_START_TIME`,
        `iddtl`.`END_TIME` as `JOB_END_TIME`,
        `iddtl`.`AH_RUNTIME` as `JOB_AH_RUNTIME`,
        `iddtl`.`AH_ERT` as `JOB_AH_ERT`,
        `iddtl`.`RUN_HIST_URL` as `JOB_RUN_URL`,
        `iddtl`.`JOB_DURATION` as `JOB_DURATION`,
        `iddtl`.`Lvl` as `JOB_LVL`,
        `iddtl`.`ERT_EXCEEDED_IND` as `JOB_ERT_EXCEEDED_IND`,
        `iddtl`.`JOB_FAIL_IND` as `JOB_FAIL_IND`,
        `iddtl`.`SLA_IND` as `JOB_SLA_IND`,
        rank() OVER (
          PARTITION BY `idd2`.`PARENT_RUN_ID`,
          `iddtl`.`JOB_NAME`
          ORDER BY
            `iddtl`.`JOB_RUN_ID` desc
          ) AS `jb_rnk`
    from (
      select `temp1`.*
      from (
        select `idd`.`PARENT_RUN_ID` AS `PARENT_RUN_ID`,
            `idd`.`PARENT_JOB_NAME` AS `PARENT_JOB_NAME`,
            `idd`.`AH_CLIENT` AS `AH_CLIENT`,
            `idd`.`LAST_REFRESH_DT` AS `LAST_REFRESH_DT`,
            `idd`.`RUN_SEQ` AS `RUN_SEQ`,
            `idd`.`DOMAIN_NAME` AS `DOMAIN_NAME`,
            `idd`.`PRODUCT_NAME` AS `PRODUCT_NAME`,
            `idd`.`APPLICATION_NAME` AS `APPLICATION_NAME`,
            `idd`.`STATUS` AS `STATUS`,
            `idd`.`RETURN_CODE_TEXT` AS `RETURN_CODE_TEXT`,
            `idd`.`JOB_FLOW_PRIORITY` AS `JOB_FLOW_PRIORITY`,
            `idd`.`ACTIVATION_TIME` AS `ACTIVATION_TIME`,
            `idd`.`START_TIME` AS `START_TIME`,
            `idd`.`END_TIME` AS `END_TIME`,
            `idd`.`CONV_END_TIME` AS `CONV_END_TIME`,
            `idd`.`SLA_TIME_DATE` AS `SLA_TIME_DATE`,
            `idd`.`HARD_SLA_STATUS` AS `HARD_SLA_STATUS`,
            `idd`.`SLA_TIME` AS `SLA_TIME`,
            `idd`.`SLA_DIFF_MINS` AS `SLA_DIFF_MINS`,
            `idd`.`ORD_KEY` AS `ORD_KEY`,
            `idd`.`SLA_DIFF` AS `SLA_DIFF`,
            `idd`.`AH_RUNTIME` AS `AH_RUNTIME`,
            `idd`.`AH_ERT` AS `AH_ERT`,
            `idd`.`TOTAL_JOBS` AS `TOTAL_JOBS`,
            `idd`.`RUNNING_JOBS` AS `RUNNING_JOBS`,
            `idd`.`FAILED_JOBS` AS `FAILED_JOBS`,
            `idd`.`ERT_EXCEEDED_JOBS` AS `ERT_EXCEEDED_JOBS`,
            `idd`.`RUN_HIST_URL` AS `RUN_HIST_URL`,
            `idd`.`BSNS_SLA_TIME` AS `BSNS_SLA_TIME`,
            `idd`.`BSNS_SLA_TIME_DATE` AS `BSNS_SLA_TIME_DATE`,
            `idd`.`BSNS_SLA_DIFF_MINS` AS `BSNS_SLA_DIFF_MINS`,
            `idd`.`BSNS_SLA_DIFF` AS `BSNS_SLA_DIFF`,
            `idd`.`BSNS_SLA_STATUS` AS `BSNS_SLA_STATUS`,
            `idd`.`SLA_TYPE` AS `SLA_TYPE`,
            `idd`.`SOURCE_NM` AS `SOURCE_NM`,
            `idd`.`CREATED_DT` AS `CREATED_DT`,
            `idd`.`UPDATED_DT` AS `UPDATED_DT`,
            `idd`.`JIRA_PROJECT_KEY` AS `JIRA_PROJECT_KEY`,
            `idd`.`DT_JIRA_COMPONENT` AS `JIRA_COMPONENT`,
            `idd`.`DT_XMATTER_GRP_NM` AS `XMATTER_GRP`,
          rank() OVER (
            PARTITION BY `idd`.`PARENT_JOB_NAME`
            ORDER BY `idd`.`ACTIVATION_TIME` desc
          ) AS `RUN_SEQUENCE`
        from `INT_DASH_DATA` `idd`
        where `idd`.`SLA_TYPE` in ('B', 'B&I')
        {filter_condition}
        ) `temp1`
      where `temp1`.`RUN_SEQUENCE`=1) `idd2`
      left join `INT_DASH_DTL` `iddtl` ON`idd2`.`PARENT_RUN_ID` = `iddtl`.`PARENT_RUN_ID` and (
        `iddtl`.`ERT_EXCEEDED_IND` = "Y"
        or `iddtl`.`JOB_FAIL_IND` = "Y"
      )) `temp2`
            where `temp2`.`jb_rnk` = 1) `wjrn`
        left join `INTEGRATED_DASHBOARD_ASSIGNMENT` `ida`
      ON `wjrn`.`PARENT_RUN_ID` = `ida`.`PARENT_RUN_ID`
        and `wjrn`.`JOB_NAME` = `ida`.`JOB_NAME`
    left join (
      SELECT
        `idam`.`PARENT_RUN_ID` as `PARENT_RUN_ID`,
        `idam`.`JOB_NAME` as `JOB_NAME`,
        `idam`.`ASSIGNED_USER` as `ASSIGNED_USER`,
        `idam`.`USER_ROLE` as `USER_ROLE`
      FROM `INT_DASH_ASSIGNMENT_MAPPING` `idam`
      LEFT JOIN `INT_DASH_ASSIGNMENT_MAPPING` `idam2`
        ON `idam`.`PARENT_RUN_ID` = `idam2`.`PARENT_RUN_ID`
          AND `idam`.`JOB_NAME` = `idam2`.`JOB_NAME`
                    AND `idam`.`ASSIGNMENT_MAPPING_SKEY` < `idam2`.`ASSIGNMENT_MAPPING_SKEY`
      WHERE `idam2`.`ASSIGNMENT_MAPPING_SKEY` is NULL
    ) `rn` ON `ida`.`PARENT_RUN_ID` = `rn`.`PARENT_RUN_ID`
        and `ida`.`JOB_NAME` = `rn`.`JOB_NAME`
"""

domain_status_count_sql = """
SELECT a.DOMAIN_NAME,
       a.RSTATUS,
       a.TOTAL_COUNT
FROM
(
    select DOMAIN_NAME,
           (case
                when RETURN_CODE_TEXT in ( 'CANCELLED', 'SUCCESS' ) then
                    RETURN_CODE_TEXT
                when HARD_SLA_STATUS = "Missed" then
                    'SLA_MISS'
                when FAILED_JOBS > 0 then
                    'FAILED'
                when ERT_EXCEEDED_JOBS > 0 then
                    'ERT'
            end
           ) as RSTATUS,
           count(*) as TOTAL_COUNT
    from dfs.INT_DASH_DATA idd
    group by DOMAIN_NAME,
             RSTATUS
    UNION
    SELECT DOMAIN_NAME,
           'RUNNING' as RSTATUS,
           COUNT(*) as TOTAL_COUNT
    from INT_DASH_DATA
    where RETURN_CODE_TEXT = 'RUNNING'
    GROUP BY DOMAIN_NAME
) a
WHERE a.RSTATUS IS NOT NULL
"""

ecomm_domain_status_count_sql = """
SELECT 'EComm' as DOMAIN_NAME,
       a.RSTATUS,
       a.TOTAL_COUNT
FROM
(
    select DOMAIN_NAME,
           (case
                when RETURN_CODE_TEXT in ( 'CANCELLED', 'SUCCESS' ) then
                    RETURN_CODE_TEXT
                when HARD_SLA_STATUS = "Missed" then
                    'SLA_MISS'
                when FAILED_JOBS > 0 then
                    'FAILED'
                when ERT_EXCEEDED_JOBS > 0 then
                    'ERT'
            end
           ) as RSTATUS,
           count(*) as TOTAL_COUNT
    FROM dfs.INT_DASH_DATA idd
    WHERE (idd.PARENT_JOB_NAME REGEXP '.GEC|CEXP' OR idd.PRODUCT_NAME LIKE "%ECOMM%" OR idd.APPLICATION_NAME LIKE "%ECOMM%")
    group by RSTATUS
    UNION
    SELECT DOMAIN_NAME,
           'RUNNING' as RSTATUS,
           COUNT(*) as TOTAL_COUNT
    from INT_DASH_DATA
    where (RETURN_CODE_TEXT = 'RUNNING' AND
    (PARENT_JOB_NAME REGEXP '.GEC|CEXP' OR PRODUCT_NAME LIKE "%ECOMM%" OR APPLICATION_NAME LIKE "%ECOMM%")
    )
) a
WHERE a.RSTATUS IS NOT NULL;
"""

query_failed_info_workflow_statement = """
                select * from (select *,ROW_number() over (
                partition by JOB_NAME order by CREATED_DT desc)
                issue_number from INTEGRATED_DASHBOARD_ASSIGNMENT)
                assignment_table inner join INT_DASH_DATA_HISTORY_VW on
                assignment_table.PARENT_RUN_ID = INT_DASH_DATA_HISTORY_VW.PARENT_RUN_ID
                WHERE INT_DASH_DATA_HISTORY_VW.PARENT_JOB_NAME = 'filter_domain_name'
"""

query_failed_info_workflow_statement = """
                select DISTINCT dash.PARENT_RUN_ID,
                dash.PARENT_JOB_NAME, dash.RETURN_CODE_TEXT, assign.JOB_NAME,assign.STATUS,
                assign.ZOOM_LINK,assign.ROOT_CAUSE,assign.ERROR,assign.CLOSE_CODE,
                assign.ERT_ALERT, assign.SLAMISS_ALERT, assign.FAILURE_ALERT,
                assign.SERVICE_NOW,assign.JIRA, assign.CREATED_DT, assign.UPDATED_DT,
                mapping.ASSIGNED_USER,JSON_ARRAYAGG(
                  JSON_OBJECT(
                    'NOTE', idn.Notes ,
                    'NOTES_SKEY', idn.NOTES_SKEY ,
                    'UPDATED_DT', idn.UPDATED_DT ,
                    'UPDATED_USER', idn.UPDATED_USER
                    )
                  ) AS NOTES
                from (select * from (select *,ROW_NUMBER() over (
                PARTITION by job_name order by ASSIGNMENT_MAPPING_SKEY desc)
                rn from INT_DASH_ASSIGNMENT_MAPPING)
                main_table where rn = 1) mapping
                inner join INTEGRATED_DASHBOARD_ASSIGNMENT assign
                on assign.PARENT_RUN_ID = mapping.PARENT_RUN_ID
                and assign.job_name=mapping.job_name
                inner join INT_DASH_DATA_HISTORY_VW dash on
                dash.PARENT_RUN_ID = assign.PARENT_RUN_ID
                left join INTEGRATED_DASHBOARD_NOTES idn on
                assign.PARENT_RUN_ID=idn.PARENT_RUN_ID and assign.JOB_NAME=idn.JOB_NAME
                where dash.PARENT_JOB_NAME='filter_workflow_name'
                GROUP BY dash.PARENT_RUN_ID, dash.PARENT_JOB_NAME,assign.JOB_NAME
"""
workflow_primary_sql = """
select `wjrn`.*,
    `ida`.`JIRA` as `JIRA`,
    `ida`.`SERVICE_NOW` as `SERVICE_NOW`,
    case
        when `ida`.`STATUS` IS NOT NULL then `ida`.`STATUS`
        when (
          `wjrn`.FAILED_JOBS > 0
          or `wjrn`.`ERT_EXCEEDED_JOBS` > 0
          or `wjrn`.`BSNS_SLA_STATUS` = "Missed"
        )  then "OPEN"
        else "NA"
    end as `ASSIGNMENT_STATUS`,
    `ida`.`ZOOM_LINK` as `ZOOM_LINK`,
    `ida`.`CLOSE_CODE` as `CLOSE_CODE`,
    `ida`.`ROOT_CAUSE` as `ROOT_CAUSE`,
    `ida`.`ERROR` as `ERROR`,
    case
        when `rn`.`ASSIGNED_USER` IS NOT NULL then `rn`.`ASSIGNED_USER`
        when (
          `wjrn`.FAILED_JOBS > 0
          or `wjrn`.`ERT_EXCEEDED_JOBS` > 0
          or `wjrn`.`BSNS_SLA_STATUS` = "Missed"
        )  then "Not Assigned"
        else "NA"
    end as `ASSIGNED_USER`,
    `rn`.`USER_ROLE` as `USER_ROLE`
from (
  select `temp2`.*
    from (
    select `idd2`.*,
        `iddtl`.`JOB_NAME` as `JOB_NAME`,
        `iddtl`.`JOB_RUN_ID` as `JOB_RUN_ID`,
        `iddtl`.`RETURN_CODE_TEXT` as `JOB_STATUS`,
        `iddtl`.`START_TIME` as `JOB_START_TIME`,
        `iddtl`.`END_TIME` as `JOB_END_TIME`,
        `iddtl`.`AH_RUNTIME` as `JOB_AH_RUNTIME`,
        `iddtl`.`AH_ERT` as `JOB_AH_ERT`,
        `iddtl`.`RUN_HIST_URL` as `JOB_RUN_URL`,
        `iddtl`.`JOB_DURATION` as `JOB_DURATION`,
        `iddtl`.`Lvl` as `JOB_LVL`,
        `iddtl`.`ERT_EXCEEDED_IND` as `JOB_ERT_EXCEEDED_IND`,
        `iddtl`.`JOB_FAIL_IND` as `JOB_FAIL_IND`,
        `iddtl`.`SLA_IND` as `JOB_SLA_IND`,
        rank() OVER (
          PARTITION BY `idd2`.`PARENT_RUN_ID`,
          `iddtl`.`JOB_NAME`
          ORDER BY
            `iddtl`.`JOB_RUN_ID` desc
          ) AS `jb_rnk`
    from (
      select `temp1`.*
      from (
        select `idd`.`PARENT_RUN_ID` AS `PARENT_RUN_ID`,
            `idd`.`PARENT_JOB_NAME` AS `PARENT_JOB_NAME`,
            `idd`.`AH_CLIENT` AS `AH_CLIENT`,
            `idd`.`LAST_REFRESH_DT` AS `LAST_REFRESH_DT`,
            `idd`.`RUN_SEQ` AS `RUN_SEQ`,
            `idd`.`DOMAIN_NAME` AS `DOMAIN_NAME`,
            `idd`.`PRODUCT_NAME` AS `PRODUCT_NAME`,
            `idd`.`APPLICATION_NAME` AS `APPLICATION_NAME`,
            `idd`.`STATUS` AS `STATUS`,
            `idd`.`RETURN_CODE_TEXT` AS `RETURN_CODE_TEXT`,
            `idd`.`JOB_FLOW_PRIORITY` AS `JOB_FLOW_PRIORITY`,
            `idd`.`ACTIVATION_TIME` AS `ACTIVATION_TIME`,
            `idd`.`START_TIME` AS `START_TIME`,
            `idd`.`END_TIME` AS `END_TIME`,
            `idd`.`CONV_END_TIME` AS `CONV_END_TIME`,
            `idd`.`SLA_TIME_DATE` AS `SLA_TIME_DATE`,
            `idd`.`HARD_SLA_STATUS` AS `HARD_SLA_STATUS`,
            `idd`.`SLA_TIME` AS `SLA_TIME`,
            `idd`.`SLA_DIFF_MINS` AS `SLA_DIFF_MINS`,
            `idd`.`ORD_KEY` AS `ORD_KEY`,
            `idd`.`SLA_DIFF` AS `SLA_DIFF`,
            `idd`.`AH_RUNTIME` AS `AH_RUNTIME`,
            `idd`.`AH_ERT` AS `AH_ERT`,
            `idd`.`TOTAL_JOBS` AS `TOTAL_JOBS`,
            `idd`.`RUNNING_JOBS` AS `RUNNING_JOBS`,
            `idd`.`FAILED_JOBS` AS `FAILED_JOBS`,
            `idd`.`ERT_EXCEEDED_JOBS` AS `ERT_EXCEEDED_JOBS`,
            `idd`.`RUN_HIST_URL` AS `RUN_HIST_URL`,
            `idd`.`BSNS_SLA_TIME` AS `BSNS_SLA_TIME`,
            `idd`.`BSNS_SLA_TIME_DATE` AS `BSNS_SLA_TIME_DATE`,
            `idd`.`BSNS_SLA_DIFF_MINS` AS `BSNS_SLA_DIFF_MINS`,
            `idd`.`BSNS_SLA_DIFF` AS `BSNS_SLA_DIFF`,
            `idd`.`BSNS_SLA_STATUS` AS `BSNS_SLA_STATUS`,
            `idd`.`SLA_TYPE` AS `SLA_TYPE`,
            `idd`.`SOURCE_NM` AS `SOURCE_NM`,
            `idd`.`CREATED_DT` AS `CREATED_DT`,
            `idd`.`UPDATED_DT` AS `UPDATED_DT`,
            `idd`.`JIRA_PROJECT_KEY` AS `JIRA_PROJECT_KEY`,
            `idd`.`DT_JIRA_COMPONENT` AS `JIRA_COMPONENT`,
            `idd`.`DT_XMATTER_GRP_NM` AS `XMATTER_GRP`,
          rank() OVER (
            PARTITION BY `idd`.`PARENT_JOB_NAME`
            ORDER BY `idd`.`ACTIVATION_TIME` desc
          ) AS `RUN_SEQUENCE`
        from `INT_DASH_DATA` `idd`
        {filter_condition}
        ) `temp1`
      where `temp1`.`RUN_SEQUENCE`=1) `idd2`
      left join `INT_DASH_DTL` `iddtl` ON`idd2`.`PARENT_RUN_ID` = `iddtl`.`PARENT_RUN_ID` and (
        `iddtl`.`ERT_EXCEEDED_IND` = "Y"
        or `iddtl`.`JOB_FAIL_IND` = "Y"
      )) `temp2`
            where `temp2`.`jb_rnk` = 1) `wjrn`
        left join `INTEGRATED_DASHBOARD_ASSIGNMENT` `ida`
      ON `wjrn`.`PARENT_RUN_ID` = `ida`.`PARENT_RUN_ID`
        and `wjrn`.`JOB_NAME` = `ida`.`JOB_NAME`
    left join (
      SELECT
        `idam`.`PARENT_RUN_ID` as `PARENT_RUN_ID`,
        `idam`.`JOB_NAME` as `JOB_NAME`,
        `idam`.`ASSIGNED_USER` as `ASSIGNED_USER`,
        `idam`.`USER_ROLE` as `USER_ROLE`
      FROM `INT_DASH_ASSIGNMENT_MAPPING` `idam`
      LEFT JOIN `INT_DASH_ASSIGNMENT_MAPPING` `idam2`
        ON `idam`.`PARENT_RUN_ID` = `idam2`.`PARENT_RUN_ID`
          AND `idam`.`JOB_NAME` = `idam2`.`JOB_NAME`
                    AND `idam`.`ASSIGNMENT_MAPPING_SKEY` < `idam2`.`ASSIGNMENT_MAPPING_SKEY`
      WHERE `idam2`.`ASSIGNMENT_MAPPING_SKEY` is NULL
    ) `rn` ON `ida`.`PARENT_RUN_ID` = `rn`.`PARENT_RUN_ID`
        and `ida`.`JOB_NAME` = `rn`.`JOB_NAME`
"""


actionable_wf_sql = """
SELECT *
FROM
  (SELECT `wjrn`.*,
     `ida`.`JIRA` AS `JIRA`,
     `ida`.`SERVICE_NOW` AS `SERVICE_NOW`,
     IFNULL(`ida`.`STATUS`,
     "OPEN") AS `ASSIGNMENT_STATUS`,
     `ida`.`ZOOM_LINK` AS `ZOOM_LINK`,
     `ida`.`CLOSE_CODE` AS `CLOSE_CODE`,
     `ida`.`ROOT_CAUSE` AS `ROOT_CAUSE`,
     `ida`.`ERROR` AS `ERROR`,
     IFNULL(`rn`.`ASSIGNED_USER`,
     "Not Assigned") AS `ASSIGNED_USER`,
     IFNULL(`rn`.`USER_ROLE`,
    "NA") AS `USER_ROLE`
  FROM
    (SELECT *
    FROM
      (SELECT `idd`.`PARENT_RUN_ID` AS `PARENT_RUN_ID`,
     `idd`.`PARENT_JOB_NAME` AS `PARENT_JOB_NAME`,
     `idd`.`AH_CLIENT` AS `AH_CLIENT`,
     `idd`.`LAST_REFRESH_DT` AS `LAST_REFRESH_DT`,
     `idd`.`DOMAIN_NAME` AS `DOMAIN_NAME`,
     `idd`.`PRODUCT_NAME` AS `PRODUCT_NAME`,
     `idd`.`APPLICATION_NAME` AS `APPLICATION_NAME`,
     `idd`.`RETURN_CODE_TEXT` AS `RETURN_CODE_TEXT`,
     `idd`.`JOB_FLOW_PRIORITY` AS `JOB_FLOW_PRIORITY`,
     `idd`.`ACTIVATION_TIME` AS `ACTIVATION_TIME`,
     `idd`.`START_TIME` AS `START_TIME`,
     `idd`.`END_TIME` AS `END_TIME`,
     `idd`.`CONV_END_TIME` AS `CONV_END_TIME`,
     `idd`.`SLA_TIME_DATE` AS `SLA_TIME_DATE`,
     `idd`.`ORD_KEY` AS `ORD_KEY`,
     `idd`.`AH_RUNTIME` AS `AH_RUNTIME`,
     `idd`.`AH_ERT` AS `AH_ERT`,
     `idd`.`TOTAL_JOBS` AS `TOTAL_JOBS`,
     `idd`.`RUNNING_JOBS` AS `RUNNING_JOBS`,
     `idd`.`FAILED_JOBS` AS `FAILED_JOBS`,
     `idd`.`ERT_EXCEEDED_JOBS` AS `ERT_EXCEEDED_JOBS`,
     `idd`.`RUN_HIST_URL` AS `RUN_HIST_URL`,
     `idd`.`BSNS_SLA_TIME` AS `BSNS_SLA_TIME`,
     `idd`.`BSNS_SLA_TIME_DATE` AS `BSNS_SLA_TIME_DATE`,
     `idd`.`BSNS_SLA_DIFF_MINS` AS `BSNS_SLA_DIFF_MINS`,
     `idd`.`BSNS_SLA_DIFF` AS `BSNS_SLA_DIFF`,
     `idd`.`BSNS_SLA_STATUS` AS `BSNS_SLA_STATUS`,
     `idd`.`SLA_TYPE` AS `SLA_TYPE`,
     `idd`.`SOURCE_NM` AS `SOURCE_NM`,
     `idd`.`CREATED_DT` AS `CREATED_DT`,
     `idd`.`UPDATED_DT` AS `UPDATED_DT`,
     `idd`.`JIRA_PROJECT_KEY` AS `JIRA_PROJECT_KEY`,
     `idd`.`DT_JIRA_COMPONENT` AS `JIRA_COMPONENT`,
     `idd`.`DT_XMATTER_GRP_NM` AS `XMATTER_GRP`,
     `iddtl`.`JOB_NAME` AS `JOB_NAME`,
     `iddtl`.`JOB_RUN_ID` AS `JOB_RUN_ID`,
     `iddtl`.`RETURN_CODE_TEXT` AS `JOB_STATUS`,
     `iddtl`.`START_TIME` AS `JOB_START_TIME`,
     `iddtl`.`END_TIME` AS `JOB_END_TIME`,
     `iddtl`.`AH_RUNTIME` AS `JOB_AH_RUNTIME`,
     `iddtl`.`AH_ERT` AS `JOB_AH_ERT`,
     `iddtl`.`RUN_HIST_URL` AS `JOB_RUN_URL`,
     `iddtl`.`JOB_DURATION` AS `JOB_DURATION`,
     `iddtl`.`Lvl` AS `JOB_LVL`,
     `iddtl`.`ERT_EXCEEDED_IND` AS `JOB_ERT_EXCEEDED_IND`,
     `iddtl`.`JOB_FAIL_IND` AS `JOB_FAIL_IND`,
     `iddtl`.`SLA_IND` AS `JOB_SLA_IND`,
     rank()
        OVER ( PARTITION BY `idd`.`PARENT_RUN_ID`, `iddtl`.`JOB_NAME`
      ORDER BY  `iddtl`.`JOB_RUN_ID` DESC ) AS `jb_rnk`
      FROM `INT_DASH_DATA` `idd`
      LEFT JOIN `INT_DASH_DTL` `iddtl`
        ON `idd`.`PARENT_RUN_ID` = `iddtl`.`PARENT_RUN_ID`
          AND ( `iddtl`.`ERT_EXCEEDED_IND` = "Y"
          OR `iddtl`.`JOB_FAIL_IND` = "Y" )
      WHERE `idd`.`RETURN_CODE_TEXT` NOT IN ("SUCCESS", "CANCELLED", "ENDED_INACTIVE - Task has manually been set inactive.")
          AND ( `idd`.`ERT_EXCEEDED_JOBS` > 0
          OR `idd`.`FAILED_JOBS` > 0
          OR `idd`.`BSNS_SLA_STATUS` = "Missed" )
          {filter_condition}) `wfjob`
      WHERE `wfjob`.`jb_rnk` = 1
        AND (
          `wfjob`.`JOB_STATUS` not in ('SUCCESS', 'CANCELLED', 'ENDED_INACTIVE - Task has manually been set inactive.')
          or `wfjob`.`JOB_STATUS`is NULL
          )) wjrn
      LEFT JOIN ( `INTEGRATED_DASHBOARD_ASSIGNMENT` `ida` )
        ON `wjrn`.`PARENT_RUN_ID` = `ida`.`PARENT_RUN_ID`
          AND `wjrn`.`JOB_NAME` = `ida`.`JOB_NAME`
      LEFT JOIN
        (SELECT `idam`.`PARENT_RUN_ID` AS `PARENT_RUN_ID`,
     `idam`.`JOB_NAME` AS `JOB_NAME`,
     `idam`.`ASSIGNED_USER` AS `ASSIGNED_USER`,
     `idam`.`USER_ROLE` AS `USER_ROLE`
        FROM `INT_DASH_ASSIGNMENT_MAPPING` `idam`
        LEFT JOIN `INT_DASH_ASSIGNMENT_MAPPING` `idam2`
          ON `idam`.`PARENT_RUN_ID` = `idam2`.`PARENT_RUN_ID`
            AND `idam`.`JOB_NAME` = `idam2`.`JOB_NAME`
            AND `idam`.`ASSIGNMENT_MAPPING_SKEY` < `idam2`.`ASSIGNMENT_MAPPING_SKEY`
        WHERE `idam2`.`ASSIGNMENT_MAPPING_SKEY` is NULL ) `rn`
          ON `ida`.`PARENT_RUN_ID` = `rn`.`PARENT_RUN_ID`
            AND `ida`.`JOB_NAME` = `rn`.`JOB_NAME` ) `vw`
      WHERE `vw`.`ASSIGNMENT_STATUS` != "CLOSED"
"""
issues_pending_closure_sql = """
SELECT *
FROM
  (SELECT `idd`.`PARENT_RUN_ID` AS `PARENT_RUN_ID`,
     `idd`.`PARENT_JOB_NAME` AS `PARENT_JOB_NAME`,
     `idd`.`AH_CLIENT` AS `AH_CLIENT`,
     `idd`.`LAST_REFRESH_DT` AS `LAST_REFRESH_DT`,
     `idd`.`DOMAIN_NAME` AS `DOMAIN_NAME`,
     `idd`.`PRODUCT_NAME` AS `PRODUCT_NAME`,
     `idd`.`APPLICATION_NAME` AS `APPLICATION_NAME`,
     `idd`.`RETURN_CODE_TEXT` AS `RETURN_CODE_TEXT`,
     `idd`.`JOB_FLOW_PRIORITY` AS `JOB_FLOW_PRIORITY`,
     `idd`.`ACTIVATION_TIME` AS `ACTIVATION_TIME`,
     `idd`.`START_TIME` AS `START_TIME`,
     `idd`.`END_TIME` AS `END_TIME`,
     `idd`.`CONV_END_TIME` AS `CONV_END_TIME`,
     `idd`.`SLA_TIME_DATE` AS `SLA_TIME_DATE`,
     `idd`.`ORD_KEY` AS `ORD_KEY`,
     `idd`.`AH_RUNTIME` AS `AH_RUNTIME`,
     `idd`.`AH_ERT` AS `AH_ERT`,
     `idd`.`TOTAL_JOBS` AS `TOTAL_JOBS`,
     `idd`.`RUNNING_JOBS` AS `RUNNING_JOBS`,
     `idd`.`FAILED_JOBS` AS `FAILED_JOBS`,
     `idd`.`ERT_EXCEEDED_JOBS` AS `ERT_EXCEEDED_JOBS`,
     `idd`.`RUN_HIST_URL` AS `RUN_HIST_URL`,
     `idd`.`BSNS_SLA_TIME` AS `BSNS_SLA_TIME`,
     `idd`.`BSNS_SLA_TIME_DATE` AS `BSNS_SLA_TIME_DATE`,
     `idd`.`BSNS_SLA_DIFF_MINS` AS `BSNS_SLA_DIFF_MINS`,
     `idd`.`BSNS_SLA_DIFF` AS `BSNS_SLA_DIFF`,
     `idd`.`BSNS_SLA_STATUS` AS `BSNS_SLA_STATUS`,
     `idd`.`SLA_TYPE` AS `SLA_TYPE`,
     `idd`.`SOURCE_NM` AS `SOURCE_NM`,
     `idd`.`CREATED_DT` AS `CREATED_DT`,
     `idd`.`UPDATED_DT` AS `UPDATED_DT`,
     `idd`.`JIRA_PROJECT_KEY` AS `JIRA_PROJECT_KEY`,
     `idd`.`DT_JIRA_COMPONENT` AS `JIRA_COMPONENT`,
     `idd`.`DT_XMATTER_GRP_NM` AS `XMATTER_GRP`,
     `iddtl`.`JOB_NAME` AS `JOB_NAME`,
     `iddtl`.`JOB_RUN_ID` AS `JOB_RUN_ID`,
     `iddtl`.`RETURN_CODE_TEXT` AS `JOB_STATUS`,
     `iddtl`.`START_TIME` AS `JOB_START_TIME`,
     `iddtl`.`END_TIME` AS `JOB_END_TIME`,
     `iddtl`.`AH_RUNTIME` AS `JOB_AH_RUNTIME`,
     `iddtl`.`AH_ERT` AS `JOB_AH_ERT`,
     `iddtl`.`RUN_HIST_URL` AS `JOB_RUN_URL`,
     `iddtl`.`JOB_DURATION` AS `JOB_DURATION`,
     `iddtl`.`Lvl` AS `JOB_LVL`,
     `iddtl`.`ERT_EXCEEDED_IND` AS `JOB_ERT_EXCEEDED_IND`,
     `iddtl`.`JOB_FAIL_IND` AS `JOB_FAIL_IND`,
     `iddtl`.`SLA_IND` AS `JOB_SLA_IND`,
     rank()
    OVER ( PARTITION BY `idd`.`PARENT_RUN_ID`, `iddtl`.`JOB_NAME`
  ORDER BY  `iddtl`.`JOB_RUN_ID` DESC ) AS `jb_rnk`,
     `ida`.`JIRA` AS `JIRA`,
     `ida`.`SERVICE_NOW` AS `SERVICE_NOW`,
     `ida`.`STATUS` AS `ASSIGNMENT_STATUS`,
     `ida`.`ZOOM_LINK` AS `ZOOM_LINK`,
     `ida`.`CLOSE_CODE` AS `CLOSE_CODE`,
     `ida`.`ROOT_CAUSE` AS `ROOT_CAUSE`,
     `ida`.`ERROR` AS `ERROR`,
     `rn`.`ASSIGNED_USER` AS `ASSIGNED_USER`,
     `rn`.`USER_ROLE` AS `USER_ROLE`
  FROM `INTEGRATED_DASHBOARD_ASSIGNMENT` `ida`
  INNER JOIN
    (SELECT `idam`.`PARENT_RUN_ID` AS `PARENT_RUN_ID`,
     `idam`.`JOB_NAME` AS `JOB_NAME`,
     IFNULL(`idam`.`ASSIGNED_USER`,
     "Not Assigned") AS `ASSIGNED_USER`,
     `idam`.`USER_ROLE` AS `USER_ROLE`
    FROM `INT_DASH_ASSIGNMENT_MAPPING` `idam`
    LEFT JOIN `INT_DASH_ASSIGNMENT_MAPPING` `idam2`
      ON `idam`.`PARENT_RUN_ID` = `idam2`.`PARENT_RUN_ID`
        AND `idam`.`JOB_NAME` = `idam2`.`JOB_NAME`
        AND `idam`.`ASSIGNMENT_MAPPING_SKEY` < `idam2`.`ASSIGNMENT_MAPPING_SKEY`
    WHERE `idam2`.`ASSIGNMENT_MAPPING_SKEY` is NULL ) `rn`
      ON `ida`.`STATUS` = "WIP"
        AND `ida`.`PARENT_RUN_ID` = `rn`.`PARENT_RUN_ID`
        AND `ida`.`JOB_NAME` = `rn`.`JOB_NAME`
    INNER JOIN `INT_DASH_DATA` `idd`
      ON `ida`.`PARENT_RUN_ID` = `idd`.`PARENT_RUN_ID`
      {filter_condition}
    INNER JOIN `INT_DASH_DTL` `iddtl`
      ON `idd`.`PARENT_RUN_ID` = `iddtl`.`PARENT_RUN_ID`
        AND `ida`.`JOB_NAME` = `iddtl`.`JOB_NAME` ) `vw`
  WHERE `vw`.`jb_rnk` = 1;
"""
get_pot_sla_miss = """
with raw_data AS (
  SELECT
    DOMAIN_NAME ,
    PRODUCT_NAME ,
    APPLICATION_NAME ,
    PARENT_JOB_NAME ,
    RETURN_CODE_TEXT ,
    BSNS_SLA_TIME_DATE ,
    BSNS_IMPACT_JUSTIFICATION ,
    BSNS_SLA_DIFF_MINS ,
    CC_LIST ,
    TO_LIST,
    MIN_DIFF,
    PARENT_RUN_ID,
    JOB_NAME,
    JIRA,
    CREATED_DT,
    SERVICE_NOW,
    ZOOM_LINK,
    CLOSE_CODE,
    ROOT_CAUSE,
    ERROR
  FROM
    (
     SELECT
       idd.PARENT_RUN_ID as PARENT_RUN_ID ,
       idd.DOMAIN_NAME as DOMAIN_NAME ,
       satm.APPLICATION_NAME as APPLICATION_NAME ,
       idd.PRODUCT_NAME as PRODUCT_NAME  ,
       idd.PARENT_JOB_NAME as PARENT_JOB_NAME ,
       idd.RETURN_CODE_TEXT as RETURN_CODE_TEXT,
       idd.BSNS_SLA_TIME_DATE as  BSNS_SLA_TIME_DATE,
       ida.JOB_NAME as JOB_NAME,
       idd.BSNS_SLA_DIFF_MINS as BSNS_SLA_DIFF_MINS ,
       ida.JIRA as JIRA,
       ida.CREATED_DT as CREATED_DT,
       ida.SERVICE_NOW as SERVICE_NOW,
       ida.ZOOM_LINK  as  ZOOM_LINK,
       ida.CLOSE_CODE as CLOSE_CODE,
       ida.ROOT_CAUSE as ROOT_CAUSE,
       ida.ERROR as  ERROR,
       TIMESTAMPDIFF(SECOND,CONVERT_TZ(now(),'+00:00','-05:00'),
       idd.BSNS_SLA_TIME_DATE ) /60  as MIN_DIFF  ,
       now() as curr_time ,
       CONVERT_TZ(now(),'+00:00','-05:00') as CST_time ,
       satm.BSNS_IMPACT_JUSTIFICATION as  BSNS_IMPACT_JUSTIFICATION,
       'gd-foundsvcs-hadoop@email.wal-mart.com' as CC_LIST ,
       'dfs-mgmt-team@email.wal-mart.com,dsi-mi-comms@wal-mart.com' as TO_LIST ,
       row_number()
       OVER (partition by PARENT_JOB_NAME ORDER BY ACTIVATION_TIME DESC) rn
      FROM
        INT_DASH_DATA  idd  inner join SLA_APP_TEAM_METADATA satm
        on satm.WORKFLOW_NAME=idd.PARENT_JOB_NAME
        left outer join
        INTEGRATED_DASHBOARD_ASSIGNMENT   ida
        on ida.PARENT_RUN_ID= idd.PARENT_RUN_ID
        and idd.PARENT_JOB_NAME = ida.JOB_NAME
       where
        idd.RETURN_CODE_TEXT != 'SUCCESS' and idd.SLA_TYPE = 'B'
    ) main_table
  WHERE
    main_table.rn = 1 )
    select * from raw_data
    where (MIN_DIFF < 60 and MIN_DIFF > -60 )
    """
error_log_search_sql = """
SELECT `ida`.`JOB_NAME` AS `JOB_NAME`,
     `ida`.`ERROR` AS `ERROR`,
     `ida`.`JIRA` AS `JIRA`,
     `ida`.`SERVICE_NOW` AS `SERVICE_NOW`,
     `ida`.`ZOOM_LINK` AS `ZOOM_LINK`,
     `ida`.`ROOT_CAUSE` AS `ROOT_CAUSE`,
     `ida`.`CLOSE_CODE` AS `CLOSE_CODE`,
     `ida`.`STATUS` AS `STATUS`,
     `ida`.`ERT_ALERT` AS `ERT_ALERT`,
     `ida`.`FAILURE_ALERT` AS `FAILURE_ALERT`,
     `ida`.`SLAMISS_ALERT` AS `SLAMISS_ALERT`,
     `ida`.`CREATED_DT` AS `CREATED_DT`,
     `ida`.`UPDATED_DT` AS `UPDATED_DT`,
     JSON_ARRAYAGG(
      JSON_OBJECT(
        'NOTE', `note`.`Notes`,
        'NOTES_SKEY', `note`.`NOTES_SKEY`,
        'UPDATED_DT', `note`.`UPDATED_DT`,
        'UPDATED_USER', `note`.`UPDATED_USER`
        )
      ) AS NOTES
FROM
  `INTEGRATED_DASHBOARD_ASSIGNMENT` `ida`
LEFT JOIN `INTEGRATED_DASHBOARD_NOTES` `note`
  ON `ida`.`PARENT_RUN_ID`=`note`.`PARENT_RUN_ID`
    AND `ida`.`JOB_NAME` =`note`.`JOB_NAME`
WHERE `ida`.ERROR regexp '{search_pattern}'
GROUP BY  `ida`.`JOB_NAME`
ORDER BY  `ida`.`CREATED_DT` desc;
"""
get_sla_health = """
WITH raw_data as
(select DOMAIN_NAME , WRKFLW_NM ,PARENT_RUN_ID, SLA_LST_JOB_NM , FAILED_JOBS,
ERT_EXCEEDED_JOBS ,BSNS_SLA_TIME_DATE,
RETURN_CODE_TEXT ,SLA_DIFF_MINS ,SOURCE_NM  ,START_TIME , END_TIME ,SLA_DIFF  ,SLA_STATUS
from ( select dd.DOMAIN_NAME  as DOMAIN_NAME  ,wsd.WRKFLW_NM as  WRKFLW_NM,
wsd.SLA_LST_JOB_NM  as  SLA_LST_JOB_NM  , dd.START_TIME as START_TIME,
dd.END_TIME as END_TIME  ,dd.SLA_DIFF as SLA_DIFF  , dd.HARD_SLA_STATUS as SLA_STATUS,
dd.FAILED_JOBS  as FAILED_JOBS  , dd.ERT_EXCEEDED_JOBS as ERT_EXCEEDED_JOBS,
dd.BSNS_SLA_TIME_DATE as BSNS_SLA_TIME_DATE,
dd.RETURN_CODE_TEXT as RETURN_CODE_TEXT ,dd.SLA_DIFF_MINS as  SLA_DIFF_MINS,
dd.PARENT_RUN_ID  as  PARENT_RUN_ID, dd.SOURCE_NM as SOURCE_NM,
(TIMESTAMPDIFF(SECOND,now(),dd.START_TIME ) /60 ) as Time_left,
row_number() over (partition by PARENT_JOB_NAME order by START_TIME desc ) rn
from WRKFLW_DTL wsd
join
dfs.INT_DASH_DATA   dd
on
wsd.WRKFLW_NM = dd.PARENT_JOB_NAME
where  dd.SLA_TYPE in ('B','B&I') and wsd.JOB_FREQUENCY not in ('Weekly') )
data  WHERE data.rn =1 ),
dum_dom_sta AS(
select dm.DOMAIN_NAME ,UPPER(st.STATUS)  as STATUS from (
select distinct  DOMAIN_NAME as DOMAIN_NAME from INT_DASH_DATA where DOMAIN_NAME <> 'DFS') dm,
(
select 'Job_Active_SLA_Not_Missed' as STATUS from dual
union
select 'Met' as STATUS  from dual
union
select 'Missed' as STATUS  from dual
union
select 'RUNNING' as STATUS  from dual
union
select 'SUCCESS' as STATUS  from dual
union
select 'FAILED' as STATUS  from dual
union
select 'Total_Count' as STATUS  from dual ) st),
total_cnt
as ( SELECT DN as DOMAIN_NAME ,'Total_Count' as Total_Count,
    count(1) as Cnt from ( select DOMAIN_NAME DN,
  BSNS_SLA_TIME_DATE bstn
  from
    (
      select
        dd.DOMAIN_NAME  as DOMAIN_NAME,
        wsd.WRKFLW_NM as  WRKFLW_NM,
        wsd.SLA_LST_JOB_NM  as  SLA_LST_JOB_NM,
        dd.START_TIME as START_TIME,
        dd.END_TIME as END_TIME,
        dd.SLA_DIFF as SLA_DIFF,
        dd.HARD_SLA_STATUS as SLA_STATUS,
        dd.FAILED_JOBS  as FAILED_JOBS,
        dd.ERT_EXCEEDED_JOBS as ERT_EXCEEDED_JOBS,
        dd.BSNS_SLA_TIME_DATE as BSNS_SLA_TIME_DATE,
        dd.RETURN_CODE_TEXT as RETURN_CODE_TEXT,
        dd.SLA_DIFF_MINS as  SLA_DIFF_MINS,
        dd.PARENT_RUN_ID  as  PARENT_RUN_ID,
        dd.SOURCE_NM as SOURCE_NM,
        row_number() over
        (partition by PARENT_JOB_NAME order by START_TIME desc ) rn
          from WRKFLW_DTL wsd
          join
          dfs.INT_DASH_DATA   dd
          on
          wsd.WRKFLW_NM = dd.PARENT_JOB_NAME
          where  dd.SLA_TYPE in ('B','B&I')
      ) data  WHERE data.rn =1 )
      raw_data
      where raw_data.bstn is not null
      group by DOMAIN_NAME ,Total_Count ),
 wf_status
 as
 (
 SELECT DOMAIN_NAME ,upper(RETURN_CODE_TEXT)  as STATUS,
count(1) as cnt  FROM raw_data group by DOMAIN_NAME ,RETURN_CODE_TEXT
 union
 SELECT DOMAIN_NAME , upper(SLA_STATUS)  as STATUS ,count(1)  as cnt
 FROM raw_data where CURDATE()=date(START_TIME) group by DOMAIN_NAME , SLA_STATUS
 union
 select DOMAIN_NAME ,upper('Total_Count') ,Cnt as cnt  from total_cnt)
 select dum_dom_sta.DOMAIN_NAME , dum_dom_sta.STATUS,
 case when wf_status.cnt is null then 0
 else
 wf_status.cnt end as STATUS_CNT
 from dum_dom_sta
 left outer join
 wf_status
  on
 dum_dom_sta.DOMAIN_NAME=wf_status.DOMAIN_NAME
 and
 dum_dom_sta.STATUS=wf_status.STATUS
 order by dum_dom_sta.DOMAIN_NAME , dum_dom_sta.STATUS
"""
