
options set=EASYSOFT_UNICODE=YES;

/****************************************************************************************************;
*** Program Name : Matching_macro_&aid._2D_nofu.sas                                                                      ;
*** Author  : Germain LONNET                                                                         ;
*** Creation Date    : 02NOV2022                
***--------------------------------------------------------------------------------------------------;
    Description / purpose : matching macro for 2nd dose without condition on 3 months fu minimum                                                           ; 
*****************************************************************************************************;

/* definition of the libnames */

libname i_local odbc dsn="RWDE" pwd="%sysget(DATABRICKS_PWD)" schema="%sysget(DOMINO_STARTING_USERNAME)" ;



/* creation of a test dataset for &aid. 2-doses */

%macro matching_2D_RA_IBD_nofu(aid=, yr=);

/* selecting all variables use for the PS score */



/* calculation of the PSCORE */

/* needs to create a permanent library to store the results of the logisitc procedure ???? */


PROC LOGISTIC data=I_LOCAL.zos97_matching0_&aid._2D_nofu_&yr.(drop=age);
   class vax agegrp medication_use sex race COST_LVL COMOR CONCO_VAX nb_INPAT nb_OUTPT_ER Region Prev_use ;
   model Vax(Event='Vaccinated')= agegrp medication_use index_dt sex race COST_LVL COMOR CONCO_VAX nb_INPAT nb_OUTPT_ER Region Prev_use / link=cloglog;
   output out=&aid._2D p=pscore;
run;

/* storing the index dates related to only one observation  */

PROC SQL ;
    CREATE TABLE BAD_index as
    SELECT index_dt, COUNT(*) as bad_ind
    FROM I_LOCAL.zos97_matching0_&aid._2D_nofu_&yr.
    WHERE vax='Vaccinated'
    GROUP BY index_dt
    HAVING COUNT(*) = 1
    ORDER BY index_dt;
QUIT;

/* matching vaccinated subjects with only one record by index date */
/*  */
/* getting the difference of PS score for related subjects  */

PROC SQL;
    CREATE TABLE match_bad_index AS
    SELECT DISTINCT a.*, b.pscore as pscore_vax, ABS(a.pscore - pscore_vax) AS diff_pscore
    FROM (
        SELECT a.*
        FROM (
            SELECT *
            FROM &aid._2D
            WHERE vax= 'Unvaccinated') AS A INNER JOIN bad_index AS B
        ON a.index_dt=b.index_dt
        ) AS A
    INNER JOIN (
        SELECT * 
        FROM &aid._2D
        WHERE vax='Vaccinated') AS B
    ON a.index_dt=b.index_dt 
    and a.agegrp=b.agegrp and a.medication_use=b.medication_use;
    
QUIT;

/* calculating the rank to get the three lowest differences of PS */

PROC SORT data=match_bad_index;
     by index_dt diff_pscore; 
run;

data match_bad_index_rank;
    set match_bad_index;
    by index_dt diff_pscore;
    retain rank;
    if first.index_dt then rank = 1;
    else rank = rank + 1;
    if rank <= 3 then output;
run;

/* creation of the final dataset */

PROC SQL;
    CREATE TABLE match_bad_index_final AS
    SELECT vax, enrolid, index_dt, agegrp, sex, race, region, nb_inpat, nb_outpt_er,
    cost_lvl, comor, conco_vax, prev_use, medication_use, pscore, pscore as ps,
    0.33 as _MATCHWGT_, 9999999999 as matched_ID, 0 as iteration
    FROM match_bad_index_rank
    UNION
    SELECT vax, enrolid, a.index_dt, agegrp, sex, race, region, nb_inpat, nb_outpt_er,
    cost_lvl, comor, conco_vax, prev_use, medication_use, pscore, pscore as ps,
    1 as _MATCHWGT_, 9999999999 as matched_ID, 0 as iteration
    FROM (
        SELECT *
        FROM &aid._2D
        WHERE vax='Vaccinated') AS A 
    INNER JOIN bad_index AS B
    ON a.index_dt=b.index_dt;
QUIT;
    
/* END OF ADDING SUBJECTS WITH INDEX DATES HAVING ONLY ONE OBSERVATION */


/* getting the total number of iterations */

PROC SQL ;
    SELECT COUNT (DISTINCT index_dt) into: nbtot
    FROM (
        SELECT vax, index_dt
        FROM I_LOCAL.zos97_matching0_&aid._2D_nofu_&yr.
        WHERE index_dt not in (SELECT DISTINCT index_dt FROM bad_index)) AS A
    WHERE vax = 'Vaccinated' 
    ORDER BY index_dt;
QUIT;

%let nbtot = %sysfunc(compress(&nbtot.));
PROC SQL noprint;
    SELECT DISTINCT index_dt format=date9. into :index1 - :index&nbtot.
    FROM (
        select distinct datepart(index_dt) AS index_dt
        FROM (
        SELECT vax, index_dt
        FROM (
            SELECT vax, index_dt
            FROM I_LOCAL.zos97_matching0_&aid._2D_nofu_&yr.
            WHERE vax = 'Vaccinated') AS A
        WHERE index_dt not in (SELECT DISTINCt index_dt FROM bad_index)) AS A
    ) AS A

    ORDER BY index_dt;
QUIT;

/* matching process for each index dates (iterations) */

%do i=1 %to 5;

PROC PSMATCH data=&aid._2D (where=(PSCORE >= 0.000000001 and PSCORE < 1 and datepart(index_dt) = "&&index&i."d));
    CLASS vax agegrp medication_use sex race COST_LVL COMOR CONCO_VAX nb_INPAT nb_OUTPT_ER Region Prev_use;
    MATCH CALIPER = 0.1 DISTANCE = PS EXACT = (agegrp medication_use)  METHOD = GREEDY (K=3 ORDER=RANDOM);
    OUTPUT OUT (OBS=MATCH) = %if &i. = 1 %then %do; matching_&aid._2D_&yr. %end; %else %do; matching_&aid._2D_&i._&yr. %end;
    matchid=matched_ID PS=PS;
    PSDATA  treatvar=Vax(Treated="Vaccinated") ps=pscore;
run;

%if &i. ne 1 %then %do;

data matching_&aid._2D_&i.2_&yr.;
    set matching_&aid._2D_&i._&yr.;
    iteration = &i.;
run;

PROC SQL;
    INSERT INTO matching_&aid._2D2_&yr.
    SELECT *
    FROM matching_&aid._2D_&i.2_&yr.;
QUIT;
%end;

%else %do;
data  matching_&aid._2D2_&yr.;
    set  matching_&aid._2D_&yr.;
    iteration = &i.;
run;
%end;


%end;

libname MATCH '/mnt/artifacts';


data MATCH.zos97_matching_&aid._2D_nofu_&yr.;
    set matching_&aid._2D2_&yr. match_bad_index_final;
run;


%mend;
%matching_2D_RA_IBD_nofu(aid=RA, yr=2018);