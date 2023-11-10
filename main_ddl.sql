create or replace schema TIME_SERIES;

create or replace TABLE BASE_DAY (
	DAY_INDEX NUMBER(18,0),
	DATE_VALUE DATE,
	DAY_NAME VARCHAR(3),
	MONTH_END DATE,
	YEAR NUMBER(4,0),
	YEAR_MONTH NUMBER(2,0),
	YEAR_WEEK NUMBER(2,0),
	YEAR_DAY NUMBER(2,0),
	DAYS_IN_MONTH NUMBER(2,0),
	DAY_OF_WEEK_SUN NUMBER(2,0),
	DAY_OF_WEEK_FRI NUMBER(2,0),
	DAY_OF_WEEK_THU NUMBER(2,0),
	WEEK_END_SUN DATE,
	WEEK_END_FRI DATE,
	WEEK_END_THU DATE,
	primary key (DAY_INDEX)
);
create or replace TABLE BASE_MILLISECOND (
	MS_INDEX NUMBER(19,0),
	MS2 NUMBER(26,0),
	MS4 NUMBER(26,0),
	MS5 NUMBER(26,0),
	MS6 NUMBER(26,0),
	MS8 NUMBER(26,0),
	MS10 NUMBER(26,0),
	MS20 NUMBER(26,0),
	MS30 NUMBER(26,0),
	MS40 NUMBER(26,0),
	MS50 NUMBER(26,0),
	MS100 NUMBER(26,0),
	MS200 NUMBER(26,0),
	MS300 NUMBER(26,0),
	MS400 NUMBER(26,0),
	MS500 NUMBER(26,0),
	primary key (MS_INDEX)
);
create or replace TABLE BASE_SECOND (
	SECOND_INDEX NUMBER(19,0),
	TM TIME(9),
	S2 NUMBER(26,0),
	S4 NUMBER(26,0),
	S3 NUMBER(26,0),
	S5 NUMBER(26,0),
	S10 NUMBER(26,0),
	S15 NUMBER(26,0),
	S20 NUMBER(26,0),
	S30 NUMBER(26,0),
	M1 NUMBER(26,0),
	M2 NUMBER(26,0),
	M3 NUMBER(26,0),
	M5 NUMBER(26,0),
	M10 NUMBER(26,0),
	M15 NUMBER(26,0),
	M20 NUMBER(26,0),
	M30 NUMBER(26,0),
	H1 NUMBER(26,0),
	H2 NUMBER(26,0),
	H3 NUMBER(26,0),
	H4 NUMBER(26,0),
	H6 NUMBER(26,0),
	H12 NUMBER(26,0),
	S1M NUMBER(1,0),
	S2M NUMBER(1,0),
	S3M NUMBER(1,0),
	S4M NUMBER(1,0),
	S5M NUMBER(1,0),
	S10M NUMBER(1,0),
	S15M NUMBER(1,0),
	S20M NUMBER(1,0),
	S30M NUMBER(1,0),
	M1M NUMBER(1,0),
	M2M NUMBER(1,0),
	M3M NUMBER(1,0),
	M5M NUMBER(1,0),
	M10M NUMBER(1,0),
	M15M NUMBER(1,0),
	M20M NUMBER(1,0),
	M30M NUMBER(1,0),
	H1M NUMBER(1,0),
	H2M NUMBER(1,0),
	H3M NUMBER(1,0),
	H4M NUMBER(1,0),
	H6M NUMBER(1,0),
	H12M NUMBER(1,0),
	D1M NUMBER(1,0),
	HOUR_INDEX NUMBER(2,0),
	MINUTE_INDEX NUMBER(2,0)
);
create or replace TABLE EQ (
	EQ_UID NUMBER(10,0),
	EQ_NAME VARCHAR(100),
	DSCR VARCHAR(1000)
);
create or replace TABLE EQ_TAG (
	EQ_UID NUMBER(10,0),
	TAG_UID NUMBER(10,0),
	ATTR_NAME VARCHAR(100)
);
create or replace TABLE TAG (
	TAG_UID NUMBER(10,0) NOT NULL,
	TAGNAME VARCHAR(100),
	DSCR VARCHAR(1000),
	primary key (TAG_UID)
);
create or replace TABLE TAG_EVENT (
	EVENT_UID NUMBER(10,0),
	EVENT_TYPE_UID NUMBER(10,0),
	TAG_UID NUMBER(10,0),
	START_TS TIMESTAMP_NTZ(9),
	END_TS TIMESTAMP_NTZ(9)
);
create or replace TABLE TAG_GROUP (
	GROUP_NAME VARCHAR(100),
	TAG_UID NUMBER(10,0)
);
create or replace TRANSIENT TABLE TAG_VALUE (
	TAG_UID NUMBER(10,0),
	D DATE,
	TS TIMESTAMP_NTZ(9),
	VAL NUMBER(38,12),
	DAY_INDEX NUMBER(10,0),
	SECOND_INDEX NUMBER(7,0),
	MS_INDEX NUMBER(4,0)
);

create or replace view TAG_VALUE_DV(
	TAGNAME,
	D,
	TS,
	VAL,
	TAG_UID
) as 
select
    t.tagname,
    tv.d,
    tv.ts,
    tv.val,
    tv.tag_uid
from
    tag t,
    tag_value tv
where
    t.tag_uid = tv.tag_uid;
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
	TYPE = csv
	SKIP_HEADER = 1
	NULL_IF = ('NULL', 'null')
	COMPRESSION = gzip
;

CREATE OR REPLACE FUNCTION "TAG_VALUE_LAST_INTERPOLATE"("V_STARTTIME" TIMESTAMP_NTZ(9), "V_ENDTIME" TIMESTAMP_NTZ(9), "V_TAGLIST" VARCHAR(16777216), "V_SAMPLERATE" VARCHAR(10))
RETURNS TABLE ("TS" TIMESTAMP_NTZ(9), "TAG_UID" NUMBER(38,0), "VAL" NUMBER(38,12))
LANGUAGE SQL
AS '
    -- creates a table of signal UIDs ("tag" uids in historian speak) from the input parameter
with taglist as 
    (select to_number(trim(value))tag_uid from table(split_to_table(v_TagList,'',''))),
    -- sets the start time for gathering raw tag data as the earliest row for all of the tags
    -- in the parameter list which have data before (or equal to) the StartTime parameter
    -- This ensures that we have values to interpolate from if there is no value in the raw
    -- table at the StartTime requested (for each tag)    
    mn as ( -- get latest "before" timestamp for each tag
    select nvl(min(ts), v_StartTime) ts from (
        select tag_uid, max(ts) ts from tag_value  -- gets the earliest tag times for each tag
        where tag_uid in (select tag_uid from taglist)  
        and d between dateadd(''day'', -30, v_StartTime) and v_StartTime
        and ts <= v_StartTime
        group by tag_uid)),
    -- gets all of the raw tag values over period including earlier and later than specified
    -- to enable interpolation at start and end
    vals as
    (select 
          tv.tag_uid,
          tv.ts,
          tv.d,
          tv.second_index,
          tv.ms_index,
          avg(tv.val) val -- averaging may not be necessary if source data does not have duplicates
      from 
          tag_value tv
      where  
          tv.d between dateadd(day, -30, v_StartTime) and dateadd(day, 30, v_EndTime) -- limit improves pruning
          -- will have issues of course if there is no prior value within 30 days of requested data
          and tv.ts between (select ts from mn) and v_EndTime
      group by tv.tag_uid,
          tv.ts,
          tv.d,
          tv.second_index,
          tv.ms_index),
    -- generate the set of rows at over the required range and sample rate for each tag
    -- the base_second table contains standard aggregation markers to select from the 86400
    -- possible values per day.  Note this function does not interpolate at the millisecond level          
    tag_times as(
      select
        t.tag_uid,
        d.day_index,
        d.date_value,
        s.second_index,
        timestampadd(second, s.second_index, d.date_value) ts,
        1 include
      from
          base_day d,
          base_second s,
          taglist t
      where
          d.date_value between date_trunc(day, v_StartTime) and date_trunc(day, dateadd(day, 1, v_EndTime))
          and timestampadd(''second'', s.second_index, d.date_value) between v_StartTime and v_EndTime
          and decode(v_SampleRate, ''S1'', s.s1m, ''S2'', s.s2m, ''S4'', s.s4m, ''S5'', s.s5m, ''S10'', s.s10m, ''S20'', s.s20m, ''S30'', s.s30m, 
                     ''M1'', s.m1m, ''M2'', s.m2m, ''M5'', s.m5m, ''M10'', s.m10m, ''M20'', s.m20m, ''M30'', s.m30m, 
                     ''H1'', s.h1m, ''H2'', s.h2m, ''H3'', s.h3m, ''H4'', s.h4m, ''H6'', s.h6m, ''H12'', s.h12m, s.d1m ) = 0),
    -- a full outer join containing both the generated tag\\time rows unioned with 
    -- the raw values over the required time range                     
    tag_times_values as (
      select
          nvl(i.ts, r.ts) ts,
          nvl(i.tag_uid, r.tag_uid) tag_uid,
          r.val rawval,
          i.include
      from
          vals r full outer join tag_times i on i.tag_uid = r.tag_uid and i.date_value = r.d and i.ts = r.ts
    ),
    interp as (
      select 
      ts,
      tag_uid,
      rawval,
      last_value(rawval ignore nulls) over (partition by tag_uid order by ts rows between unbounded preceding and current row) as ival,
      include
    from tag_times_values
    )
select 
  ts,
  tag_uid,
  ival val
from interp 
where include = 1
order by tag_uid, ts 
';
CREATE OR REPLACE SECURE FUNCTION "TAG_VALUE_LAST_INTERPOLATE_S"("V_STARTTIME" TIMESTAMP_NTZ(9), "V_ENDTIME" TIMESTAMP_NTZ(9), "V_TAGLIST" VARCHAR(16777216), "V_SAMPLERATE" VARCHAR(10))
RETURNS TABLE ("TS" TIMESTAMP_NTZ(9), "TAG_UID" NUMBER(38,0), "VAL" NUMBER(38,12))
LANGUAGE SQL
AS '
    -- creates a table of signal UIDs ("tag" uids in historian speak) from the input parameter
with taglist as 
    (select to_number(trim(value))tag_uid from table(split_to_table(v_TagList,'',''))),
    -- sets the start time for gathering raw tag data as the earliest row for all of the tags
    -- in the parameter list which have data before (or equal to) the StartTime parameter
    -- This ensures that we have values to interpolate from if there is no value in the raw
    -- table at the StartTime requested (for each tag)    
    mn as ( -- get latest "before" timestamp for each tag
    select nvl(min(ts), v_StartTime) ts from (
        select tag_uid, max(ts) ts from tag_value  -- gets the earliest tag times for each tag
        where tag_uid in (select tag_uid from taglist)  
        and d between dateadd(''day'', -30, v_StartTime) and v_StartTime
        and ts <= v_StartTime
        group by tag_uid)),
    -- gets all of the raw tag values over period including earlier and later than specified
    -- to enable interpolation at start and end
    vals as
    (select 
          tv.tag_uid,
          tv.ts,
          tv.d,
          tv.second_index,
          tv.ms_index,
          avg(tv.val) val -- averaging may not be necessary if source data does not have duplicates
      from 
          tag_value tv
      where  
          tv.d between dateadd(day, -30, v_StartTime) and dateadd(day, 30, v_EndTime) -- limit improves pruning
          -- will have issues of course if there is no prior value within 30 days of requested data
          and tv.ts between (select ts from mn) and v_EndTime
      group by tv.tag_uid,
          tv.ts,
          tv.d,
          tv.second_index,
          tv.ms_index),
    -- generate the set of rows at over the required range and sample rate for each tag
    -- the base_second table contains standard aggregation markers to select from the 86400
    -- possible values per day.  Note this function does not interpolate at the millisecond level          
    tag_times as(
      select
        t.tag_uid,
        d.day_index,
        d.date_value,
        s.second_index,
        timestampadd(second, s.second_index, d.date_value) ts,
        1 include
      from
          base_day d,
          base_second s,
          taglist t
      where
          d.date_value between date_trunc(day, v_StartTime) and date_trunc(day, dateadd(day, 1, v_EndTime))
          and timestampadd(''second'', s.second_index, d.date_value) between v_StartTime and v_EndTime
          and decode(v_SampleRate, ''S1'', s.s1m, ''S2'', s.s2m, ''S4'', s.s4m, ''S5'', s.s5m, ''S10'', s.s10m, ''S20'', s.s20m, ''S30'', s.s30m, 
                     ''M1'', s.m1m, ''M2'', s.m2m, ''M5'', s.m5m, ''M10'', s.m10m, ''M20'', s.m20m, ''M30'', s.m30m, 
                     ''H1'', s.h1m, ''H2'', s.h2m, ''H3'', s.h3m, ''H4'', s.h4m, ''H6'', s.h6m, ''H12'', s.h12m, s.d1m ) = 0),
    -- a full outer join containing both the generated tag\\time rows unioned with 
    -- the raw values over the required time range                     
    tag_times_values as (
      select
          nvl(i.ts, r.ts) ts,
          nvl(i.tag_uid, r.tag_uid) tag_uid,
          r.val rawval,
          i.include
      from
          vals r full outer join tag_times i on i.tag_uid = r.tag_uid and i.date_value = r.d and i.ts = r.ts
    ),
    interp as (
      select 
      ts,
      tag_uid,
      rawval,
      last_value(rawval ignore nulls) over (partition by tag_uid order by ts rows between unbounded preceding and current row) as ival,
      include
    from tag_times_values
    )
select 
  ts,
  tag_uid,
  ival val
from interp 
where include = 1
order by tag_uid, ts 
';
CREATE OR REPLACE FUNCTION "TAG_VALUE_LIN_INTERPOLATE"("V_STARTTIME" TIMESTAMP_NTZ(9), "V_ENDTIME" TIMESTAMP_NTZ(9), "V_TAGLIST" VARCHAR(16777216), "V_SAMPLERATE" VARCHAR(10))
RETURNS TABLE ("TS" TIMESTAMP_NTZ(9), "TAG_UID" NUMBER(38,0), "VAL" NUMBER(38,12))
LANGUAGE SQL
AS '
    -- creates a table of signal UIDs ("tag" uids in historian speak) from the input parameter
with taglist as 
    (select to_number(trim(value))tag_uid from table(split_to_table(v_TagList,'',''))),
    -- sets the start time for gathering raw tag data as the earliest row for all of the tags
    -- in the parameter list which have data before (or equal to) the StartTime parameter
    -- This ensures that we have values to interpolate from if there is no value in the raw
    -- table at the StartTime requested (for each tag)
    mn as ( 
    select nvl(min(ts), v_StartTime) ts from (
        select tag_uid, max(ts) ts from tag_value  -- gets the earliest tag times for each tag
        where tag_uid in (select tag_uid from taglist)  
        and d between dateadd(day, -30, v_StartTime) and v_StartTime
        and ts <= v_StartTime
        group by tag_uid)),
    -- as with the start time this gets the latest row from the raw table required
    -- to ensure we have something to interpolate to if there is no value for a tag
    -- at the EndTime requested
    mx as ( -- get earliest "after" timestamp for each tag
        select nvl(max(ts), v_EndTime) ts from (
        select tag_uid, min(ts) ts from tag_value -- gets the latest tag times for each tag
        where tag_uid in (select tag_uid from taglist)  
        and d between dateadd(day, -1, v_EndTime) and dateadd(day, 30, v_EndTime)
        and ts >= v_EndTime
        group by tag_uid)),
    -- gets all of the raw tag values over period including earlier and later than specified 
    -- to enable interpolation at start and end
    vals as (
    select 
        tv.tag_uid,
        tv.ts,
        tv.d,
        tv.second_index,
        tv.ms_index,
        avg(tv.val) val -- averaging may not be necessary if source data does not have duplicates
    from 
        tag_value tv
    where  
        tag_uid in (select tag_uid from taglist)
        and tv.d between dateadd(day, -30, v_StartTime) and dateadd(day, 30, v_EndTime) -- limit improves pruning
        -- will have issues of course if there is no prior value within 30 days of requested data
        and tv.ts between (select ts from mn) and (select ts from mx)
    group by tv.tag_uid,
        tv.ts,
        tv.d,
        tv.second_index,
        tv.ms_index),
    -- generate the set of rows at over the required range and sample rate for each tag
    -- the base_second table contains standard aggregation markers to select from the 86400
    -- possible values per day.  Note this function does not interpolate at the millisecond level
    tag_times as
    (
    select
      t.tag_uid,
      d.day_index,
      d.date_value,
      s.second_index,
      timestampadd(second, s.second_index, d.date_value) ts,
      1 include
    from
        base_day d,
        base_second s,
        taglist t
    where
        d.date_value between date_trunc(day, v_StartTime) and date_trunc(day, dateadd(day, 1, v_EndTime))
        and timestampadd(''second'', s.second_index, d.date_value) between v_StartTime and v_EndTime
        and decode(v_SampleRate, ''S1'', s.s1m, ''S2'', s.s2m, ''S4'', s.s4m, ''S5'', s.s5m, ''S10'', s.s10m, ''S20'', s.s20m, ''S30'', s.s30m, 
                   ''M1'', s.m1m, ''M2'', s.m2m, ''M5'', s.m5m, ''M10'', s.m10m, ''M20'', s.m20m, ''M30'', s.m30m, 
                   ''H1'', s.h1m, ''H2'', s.h2m, ''H3'', s.h3m, ''H4'', s.h4m, ''H6'', s.h6m, ''H12'', s.h12m, s.d1m) = 0
      ),
    -- a full outer join containing both the generated tag\\time rows unioned with 
    -- the raw values over the required time range  
    tag_times_vals as (
        select
          nvl(i.ts, r.ts) ts,
          nvl(i.tag_uid, r.tag_uid) tag_uid,
          r.ts rawts,
          r.val rawval,
          last_value(r.val ignore nulls) over (partition by nvl(i.tag_uid, r.tag_uid) order by nvl(i.ts, r.ts) rows between unbounded preceding and current row) as last_val,
          first_value(r.val ignore nulls) over (partition by nvl(i.tag_uid, r.tag_uid) order by nvl(i.ts, r.ts) rows between current row and unbounded following) as next_val,
          last_value(r.ts ignore nulls) over (partition by nvl(i.tag_uid, r.tag_uid) order by nvl(i.ts, r.ts) rows between unbounded preceding and current row) as last_val_ts,    
          first_value(r.ts ignore nulls) over (partition by nvl(i.tag_uid, r.tag_uid) order by nvl(i.ts, r.ts) rows between current row and unbounded following) as next_val_ts,              
          i.include
        from
          vals r full outer join tag_times i on i.tag_uid = r.tag_uid and i.date_value = r.d and i.ts = r.ts
        ),
     -- the interpolated values using linear interpolation
     interp as (
        select
            ts,
            tag_uid,
            rawval,
            last_val,
            next_val,
            last_val_ts,
            next_val_ts,
            include,
            timestampdiff(second, last_val_ts, next_val_ts) tdif_base,
            timestampdiff(second, last_val_ts, ts) tdif,
            nvl2(last_val_ts, nvl(rawval, decode(next_val - last_val, 0, last_val, last_val + (next_val - last_val) / tdif_base * tdif)), null) ival
       from
        tag_times_vals
       where include = 1
         )
    select 
        ts,
        tag_uid,
        ival
    from interp 
    order by tag_uid, ts
';
CREATE OR REPLACE FUNCTION "TAG_VALUE_LIN_INTERPOLATE_ON_TAG"("V_STARTTIME" TIMESTAMP_NTZ(9), "V_ENDTIME" TIMESTAMP_NTZ(9), "V_TAGLIST" VARCHAR(16777216), "V_SAMPLERATE" VARCHAR(10))
RETURNS TABLE ("TS" TIMESTAMP_NTZ(9), "TAG_UID" NUMBER(38,0), "VAL" NUMBER(38,12))
LANGUAGE SQL
AS '
    -- creates a table of signal UIDs ("tag" uids in historian speak) from the input parameter
    -- uses the first tag in the taglist as the time periods generator - all tag values for 
    -- subsequent tags in the taglist will be interpolated to the times of the first tag
with taglist as 
    (select to_number(trim(value))tag_uid from table(split_to_table(v_TagList,'',''))),
    -- sets the start time for gathering raw tag data as the earliest row for all of the tags
    -- in the parameter list which have data before (or equal to) the StartTime parameter
    -- This ensures that we have values to interpolate from if there is no value in the raw
    -- table at the StartTime requested (for each tag)
mn as ( 
    select nvl(min(ts), v_StartTime) ts from (
        select tag_uid, max(ts) ts from tag_value  -- gets the earliest tag times for each tag
        where tag_uid in (select tag_uid from taglist)  
        and d between dateadd(day, -30, v_StartTime) and v_StartTime
        and ts <= v_StartTime
        group by tag_uid)),
    -- as with the start time this gets the latest row from the raw table required
    -- to ensure we have something to interpolate to if there is no value for a tag
    -- at the EndTime requested
mx as ( -- get earliest "after" timestamp for each tag
        select nvl(max(ts), v_EndTime) ts from (
        select tag_uid, min(ts) ts from tag_value -- gets the latest tag times for each tag
        where tag_uid in (select tag_uid from taglist)  
        and d between dateadd(day, -1, v_EndTime) and dateadd(day, 30, v_EndTime)
        and ts >= v_EndTime
        group by tag_uid)),
    -- gets all of the raw tag values over period including earlier and later than specified 
    -- to enable interpolation at start and end
vals as (
    select 
        tv.tag_uid,
        tv.ts,
        tv.d,
        tv.second_index,
        tv.ms_index,
        avg(tv.val) val -- averaging may not be necessary if source data does not have duplicates
    from 
        tag_value tv
    where  
        tag_uid in (select tag_uid from taglist)
        and tv.d between dateadd(day, -30, v_StartTime) and dateadd(day, 30, v_EndTime) -- limit improves pruning
        -- will have issues of course if there is no prior value within 30 days of requested data
        and tv.ts between (select ts from mn) and (select ts from mx)
    group by tv.tag_uid,
        tv.ts,
        tv.d,
        tv.second_index,
        tv.ms_index),
    -- generate the set of rows at over the required range and sample rate for each tag
    -- the base_second table contains standard aggregation markers to select from the 86400
    -- possible values per day.  Note this function does not interpolate at the millisecond level
tag_times as
    (
    select
      t.tag_uid,
      v.day_index,
      v.d date_value,
      v.second_index,
      v.ts,
      1 include
    from
        tag_value v,
        taglist t
    where
        v.tag_uid = to_number(split_part(V_TAGLIST,'','',1))
        and v.d between date_trunc(day, v_StartTime) and date_trunc(day, dateadd(day, 1, v_EndTime))
        and v.ts between v_StartTime and v_EndTime
      ),
    -- a full outer join containing both the generated tag\\\\time rows unioned with 
    -- the raw values over the required time range  
    tag_times_vals as (
        select
          nvl(i.ts, r.ts) ts,
          nvl(i.tag_uid, r.tag_uid) tag_uid,
          r.ts rawts,
          r.val rawval,
          last_value(r.val ignore nulls) over (partition by nvl(i.tag_uid, r.tag_uid) order by nvl(i.ts, r.ts) rows between unbounded preceding and current row) as last_val,
          first_value(r.val ignore nulls) over (partition by nvl(i.tag_uid, r.tag_uid) order by nvl(i.ts, r.ts) rows between current row and unbounded following) as next_val,
          last_value(r.ts ignore nulls) over (partition by nvl(i.tag_uid, r.tag_uid) order by nvl(i.ts, r.ts) rows between unbounded preceding and current row) as last_val_ts,    
          first_value(r.ts ignore nulls) over (partition by nvl(i.tag_uid, r.tag_uid) order by nvl(i.ts, r.ts) rows between current row and unbounded following) as next_val_ts,              
          i.include
        from
          vals r full outer join tag_times i on i.tag_uid = r.tag_uid and i.date_value = r.d and i.ts = r.ts
        ),
     -- the interpolated values using linear interpolation
interp as (
        select
            ts,
            tag_uid,
            rawval,
            last_val,
            next_val,
            last_val_ts,
            next_val_ts,
            include,
            timestampdiff(second, last_val_ts, next_val_ts) tdif_base,
            timestampdiff(second, last_val_ts, ts) tdif,
            nvl2(last_val_ts, nvl(rawval, decode(next_val - last_val, 0, last_val, last_val + (next_val - last_val) / tdif_base * tdif)), null) ival
       from
        tag_times_vals
       where include = 1
         )
select 
    ts,
    tag_uid,
    ival
from interp 
order by tag_uid, ts
';
CREATE OR REPLACE SECURE FUNCTION "TAG_VALUE_LIN_INTERPOLATE_S"("V_STARTTIME" TIMESTAMP_NTZ(9), "V_ENDTIME" TIMESTAMP_NTZ(9), "V_TAGLIST" VARCHAR(16777216), "V_SAMPLERATE" VARCHAR(10))
RETURNS TABLE ("TS" TIMESTAMP_NTZ(9), "TAG_UID" NUMBER(38,0), "VAL" NUMBER(38,12))
LANGUAGE SQL
AS '
    -- creates a table of signal UIDs ("tag" uids in historian speak) from the input parameter
with taglist as 
    (select to_number(trim(value))tag_uid from table(split_to_table(v_TagList,'',''))),
    -- sets the start time for gathering raw tag data as the earliest row for all of the tags
    -- in the parameter list which have data before (or equal to) the StartTime parameter
    -- This ensures that we have values to interpolate from if there is no value in the raw
    -- table at the StartTime requested (for each tag)
    mn as ( 
    select nvl(min(ts), v_StartTime) ts from (
        select tag_uid, max(ts) ts from tag_value  -- gets the earliest tag times for each tag
        where tag_uid in (select tag_uid from taglist)  
        and d between dateadd(day, -30, v_StartTime) and v_StartTime
        and ts <= v_StartTime
        group by tag_uid)),
    -- as with the start time this gets the latest row from the raw table required
    -- to ensure we have something to interpolate to if there is no value for a tag
    -- at the EndTime requested
    mx as ( -- get earliest "after" timestamp for each tag
        select nvl(max(ts), v_EndTime) ts from (
        select tag_uid, min(ts) ts from tag_value -- gets the latest tag times for each tag
        where tag_uid in (select tag_uid from taglist)  
        and d between dateadd(day, -1, v_EndTime) and dateadd(day, 30, v_EndTime)
        and ts >= v_EndTime
        group by tag_uid)),
    -- gets all of the raw tag values over period including earlier and later than specified 
    -- to enable interpolation at start and end
    vals as (
    select 
        tv.tag_uid,
        tv.ts,
        tv.d,
        tv.second_index,
        tv.ms_index,
        avg(tv.val) val -- averaging may not be necessary if source data does not have duplicates
    from 
        tag_value tv
    where  
        tag_uid in (select tag_uid from taglist)
        and tv.d between dateadd(day, -30, v_StartTime) and dateadd(day, 30, v_EndTime) -- limit improves pruning
        -- will have issues of course if there is no prior value within 30 days of requested data
        and tv.ts between (select ts from mn) and (select ts from mx)
    group by tv.tag_uid,
        tv.ts,
        tv.d,
        tv.second_index,
        tv.ms_index),
    -- generate the set of rows at over the required range and sample rate for each tag
    -- the base_second table contains standard aggregation markers to select from the 86400
    -- possible values per day.  Note this function does not interpolate at the millisecond level
    tag_times as
    (
    select
      t.tag_uid,
      d.day_index,
      d.date_value,
      s.second_index,
      timestampadd(second, s.second_index, d.date_value) ts,
      1 include
    from
        base_day d,
        base_second s,
        taglist t
    where
        d.date_value between date_trunc(day, v_StartTime) and date_trunc(day, dateadd(day, 1, v_EndTime))
        and timestampadd(''second'', s.second_index, d.date_value) between v_StartTime and v_EndTime
        and decode(v_SampleRate, ''S1'', s.s1m, ''S2'', s.s2m, ''S4'', s.s4m, ''S5'', s.s5m, ''S10'', s.s10m, ''S20'', s.s20m, ''S30'', s.s30m, 
                   ''M1'', s.m1m, ''M2'', s.m2m, ''M5'', s.m5m, ''M10'', s.m10m, ''M20'', s.m20m, ''M30'', s.m30m, 
                   ''H1'', s.h1m, ''H2'', s.h2m, ''H3'', s.h3m, ''H4'', s.h4m, ''H6'', s.h6m, ''H12'', s.h12m, s.d1m) = 0
      ),
    -- a full outer join containing both the generated tag\\time rows unioned with 
    -- the raw values over the required time range  
    tag_times_vals as (
        select
          nvl(i.ts, r.ts) ts,
          nvl(i.tag_uid, r.tag_uid) tag_uid,
          r.ts rawts,
          r.val rawval,
          last_value(r.val ignore nulls) over (partition by nvl(i.tag_uid, r.tag_uid) order by nvl(i.ts, r.ts) rows between unbounded preceding and current row) as last_val,
          first_value(r.val ignore nulls) over (partition by nvl(i.tag_uid, r.tag_uid) order by nvl(i.ts, r.ts) rows between current row and unbounded following) as next_val,
          last_value(r.ts ignore nulls) over (partition by nvl(i.tag_uid, r.tag_uid) order by nvl(i.ts, r.ts) rows between unbounded preceding and current row) as last_val_ts,    
          first_value(r.ts ignore nulls) over (partition by nvl(i.tag_uid, r.tag_uid) order by nvl(i.ts, r.ts) rows between current row and unbounded following) as next_val_ts,              
          i.include
        from
          vals r full outer join tag_times i on i.tag_uid = r.tag_uid and i.date_value = r.d and i.ts = r.ts
        ),
     -- the interpolated values using linear interpolation
     interp as (
        select
            ts,
            tag_uid,
            rawval,
            last_val,
            next_val,
            last_val_ts,
            next_val_ts,
            include,
            timestampdiff(second, last_val_ts, next_val_ts) tdif_base,
            timestampdiff(second, last_val_ts, ts) tdif,
            nvl2(last_val_ts, nvl(rawval, decode(next_val - last_val, 0, last_val, last_val + (next_val - last_val) / tdif_base * tdif)), null) ival
       from
        tag_times_vals
       where include = 1
         )
    select 
        ts,
        tag_uid,
        ival
    from interp 
    order by tag_uid, ts
';
CREATE OR REPLACE FUNCTION "TAG_VALUE_NEXT_INTERPOLATE"("V_STARTTIME" TIMESTAMP_NTZ(9), "V_ENDTIME" TIMESTAMP_NTZ(9), "V_TAGLIST" VARCHAR(16777216), "V_SAMPLERATE" VARCHAR(10))
RETURNS TABLE ("TS" TIMESTAMP_NTZ(9), "TAG_UID" NUMBER(38,0), "VAL" NUMBER(38,12))
LANGUAGE SQL
AS '
-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
-- Chris Waters
-- Updated 2022 5 6
-- Function generates next value interpolation of raw signal values stored in 
-- TAG_VALUE table.  Raw values can be stored at any sample rate although
-- interpolation is done only to 1 second minimmum granularity
--
-- v_TagList is a comma delimited varchar listing of all required tag_uids
-- v_SampleRate is a varchar that qualifies the interval between interpolated
--      values.  It can be set to S1 (one second), S2, S4, S5, S10, S20, S30
--      M1 (one minute), M2, M5, M10, M20, M30, H1, H2, H3, H4, H6, H12, D1 (one day)
--
-- if vStartTime occurs before values are present in the raw table (tag_value)
-- or vEndTime occurs after values are present,
-- the function returns null records until the first available value and null records
-- after the last available value.
-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
    -- creates a table of signal UIDs ("tag" uids in historian speak) from the input parameter
with taglist as 
    (select to_number(trim(value))tag_uid from table(split_to_table(v_TagList,'',''))),
    -- sets the start time for gathering raw tag data as the earliest row for all of the tags
    -- in the parameter list which have data before (or equal to) the StartTime parameter
    -- This ensures that we have values to interpolate from if there is no value in the raw
    -- table at the StartTime requested (for each tag)    
    mn as ( -- get latest "before" timestamp for each tag
    select nvl(min(ts), v_StartTime) ts from (
        select tag_uid, max(ts) ts from tag_value  -- gets the earliest tag times for each tag
        where tag_uid in (select tag_uid from taglist)  
        and d between dateadd(''day'', -30, v_StartTime) and v_StartTime
        and ts <= v_StartTime
        group by tag_uid)),
    -- gets all of the raw tag values over period including earlier and later than specified
    -- to enable interpolation at start and end
    vals as
    (select 
          tv.tag_uid,
          tv.ts,
          tv.d,
          tv.second_index,
          tv.ms_index,
          avg(tv.val) val -- averaging may not be necessary if source data does not have duplicates
      from 
          tag_value tv
      where  
          tv.d between dateadd(day, -30, v_StartTime) and dateadd(day, 30, v_EndTime) -- limit improves pruning
          -- will have issues of course if there is no prior value within 30 days of requested data
          and tv.ts between (select ts from mn) and v_EndTime
      group by tv.tag_uid,
          tv.ts,
          tv.d,
          tv.second_index,
          tv.ms_index),
    -- generate the set of rows at over the required range and sample rate for each tag
    -- the base_second table contains standard aggregation markers to select from the 86400
    -- possible values per day.  Note this function does not interpolate at the millisecond level          
    tag_times as(
      select
        t.tag_uid,
        d.day_index,
        d.date_value,
        s.second_index,
        timestampadd(second, s.second_index, d.date_value) ts,
        1 include
      from
          base_day d,
          base_second s,
          taglist t
      where
          d.date_value between date_trunc(day, v_StartTime) and date_trunc(day, dateadd(day, 1, v_EndTime))
          and timestampadd(''second'', s.second_index, d.date_value) between v_StartTime and v_EndTime
          and decode(v_SampleRate, ''S1'', s.s1m, ''S2'', s.s2m, ''S4'', s.s4m, ''S5'', s.s5m, ''S10'', s.s10m, ''S20'', s.s20m, ''S30'', s.s30m, 
                     ''M1'', s.m1m, ''M2'', s.m2m, ''M5'', s.m5m, ''M10'', s.m10m, ''M20'', s.m20m, ''M30'', s.m30m, 
                     ''H1'', s.h1m, ''H2'', s.h2m, ''H3'', s.h3m, ''H4'', s.h4m, ''H6'', s.h6m, ''H12'', s.h12m, s.d1m ) = 0),
    -- a full outer join containing both the generated tag\\time rows unioned with 
    -- the raw values over the required time range                     
    tag_times_values as (
      select
          nvl(i.ts, r.ts) ts,
          nvl(i.tag_uid, r.tag_uid) tag_uid,
          r.val rawval,
          i.include
      from
          vals r full outer join tag_times i on i.tag_uid = r.tag_uid and i.date_value = r.d and i.ts = r.ts
    ),
    interp as (
      select 
      ts,
      tag_uid,
      rawval,
      first_value(rawval ignore nulls) over (partition by tag_uid order by ts rows between current row and unbounded following) as ival,
      include
    from tag_times_values
    )
select 
  ts,
  tag_uid,
  ival val
from interp 
where include = 1
order by tag_uid, ts 
';
CREATE OR REPLACE SECURE FUNCTION "TAG_VALUE_NEXT_INTERPOLATE_S"("V_STARTTIME" TIMESTAMP_NTZ(9), "V_ENDTIME" TIMESTAMP_NTZ(9), "V_TAGLIST" VARCHAR(16777216), "V_SAMPLERATE" VARCHAR(10))
RETURNS TABLE ("TS" TIMESTAMP_NTZ(9), "TAG_UID" NUMBER(38,0), "VAL" NUMBER(38,12))
LANGUAGE SQL
AS '
    -- creates a table of signal UIDs ("tag" uids in historian speak) from the input parameter
with taglist as 
    (select to_number(trim(value))tag_uid from table(split_to_table(v_TagList,'',''))),
    -- sets the start time for gathering raw tag data as the earliest row for all of the tags
    -- in the parameter list which have data before (or equal to) the StartTime parameter
    -- This ensures that we have values to interpolate from if there is no value in the raw
    -- table at the StartTime requested (for each tag)    
    mn as ( -- get latest "before" timestamp for each tag
    select nvl(min(ts), v_StartTime) ts from (
        select tag_uid, max(ts) ts from tag_value  -- gets the earliest tag times for each tag
        where tag_uid in (select tag_uid from taglist)  
        and d between dateadd(''day'', -30, v_StartTime) and v_StartTime
        and ts <= v_StartTime
        group by tag_uid)),
    -- gets all of the raw tag values over period including earlier and later than specified
    -- to enable interpolation at start and end
    vals as
    (select 
          tv.tag_uid,
          tv.ts,
          tv.d,
          tv.second_index,
          tv.ms_index,
          avg(tv.val) val -- averaging may not be necessary if source data does not have duplicates
      from 
          tag_value tv
      where  
          tv.d between dateadd(day, -30, v_StartTime) and dateadd(day, 30, v_EndTime) -- limit improves pruning
          -- will have issues of course if there is no prior value within 30 days of requested data
          and tv.ts between (select ts from mn) and v_EndTime
      group by tv.tag_uid,
          tv.ts,
          tv.d,
          tv.second_index,
          tv.ms_index),
    -- generate the set of rows at over the required range and sample rate for each tag
    -- the base_second table contains standard aggregation markers to select from the 86400
    -- possible values per day.  Note this function does not interpolate at the millisecond level          
    tag_times as(
      select
        t.tag_uid,
        d.day_index,
        d.date_value,
        s.second_index,
        timestampadd(second, s.second_index, d.date_value) ts,
        1 include
      from
          base_day d,
          base_second s,
          taglist t
      where
          d.date_value between date_trunc(day, v_StartTime) and date_trunc(day, dateadd(day, 1, v_EndTime))
          and timestampadd(''second'', s.second_index, d.date_value) between v_StartTime and v_EndTime
          and decode(v_SampleRate, ''S1'', s.s1m, ''S2'', s.s2m, ''S4'', s.s4m, ''S5'', s.s5m, ''S10'', s.s10m, ''S20'', s.s20m, ''S30'', s.s30m, 
                     ''M1'', s.m1m, ''M2'', s.m2m, ''M5'', s.m5m, ''M10'', s.m10m, ''M20'', s.m20m, ''M30'', s.m30m, 
                     ''H1'', s.h1m, ''H2'', s.h2m, ''H3'', s.h3m, ''H4'', s.h4m, ''H6'', s.h6m, ''H12'', s.h12m, s.d1m ) = 0),
    -- a full outer join containing both the generated tag\\time rows unioned with 
    -- the raw values over the required time range                     
    tag_times_values as (
      select
          nvl(i.ts, r.ts) ts,
          nvl(i.tag_uid, r.tag_uid) tag_uid,
          r.val rawval,
          i.include
      from
          vals r full outer join tag_times i on i.tag_uid = r.tag_uid and i.date_value = r.d and i.ts = r.ts
    ),
    interp as (
      select 
      ts,
      tag_uid,
      rawval,
      first_value(rawval ignore nulls) over (partition by tag_uid order by ts rows between current row and unbounded following) as ival,
      include
    from tag_times_values
    )
select 
  ts,
  tag_uid,
  ival val
from interp 
where include = 1
order by tag_uid, ts 
';


-- insert the values needed for the subquery tile on the dashboard
insert into tag_group(group_name, tag_uid) values('MINS1', 1), ('MINS1', 2);
