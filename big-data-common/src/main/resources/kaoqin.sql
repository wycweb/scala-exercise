-- auto-generated definition
create table attendance
(
    username          text null,
    department        text null,
    user_id           text null,
    date              text null,
    after_work_time   text null,
    after_work_result text null
);

select username,
       user_id,
       department,
       case
           when after_work_time like '21:%' then '九点'
           when after_work_time like '22:%' then '十点'
           when after_work_time like '23:%' then '十点'
           when after_work_time like '24:%' then '十点'
           when after_work_time like '次日%' then '十点'
           end as xiaban,
       count(*)
from attendance
where date not like '%星期日'
  and date not like '%星期六'
  and after_work_result = '正常'
group by xiaban, user_id, username,department
having xiaban is not null;

