-- auto-generated definition
create table attendance
(
    username          varchar(64) null,
    department        varchar(64) null,
    user_id           varchar(64) null,
    date              varchar(64) null,
    status            varchar(64),
    after_work_time   varchar(64) null,
    after_work_result varchar(64) null
);

select count(*) from attendance;
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
where status != '休息'
  and after_work_result = '正常'
group by xiaban, user_id, username, department
having xiaban is not null;