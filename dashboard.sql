--Сколько у нас пользователей заходят на сайт?
select
    source,
    to_char(visit_date, 'DD-MM-YYYY') as visit_date,
    count(visitor_id) as distinct_visitor
from sessions
group by 1
order by 2 desc

--Какие каналы их приводят на сайт? Хочется видеть по дням/неделям/месяцам
select
    S.SOURCE as UTM_SOURCE,
    S.MEDIUM as UTM_MEDIUM,
    S.CAMPAIGN as UTM_CAMPAIGN,
    to_char(S.VISIT_DATE, 'DD-MM-YYYY') as VISIT_DATE,
    extract(week from S.VISIT_DATE) as VISIT_WEEK,
    extract(month from S.VISIT_DATE) as VISIT_MONTH,
    count(S.VISITOR_ID) as COUNT_VISITOR
from SESSIONS as S
group by 1, 2, 3, 4, 5, 6


--используем конструкцию из данного запроса для ответа на вопрос выше по модели Last Paid Click
with LAST_PAID_CLICK as (
    select
        S.VISITOR_ID,
        S.VISIT_DATE,
        S.SOURCE as UTM_SOURCE,
        S.MEDIUM as UTM_MEDIUM,
        S.CAMPAIGN as UTM_CAMPAIGN,
        L.LEAD_ID,
        L.CREATED_AT,
        L.AMOUNT,
        L.CLOSING_REASON,
        L.STATUS_ID,
        row_number()
            over (partition by S.VISITOR_ID order by S.VISIT_DATE desc)
        as RN
    from SESSIONS as S
    left join LEADS as L
        on
            S.VISITOR_ID = L.VISITOR_ID
            and S.VISIT_DATE <= L.CREATED_AT
    where MEDIUM in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
)

select
    UTM_SOURCE,
    UTM_MEDIUM,
    to_char(VISIT_DATE, 'DD-MM-YYYY') as VISIT_DATE,
    count(VISITOR_ID) as COUNT_VISITORS
from LAST_PAID_CLICK
where RN = 1
group by 1, 2, 3
order by 4 desc


--Сколько лидов к нам приходят?
select count(distinct lead_id) as count_leads
from leads


--конверсия лидов в клиентов, конверсия из клика в лид
with tab_leads as (
    select
        count(visitor_id) as count_lead,
        (
            select count(status_id) from leads where status_id = 142
        ) as count_clients
    from leads
)

select
    *,
    round(
        count_lead * 100.0 / (select count(visitor_id) from sessions), 2
    ) as cr_cl,
    round(count_clients * 100.0 / count_lead, 2) as cr_l
from tab_leads


--Сколько мы тратим по разным каналам в динамике?
with tab_cost as (
    select
        sum(daily_spent) as cost_vk,
        (select sum(daily_spent) from ya_ads) as cost_yandex
    from vk_ads
)

select
    *,
    cost_vk + cost_yandex as total_costs
from tab_cost;

--Общие затраты по рекламе
select
    to_char(campaign_date, 'DD-MM-YYYY') as visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    sum(daily_spent) as total_cost
from vk_ads
group by 1, 2, 3, 4
union all
select
    to_char(ya.campaign_date, 'DD-MM-YYYY') as visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    sum(daily_spent) as total_cost
from ya_ads as ya
group by 1, 2, 3, 4


--Окупаются ли каналы?
with cost_recovery as (
    select
        s.visit_date,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        s.visitor_id,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        null as total_cost,
        row_number()
            over (partition by s.visitor_id order by s.visit_date desc)
        as rn
    from sessions as s
    left join leads as l
        on
            s.visitor_id = l.visitor_id
            and s.visit_date <= l.created_at
    where s.medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

aggregate_last_paid_click_2 as (
    select
        to_char(visit_date, 'DD-MM-YYYY') as visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        count(visitor_id) as visitors_count,
        null as total_cost,
        sum(case when lead_id is not null then 1 else 0 end) as leads_count,
        sum(
            case
                when
                    closing_reason = 'Успешно реализовано' or status_id = 142
                    then 1
                else 0
            end
        ) as purchases_count,
        sum(amount) as revenue
    from cost_recovery
    where rn = 1
    group by 1, 2, 3, 4

    union all
    select
        to_char(va.campaign_date, 'DD-MM-YYYY') as visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        null as visitors_count,
        va.daily_spent as total_cost,
        null as leads_count,
        null as purchases_count,
        null as revenue
    from vk_ads as va
    union all
    select
        to_char(ya.campaign_date, 'DD-MM-YYYY') as visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        null as visitors_count,
        ya.daily_spent as total_cost,
        null as leads_count,
        null as purchases_count,
        null as revenue
    from ya_ads as ya
)

select
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    sum(visitors_count) as visitors_count,
    coalesce(sum(total_cost), 0) as total_cost,
    sum(leads_count) as leads_count,
    sum(purchases_count) as purchases_count,
    coalesce(sum(revenue), 0) as revenue,
    coalesce(sum(total_cost) / sum(visitors_count), 0) as cpu
from aggregate_last_paid_click_2
where utm_source in ('yandex', 'vk')
group by 1, 2, 3, 4
order by
    9 desc nulls last,
    5 desc,
    2 asc,
    3 asc,
    4 asc


--за сколько дней с момента перехода по рекламе закрывается 90% лидов
with close_leads as (
    select
        s.visitor_id,
        s.visit_date,
        s.source,
        s.medium,
        s.campaign,
        l.created_at,
        l.closing_reason,
        l.status_id,
        l.lead_id,
        coalesce(l.amount, 0) as amount,
        row_number()
            over (partition by s.visitor_id order by s.visit_date desc)
        as rn,
        l.created_at - s.visit_date as days
    from sessions as s
    left join leads as l
        on
            s.visitor_id = l.visitor_id
            and s.visit_date <= l.created_at
    where s.medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
)

select
    cl.visit_date,
    cl.created_at,
    cl.lead_id,
    cl.days,
    sum(cl.amount) as amount,
    ntile(10) over (order by cl.days) as group_ntile
from close_leads as cl
where
    cl.rn = 1
    and cl.status_id = 142
group by 1, 2, 3, 4
order by 4



--Заметна ли корреляция между запуском рекламной компании и органикой?
--Строим график в Google Sheets и Superset
with tab_cor as (
    select
        s.visitor_id,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        l.lead_id,
        l.amount,
        l.closing_reason,
        l.status_id,
        to_char(s.visit_date, 'DD-MM-YYYY') as visit_date,
        to_char(l.created_at, 'DD-MM-YYYY') as created_at,
        row_number()
            over (partition by s.visitor_id order by s.visit_date desc)
        as rn
    from sessions as s
    left join leads as l
        on
            s.visitor_id = l.visitor_id
            and s.visit_date <= l.created_at
    where s.source = 'vk'
--where s.source in ('organic', 'yandex', 'vk')
)

select
    utm_source,
    visit_date,
    count(visitor_id)
from tab_cor
where rn = 1
group by 1, 2
order by
    visit_date asc nulls last,
    utm_source asc nulls last;
