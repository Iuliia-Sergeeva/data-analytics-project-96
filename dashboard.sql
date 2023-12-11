--Сколько у нас пользователей заходят на сайт?
select
to_char(visit_date, 'DD-MM-YYYY') as visit_date,
count(visitor_id) as distinct_visitor,
source
from sessions
group by 2
order by 1 desc;


--Какие каналы их приводят на сайт? Хочется видеть по дням/неделям/месяцам
select
"source",
medium as utm_medium,
coalesce(campaign, 'organic') as utm_campaign,
to_char(visit_date, 'DD-MM-YYYY') as visit_date,
count(visitor_id) as count_visitor,
extract(WEEK from visit_date) as visit_WEEK,
extract(month from visit_date) as visit_month
from sessions s
group by 1,2,3,4,6,7;


--используем конструкцию из данного запроса для ответа на вопрос выше по модели Last Paid Click
with Last_Paid_Click as (
    select
        s.visitor_id,
        s.visit_date,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        row_number()
            over (partition by s.visitor_id order by s.visit_date desc) as rn
from sessions s
left join leads l
on l.visitor_id = s.visitor_id
and l.created_at >= s.visit_date 
where medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
)
select
    utm_source,
    utm_medium,
    to_char(visit_date, 'DD-MM-YYYY') as visit_date,
    count(visitor_id) as count_visitors
from Last_Paid_Click
where rn = 1
group by 1,2,3
order by 4 desc;


--Сколько лидов к нам приходят?
select count(distinct lead_id) as count_leads
from leads;


--конверсия лидов в клиентов, конверсия из клика в лид
with tab_leads as(
select count(visitor_id) as count_lead,
(select count(status_id) from leads where status_id=142) as count_clients
from leads)
select *,
round(count_lead * 100.0 /(select count(visitor_id) from sessions), 2) as CR_cl,
round(count_clients * 100.0 /count_lead, 2) as CR_l
from tab_leads;


--Сколько мы тратим по разным каналам в динамике?
with tab_cost as(
select sum(daily_spent) as cost_vk,
(select sum(daily_spent) from ya_ads ya) as cost_yandex
from vk_ads va)
select *, cost_vk + cost_yandex as total_costs
from tab_cost

costs as(
	select
		to_char(campaign_date, 'DD-MM-YYYY') as visit_date,
		utm_source,
		utm_medium,
		utm_campaign,
		sum(daily_spent) as total_cost
	from vk_ads as va
	group by 1,2,3,4
	union all
	select
		to_char(ya.campaign_date, 'DD-MM-YYYY') as visit_date,
		utm_source,
		utm_medium,
		utm_campaign,
		sum(daily_spent) as total_cost        
	from ya_ads as ya
	group by 1,2,3,4)
;


--Окупаются ли каналы?
with tab as (
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
        on s.visitor_id = l.visitor_id
        and s.visit_date <= l.created_at
    where medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

aggregate_last_paid_click as (
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
    from tab
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
    from ya_ads as ya)
select
	visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    sum(visitors_count) as visitors_count,
    coalesce(sum(total_cost),0) as total_cost,
    sum(leads_count) as leads_count,
    sum(purchases_count) as purchases_count,
    coalesce(sum(revenue),0) as revenue,
    coalesce(sum(total_cost)/sum(visitors_count),0) as cpu  
from aggregate_last_paid_click
where utm_source in ('yandex', 'vk')
group by 1, 2, 3, 4
order by
    revenue desc nulls last,
    visitors_count desc,
    utm_source asc,
    utm_medium asc,
    utm_campaign asc
;


--за сколько дней с момента перехода по рекламе закрывается 90% лидов
with close_leads as (
    select
        s.visitor_id,
        visit_date,
        source,
        medium,
        campaign,
        created_at,
        closing_reason,
        status_id,
        lead_id,
        coalesce (amount, 0) as amount,
        row_number ()
            over (partition by s.visitor_id order by visit_date desc)
        as rn,
        created_at - visit_date as days
    from sessions as s
    left join leads
        on s.visitor_id = leads.visitor_id
        and visit_date <= created_at
    where medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
)

select
    cl.visit_date,
    cl.created_at,
    cl.lead_id,
    cl.days,
    sum(cl.amount),
    ntile (10) over (order by cl.days) as group_ntile
from close_leads as cl
where cl.rn = 1
and cl.status_id = 142
group by 1,2,3,4
order by 4;


--Есть ли заметная корреляция между запуском рекламной компании и ростом органики?
--Строим график в Google Sheets и Superset
with tab_cor as (
    select
        s.visitor_id,
        to_char(s.visit_date, 'DD-MM-YYYY') as visit_date,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        l.lead_id,
        to_char(l.created_at,'DD-MM-YYYY') as created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        row_number()
            over (partition by s.visitor_id order by s.visit_date desc) as rn
from sessions s
left join leads l
on l.visitor_id = s.visitor_id
and l.created_at >= s.visit_date
where s.source = 'vk'
--where s.source in ('organic', 'yandex', 'vk')
)
select
    utm_source,
    visit_date,
    count(visitor_id)
from tab_cor
where rn = 1
group by 1,2
order by
    visit_date asc nulls last,
    utm_source asc nulls last
;