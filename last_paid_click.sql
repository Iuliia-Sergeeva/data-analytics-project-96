--Шаг 2. Сценарий атрибуции
--запрос для атрибуции лидов по модели Last Paid Click (топ-10 записей)
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
    visitor_id,
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
from Last_Paid_Click
where rn = 1
order by
    amount desc nulls last,
    visit_date asc nulls last,
    utm_source asc nulls last,
    utm_medium asc nulls last,
    utm_campaign asc nulls last
;
