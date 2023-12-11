-- Построим витрину данных:
--visit_date — дата визита
--utm_source / utm_medium / utm_campaign — метки пользователя
--visitors_count — количество визитов в этот день с этими метками
--total_cost — затраты на рекламу
--leads_count — количество лидов, которые оставили визиты, кликнувшие в этот день с этими метками
--purchases_count — количество успешно закрытых лидов (closing_reason = “Успешно реализовано” или status_code = 142)
--revenue — деньги с успешно закрытых лидов
--Требования для сортировки
--revenue — от большего к меньшему, null записи идут последними
--visit_date — от ранних к поздним
--visitors_count — в убывающем порядке
--utm_source, utm_medium, utm_campaign — в алфавитном порядке

--Шаг 3. Расчет расходов
--Посчитать расходы на рекламу по модели атрибуции Last Paid Click
--Создать и написать для агрегации данных из модели атрибуции Last Paid Click aggregate_last_paid_click.sql
--Сохранить топ-15 записей в aggregate_last_paid_click.csv согласно требованиям по сортировке

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
        on
            s.visitor_id = l.visitor_id
            and s.visit_date <= l.created_at
    where medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

aggregate_last_paid_click as (
    select
        to_char(visit_date, 'YYYY-MM-DD') as visit_date,
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
        to_char(va.campaign_date, 'YYYY-MM-DD') as visit_date,
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
        to_char(ya.campaign_date, 'YYYY-MM-DD') as visit_date,
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
    sum(visitors_count) as visitors_count,
    utm_source,
    utm_medium,
    utm_campaign,
    sum(total_cost) as total_cost,
    sum(leads_count) as leads_count,
    sum(purchases_count) as purchases_count,
    sum(revenue) as revenue
from aggregate_last_paid_click
group by 1, 3, 4, 5
order by
    revenue desc nulls last,
    visit_date asc,
    visitors_count desc,
    utm_source asc,
    utm_medium asc,
    utm_campaign asc;