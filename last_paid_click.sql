--Шаг 2. Сценарий атрибуции
--запрос для атрибуции лидов
--по модели Last Paid Click
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
            over (
                partition by S.VISITOR_ID
                order by S.VISIT_DATE desc
            )
        as RN
    from SESSIONS as S
    left join LEADS as L
        on
            S.VISITOR_ID = L.VISITOR_ID
            and S.VISIT_DATE <= L.CREATED_AT
    where
        MEDIUM
        in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
)

select
    VISITOR_ID,
    VISIT_DATE,
    UTM_SOURCE,
    UTM_MEDIUM,
    UTM_CAMPAIGN,
    LEAD_ID,
    CREATED_AT,
    AMOUNT,
    CLOSING_REASON,
    STATUS_ID
from LAST_PAID_CLICK
where RN = 1
order by
    AMOUNT desc nulls last,
    VISIT_DATE asc nulls last,
    UTM_SOURCE asc nulls last,
    UTM_MEDIUM asc nulls last,
    UTM_CAMPAIGN asc nulls last;
