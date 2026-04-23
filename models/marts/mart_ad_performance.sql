with meta as (
    select * from {{ ref('stg_meta_ads') }}
),

shopify as (
    select * from {{ ref('int_orders_with_channel') }}
),

-- actual shopify revenue from paid social by date
shopify_paid_social as (
    select
        cast(created_at as date)                                                        as order_date,
        round(sum(case when financial_status = 'paid' then order_total else 0 end), 2) as actual_daily_revenue,
        count(case when financial_status = 'paid' then order_id end)                   as actual_daily_orders
    from shopify
    where channel = 'Paid Social'
    group by cast(created_at as date)
),

-- meta spend and reported revenue by campaign by date
meta_by_campaign as (
    select
        campaign_id,
        campaign_name,
        audience_type,
        objective,
        ad_date,
        sum(spend)                      as spend,
        sum(impressions)                as impressions,
        sum(link_clicks)                as link_clicks,
        sum(meta_reported_purchases)    as meta_reported_purchases,
        sum(meta_reported_revenue)      as meta_reported_revenue,
        round(avg(ctr), 4)              as avg_ctr,
        round(avg(cpm), 2)              as avg_cpm
    from meta
    group by campaign_id, campaign_name, audience_type, objective, ad_date
),

final as (
    select
        m.campaign_id,
        m.campaign_name,
        m.audience_type,
        m.objective,
        m.ad_date,
        m.spend,
        m.impressions,
        m.link_clicks,
        m.avg_ctr,
        m.avg_cpm,
        m.meta_reported_purchases,
        m.meta_reported_revenue,

        -- actual shopify revenue on same day
        coalesce(s.actual_daily_revenue, 0)     as shopify_actual_revenue,
        coalesce(s.actual_daily_orders, 0)      as shopify_actual_orders,

        -- gap
        round(m.meta_reported_revenue - coalesce(s.actual_daily_revenue, 0), 2)    as overreport_amount,

        -- roas
        round(safe_divide(coalesce(s.actual_daily_revenue, 0), m.spend), 2)        as true_roas,
        round(safe_divide(m.meta_reported_revenue, m.spend), 2)                    as meta_reported_roas

    from meta_by_campaign m
    left join shopify_paid_social s on m.ad_date = s.order_date
)

select * from final
order by ad_date, spend desc