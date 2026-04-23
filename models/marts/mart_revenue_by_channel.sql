with orders as (
    select * from {{ ref('int_orders_with_channel') }}
),

meta as (
    select * from {{ ref('stg_meta_ads') }}
),

klaviyo as (
    select * from {{ ref('stg_klaviyo_campaigns') }}
),


-- actual shopify store revenue
shopify_by_channel as (

    select
        channel,
        count(order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        round(sum(case when financial_status = 'paid' then order_total else 0 end),2) as actual_revenue,
        round(sum(case when financial_status = 'paid' then net_revenue else 0 end),2) as actual_net_revenue,
        round(sum(case when financial_status = 'paid' and is_subscription = true then order_total else 0 end), 2) as subscription_revenue

    from orders 
    group by channel
),

-- meta total reported revenue
meta_totals as (
    select
        'Paid Social'                                   as channel,
        round(sum(spend),2)                                      as total_spend,
        round(sum(meta_reported_purchases),2)                   as meta_reported_orders,
        round(sum(meta_reported_revenue),2)                     as meta_reported_revenue
    from meta
),

-- klaviyo total reported revenue
klaviyo_totals as (
    select
        'Email'                                         as channel,
        round(sum(klaviyo_reported_orders),2)                    as klaviyo_reported_orders,
        round(sum(klaviyo_reported_revenue),2)                  as klaviyo_reported_revenue
    from klaviyo
),

final as (

    select 
        s.channel,
        s.total_orders,
        m.meta_reported_orders,
        k.klaviyo_reported_orders,
        m.total_spend,
        s.actual_revenue,
        m.meta_reported_revenue,
        k.klaviyo_reported_revenue,
        round(m.meta_reported_revenue - s.actual_revenue,2) as meta_overreport_amount,
        round(k.klaviyo_reported_revenue - s.actual_revenue,2) as klaviyo_overreport_amount,
        round(safe_divide( m.meta_reported_revenue - s.actual_revenue, s.actual_revenue) * 100, 1) as meta_overreport_pct,
        round(safe_divide(s.actual_revenue, m.total_spend),2) as true_roas

    from shopify_by_channel s
    left join meta_totals m on s.channel = m.channel
    left join klaviyo_totals k on s.channel = k.channel
)