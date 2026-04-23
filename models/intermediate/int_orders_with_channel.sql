with shopify as (
    select * from {{ ref('stg_shopify_orders') }}
),

final as (
    select
        order_id,
        customer_id,
        customer_email,
        customer_name,
        city,
        country,
        product_name,
        product_type,
        product_category,
        is_subscription,
        subtotal,
        discount_amount,
        shipping,
        taxes,
        order_total,
        order_total - discount_amount       as net_revenue,
        financial_status,
        fulfillment_status,
        payment_method,
        traffic_source,
        created_at,

        -- clean channel label
        case
            when traffic_source = 'paid_social'    then 'Paid Social'
            when traffic_source = 'email'          then 'Email'
            when traffic_source = 'organic_search' then 'Organic Search'
            when traffic_source = 'direct'         then 'Direct'
            when traffic_source = 'referral'       then 'Referral'
            when traffic_source = 'sms'            then 'SMS'
            else 'Unknown'
        end as channel


    from shopify
    qualify row_number() over (partition by order_id order by created_at) = 1
)

select * from final