with source as (
    select * from {{ source('ag1_raw', 'raw_shopify_orders') }}
),

cleaned as (
    select
        -- ids
        Name                        as order_id,
        Customer_ID                 as customer_id,

        -- customer
        Email                       as customer_email,
        Billing_Name                as customer_name,
        Billing_City                as city,
        Billing_Country             as country,

        -- product
        Lineitem_name               as product_name,
        Lineitem_price              as product_price,
        Lineitem_quantity           as quantity,
        Product_Type                as product_type,
        Product_Category            as product_category,
        Is_Subscription             as is_subscription,

        -- financials
        Subtotal                    as subtotal,
        Discount_Amount             as discount_amount,
        Shipping                    as shipping,
        Taxes                       as taxes,
        Total                       as order_total,

        -- status
        Financial_Status            as financial_status,
        Fulfillment_Status          as fulfillment_status,
        Payment_Method              as payment_method,
        Source_name                 as traffic_source,

        -- dates
        Created_at                  as created_at

    from source
    where
        Total > 0
        and Name is not null
        and Created_at <= current_datetime() -- prevents records that never happened.
)

select * from cleaned