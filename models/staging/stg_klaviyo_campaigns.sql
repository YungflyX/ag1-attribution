with source as (
    select * from {{ source('ag1_raw', 'raw_klaviyo_campaigns') }}
),

cleaned as (
    select
        -- ids
        Campaign_ID                     as campaign_id,

        -- names
        Campaign_Name                   as campaign_name,
        Subject                         as subject,
        Type                            as channel_type,
        Source                          as source_type,

        -- dates
        Send_Time                       as send_time,
        Status                          as status,

        -- delivery
        Recipients                      as recipients,
        Delivered                       as delivered,
        Bounced                         as bounced,

        -- engagement
        Opened__Unique                 as unique_opens,
        Open_Rate                       as open_rate,
        Clicked__Unique                as unique_clicks,
        Click_Rate                      as click_rate,
        Unsubscribed                    as unsubscribed,
        Spam_Complaints                 as spam_complaints,

        -- conversions — these are KLAVIYO REPORTED (inflated)
        Placed_Orders                   as klaviyo_reported_orders,
        Revenue                         as klaviyo_reported_revenue,
        Revenue_Per_Recipient           as revenue_per_recipient,
        Conversion_Rate                 as conversion_rate

    from source
    where
        Recipients > 0
)

select * from cleaned