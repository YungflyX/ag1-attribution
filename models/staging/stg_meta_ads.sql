with source as (
    select * from {{ source('ag1_raw', 'raw_meta_ads') }}
),

cleaned as (
    select
        -- ids
        Campaign_ID                                     as campaign_id,
        Ad_set_ID                                       as ad_set_id,
        Ad_ID                                           as ad_id,

        -- names
        Campaign_name                                   as campaign_name,
        Ad_set_name                                     as ad_set_name,
        Ad_name                                         as ad_name,
        Audience_type                                   as audience_type,
        Objective                                       as objective,

        -- date
        Day                                             as ad_date,

        -- spend
        Amount_spent__USD                               as spend,

        -- reach
        Reach                                           as reach,
        Impressions                                     as impressions,
        CPM__cost_per_1_000_impressions                 as cpm,

        -- clicks
        Link_clicks                                     as link_clicks,
        CPC__cost_per_link_click                        as cpc,
        CTR__link_click_through_rate                    as ctr,

        -- conversions
        Purchases__website                              as meta_reported_purchases,
        Purchase_ROAS__return_on_ad_spend               as meta_reported_roas,
        Website_purchase_conversion_value               as meta_reported_revenue

    from source
    where
        Amount_spent__USD > 0
        and Day <= current_date()
)

select * from cleaned