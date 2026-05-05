{{ config(materialized='table') }}

with reviews as (
    select * from {{ ref('stg_reviews') }}
),

weekly_aggregates as (
    select
        place_id,
        business_name,
        week_start,
        count(*) as total_reviews,
        round(avg(sentiment_score), 2) as avg_sentiment_score,
        round(min(sentiment_score), 2) as min_sentiment_score,
        round(max(sentiment_score), 2) as max_sentiment_score,
        sum(case when sentiment_label = 'positive' then 1 else 0 end) as positive_count,
        sum(case when sentiment_label = 'neutral'  then 1 else 0 end) as neutral_count,
        sum(case when sentiment_label = 'negative' then 1 else 0 end) as negative_count,
        round(
            100.0 * sum(case when sentiment_label = 'positive' then 1 else 0 end) / count(*),
            1
        ) as positive_pct,
        round(
            100.0 * sum(case when sentiment_label = 'negative' then 1 else 0 end) / count(*),
            1
        ) as negative_pct,
        round(avg(word_count), 1) as avg_word_count
    from reviews
    group by place_id, business_name, week_start
)

select * from weekly_aggregates
order by week_start desc, avg_sentiment_score desc
