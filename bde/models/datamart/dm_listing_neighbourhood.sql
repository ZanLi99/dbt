with dm_listing_neighbourhood as (
  select
    l.listing_neighbourhood,
  	to_char(l.scraped_date, 'MM/YYYY') as month_year,
    round(sum(case when l.has_availability = true then  1 else  0 end )::numeric / nullif(count(l.has_availability),0) *100,2) as Active_listings_ratio,
  	min(case when l.has_availability = true then l.price end) as minimum_price,
  	max(case when l.has_availability = true then l.price end) as maximum_price,
  	round(percentile_cont(0.5) within group (order by case when l.has_availability = true then l.price end)::numeric,2) as median_price,
  	round(avg(case when l.has_availability = true then l.price end),2) as average_price,
    count(distinct host_id) as distinct_host,
  	round(sum(case when  h.host_is_superhost  = true then  1 else  0 end )::numeric / nullif(count(h.host_is_superhost),0) *100,2) as Superhost_ratio,
    round(avg(case when l.has_availability = true then l.review_scores_rating end),2) as average_review_scores_rating,
 	round((lead(sum(case when l.has_availability = true then 1 else 0 end)) over (partition by listing_neighbourhood order by to_char(l.scraped_date, 'MM/YYYY')) - sum(case when l.has_availability = true then 1 else 0 end))::numeric / nullif(sum(case when l.has_availability = true then 1 else 0 end),0)::numeric *100,2) as active_change,
 	round((lead(sum(case when l.has_availability = false then 1 else 0 end)) over (partition by listing_neighbourhood order by to_char(l.scraped_date, 'MM/YYYY')) - sum(case when l.has_availability = false then 1 else 0 end))::numeric / nullif(sum(case when l.has_availability = true then 1 else 0 end),0)::numeric *100,2) as inactive_change,
    (sum(case when l.has_availability = true then 30 - l.availability_30  else 0 end)) as stays,
	round(sum(case when l.has_availability = true then (30 - l.availability_30)*l.price  else 0 end) / nullif(count(distinct host_id),0)::numeric,2) as avg_estimate_revenue
  from 
   {{ ref('fact_listing')}} l
  left join  
    {{ ref('dim_host')}} h
  on
    l.id = h.id
  group by 
  	l.listing_neighbourhood,
  	to_char(l.scraped_date, 'MM/YYYY')
  order by 
  	l.listing_neighbourhood, 
  	to_char(l.scraped_date, 'MM/YYYY')
)
select * from dm_listing_neighbourhood



