  with dm_property_type as (
  select
    p.property_type as property_type,
 	p.room_type as room_type,
 	p.accommodates as accommodates,
 	to_char(l.scraped_date, 'MM/YYYY') as month_year,
    round(sum(case when p.has_availability = true then  1 else  0 end )::numeric / nullif(count(p.has_availability),0)*100,2) as Active_listings_ratio,
  	min(case when p.has_availability = true then p.price end) as minimum_price,
  	max(case when p.has_availability = true then p.price end) as maximum_price,
  	round(percentile_cont(0.5) within group (order by case when p.has_availability = true then p.price end)::numeric,2) as median_price,
  	round(avg(case when p.has_availability = true then p.price end),2) as average_price,
    count(distinct host_id) as distinct_host,
  	round(sum(case when  h.host_is_superhost  = true then  1 else  0 end )::numeric / nullif(count(h.host_is_superhost),0) *100,2) as Superhost_ratio,
  	round(avg(case when p.has_availability = true then r.review_scores_rating end),2) as average_review_scores_rating,
 	round((lead(sum(case when p.has_availability = true then 1 else 0 end)) over (partition by p.property_type  order by to_char(l.scraped_date, 'MM/YYYY')) 
        - sum(case when p.has_availability = true then 1 else 0 end))::numeric / nullif(sum(case when p.has_availability = true then 1 else 0 end),0)::numeric *100,2) as active_change,
 	round((lead(sum(case when p.has_availability = false then 1 else 0 end)) over (partition by p.property_type order by to_char(l.scraped_date, 'MM/YYYY')) 
        - sum(case when p.has_availability = false then 1 else 0 end))::numeric / nullif(sum(case when p.has_availability = true then 1 else 0 end),0)::numeric *100,2) as inactive_change,
    (sum(case when p.has_availability = true then 30 - p.availability_30  else 0 end)) as stays,
	round(avg(case when p.has_availability = true then (30 - p.availability_30)*p.price  else 0 end),2) as avg_estimate_revenue
  from 
    {{ ref('dim_property')}} p
  join 
  	{{ ref('dim_listing')}} l
  on p.id = l.id
  join  
    {{ ref('dim_host')}} h
  on p.id = h.id
  join 
  	{{ ref('fact_review')}} r
  on
    p.id = r.id
  group by 
 	p.property_type,
 	p.room_type,
 	p.accommodates,
 	month_year
  order by 
    p.property_type,
    p.room_type,
    p.accommodates,
  	to_char(l.scraped_date, 'MM/YYYY')
)
select * from dm_property_type



