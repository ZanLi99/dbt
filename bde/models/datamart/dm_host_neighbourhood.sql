with dm_host_neighbourhood as (
	select 
		l.id, 
		l.scraped_date, 
		h.host_id,
		h.host_name,
		h.host_neighbourhood,
		s.lga_name,
		p.price,
		p.has_availability,
		p.availability_30 
	from 
		{{ ref('dim_listing')}} l 
	join 
		{{ ref('dim_host')}} h
	on l.id = h.id
	join {{ ref('dim_property')}} p
	on l.id =p.id 
	join
		{{ ref('dim_lga')}} s
	on
		lower(h.host_neighbourhood) = lower(s.suburb_name) 
)
select 
	lga_name as host_neighbourhood_lga,
	to_char(scraped_date, 'MM/YYYY') as month_year,
	count(distinct host_id) as distinct_host,
	round(sum(case when has_availability = true then 30 - availability_30  else 0 end * price),2) as estimate_revenue,
	round(sum(case when has_availability = true then 30 - availability_30  else 0 end * price) / nullif(count(distinct host_id),0)::numeric,2) as per_host_estimate_revenue
from dm_host_neighbourhood
group by 
	host_neighbourhood_lga,
	to_char(scraped_date, 'MM/YYYY')
order by 
    host_neighbourhood_lga,
    to_char(scraped_date, 'MM/YYYY')