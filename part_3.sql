-- Question 1
with population as(
	select 
		cast(replace(o.lga_code_2016,'LGA','') as integer) as lga_code,
		o.age_0_4_yr_m +o.age_5_14_yr_m +o.age_15_19_yr_m as population_under_20,
		o.age_0_4_yr_m +o.age_5_14_yr_m +o.age_15_19_yr_m +o.age_20_24_yr_m +o.age_25_34_yr_m as population_under_35,
		round(((o.age_20_24_yr_m+o.age_25_34_yr_m) :: numeric/ (o.age_0_4_yr_m +o.age_5_14_yr_m +o.age_15_19_yr_m +o.age_20_24_yr_m +o.age_25_34_yr_m))*100  ,2) as rate_20_34
	from 
		warehouse.fact_one o
),
listing as (
	select 
		l.listing_neighbourhood,
		round(sum(case when p.has_availability = true then 30 - p.availability_30  else 0 end * p.price),2) as estimate_revenue 
	from warehouse.dim_listing l
	join warehouse.dim_property p
	on p.id = l.id
	group by l.listing_neighbourhood 
),
order_list as (
select 
	a.lga_name,
	l.listing_neighbourhood,
	p.population_under_20,
	p.population_under_35,
	rate_20_34,
	l.estimate_revenue,
	row_number() over (order by l.estimate_revenue) as row_num,
    count(*) over () as total_rows
from 
	warehouse.dim_lga a
join 
	population p 
on 
	a.lga_code = p.lga_code
join 
	listing l 
on 
	lower(l.listing_neighbourhood) = lower(a.suburb_name)
order by 
	l.estimate_revenue desc, p.population_under_35 desc
)
select 
	lga_name,
    listing_neighbourhood,
    population_under_20,
    population_under_35,
    rate_20_34,
    estimate_revenue 
from order_list
where 
	row_num = 1 or row_num = total_rows;

-- Question 2
with neighbourhood as(
select   
	l.listing_neighbourhood as listing_neighbourhood,
	round(sum(case when has_availability = true then 30 - availability_30  else 0 end * price),2) as estimate_revenue
 from warehouse.fact_listing l
 join 
 	warehouse.dim_host h
 on l.id = h.id
 join
 	warehouse.dim_property p 
 on p.id = l.id
 group by
 	l.listing_neighbourhood
 order by 
 	estimate_revenue desc
 limit 5
 ),
 rank_table as (
 select
 	l.listing_neighbourhood,
 	p.property_type, 
 	p.room_type,
 	l.accommodates,
 	(sum(case when l.has_availability = true then 30 - l.availability_30  else 0 end)) as stays,
    row_number() over (partition by l.listing_neighbourhood order by (sum(case when l.has_availability = true then 30 - l.availability_30  else 0 end)) desc) as row_num
 from warehouse.fact_listing l 
 join 
 	warehouse.dim_host h
 on l.id = h.id
 join
 	warehouse.dim_property p 
 on p.id = l.id
 join neighbourhood n
 on n.listing_neighbourhood = l.listing_neighbourhood
 group by l.listing_neighbourhood,p.property_type, p.room_type,l.accommodates 
 order by l.listing_neighbourhood desc, stays desc
 )
 select 
 	listing_neighbourhood,
 	property_type,
 	room_type,
 	accommodates,
 	stays
 from rank_table 
 where row_num = 1
 order by stays desc;

 --Question 3
with room_table as (
	select dh.host_id, dp.property_type,dp.room_type, dp.accommodates  
	from 
		warehouse.dim_host dh 
	join
		warehouse.dim_listing fl 
	on dh.id = fl.id
	join 
		warehouse.dim_property dp 
	on dp.id = dh.id
	group by dh.host_id, dp.property_type,dp.room_type, dp.accommodates
),
multiple_host as ( 
	select host_id from room_table
	group by host_id
	having count(host_id) > 1
),
host_area as (
	select id, lga_name from warehouse.dim_host dh 
	join warehouse.dim_lga dl 
	on lower(dh.host_neighbourhood) = lower(dl.suburb_name)
	join multiple_host mh
	on mh.host_id = dh.host_id
),
listing_area as (
	select id, lga_name from warehouse.dim_listing li 
	join warehouse.dim_lga dl 
	on lower(li.listing_neighbourhood) = lower(dl.suburb_name) 
)
select 
    round(sum(case when h.lga_name = l.lga_name then 1 else 0 end)::numeric / nullif(count(*),0)::numeric * 100,2) as local_rate
from host_area h
join listing_area l
on h.id = l.id

--Question 4
with room_table as (
select dh.host_id, dp.property_type,dp.room_type, dp.accommodates  from 
warehouse.dim_host dh 
join
warehouse.dim_listing fl 
on dh.id = fl.id
join 
warehouse.dim_property dp 
on dp.id = dh.id
group by dh.host_id, dp.property_type,dp.room_type, dp.accommodates
),
single_host as ( 
select host_id from room_table
group by host_id
having count(host_id) = 1
),
month_repay as (
select 
	cast(replace(lga_code_2016,'LGA','') as integer) as lga_code,
	ft.median_mortgage_repay_monthly as monthly_repay
from warehouse.fact_two ft 
),
lga_repay as (
select * 
from warehouse.dim_lga dl
join month_repay mr 
on mr.lga_code = dl.lga_code
),
host_area as (
select 
	sh.host_id,
	dh.host_name,
	fl.listing_neighbourhood,
	round(sum(case when has_availability = true then 30 - availability_30  else 0 end * price),2) as estimate_revenue 
from 
	single_host sh
join 
	warehouse.dim_host dh 
on sh.host_id = dh.host_id
join
	warehouse.dim_listing fl
on fl.id = dh.id 
join 
	warehouse.dim_property dp
on dp.id =dh.id 
group by sh.host_id, dh.host_name, fl.listing_neighbourhood 
),
revenue_table as (
select 
	ha.host_id,
	ha.host_name,
	ha.listing_neighbourhood,
	ha.estimate_revenue,
	lr.monthly_repay
from 
	host_area ha
join
	lga_repay lr
on lower(ha.listing_neighbourhood) = lower(lr.suburb_name)
order by estimate_revenue desc
)
select 
	round(
		sum(case when estimate_revenue > monthly_repay*12 then 1 else 0 end)::numeric /
		count(*)::numeric ,2
		) as covered
from revenue_table;







	