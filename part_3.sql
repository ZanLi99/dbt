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
		round(sum(case when has_availability = true then 30 - availability_30  else 0 end * price),2) as estimate_revenue 
	from warehouse.fact_listing l
	group by l.listing_neighbourhood 
),
lga as (
	select 
		a.lga_code,
		a.lga_name,
		s.suburb_name 
	from 
		warehouse.dim_lga a
	join 
		warehouse.dim_suburb s
	on lower(s.lga_name) = lower(a.lga_name)
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
	lga a 
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
select dh.host_id, dp.property_type,dp.room_type, fl.accommodates  from 
warehouse.dim_host dh 
join
warehouse.fact_listing fl 
on dh.id = fl.id
join 
warehouse.dim_property dp 
on dp.id = dh.id
group by dh.host_id, dp.property_type,dp.room_type, fl.accommodates
),
multiple_host as ( 
select host_id from room_table
group by host_id
having count(host_id) > 1
),
count_host as (
select 
	mh.host_id,
	dh.host_name,
	ds.lga_name,
	count(fl.listing_neighbourhood)
from multiple_host mh
join warehouse.dim_host dh 
on mh.host_id = dh.host_id
join warehouse.fact_listing fl 
on fl.id = dh.id
join warehouse.dim_suburb ds 
on lower(ds.suburb_name) = lower(fl.listing_neighbourhood)
group by mh.host_id,dh.host_name,ds.lga_name
order by mh.host_id, dh.host_name
)
select round(count(distinct host_id)::numeric / count(host_id)::numeric,2) as same_area_rate from count_host;
--Question 4
with room_table as (
select dh.host_id, dp.property_type,dp.room_type, fl.accommodates  from 
warehouse.dim_host dh 
join
warehouse.fact_listing fl 
on dh.id = fl.id
join 
warehouse.dim_property dp 
on dp.id = dh.id
group by dh.host_id, dp.property_type,dp.room_type, fl.accommodates
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
lga_table as (
select 
	mr.lga_code, 
	mr.monthly_repay,
	dl.lga_name,
	ds.suburb_name 
from month_repay mr
join warehouse.dim_lga dl  
on mr.lga_code = dl.lga_code
join warehouse.dim_suburb ds 
on lower(dl.lga_name) = lower(ds.lga_name)
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
	warehouse.fact_listing fl
on fl.id = dh.id 
group by sh.host_id, dh.host_name, fl.listing_neighbourhood 
),
revenue_table as (
select 
	ha.host_id,
	ha.host_name,
	ha.listing_neighbourhood,
	ha.estimate_revenue,
	lt.monthly_repay
from 
	host_area ha
join
	lga_table lt
on lower(ha.listing_neighbourhood) = lower(lt.suburb_name)
order by estimate_revenue desc
)
select 
	round(
		sum(case when estimate_revenue > monthly_repay*12 then 1 else 0 end)::numeric /
		count(*)::numeric ,2
		) as covered
from revenue_table;







	