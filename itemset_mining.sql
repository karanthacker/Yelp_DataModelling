-- creating business_id-user_id unique association table

create table biz_user as
    select user_id,business_id from review group by user_id, business_id;

-- finding singletons with high frequency (over 70 reviews from unique users)
drop table business_nevada;

create table business_nevada
as
select b.business_id from business b join biz_user bu on b.business_id = bu.business_id
                   where state = 'NV' group by b.business_id having count(*) > 70;

select count(*) from business_nevada;

-- finding pairs (support is  >50)
drop table business_nevada_pairs;
create table business_nevada_pairs
as
    select r1.business_id "business_1",r2.business_id "business_2" from biz_user r1 join biz_user r2 on r1.user_id = r2.user_id
                          where r1.business_id in ( select business_id from business_nevada) and
                                r2.business_id in  ( select business_id from business_nevada) and
                                r1.business_id > r2.business_id
    group by r1.business_id, r2.business_id having count(*) > 50;

select count(*) from business_nevada_pairs;

-- finding triplets (support is  >50)
drop table business_nevada_triplets;

create table business_nevada_triplets
    as
    select  t.business_1, t.business_2, t.business_3 from
        (select r1.business_id as "business_1",r2.business_id as "business_2",
                r3.business_id as "business_3"
        from biz_user r1 join biz_user r2 on r1.user_id = r2.user_id
                        join biz_user r3 on r2.user_id = r3.user_id
         where r1.business_id > r2.business_id  and r2.business_id> r3.business_id) t join
             business_nevada_pairs bzp on bzp.business_1 = t.business_1
                    and bzp.business_2 = t.business_2
        group by 1,2,3 having  count(*)>50;

select count(*) from business_nevada_triplets;


-- viewing triplet info from the business table including name, location, address
select * from business where business_id in ('wz8ZPfySQczcPgSyd33-HQ','PY9GRfzr4nTZeINf346QOw','-OKB11ypR4C8wWlonBFIGw');
select * from business where business_id in ('wz8ZPfySQczcPgSyd33-HQ','PY9GRfzr4nTZeINf346QOw','ld_H5-FpZOWm_tkzwkPYQQ');

