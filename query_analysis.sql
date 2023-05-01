-- find top 50 rated services in california state
select name,stars from business
                  where review_count>100 and state = 'CA' and is_open is TRUE
                                order by stars desc limit 50;

-- find all the pizza places in the data set
select name from business
where business_id in
      (select business_id from business_category join category_id ci on business_category.category_id = ci.id where ci.category = 'Pizza');

-- find services  between co-ordinates
select business_id,name from business where latitude > 30 and latitude<38
    and longitude < -117.401979 and  longitude>-122.460930;


-- finding reviewer who garnered most useful 'review' votes

select user_id,name,useful from users order by useful desc limit 1;

-- finding reviewer name of the review which was found most useful
select users.user_id,name,review_id,text,r.useful from users
    join review r on users.user_id = r.user_id
                                   order by r.useful desc limit 1;