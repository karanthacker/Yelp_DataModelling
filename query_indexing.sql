--index query 1
create index idx_business_stars_state on business(stars,state);

--index query 2
create index idx_business_category_id on business_category(category_id);
create index idx_category_id_id on category_id(id);

--index query 3
create index idx_business_longitude on business(longitude);
create index idx_business_latitude on business(latitude);

--index query 4
create index idx_users_useful on users(useful desc) ;

--index query 5
CREATE INDEX idx_review_user_id ON review (user_id);