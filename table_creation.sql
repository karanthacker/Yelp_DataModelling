create table users(
    user_id varchar(50),
    name varchar,
    yelping_since timestamp,
    review_count integer,
    compliment_list integer,
    compliment_photos integer,
    compliment_profile integer,
    fans integer,
    funny integer,
    useful integer,
    primary key (user_id)
);

create table business(
    address varchar,
    business_id varchar(50),
    city varchar,
    is_open boolean,
    latitude float,
    longitude float,
    name varchar,
    postal_code varchar(10),
    review_count integer,
    stars float,
    state varchar(3),
    primary key (business_id)
               );

create table category_id(
  category varchar,
  id integer,
  primary key (id)
);

create table business_category(
    business_id varchar(50),
    category_id integer,
    foreign key (business_id) references business,
    foreign key (category_id) references category_id,
    primary key (business_id,category_id)
);

create table hours(
    business_id varchar,
  	monday varchar,
  	tuesday varchar,
  	wednesday varchar,
  	thursday varchar,
  	friday varchar,
  	saturday varchar,
  	sunday varchar,
  	primary key(business_id),
  	foreign key (business_id)
  REFERENCES business(business_id)
);

create table review(
    review_id varchar,
    business_id varchar,
  	user_id varchar,
  	date timestamp,
  	stars float,
  	useful integer,
  	cool integer,
  	funny integer,
  	text TEXT,
  	PRIMARY KEY (review_id),
  	FOREIGN KEY (business_id) REFERENCES business,
  	FOREIGN KEY (user_id) REFERENCES users
);

create table tip(
	user_id varchar,
  	business_id varchar,
  	text varchar,
  	date timestamp,
  	likes integer,
  	PRIMARY KEY (user_id, business_id),
  	FOREIGN KEY (user_id) REFERENCES users,
  	FOREIGN KEY (business_id) REFERENCES business
);

create table elite_years(
	user_id VARCHAR,
  	year integer,
  	PRIMARY KEY (user_id,year),
  	FOREIGN KEY (user_id) REFERENCES users
);

create table friend(
	user_id VARCHAR,
  	friend_id VARCHAR,
  	PRIMARY KEY (user_id,friend_id),
  	FOREIGN KEY (user_id) REFERENCES users
);

create table checkin(
	id SERIAL,
  	business_id VARCHAR,
  	date timestamp,
  	PRIMARY KEY (id),
  	FOREIGN KEY (business_id) REFERENCES business
);
