DO $$
DECLARE
  filepath text;
BEGIN
  FOR filepath IN SELECT pg_ls_dir('/Users/karanthacker/yelp_users') AS filename
  LOOP
    IF filepath LIKE '%.csv' THEN
      EXECUTE format('COPY users FROM %L CSV HEADER', concat('/Users/karanthacker/yelp_users/',filepath));
    END IF;
  END LOOP;
END $$;

DO $$
DECLARE
  filepath text;
BEGIN
  FOR filepath IN SELECT pg_ls_dir('/Users/karanthacker/yelp_business') AS filename
  LOOP
    IF filepath LIKE '%.csv' THEN
      EXECUTE format('COPY business FROM %L CSV HEADER', concat('/Users/karanthacker/yelp_business/',filepath));
    END IF;
  END LOOP;
END $$;

DO $$
DECLARE
  filepath text;
BEGIN
  FOR filepath IN SELECT pg_ls_dir('/Users/karanthacker/yelp_category_id') AS filename
  LOOP
    IF filepath LIKE '%.csv' THEN
      EXECUTE format('COPY category_id FROM %L CSV ', concat('/Users/karanthacker/yelp_category_id/',filepath));
    END IF;
  END LOOP;
END $$;

DO $$
DECLARE
  filepath text;
BEGIN
  FOR filepath IN SELECT pg_ls_dir('/Users/karanthacker/yelp_business_category') AS filename
  LOOP
    IF filepath LIKE '%.csv' THEN
      EXECUTE format('COPY business_category FROM %L CSV HEADER', concat('/Users/karanthacker/business_category/',filepath));
    END IF;
  END LOOP;
END $$;

DO $$
DECLARE
  filepath text;
BEGIN
  FOR filepath IN SELECT pg_ls_dir('/Users/karanthacker/yelp_hours') AS filename
  LOOP
    IF filepath LIKE '%.csv' THEN
      EXECUTE format('COPY hours FROM  %L CSV NULL '''' ', concat('/Users/karanthacker/yelp_hours/',filepath));
    END IF;
  END LOOP;
END $$;

DO $$
DECLARE
  filepath text;
BEGIN
  FOR filepath IN SELECT pg_ls_dir('/Users/karanthacker/yelp_reviews') AS filename
  LOOP
    IF filepath LIKE '%.csv' THEN
      EXECUTE format('COPY review FROM %L CSV DELIMITER E''|''  HEADER ', concat('/Users/karanthacker/yelp_reviews/',filepath));
    END IF;
  END LOOP;
END $$;

DO $$
DECLARE
  filepath text;
BEGIN
  FOR filepath IN SELECT pg_ls_dir('/Users/karanthacker/yelp_tips') AS filename
  LOOP
    IF filepath LIKE '%.csv' THEN
      EXECUTE format('COPY tip FROM %L CSV HEADER NULL '''' ', concat('/Users/karanthacker/yelp_tips/',filepath));
    END IF;
  END LOOP;
END $$;

DO $$
DECLARE
  filepath text;
BEGIN
  FOR filepath IN SELECT pg_ls_dir('/Users/karanthacker/yelp_tips') AS filename
  LOOP
    IF filepath LIKE '%.csv' THEN
      EXECUTE format('COPY tip FROM %L CSV HEADER NULL '''' ', concat('/Users/karanthacker/yelp_tips/',filepath));
    END IF;
  END LOOP;
END $$;

DO $$
DECLARE
  filepath text;
BEGIN
  FOR filepath IN SELECT pg_ls_dir('/Users/karanthacker/yelp_checkin') AS filename
  LOOP
    IF filepath LIKE '%.csv' THEN
      EXECUTE format('COPY checkin FROM %L CSV HEADER NULL '''' ', concat('/Users/karanthacker/yelp_checkin/',filepath));
    END IF;
  END LOOP;
END $$;

DO $$
DECLARE
  filepath text;
BEGIN
  FOR filepath IN SELECT pg_ls_dir('/Users/karanthacker/yelp_elite_years') AS filename
  LOOP
    IF filepath LIKE '%.csv' THEN
      EXECUTE format('COPY elite_years FROM %L CSV HEADER NULL '''' ', concat('/Users/karanthacker/yelp_elite_years/',filepath));
    END IF;
  END LOOP;
END $$;

DO $$
DECLARE
  filepath text;
BEGIN
  FOR filepath IN SELECT pg_ls_dir('/Users/karanthacker/yelp_friend') AS filename
  LOOP
    IF filepath LIKE '%.csv' THEN
      EXECUTE format('COPY friend FROM %L CSV HEADER NULL '''' ', concat('/Users/karanthacker/yelp_friend/',filepath));
    END IF;
  END LOOP;
END $$;
