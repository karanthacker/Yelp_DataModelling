import json
import pymongo

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["Yelp_db"]
mongo_collection = mongo_db["Business"]

if __name__ == "__main__":
    
    business_data = []
    with open('yelp_academic_dataset_business.json') as f:
        for line in f:
            # Parse the JSON data
            data = json.loads(line)

            # Set the '_id' field to be the same as the 'business_id' field
            data['_id'] = data['business_id']
            del data['business_id']
            business_data.append(data)


    mongo_collection.insert_many(business_data)

    checkin_data = []
    with open('yelp_academic_dataset_checkin.json') as f:
        for line in f:
            # Parse the JSON data
            data = json.loads(line)
            # Set the '_id' field to be the same as the 'business_id' field
            data['_id'] = data['business_id']
            del data['business_id']
            checkin_data.append(data)

    mongo_collection.insert_many(checkin_data)

    user_data = []
    with open('yelp_academic_dataset_user.json') as f:
        for line in f:
            # Parse the JSON data
            data = json.loads(line)

            # Set the '_id' field to be the same as the 'user_id' field
            data['_id'] = data['user_id']
            del data['user_id']
            user_data.append(data)
    mongo_collection.insert_many(user_data)

    review_data = []
    with open('yelp_academic_dataset_review.json') as f:
        for line in f:
            # Parse the JSON data
            data = json.loads(line)

            # Set the '_id' field to be the same as the 'review_id' field
            data['_id'] = data['review_id']
            del data['review_id']
            user_data.append(data)
    mongo_collection.insert_many(review_data)

    tip_data = []
    with open('yelp_academic_dataset_tip.json') as f:
        for line in f:
            # Parse the JSON data
            data = json.loads(line)

            tip_data.append(data)
    mongo_collection.insert_many(tip_data)

    mongo_client.close()