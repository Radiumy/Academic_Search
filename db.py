import pymongo

class AcademicDB:
    def __init__(self, uri="mongoDB URL"):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client["academic_crawler"]
        self.collection = self.db["scholars"]

    def save_person(self, person_data):
        """
        person_data
        """
        self.collection.update_one(
            {"name": person_data["name"], "affiliations": person_data["affiliations"]},
            {"$set": person_data},
            upsert=True
        )
        print(f"Successfully saved/updated: {person_data['name']}")

# db = AcademicDB()
# db.save_person(your_json_data)
