from pymongo import MongoClient

def connect_to_mongo():
    client = MongoClient("mongodb://localhost:27017/") # 연결 문자열을 필요에 따라 변경
    db = client["report_database"] # 데이터베이스 이름
    return db

def insert_data_into_mongo(data):
    db = connect_to_mongo()
    collection = db["reports"]
    
    #데이터 삽입
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

    print("데이터가 MongoDB에 성공적으로 저장되었습니다!!")