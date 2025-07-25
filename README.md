# Redis
import redis
import csv
import codecs
from traceback import print_stack


class Redis_Client:
    def __init__(self):
        self.host = "redis-14013.c266.us-east-1-3.ec2.redns.redis-cloud.com"
        self.port = 14013
        self.password = "sr987654321"
        self.username = "default"
        self.client = None

    def connect(self):
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                username=self.username,
                decode_responses=True
            )
            if self.client.ping():
                print("✅ Connected to Redis successfully")
            else:
                print("⚠️ Ping failed.")
        except Exception as e:
            print(f"❌ Connection error: {e}")
    
rs = Redis_Client()
rs.connect()
#rs.load_users("users.txt")

def load_users(self, file):
    result = 0
    try:
        with open(file, 'r', encoding='utf-8') as f:
            content = f.read()
            # Split users using "user:"
            user_blocks = content.split('"user:')
            for block in user_blocks:
                block = block.strip()
                if not block:
                    continue
                tokens = block.replace('"', '').split()
                user_dict = {'id': tokens[0]}
                for i in range(1, len(tokens) - 1, 2):
                    key = tokens[i]
                    value = tokens[i + 1]
                    user_dict[key] = value
                redis_key = f"user:{user_dict['id']}"
                self.client.hset(redis_key, mapping=user_dict)
                print(f"✅ Loaded user: {user_dict['id']}")
                result += 1
        print(f"✅ Loaded {result} users into Redis")
        return result
    except Exception as e:
        print(f"❌ Failed to load users: {e}")
        return 0
load_users(rs, r"C:\Users\~Azij~\OneDrive\Documents\Python\redis\users.txt")

def load_scores(self, file):
    result = 0
    try:
        pipe = self.client.pipeline()
        with open(file, 'r', encoding='utf-8') as f:
            next(f)  # skip the header line
            for line in f:
                parts = line.strip().split(',')
                if len(parts) != 3:
                    continue  # skip bad rows
                user_id, leaderboard_id, score = parts
                try:
                    score = float(score)
                except ValueError:
                    print(f"⚠️ Invalid score: {score}")
                    continue
                pipe.zadd(f"leaderboard:{leaderboard_id}", {user_id: score})
                result += 1
        pipe.execute()
        print(f"✅ Loaded {result} scores into Redis")
        return result
    except Exception as e:
        print(f"❌ Failed to load scores: {e}")
        return 0


#call load_scores
load_scores(rs, r"C:\Users\~Azij~\OneDrive\Documents\Python\redis\userscores.csv")

def query1(self, usr):
    print(f"🔍 Executing query1: Get all attributes for user:{usr}")

    try:
        key = f"user:{usr}"
        user_data = self.client.hgetall(key)

        if not user_data:
            print(f"❌ No data found for user:{usr}")
            return {}

        print(f"✅ User {usr} data:")
        for k, v in user_data.items():
            print(f"  {k}: {v}")

        return user_data

    except Exception as e:
        print(f"❌ query1 failed: {e}")
        return {}

# Call the function using the rs instance and provide the user id
query1(rs, 100)

def query2(self, usr):
    print(f"🔍 Executing query2: Get coordinates for user:{usr}")
    try:
        key = f"user:{usr}"
        user_data = self.client.hgetall(key)

        if not user_data:
            print(f"❌ No user found with ID {usr}")
            return None

        latitude = user_data.get("latitude")
        longitude = user_data.get("longitude")

        if latitude and longitude:
            print(f"📍 Coordinates for user {usr}: ({latitude}, {longitude})")
            return (latitude, longitude)
        else:
            print(f"⚠️ Latitude or Longitude missing for user {usr}")
            return None
    except Exception as e:
        print(f"❌ query2 failed: {e}")
        return None
query2(rs, 100)

def query3(self):
    print("🔍 Executing query3: Get keys & last names of users whose IDs do not start with an odd number")

    cursor = 1280  # starting point (per assignment instruction)
    result_keys = []
    result_lastnames = []

    try:
        while True:
            cursor, keys = self.client.scan(cursor=cursor, match='user:*', count=100)
            for key in keys:
                user_id_str = key.split(':')[1]  # extract just the ID

                if not user_id_str or not user_id_str[0].isdigit():
                    continue  # skip invalid IDs

                if user_id_str[0] in {'1', '3', '5', '7', '9'}:
                    continue  # starts with odd number → skip

                last_name = self.client.hget(key, 'last_name')
                result_keys.append(key)
                result_lastnames.append(last_name)

            if cursor == 0:
                break

        print(f"✅ Found {len(result_keys)} matching users.")
        for key, last in zip(result_keys[:10], result_lastnames[:10]):
            print(f"{key} → {last}")
        return result_keys, result_lastnames

    except Exception as e:
        print(f"❌ query3 failed: {e}")
        return [], []
query3(rs)

from redis import Redis
from redis.commands.search import Client
from redis.commands.search.query import Query
from redis.commands.search.field import TextField, TagField, NumericField
from redis.commands.search.index_definition import IndexDefinition, IndexType

def create_user_index(self):
    try:
        print("⚙️ Creating RediSearch index on user:* keys")
        schema = (
            TextField("gender"),
            TagField("country"),
            NumericField("latitude"),
            TextField("first_name")
        )
        definition = IndexDefinition(prefix=["user:"], index_type=IndexType.HASH)
        self.client.ft("user_index").create_index(fields=schema, definition=definition)
        print("✅ Index created successfully")

    except Exception as e:
        if "Index already exists" in str(e):
            print("ℹ️ Index already exists — skipping creation")
        else:
            print(f"❌ Failed to create index: {e}")
rs = Redis_Client()
rs.connect()
rs.create_user_index()
rs.query4()

def query4(self):
    print("🔍 Executing query4: Female users in China or Russia with latitude 40–46")

    try:
        query_str = "@gender:female @country:{China|Russia} @latitude:[40 46]"
        query = Query(query_str).paging(0, 20)  # get first 20 results
        results = self.client.ft("user_index").search(query)

        for doc in results.docs:
            print(f"👤 {doc.id}: {doc.first_name} ({doc.gender}, {doc.country}, lat={doc.latitude})")

        return results.docs

    except Exception as e:
        print(f"❌ query4 failed: {e}")
        return []
query4(rs)

def query5(self):
    print("🔍 Executing query5: Top 10 players in leaderboard:2")

    try:
        top_users = self.client.zrevrange("leaderboard:2", 0, 9)
        print(f"🏆 Top user keys: {top_users}")

        emails = []
        for uid in top_users:
            email = self.client.hget(uid, "email")  # ✅ FIXED: don't prepend "user:"
            if email:
                emails.append((uid, email))
            else:
                emails.append((uid, "❌ Email not found"))

        print("📧 Top 10 Player Emails:")
        for uid, email in emails:
            print(f"User {uid}: {email}")

        return emails

    except Exception as e:
        print(f"❌ query5 failed: {e}")
        return []

# Use the existing rs instance and call query5 with rs as self
query5(rs)


