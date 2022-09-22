#
# Copyright 2022 DMetaSoul
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from pymongo import MongoClient
from urllib.parse import quote_plus
from decimal import Decimal
import json


def update_item(item):
    if not item:
        return
    if '_id' in item:
        item.pop('_id')
    if 'user_id' in item and not isinstance(item['user_id'], str):
        item['user_id'] = str(item['user_id'])
    if "movie_id" in item and not isinstance(item["movie_id"], str):
        item["movie_id"] = str(item["movie_id"])


class MongodbSource(object):
    def __init__(self, host="localhost", port=27017, user="root", password="example"):
        uri = "mongodb://%s:%s@%s:%d" % (
            quote_plus(user), quote_plus(password), host, port)
        self._client = MongoClient(uri)

    def info(self):
        print(self._client.is_mongos)

    def insert_object(self, table, collect, data):
        self._client.get_database()
        self._client[collect][table].insert_one(data)

    def insert_json(self, table, collect, json_file_name):
        with open(json_file_name, encoding='utf-8') as json_file:
            json_data = []
            # json_data.extend(json.loads(json_file.read()))
            for line in json_file.readlines():
                json_data.append(json.loads(line))
            for item in json_data:
                update_item(item)
                self._client[collect][table].insert_one(item)

    def count(self, table, collect):
        return self._client[collect][table].count_documents({})


def put_demo_data(collection="movielens", host="192.168.221.128", port=27017):
    mangodb_source = MongodbSource(host, port)
    mangodb_source.insert_json("user", collection, "./demo/user.json")
    print("user insert over!", mangodb_source.count("user", collection))
    mangodb_source.insert_json("item", collection, "./demo/item.json")
    print("item insert over!", mangodb_source.count("item", collection))
    mangodb_source.insert_json("item_feature", collection, "./demo/item_feature.json")
    print("item_feature insert over!", mangodb_source.count("item_feature", collection))
    mangodb_source.insert_json("itemcf", collection, "./demo/itemcf.json")
    print("itemcf insert over!", mangodb_source.count("itemcf", collection))
    mangodb_source.insert_json("swing", collection, "./demo/swing.json")
    print("swing insert over!", mangodb_source.count("swing", collection))


def put_jpa_data(collection="jpa", host="127.0.0.1", port=27017):
    mangodb_source = MongodbSource(host, port)
    mangodb_source.insert_json("amazonfashion_user_feature", collection, "./jpa/amazonfashion_user_feature.json")
    print("user_feature insert over!", mangodb_source.count("amazonfashion_user_feature", collection))
    mangodb_source.insert_json("amazonfashion_item_feature", collection, "./jpa/amazonfashion_item_feature.json")
    print("item insert over!", mangodb_source.count("amazonfashion_item_feature", collection))
    mangodb_source.insert_json("amazonfashion_item_summary", collection, "./jpa/amazonfashion_item_summary.json")
    print("summary insert over!", mangodb_source.count("amazonfashion_item_summary", collection))
    mangodb_source.insert_json("amazonfashion_swing", collection, "./jpa/amazonfashion_swing.json")
    print("swing insert over!", mangodb_source.count("amazonfashion_swing", collection))


if __name__ == "__main__":
    # put_demo_data("movielens", "192.168.221.128")
    # put_demo_data("movielens", "127.0.0.1")
    put_jpa_data()
