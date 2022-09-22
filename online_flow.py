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
from attrs import frozen
from attrs import field


@frozen
class ServiceInfo(object):
    image: str
    collection: list[str]
    environment: dict[str, any]


@frozen
class DataSource(object):
    table: str
    serviceName: str
    collection: str
    columns: list[dict[str, any]]


@frozen
class MilvusInfo(object):
    collection: str
    fields: list[str]
    serviceName: str


@frozen
class RandomModelInfo(object):
    name: str
    bound: int
    source: DataSource


@frozen
class CFModelInfo(object):
    name: str
    source: DataSource


@frozen
class TwoTowerModelInfo(object):
    name: str
    model: str
    milvus: MilvusInfo


@frozen
class RankModelInfo(object):
    name: str
    model: str
    column_info: dict[str, list[str]]


@frozen
class FeatureInfo(object):
    user: DataSource
    item: DataSource
    summary: DataSource
    request: list[dict[str, any]]
    user_key_name: str
    item_key_name: str
    user_item_ids_name: str
    user_item_ids_split: str


@frozen
class OnlineFlow(object):
    source: FeatureInfo
    random_model: RankModelInfo
    cf_models: list[CFModelInfo]
    twotower_models: list[TwoTowerModelInfo]
    rank_models: list[RankModelInfo]
    services: dict[str, ServiceInfo]

    def update(self, new_flow):
        if not new_flow:
            return
        if new_flow.source:
            new_source = new_flow.source
            if new_source.user:
                self.source.user = new_source.user
            if new_source.item:
                self.source.user = new_source.item
            if new_source.summary:
                self.source.summary = new_source.summary
            if new_source.request:
                self.source.request = new_source.request
            if new_source.user_key_name:
                self.source.user_key_name = new_source.user_key_name
            if new_source.item_key_name:
                self.source.item_key_name = new_source.item_key_name
            if new_source.user_item_ids_name:
                self.source.user_item_ids_name = new_source.user_item_ids_name
            if new_source.request:
                self.source.user_item_ids_split = new_source.user_item_ids_split
        self.random_model = new_flow.random_model
        self.cf_models = new_flow.cf_models
        self.twotower_models = new_flow.twotower_models
        self.rank_models = new_flow.rank_models
        if self.services is None:
            self.services = {}
        self.services.update(new_flow.services)


def set_service_default_value(services, ds):
    if not ds:
        raise ValueError("online_flow datasource must not be None")
    if services is None:
        raise ValueError("online_flow services must not be None")
    if ds.serviceName not in services:
        if not ds.collection:
            ds.collection = ds.serviceName
        if not str(ds.serviceName).startswith("mongo"):
            ds.serviceName = "mongo_%s" % ds.serviceName
        services[ds.serviceName] = ServiceInfo("mongo:6.0.1", [ds.collection], {
            "MONGO_INITDB_ROOT_USERNAME": "root", "MONGO_INITDB_ROOT_PASSWORD": "example"})
    else:
        if ds.collection and ds.collection not in services[ds.serviceName].collection:
            services[ds.serviceName].collection.append(ds.collection)
        if services[ds.serviceName].image.startswith("mongo") and not str(ds.serviceName).startswith("mongo"):
            info = services.pop(ds.serviceName)
            ds.serviceName = "mongo_%s" % ds.serviceName
            services[ds.serviceName] = info
        if services[ds.serviceName].image.startswith("redis") and not str(ds.serviceName).startswith("redis"):
            info = services.pop(ds.serviceName)
            ds.serviceName = "redis_%s" % ds.serviceName
            services[ds.serviceName] = info
        if services[ds.serviceName].image.startswith("mysql") and not str(ds.serviceName).startswith("mysql"):
            info = services.pop(ds.serviceName)
            ds.serviceName = "mysql_%s" % ds.serviceName
            services[ds.serviceName] = info


def set_flow_default_value(online_flow):
    if not online_flow or not online_flow.source:
        raise ValueError("online_flow or source must not be None")
    if not online_flow.services:
        online_flow.services = dict()
    source = online_flow.source
    set_service_default_value(online_flow.services, source.user)
    set_service_default_value(online_flow.services, source.item)
    if source.summary:
        set_service_default_value(online_flow.services, source.summary)
        if not source.summary.columns:
            raise ValueError("summary columns must not be empty")
    if not source.user_key_name:
        source.user_key_name = "user_id"
    if not source.item_key_name:
        source.item_key_name = "item_id"
    if not source.user_item_ids_name:
        source.user_item_ids_name = "user_bhv_item_seq"
    if not source.user_item_ids_split:
        source.user_item_ids_split = "\u0001"
    if not source.request:
        source.request = [{source.user_key_name: "str", source.item_key_name: "str"}]
    if not source.user.columns:
        raise ValueError("user columns must not be empty")
    if not source.item.columns:
        raise ValueError("item columns must not be empty")
    if online_flow.random_model:
        if not online_flow.random_model.name:
            raise ValueError("random_model model name must not be empty")
        if online_flow.random_model.bound <= 0:
            online_flow.random_model.bound = 1000
        set_service_default_value(online_flow.services, online_flow.random_model.source)
    if online_flow.cf_models:
        for info in online_flow.cf_models:
            if not info.name:
                raise ValueError("cf_models model name must not be empty")
            set_service_default_value(online_flow.services, info.source)
            if not info.source.columns:
                info.source.columns = [{"key": "str"}, {"value": {"list_struct": {"_1": "str", "_2": "double"}}}]
    if online_flow.twotower_models:
        for info in online_flow.twotower_models:
            if not info.name or not info.model or not info.milvus:
                raise ValueError("twotower_models model name or model must not be empty")
            if not info.milvus.collection:
                raise ValueError("milvus.collection must not be empty")
            if info.milvus.serviceName not in online_flow.services:
                if not str(info.milvus.serviceName).startswith("milvus"):
                    info.milvus.serviceName = "milvus_%s" % info.milvus.serviceName
                    online_flow.services[info.milvus.serviceName] = ServiceInfo("milvusdb/milvus:v2.0.1",
                                                                                [info.milvus.collection], {})
            else:
                if info.milvus.collection not in online_flow.services[info.milvus.serviceName].collection:
                    online_flow.services[info.milvus.serviceName].collection.append(info.milvus.collection)
                if online_flow.services[info.milvus.serviceName].image.startswith("milvus")\
                        and not str(info.milvus.serviceName).startswith("milvus"):
                    milvus_info = online_flow.services.pop(info.milvus.serviceName)
                    info.milvus.serviceName = "milvus_%s" % info.milvus.serviceName
                    online_flow.services[info.milvus.serviceName] = milvus_info
    if online_flow.rank_models:
        for info in online_flow.rank_models:
            if not info.name or not info.model:
                raise ValueError("rank_models model name or model must not be empty")
            if not info.column_info:
                info.column_info = [{"dnn_sparse": [source.item_key_name]}, {"lr_sparse": [source.item_key_name]}]
