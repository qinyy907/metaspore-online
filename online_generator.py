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
from common import DumpToYaml
from compose_config import OnlineDockerCompose
from online_flow import OnlineFlow, ServiceInfo, DataSource, FeatureInfo, CFModelInfo, RankModelInfo, \
    set_flow_default_value
from service_config import get_source_option, Source, Condition, FieldAction, \
    FeatureConfig, RecommendConfig, TransformConfig, Chain, ExperimentItem, OnlineServiceConfig


def append_source_table(feature_config, name, datasource):
    if feature_config is None or not datasource:
        raise ValueError("datasource must set!")
    source_name = datasource.serviceName
    if datasource.collection:
        source_name = "%s_%s" % (datasource.serviceName, datasource.collection)
    feature_config.add_sourceTable(name=name, source=source_name, table=datasource.table,
                                   columns=datasource.columns)


def columns_has_key(columns, key):
    if not columns or not key:
        return False
    for field in columns:
        if key in field:
            return True
    return False


class OnlineGenerator(object):
    def __init__(self, **kwargs):
        self.configure = kwargs.get("configure")
        if not self.configure or not isinstance(self.configure, OnlineFlow):
            raise ValueError("MetaSpore Online need input online configure data!")
        set_flow_default_value(self.configure)

    def gen_docker_compose(self):
        online_docker_compose = OnlineDockerCompose()
        if "recommend" not in self.configure.services:
            online_docker_compose.add_service("recommend", "container_recommend_service")
        if "model" not in self.configure.services:
            online_docker_compose.add_service("model", "container_model_service")
        if self.configure.services:
            for name, info in self.configure.services.items():
                online_docker_compose.add_service(name, "container_%s_service" % name,
                                                  image=info.image, environment=info.environment)
        online_recommend_service = online_docker_compose.services.get("recommend")
        if not online_recommend_service:
            raise ValueError("container_recommend_service init fail!")
        if online_docker_compose.services:
            for name, service in online_docker_compose.services.items():
                if name == "recommend" or not service.ports:
                    continue
                online_recommend_service.add_env("%s_HOST" % name.upper(), name)
                online_recommend_service.add_env("%s_PORT" % name.upper(), service.ports[0])
        return DumpToYaml(online_docker_compose)

    def gen_server_config(self):
        feature_config = FeatureConfig(source=[Source(name="request"), ])
        recommend_config = RecommendConfig()
        if not self.configure.services:
            raise ValueError("services must set!")
        for name, info in self.configure.services.items():
            if not info.collection:
                feature_config.add_source(name=name, image=info.image,
                                          options=get_source_option(self.configure, name, None))
            else:
                for db in info.collection:
                    feature_config.add_source(name="%s_%s" % (name, db), image=info.image,
                                              options=get_source_option(self.configure, name, db))
        feature_info = self.configure.source
        if not feature_info:
            raise ValueError("feature_info must set!")
        append_source_table(feature_config, "source_table_user", feature_info.user)
        append_source_table(feature_config, "source_table_item", feature_info.item)
        append_source_table(feature_config, "source_table_summary", feature_info.summary)
        user_key = feature_info.user_key_name or "user_id"
        items_key = feature_info.user_item_ids_name or "item_ids"
        item_key = feature_info.item_key_name or "item_id"
        if not columns_has_key(feature_info.user.columns, user_key) \
                or not columns_has_key(feature_info.user.columns, items_key):
            raise ValueError("user column must has user_key_name and user_item_ids_name!")
        if not columns_has_key(feature_info.item.columns, item_key):
            raise ValueError("item column must has item_key_name!")
        if not columns_has_key(feature_info.summary.columns, item_key):
            raise ValueError("summary column must has item_key_name!")
        if not feature_info.request or not columns_has_key(feature_info.request, user_key):
            raise ValueError("request column must set!")
        feature_config.add_sourceTable(
            name="source_table_request", source="request", columns=feature_info.request)

        user_fields = list()
        user_key_type = "str"
        user_key_action = FieldAction(names=[user_key], types=["str"], fields=[user_key], func="typeTransform", )
        for field_item in feature_info.user.columns:
            user_fields.extend(field_item.keys())
            if user_key in field_item:
                user_key_type = field_item.get(user_key)
        request_fields = list()
        for field_item in feature_info.request:
            request_fields.extend(field_item.keys())
            if user_key in field_item and field_item.get(user_key) != user_key_type:
                raise ValueError("request user key type set error!")
        item_fields = list()
        item_key_type = "str"
        for field_item in feature_info.item.columns:
            item_fields.extend(field_item.keys())
            if item_key in field_item:
                item_key_type = field_item.get(item_key)

        feature_config.add_feature(name="feature_user", depend=["source_table_request", "source_table_user"],
                                   select=["source_table_user.%s" % field for field in user_fields],
                                   condition=[Condition(left="source_table_request.%s" % user_key, type="left",
                                                        right="source_table_user.%s" % user_key)])
        user_profile_actions = list([user_key_action, ])
        user_profile_actions.append(FieldAction(names=["item_ids"], types=["list_str"],
                                                options={"splitor": feature_info.user_item_ids_split or "\u0001"},
                                                func="splitRecentIds", fields=[items_key]))
        user_profile_actions.append(FieldAction(names=[item_key, "item_score"], types=["str", "double"],
                                                func="recentWeight", input=["item_ids"]))
        feature_config.add_algoTransform(name="algotransform_user", taskName="UserProfile", feature=["feature_user"],
                                         fieldActions=user_profile_actions, output=[user_key, item_key, "item_score"])
        recall_services = list()
        recall_experiments = list()
        if self.configure.random_model:
            model_info = self.configure.random_model
            append_source_table(feature_config, model_info.name, model_info.source)
            feature_config.add_algoTransform(name="algotransform_random",
                                             fieldActions=list[FieldAction(names=["hash_id"], types=["int"],
                                                                           func="randomGenerator",
                                                                           options={"bound": model_info.bound}),
                                                               FieldAction(names=["score"], types=["double"],
                                                                           func="setValue",
                                                                           options={"value": 1.0})
                                             ],
                                             output=["hash_id", "score"])
            feature_config.add_feature(name="feature_random",
                                       depend=["source_table_request", "algotransform_random", model_info.name],
                                       select=["source_table_request.%s" % user_key, "algotransform_random.score",
                                               "%s.value" % model_info.name],
                                       condition=[Condition(left="algotransform_random.hash_id",
                                                            right="%s.key" % model_info.name)])
            field_actions = list()
            field_actions.append(FieldAction(names=["toItemScore.%s" % user_key, "item_score"],
                                             types=["str", "map_str_double"],
                                             func="toItemScore", fields=[user_key, "value", "score"]))
            field_actions.append(
                FieldAction(names=[user_key, item_key, "score", "origin_scores"],
                            types=["str", "str", "double", "map_str_double"],
                            func="recallCollectItem", input=["toItemScore.%s" % user_key, "item_score"]))
            algoTransform_name = "algotransform_%s" % model_info.name
            feature_config.add_algoTransform(name=algoTransform_name,
                                             taskName="ItemMatcher", feature=["feature_random"],
                                             options={"algo-name": model_info.name},
                                             fieldActions=field_actions,
                                             output=[user_key, item_key, "score", "origin_scores"])
            service_name = "recall_%s" % model_info.name
            recommend_config.add_service(name=service_name, tasks=[algoTransform_name],
                                         options={"maxReservation": 200})
            experiment_name = "recall.%s" % model_info.name
            recommend_config.add_experiment(name=experiment_name,
                                            options={"maxReservation": 100}, chains=[
                    Chain(then=[service_name], transforms=[
                        TransformConfig(name="cutOff"),
                        TransformConfig(name="updateField", option={
                            "input": ["score", "origin_scores"], "output": ["origin_scores"],
                            "updateOperator": "putOriginScores"
                        })
                    ])
                ])
            recall_experiments.append(experiment_name)
            recall_services.append(service_name)
        if self.configure.cf_models:
            for model_info in self.configure.cf_models:
                append_source_table(feature_config, model_info.name, model_info.source)
                feature_name = "feature_%s" % model_info.name
                feature_config.add_feature(name=feature_name, depend=["algotransform_user", model_info.name],
                                           select=["algotransform_user.%s" % user_key, "algotransform_user.item_score",
                                                   "%s.value" % model_info.name],
                                           condition=[Condition(left="algotransform_user.%s" % item_key, type="left",
                                                                right="%s.key" % model_info.name)])
                field_actions = list()
                field_actions.append(FieldAction(names=["toItemScore.%s" % user_key, "item_score"],
                                                 types=["str", "map_str_double"],
                                                 func="toItemScore", fields=[user_key, "value", "item_score"]))
                field_actions.append(
                    FieldAction(names=[user_key, item_key, "score", "origin_scores"],
                                types=["str", "str", "double", "map_str_double"],
                                func="recallCollectItem", input=["toItemScore.%s" % user_key, "item_score"]))
                algoTransform_name = "algotransform_%s" % model_info.name
                feature_config.add_algoTransform(name=algoTransform_name,
                                                 taskName="ItemMatcher", feature=[feature_name],
                                                 options={"algo-name": model_info.name},
                                                 fieldActions=field_actions,
                                                 output=[user_key, item_key, "score", "origin_scores"])
                service_name = "recall_%s" % model_info.name
                recommend_config.add_service(name=service_name, tasks=[algoTransform_name],
                                             options={"maxReservation": 200})
                experiment_name = "recall.%s" % model_info.name
                recommend_config.add_experiment(name=experiment_name,
                                                options={"maxReservation": 100}, chains=[
                        Chain(then=[service_name], transforms=[
                            TransformConfig(name="cutOff"),
                            TransformConfig(name="updateField", option={
                                "input": ["score", "origin_scores"], "output": ["origin_scores"],
                                "updateOperator": "putOriginScores"
                            })
                        ])
                    ])
                recall_experiments.append(experiment_name)
                recall_services.append(service_name)
        if len(recall_services) > 1:
            recommend_config.add_experiment(name="recall.multiple", options={"maxReservation": 100}, chains=[
                Chain(when=recall_services, transforms=[
                    TransformConfig(name="summaryBySchema", option={
                        "dupFields": [user_key, item_key],
                        "mergeOperator": {"score": "maxScore", "origin_scores": "mergeScoreInfo"}
                    }),
                    TransformConfig(name="updateField", option={
                        "input": ["score", "origin_scores"], "output": ["origin_scores"],
                        "updateOperator": "putOriginScores"
                    }),
                    TransformConfig(name="orderAndLimit", option={
                        "orderFields": ["score"]
                    })
                ])
            ])
            recall_experiments.append("recall.multiple")
        rank_experiments = list()
        if self.configure.rank_models:
            for model_info in self.configure.rank_models:
                feature_name = "feature_%s" % model_info.name
                select_fields = list()
                select_fields.extend(["source_table_user.%s" % key for key in user_fields])
                select_fields.extend(["source_table_item.%s" % key for key in item_fields])
                select_fields.append("rank_%s.origin_scores" % model_info.name)
                feature_config.add_feature(name=feature_name,
                                           depend=["source_table_user", "source_table_item",
                                                   "rank_%s" % model_info.name],
                                           select=select_fields,
                                           condition=[Condition(left="source_table_user.%s" % user_key,
                                                                right="rank_%s.%s" % (model_info.name, user_key)),
                                                      Condition(left="source_table_item.%s" % item_key,
                                                                right="rank_%s.%s" % (model_info.name, item_key)),
                                                      ])
                field_actions = list()
                field_actions.append(FieldAction(names=[user_key, "typeTransform.%s" % item_key],
                                                 types=["str", "str"],
                                                 func="typeTransform",
                                                 fields=[user_key, item_key]))
                field_actions.append(FieldAction(names=[item_key, "score", "origin_scores"],
                                                 types=["str", "float", "map_str_double"],
                                                 input=["typeTransform.%s" % item_key, "rankScore"],
                                                 func="rankCollectItem",
                                                 fields=["origin_scores"]))
                field_actions.append(FieldAction(names=["rankScore"], types=["float"],
                                                 algoColumns=model_info.column_info,
                                                 options={"modelName": model_info.model,
                                                          "targetKey": "output", "targetIndex": 0},
                                                 func="predictScore", input=["typeTransform.%s" % item_key]))
                algoTransform_name = "algotransform_%s" % model_info.name
                feature_config.add_algoTransform(name=algoTransform_name,
                                                 taskName="AlgoInference", feature=[feature_name],
                                                 options={"algo-name": model_info.name,
                                                          "host": "${MODEL_HOST:localhost}",
                                                          "port": "${MODEL_PORT:50000}"},
                                                 fieldActions=field_actions,
                                                 output=[user_key, item_key, "score", "origin_scores"])
                service_name = "rank_%s" % model_info.name
                recommend_config.add_service(name=service_name,
                                             preTransforms=[TransformConfig(name="summary")],
                                             columns=[{user_key: user_key_type}, {item_key: item_key_type},
                                                      {"score": "double"}, {"origin_scores": "map_str_double"}],
                                             tasks=[algoTransform_name], options={"maxReservation": 200})
                experiment_name = "rank.%s" % model_info.name
                recommend_config.add_experiment(name=experiment_name,
                                                options={"maxReservation": 100}, chains=[
                        Chain(then=[service_name], transforms=[
                            TransformConfig(name="cutOff"),
                            TransformConfig(name="updateField", option={
                                "input": ["score", "origin_scores"], "output": ["origin_scores"],
                                "updateOperator": "putOriginScores"
                            })
                        ])
                    ])
                rank_experiments.append(experiment_name)
        layers = []
        if recall_experiments:
            layer_name = "recall"
            recommend_config.add_layer(name=layer_name, bucketizer="random", experiments=[
                ExperimentItem(name=name, ratio=1.0 / len(recall_experiments)) for name in recall_experiments
            ])
            layers.append(layer_name)
        if recall_experiments:
            layer_name = "rank"
            recommend_config.add_layer(name=layer_name, bucketizer="random", experiments=[
                ExperimentItem(name=name, ratio=1.0 / len(rank_experiments)) for name in rank_experiments
            ])
            layers.append(layer_name)
        recommend_config.add_scene(name="guess-you-like", chains=[
            Chain(then=layers)],
                                   columns=[{user_key: user_key_type}, {item_key: item_key_type}])
        online_configure = OnlineServiceConfig(feature_config, recommend_config)
        return DumpToYaml(online_configure).encode("utf-8").decode("latin1")


def get_demo_jpa_flow():
    services = dict()
    services["mongo"] = ServiceInfo("mongo:6.0.1", ["jpa"], {
        "MONGO_INITDB_ROOT_USERNAME": "root",
        "MONGO_INITDB_ROOT_PASSWORD": "example"
    })
    user = DataSource("amazonfashion_user_feature", "mongo", "jpa", [{"user_id": "str"},
                                                                     {"user_bhv_item_seq": "str"}])
    item = DataSource("amazonfashion_item_feature", "mongo", "jpa", [{"item_id": "str"}, {"category": "str"}])
    summary = DataSource("amazonfashion_item_summary", "mongo", "jpa",
                         [{"item_id": "str"}, {"category": "str"}, {"title": "str"},
                          {"description": "str"},
                          {"image": "str"},
                          {"url": "str"},
                          {"price": "double"}])
    source = FeatureInfo(user, item, summary, [{"user_id": "str"}], "user_id", "item_id", "user_bhv_item_seq", "\u0001")
    cf_models = list()
    swing = DataSource("amazonfashion_swing", "mongo", "jpa",
                       [{"key": "str"}, {"value": {"list_struct": {"_1": "str", "_2": "double"}}}])
    cf_models.append(CFModelInfo("swing", swing))
    twotower_models = list()
    random_model = None
    rank_models = list()
    rank_models.append(RankModelInfo("widedeep", "movie_lens_wdl_test",
                                     [{"dnn_sparse": ["item_id"]}, {"lr_sparse": ["item_id"]}]))
    return OnlineFlow(source, random_model, cf_models, twotower_models, rank_models, services)


if __name__ == '__main__':
    pipeline = OnlineGenerator(configure=get_demo_jpa_flow())
    print(pipeline.gen_docker_compose())
    print(pipeline.gen_server_config())
