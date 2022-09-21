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
from cloud_consul import putServiceConfig
from common import DumpToYaml, S
from online_flow import ServiceInfo, DataSource, FeatureInfo, CFModelInfo, RankModelInfo, OnlineFlow
from online_generator import OnlineGenerator
from service_config import FeatureConfig, Condition, FieldAction, RecommendConfig, TransformConfig, Chain, \
    ExperimentItem, OnlineServiceConfig


def get_movielens_demo_config():
    feature_service = FeatureConfig()
    feature_service.add_source(name="request")
    feature_service.add_source(name="movielens", kind="MongoDB",
                               options={"uri": "mongodb://root:example@localhost:27017/movielens?authSource=admin"})
    feature_service.add_sourceTable(name="item", source="movielens",
                                    columns=[{"movie_id": "long"}, {"genre": "str"}, {"title": "str"},
                                             {"imdb_url": "str"}, {"queryid": "str"}])
    feature_service.add_sourceTable(name="user", source="movielens",
                                    columns=[{"user_id": "long"}, {"gender": "str"}, {"age": "int"},
                                             {"occupation": "str"}, {"zip": "str"}, {"recent_movie_ids": "str"},
                                             {"last_movie": "str"}, {"last_genre": "str"},
                                             {"user_greater_than_three_rate": "decimal"},
                                             {"user_movie_avg_rating": "double"}])
    feature_service.add_sourceTable(name="requestData", source="request",
                                    columns=[{"user_id": "long"}])
    feature_service.add_sourceTable(name="item_feature", source="movielens",
                                    columns=[{"movielens": "long"}, {"watch_volume": "double"}, {"genre": "str"},
                                             {"movie_avg_rating": "double"},
                                             {"movie_greater_than_three_rate": "decimal"},
                                             {"genre_watch_volume": "double"},
                                             {"genre_movie_avg_rating": "double"},
                                             {"genre_greater_than_three_rate": "decimal"}])
    feature_service.add_sourceTable(name="itemcf", source="movielens",
                                    columns=[{"key": "str"}, {"value": {"list_struct": {"_1": "str", "_2": "double"}}}])
    feature_service.add_sourceTable(name="swing", source="movielens",
                                    columns=[{"key": "str"}, {"value": {"list_struct": {"_1": "str", "_2": "double"}}}])
    feature_service.add_feature(name="feature_user", depend=["user", "requestData"],
                                select=["user.user_id", "last_movie", "recent_movie_ids"],
                                condition=[Condition(left="requestData.user_id", right="user.user_id")])
    feature_service.add_feature(name="feature_item_match_icf",
                                depend=["algotransform_item_match_userprofile", "itemcf"],
                                select=["algotransform_item_match_userprofile.user_id",
                                        "algotransform_item_match_userprofile.item_score", "itemcf.value"],
                                condition=[Condition(left="algotransform_item_match_userprofile.item_id",
                                                     right="itemcf.key", type="left")])
    feature_service.add_feature(name="feature_item_match_swing",
                                depend=["algotransform_item_match_userprofile", "swing"],
                                select=["algotransform_item_match_userprofile.user_id",
                                        "algotransform_item_match_userprofile.item_score", "swing.value"],
                                condition=[Condition(left="algotransform_item_match_userprofile.item_id",
                                                     right="swing.key", type="left")])
    feature_service.add_feature(name="feature_wide_and_deep", depend=["user", "item", "rank_widedeep"],
                                select=["user.user_id", "item.movie_id", "rank_widedeep.item_id",
                                        "rank_widedeep.origin_scores"],
                                condition=[Condition(left="user.user_id", right="rank_widedeep.user_id"),
                                           Condition(left="item.movie_id", right="rank_widedeep.item_id")])
    feature_service.add_algoTransform(name="algotransform_item_match_userprofile", taskName="UserProfile",
                                      feature=["feature_user"],
                                      fieldActions=[
                                          FieldAction(names=["item_ids"], types=["list_str"],
                                                      fields=["recent_movie_ids"], func="splitRecentIds",
                                                      options={"splitor": S("\u0001")}),
                                          FieldAction(names=["item_id", "item_score"], types=["str", "double"],
                                                      input=["item_ids"], func="recentWeight"),
                                          FieldAction(names=["user_id"], types=["str"],
                                                      fields=["user_id"], func="typeTransform")
                                      ], output=["user_id", "item_id", "item_score"])
    feature_service.add_algoTransform(name="algotransform_item_match_icf", taskName="ItemMatcher",
                                      feature=["feature_item_match_icf"], options={"algo-name": S("itemCF")},
                                      fieldActions=[
                                          FieldAction(names=["toItemScore.user_id", "itemScore"],
                                                      types=["str", "map_str_double"],
                                                      fields=["user_id", "value", "item_score"], func="toItemScore"),
                                          FieldAction(names=["user_id", "item_id", "score", "origin_scores"],
                                                      types=["str", "str", "double", "map_str_double"],
                                                      input=["toItemScore.user_id", "itemScore"],
                                                      func="recallCollectItem")
                                      ], output=["user_id", "item_id", "score", "origin_scores"])
    feature_service.add_algoTransform(name="algotransform_item_match_swing", taskName="ItemMatcher",
                                      feature=["feature_item_match_swing"], options={"algo-name": S("swing")},
                                      fieldActions=[
                                          FieldAction(names=["toItemScore.user_id", "itemScore"],
                                                      types=["str", "map_str_double"],
                                                      fields=["user_id", "value", "item_score"], func="toItemScore"),
                                          FieldAction(names=["user_id", "item_id", "score", "origin_scores"],
                                                      types=["str", "str", "double", "map_str_double"],
                                                      input=["toItemScore.user_id", "itemScore"],
                                                      func="recallCollectItem")
                                      ], output=["user_id", "item_id", "score", "origin_scores"])
    feature_service.add_algoTransform(name="algotransform_widedeep", taskName="AlgoInference",
                                      feature=["feature_wide_and_deep"],
                                      options={"algo-name": S("widedeep"), "host": "localhost", "port": 50000},
                                      fieldActions=[
                                          FieldAction(names=["rankScore"], types=["float"], input=["movie_id"],
                                                      func="predictScore", algoColumns=[{"dnn_sparse": ["movie_id"]},
                                                                                        {"lr_sparse": ["movie_id"]}],
                                                      options={"modelName": "movie_lens_wdl_test",
                                                               "targetKey": "output", "targetIndex": 0}),
                                          FieldAction(names=["item_id", "score", "origin_scores"],
                                                      types=["str", "float", "map_str_double"],
                                                      input=["typeTransform.item_id", "rankScore"],
                                                      func="rankCollectItem", fields=["origin_scores"]),
                                          FieldAction(names=["user_id", "typeTransform.item_id", "movie_id"],
                                                      types=["str", "str", "str"],
                                                      fields=["user_id", "item_id", "movie_id"], func="typeTransform")
                                      ], output=["user_id", "item_id", "score", "origin_scores"])
    recommend_service = RecommendConfig()
    recommend_service.add_service(name="match_swing", tasks=["algotransform_item_match_swing"],
                                  options={"algoLevel": 3, "maxReservation": 200})
    recommend_service.add_service(name="match_itemcf", tasks=["algotransform_item_match_icf"],
                                  options={"algoLevel": 3, "maxReservation": 200})
    recommend_service.add_service(name="rank_widedeep", tasks=["algotransform_widedeep"],
                                  options={"algoLevel": 3, "maxReservation": 100},
                                  preTransforms=[TransformConfig(name="summary")],
                                  columns=[{"user_id": "long"}, {"item_id": "long"}, {"score": "double"},
                                           {"origin_scores": "map_str_double"}])
    recommend_service.add_experiment(name="match.base", options={"maxReservation": 5}, chains=[
        Chain(then=["match_itemcf"], transforms=[
            TransformConfig(name="cutOff"),
            TransformConfig(name="updateField", option={
                "input": ["score", "origin_scores"], "output": ["origin_scores"], "updateOperator": "putOriginScores"
            })
        ])
    ])
    recommend_service.add_experiment(name="match.multiple", options={"maxReservation": 10}, chains=[
        Chain(when=["match_itemcf", "match_swing"], transforms=[
            TransformConfig(name="summaryBySchema", option={
                "dupFields": ["user_id", "item_id"],
                "mergeOperator": {"score": "maxScore", "origin_scores": "mergeScoreInfo"}
            }),
            TransformConfig(name="updateField", option={
                "input": ["score", "origin_scores"], "output": ["origin_scores"], "updateOperator": "putOriginScores"
            }),
            TransformConfig(name="orderAndLimit", option={
                "orderFields": ["score"]
            })
        ])
    ])
    recommend_service.add_experiment(name="rank.wideDeep", chains=[
        Chain(then=["rank_widedeep"])])
    recommend_service.add_layer(name="match", bucketizer="random", experiments=[
        ExperimentItem(name="match.base", ratio=0.5),
        ExperimentItem(name="match.multiple", ratio=0.5)
    ])
    recommend_service.add_layer(name="rank", experiments=[
        ExperimentItem(name="rank.wideDeep", ratio=1.0)
    ])
    recommend_service.add_scene(name="guess-you-like", chains=[
        Chain(then=["match", "rank"])
    ], columns=[
        {"user_id": "str"}, {"item_id": "str"}
    ])
    online_configure = OnlineServiceConfig(feature_service, recommend_service)
    return DumpToYaml(online_configure)

def gen_online_flow():
    services = dict()
    services["mongo"] = ServiceInfo("mongo:6.0.1", ["movielens"], {
        "MONGO_INITDB_ROOT_USERNAME": "root",
        "MONGO_INITDB_ROOT_PASSWORD": "example"
    })
    user = DataSource("user", "mongo", "movielens", [{"user_id": "str"}, {"gender": "str"}, {"age": "int"},
                                        {"occupation": "str"}, {"zip": "str"}, {"recent_movie_ids": "str"},
                                        {"last_movie": "str"}, {"last_genre": "str"},
                                        {"user_greater_than_three_rate": "decimal"},
                                        {"user_movie_avg_rating": "double"}])
    item = DataSource("item", "mongo", "movielens", [{"movie_id": "str"}, {"genre": "str"}, {"title": "str"},
                                        {"imdb_url": "str"}, {"queryid": "str"}])
    interact = DataSource("item_feature", "mongo", "movielens",
                          [{"movie_id": "str"}, {"watch_volume": "double"}, {"genre": "str"},
                           {"movie_avg_rating": "double"},
                           {"movie_greater_than_three_rate": "decimal"},
                           {"genre_watch_volume": "double"},
                           {"genre_movie_avg_rating": "double"},
                           {"genre_greater_than_three_rate": "decimal"}])
    source = FeatureInfo(user, item, interact, [{"user_id": "str"}], "user_id", "movie_id", "recent_movie_ids", "\u0001")
    cf_models = list()
    itemcf = DataSource("itemcf", "mongo", "movielens",
                        [{"key": "str"}, {"value": {"list_struct": {"_1": "str", "_2": "double"}}}])
    cf_models.append(CFModelInfo("itemcf", itemcf))
    swing = DataSource("swing", "mongo", "movielens",
                       [{"key": "str"}, {"value": {"list_struct": {"_1": "str", "_2": "double"}}}])
    cf_models.append(CFModelInfo("swing", swing))
    twotower_models = list()
    random_model = None
    rank_models = list()
    rank_models.append(RankModelInfo("widedeep", "movie_lens_wdl_test",
                                     [{"dnn_sparse": ["movie_id"]}, {"lr_sparse": ["movie_id"]}]))
    online = OnlineFlow(source, random_model, cf_models, twotower_models, rank_models, services)
    pipeline = OnlineGenerator(configure=online)
    print(pipeline.gen_docker_compose())
    print(pipeline.gen_server_config())

if __name__ == '__main__':
    putServiceConfig(get_movielens_demo_config())
