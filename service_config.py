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
from attrs import define
from attrs import field
from typing import Literal

from common import BaseDefaultConfig
from urllib.parse import quote_plus


def get_source_option(online_config, name, collection):
    options = {}
    if not name or not online_config or name not in online_config.services:
        return options
    service = online_config.services.get(name)
    if service.environment is None:
        service.environment = {}
    if service.image.startswith("mongo"):
        if collection:
            options["uri"] = "mongodb://{}:{}@{}/{}?authSource=admin".format(
                quote_plus(service.environment.get("MONGO_INITDB_ROOT_USERNAME") or "root"),
                quote_plus(service.environment.get("MONGO_INITDB_ROOT_PASSWORD") or "example"),
                "${%s_HOST:localhost}:${%s_PORT:27017}" % (str(name).upper(), str(name).upper()),
                collection,
        )
    return options


@define
class Source(BaseDefaultConfig):
    name: str
    kind: Literal['MongoDB', 'JDBC', 'Redis', 'Request'] = field(init=False, default="Request")
    options: dict[str, any] = field(init=False, default={})

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if "name" not in kwargs:
            raise ValueError("source config name must not be empty!")
        if "image" in kwargs:
            self.setKind(self.dict_data.pop("image"))
        if self.kind.lower() == "mongodb":
            if not self.options.get("uri") or not str(self.options.get("uri")).startswith("mongodb://"):
                raise ValueError("source mongodb config uri error!")
        if self.kind.lower() == "jdbc":
            if not self.options.get("uri") or not str(self.options.get("uri")).startswith("jdbc:"):
                raise ValueError("source jdbc config uri error!")
            if not self.options.get("user"):
                self.options["user"] = "root"
            if not self.options.get("password"):
                self.options["password"] = "example"
            if str(self.options.get("uri")).startswith("jdbc:mysql"):
                if not self.options.get("driver"):
                    self.options["driver"] = "com.mysql.cj.jdbc.Driver"
                if str(self.options["driver"]) != "com.mysql.cj.jdbc.Driver":
                    raise ValueError("source jdbc mysql config driver must be com.mysql.cj.jdbc.Driver!")
        if self.kind.lower() == "redis":
            if not self.options.get("standalone") and not self.options.get("sentinel") and not self.options.get(
                    "cluster"):
                self.options["standalone"] = {"host": "localhost", "port": 6379}
        if self.options:
            self.dict_data["options"] = self.options

    def setKind(self, image):
        if not image:
            self.kind = "Request"
        if str(image).startswith("mongo"):
            self.kind = 'MongoDB'
        elif str(image).startswith("redis"):
            self.kind = 'Redis'
        elif str(image).startswith("mysql"):
            self.kind = 'JDBC'
        # to append other database
        else:
            self.kind = "Request"
        self.dict_data["kind"] = self.kind


@define
class SourceTable(BaseDefaultConfig):
    name: str
    source: str
    columns: list[dict[str, any]]
    table: str = field(init=False)
    prefix: str = field(init=False, default="")
    sqlFilters: list[str] = field(init=False, default=[])
    filters: list[dict[str, dict[str, any]]] = field(init=False, default=[])
    options: dict[str, any] = field(init=False, default={})

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if "name" not in kwargs:
            raise ValueError("SourceTable config name must not be empty!")
        if "source" not in kwargs:
            raise ValueError("SourceTable config source must not be empty!")


@define
class Condition(BaseDefaultConfig):
    left: str
    right: str
    type: Literal['left', 'inner', 'right', "full"] = field(init=False, default="inner")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        for item in self.__attrs_attrs__:
            if item.name not in kwargs:
                setattr(self, item.name, item.default)

    def to_dict(self):
        data = {self.left: self.right}
        if self.type != 'inner':
            data["type"] = self.type
        return data


@define
class Feature(BaseDefaultConfig):
    name: str
    depend: list[str]
    select: list[str]
    condition: list[Condition] = field(init=False, default=[])
    immediateFrom: list[str] = field(init=False, default=[])
    filters: list[dict[str, dict[str, any]]] = field(init=False, default=[])

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_dict(self):
        from_tables = self.dict_data.pop("depend")
        self.dict_data["from"] = from_tables
        if self.condition:
            self.dict_data["condition"] = [x.to_dict() for x in self.condition]
        if self.immediateFrom:
            self.dict_data["immediateFrom"] = self.immediateFrom
        if self.filters:
            self.dict_data["filters"] = self.filters
        return self.dict_data


@define
class FieldAction(BaseDefaultConfig):
    names: list[str]
    types: list[str]
    fields: list[str]
    input: list[str]
    func: str
    options: dict[str, any] = field(init=False, default={})
    algoColumns: list[dict[str, list[str]]] = field(init=False, default=[])

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.fields and not self.input:
            raise ValueError("FieldAction config input or fields must not be empty!")

    def to_dict(self):
        data = dict()
        if len(self.names) == 1:
            data["name"] = self.names[0]
        else:
            data["names"] = self.names
        if len(self.types) == 1:
            data["type"] = self.types[0]
        else:
            data["types"] = self.types
        if self.fields:
            data["fields"] = self.fields[0] if len(self.fields) == 1 else self.fields
        if self.input:
            data["input"] = self.input[0] if len(self.input) == 1 else self.input
        if self.func:
            data["func"] = self.func
        if self.algoColumns:
            data["algoColumns"] = self.algoColumns
        if self.options:
            data["options"] = self.options
        return data


@define
class AlgoTransform(BaseDefaultConfig):
    name: str
    fieldActions: list[FieldAction]
    output: list[str]
    taskName: str = field(init=False, default=None)
    feature: list[str] = field(init=False, default=[])
    algoTransform: list[str] = field(init=False, default=[])
    options: dict[str, any] = field(init=False, default={})

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.feature and not self.algoTransform:
            raise ValueError("AlgoTransform config feature or algoTransform must not be empty!")

    def to_dict(self):
        if self.fieldActions:
            self.dict_data["fieldActions"] = [x.to_dict() for x in self.fieldActions]
        if self.taskName:
            self.dict_data["taskName"] = self.taskName
        if self.feature:
            self.dict_data["feature"] = self.feature[0] if len(self.feature) == 1 else self.feature
        if self.output:
            self.dict_data["output"] = self.output
        if self.algoTransform:
            self.dict_data["algoTransform"] = self.algoTransform[0] if len(
                self.algoTransform) == 1 else self.algoTransform
        if self.options:
            self.dict_data["options"] = self.options
        return self.dict_data


@define
class TransformConfig(BaseDefaultConfig):
    name: str
    option: dict[str, any] = field(init=False, default={})

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_dict(self):
        if self.option:
            self.dict_data["option"] = self.option
        return self.dict_data


@define
class Chain(BaseDefaultConfig):
    then: list[str] = field(init=False, default=[])
    when: list[str] = field(init=False, default=[])
    options: dict[str, any] = field(init=False, default={})
    transforms: list[TransformConfig] = field(init=False, default=[])

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_dict(self):
        if self.then:
            self.dict_data["then"] = self.then[0] if len(self.then) == 1 else self.then
        if self.when:
            self.dict_data["when"] = self.when[0] if len(self.when) == 1 else self.when
        if self.options:
            self.dict_data["options"] = self.options
        if self.transforms:
            self.dict_data["transforms"] = [x.to_dict() for x in self.transforms]
        return self.dict_data


@define
class ExperimentItem(BaseDefaultConfig):
    name: str
    ratio: float

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


@define
class Layer(BaseDefaultConfig):
    name: str
    bucketizer: str
    taskName: str = field(init=False, default=None)
    experiments: list[ExperimentItem] = field(init=False, default=[])
    options: dict[str, any] = field(init=False, default={})

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_dict(self):
        if self.taskName:
            self.dict_data["taskName"] = self.taskName
        if self.options:
            self.dict_data["options"] = self.options
        if self.experiments:
            self.dict_data["experiments"] = [x.to_dict() for x in self.experiments]
        return self.dict_data


@define
class Experiment(BaseDefaultConfig):
    name: str
    taskName: str = field(init=False, default=None)
    chains: list[Chain] = field(init=False, default=[])
    options: dict[str, any] = field(init=False, default={})

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_dict(self):
        if self.taskName:
            self.dict_data["taskName"] = self.taskName
        if self.options:
            self.dict_data["options"] = self.options
        if self.chains:
            self.dict_data["chains"] = [x.to_dict() for x in self.chains]
        return self.dict_data


@define
class Scene(BaseDefaultConfig):
    name: str
    taskName: str = field(init=False, default=None)
    chains: list[Chain] = field(init=False, default=[])
    columns: list[dict[str, any]] = field(init=False, default=[])
    options: dict[str, any] = field(init=False, default={})

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_dict(self):
        if self.taskName:
            self.dict_data["taskName"] = self.taskName
        if self.columns:
            self.dict_data["columns"] = self.columns
        if self.options:
            self.dict_data["options"] = self.options
        if self.chains:
            self.dict_data["chains"] = [x.to_dict() for x in self.chains]
        return self.dict_data


@define
class Service(BaseDefaultConfig):
    name: str
    taskName: str = field(init=False, default=None)
    tasks: list[str] = field(init=False, default=[])
    options: dict[str, any] = field(init=False, default={})
    preTransforms: list[TransformConfig] = field(init=False, default=[])
    transforms: list[TransformConfig] = field(init=False, default=[])
    columns: list[dict[str, any]] = field(init=False, default=[])

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_dict(self):
        if self.taskName:
            self.dict_data["taskName"] = self.taskName
        if self.tasks:
            self.dict_data["tasks"] = self.tasks
        if self.columns:
            self.dict_data["columns"] = self.columns
        if self.options:
            self.dict_data["options"] = self.options
        if self.preTransforms:
            self.dict_data["preTransforms"] = [x.to_dict() for x in self.preTransforms]
        if self.transforms:
            self.dict_data["transforms"] = [x.to_dict() for x in self.transforms]
        return self.dict_data


@define
class RecommendConfig(BaseDefaultConfig):
    layers: list[Layer] = field(default=[])
    experiments: list[Experiment] = field(default=[])
    scenes: list[Scene] = field(default=[])
    services: list[Service] = field(default=[])

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_service(self, **kwargs):
        self.services.append(Service(**kwargs))

    def add_experiment(self, **kwargs):
        self.experiments.append(Experiment(**kwargs))

    def add_layer(self, **kwargs):
        self.layers.append(Layer(**kwargs))

    def add_scene(self, **kwargs):
        self.scenes.append(Scene(**kwargs))

    def to_dict(self):
        if self.layers:
            self.dict_data["layers"] = [x.to_dict() for x in self.layers]
        if self.experiments:
            self.dict_data["experiments"] = [x.to_dict() for x in self.experiments]
        if self.scenes:
            self.dict_data["scenes"] = [x.to_dict() for x in self.scenes]
        if self.services:
            self.dict_data["services"] = [x.to_dict() for x in self.services]
        return self.dict_data


@define
class FeatureConfig(BaseDefaultConfig):
    source: list[Source] = field(default=[])
    sourceTable: list[SourceTable] = field(default=[])
    feature: list[Feature] = field(default=[])
    algoTransform: list[AlgoTransform] = field(default=[])

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_source(self, **kwargs):
        self.source.append(Source(**kwargs))

    def add_sourceTable(self, **kwargs):
        self.sourceTable.append(SourceTable(**kwargs))

    def add_feature(self, **kwargs):
        self.feature.append(Feature(**kwargs))

    def add_algoTransform(self, **kwargs):
        self.algoTransform.append(AlgoTransform(**kwargs))

    def to_dict(self):
        if self.source:
            self.dict_data["source"] = [x.to_dict() for x in self.source]
        if self.sourceTable:
            self.dict_data["sourceTable"] = [x.to_dict() for x in self.sourceTable]
        if self.feature:
            self.dict_data["feature"] = [x.to_dict() for x in self.feature]
        if self.algoTransform:
            self.dict_data["algoTransform"] = [x.to_dict() for x in self.algoTransform]
        return self.dict_data


@define
class OnlineServiceConfig(BaseDefaultConfig):
    feature_service: FeatureConfig
    recommend_service: RecommendConfig

    def to_dict(self):
        return {"feature-service": self.feature_service.to_dict(),
                "recommend-service": self.recommend_service.to_dict()}

