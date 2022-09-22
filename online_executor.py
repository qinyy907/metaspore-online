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
import subprocess

from cloud_consul import putServiceConfig
from online_flow import ServiceInfo, DataSource, FeatureInfo, CFModelInfo, OnlineFlow, set_flow_default_value
from online_generator import OnlineGenerator


def run_cmd(command):
    ret = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8",
                         timeout=10000)
    return ret.returncode


class OnlineExecutor(object):
    def __init__(self, config):
        self._config = config
        self._generator = OnlineGenerator(configure=config)

    def execute_up(self, **kwargs):
        docker_compose_yaml = kwargs.setdefault("docker_compose_file", "docker-compose.yml")
        compose_content = self._generator.gen_docker_compose()
        docker_compose = open(docker_compose_yaml, "w")
        docker_compose.write(compose_content)
        docker_compose.close()
        if run_cmd(["docker-compose", "-f", docker_compose_yaml, "up"]) == 0:
            online_recommend_config = self._generator.gen_server_config()
            putServiceConfig(online_recommend_config)
            print("online flow up success!")
        else:
            print("online flow up fail!")

    def execute_down(self, **kwargs):
        if run_cmd(["docker-compose", "down"]) == 0:
            print("online flow down success!")
        else:
            print("online flow down fail!")

    def execute_status(self, **kwargs):
        #  to do
        print("online flow execute status success!")

    def execute_reload(self, **kwargs):
        new_flow = kwargs.setdefault("configure", None)
        self._config.update(new_flow)
        self._generator = OnlineGenerator(configure=self._config)
        self.execute_down(**kwargs)
        self.execute_up(**kwargs)
        print("online flow reload success!")


if __name__ == "__main__":
    user = DataSource("amazonfashion_user_feature", "mongo", "jpa", [{"user_id": "str"},
                                                                     {"user_bhv_item_seq": "str"}])
    item = DataSource("amazonfashion_item_feature", "mongo", "jpa", [{"item_id": "str"}, {"category": "str"}])
    summary = DataSource("amazonfashion_item_summary", "mongo", "jpa",
                         [{"item_id": "str"}, {"category": "str"}, {"title": "str"},
                          {"description": "str"},
                          {"image": "str"},
                          {"url": "str"},
                          {"price": "double"}])
    source = FeatureInfo(user, item, summary, None, None, None, None, None)
    cf_models = list()
    swing = DataSource("amazonfashion_swing", "mongo", "jpa", None)
    cf_models.append(CFModelInfo("swing", swing))
    online = set_flow_default_value(OnlineFlow(source, None, cf_models, [], [], None))
    executor = OnlineExecutor(online)
    executor.execute_up()