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
import time

from cloud_consul import putServiceConfig
from online_flow import DataSource, FeatureInfo, CFModelInfo, OnlineFlow
from online_generator import OnlineGenerator, get_demo_jpa_flow
from enum import Enum

def run_cmd(command):
    ret = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
    return ret.returncode


class OnlineExecutor(object):
    def __init__(self, config):
        self._config = config
        self._generator = OnlineGenerator(configure=config)

    def execute_up(self, **kwargs):
        docker_compose_yaml = kwargs.setdefault("docker_compose_file", "docker_compose.yml")
        compose_content = self._generator.gen_docker_compose()
        docker_compose = open(docker_compose_yaml, "w")
        docker_compose.write(compose_content)
        docker_compose.close()
        if run_cmd(["docker-compose -f %s up -d" % docker_compose_yaml]) == 0:
            online_recommend_config = self._generator.gen_server_config()
            putServiceConfig(online_recommend_config)
            print("online flow up success!")
        else:
            print("online flow up fail!")

    def execute_down(self, **kwargs):
        if run_cmd(["docker-compose down"]) == 0:
            print("online flow down success!")
        else:
            print("online flow down fail!")

    def execute_status(self, **kwargs):
        pass

    def execute_reload(self, **kwargs):
        new_flow = kwargs.setdefault("configure", None)
        self._config = new_flow
        self._generator = OnlineGenerator(configure=self._config)
        self.execute_down(**kwargs)
        self.execute_up(**kwargs)
        print("online flow reload success!")


if __name__ == "__main__":
    online = get_demo_jpa_flow()
    executor = OnlineExecutor(online)
    executor.execute_up()
