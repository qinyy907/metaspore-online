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
from online_flow import DataSource, FeatureInfo, CFModelInfo, OnlineFlow
from online_generator import OnlineGenerator, get_demo_jpa_flow
from enum import Enum


class Status(Enum):
    Init = 0
    DockerCompose_Up_Success = 1
    DockerCompose_Up_Fail = 2
    Service_Config_Success = 3
    Service_Down_Success = 4
    Service_Down_Fail = 5


def run_cmd(command):
    ret = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8",
                         timeout=10000)
    return ret.returncode


class OnlineExecutor(object):
    def __init__(self, config):
        self._config = config
        self._status = Status.Init
        self._generator = OnlineGenerator(configure=config)

    def execute_up(self, **kwargs):
        docker_compose_yaml = kwargs.setdefault("docker_compose_file", "docker-compose.yml")
        compose_content = self._generator.gen_docker_compose()
        docker_compose = open(docker_compose_yaml, "w")
        docker_compose.write(compose_content)
        docker_compose.close()
        if run_cmd(["docker-compose", "-f", docker_compose_yaml, "up"]) == 0:
            self._status = Status.DockerCompose_Up_Success
            online_recommend_config = self._generator.gen_server_config()
            putServiceConfig(online_recommend_config)
            self._status = Status.Service_Config_Success
            print("online flow up success!")
        else:
            self._status = Status.DockerCompose_Up_Fail
            print("online flow up fail!")

    def execute_down(self, **kwargs):
        if self._status not in [Status.Service_Config_Success,
                                Status.DockerCompose_Up_Success]:
            print("online flow is not up!")
        else:
            if run_cmd(["docker-compose", "down"]) == 0:
                self._status = Status.Service_Down_Success
                print("online flow down success!")
            else:
                self._status = Status.Service_Down_Fail
                print("online flow down fail!")

    def execute_status(self, **kwargs):
        if self._status == Status.Init:
            print("online flow execute init success!")
        if self._status == Status.DockerCompose_Up_Success:
            print("online flow execute dockerCompose up success!")
        if self._status == Status.DockerCompose_Up_Fail:
            print("online flow execute dockerCompose up fail!")
        if self._status == Status.Service_Config_Success:
            print("online flow execute set service config success!")
        if self._status == Status.Service_Down_Success:
            print("online flow execute dockerCompose down success!")
        if self._status == Status.Service_Down_Fail:
            print("online flow execute dockerCompose down fail!")

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
