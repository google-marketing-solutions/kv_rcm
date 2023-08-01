# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Key Value Recommendation for UPR helper functions."""

import json
import logging
import types

import yaml


logging.basicConfig(
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logging.INFO,
)

_DEFAULT_CONFIGS = 'configs.yaml'


def get_configs(filename: str) -> types.SimpleNamespace:
  """Gets configuration details from yaml file.

  Args:
    filename: Relative or full path to config file. If not provided, default
      config.yaml is used.

  Returns:
    A namespace object holding configuration.
  """
  if not filename:
    filename = _DEFAULT_CONFIGS
    logging.info('Config file is not provided. Using default configs file.')

  with open(filename, 'r') as f:
    contents = yaml.safe_load(f)

  config = json.loads(
      json.dumps(contents),
      object_hook=lambda content: types.SimpleNamespace(**content))
  return config
