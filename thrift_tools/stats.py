# ==================================================================================================
# Copyright 2015 Twitter, Inc.
# --------------------------------------------------------------------------------------------------
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this work except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file, or at:
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==================================================================================================

"""
get percentile for values (https://en.wikipedia.org/wiki/Percentile)
(roughly Linear Interpolation Between Closest Ranks)

From:
  https://github.com/twitter/zktraffic/blob/master/zktraffic/stats/util.py
"""

import math


def percentile(values, percent, key=lambda k: k):

  assert isinstance(values, (list, tuple))

  idx = (len(values) - 1) * percent
  floor = math.floor(idx)
  ceil = math.ceil(idx)
  if floor == ceil:
    return key(values[int(idx)])

  a = key(values[int(floor)]) * (ceil - idx)
  b = key(values[int(ceil)]) * (idx - floor)

  return a + b

