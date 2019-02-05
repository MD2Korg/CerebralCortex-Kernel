# Copyright (c) 2019, MD2K Center of Excellence
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import traceback


class CacheHandler:
    def set_cache_value(self, key: str, value: str) -> bool:
        """
        Creates a new cache entry in the cache. Values are overwritten for existing keys.

        Args:
            key: key in the cache
            value: value associated with the key
        Returns:
            bool: True on successful insert or False otherwise.
        Raises:
            ValueError: if key is None or empty
        """
        if not key or not len(key):
            raise ValueError("Key cannot be empty.")

        qry = 'INSERT INTO cc_cache(cache_key,cache_value) VALUES(%s,%s) ON DUPLICATE KEY UPDATE cache_value = %s;'
        data = (key, value, value)
        try:
            self.execute(qry, data, commit=True)
            return True
        except Exception as e:
            self.logging.log(str(traceback.format_exc()))
            return False

    def get_cache_value(self, key: str) -> str:
        """
        Retrieves value from the cache for the given key.

        Args:
            key: key in the cache
        Returns:
            str: The value in the cache
        Raises:
            ValueError: if key is None or empty
        """
        if not key and not len(key):
            raise ValueError("Key cannot be empty.")

        qry = "select cache_value from cc_cache where cache_key=%(key)s"
        vals = {"key": str(key)}

        try:
            rows = self.execute(qry, vals)
            if len(rows) == 0:
                return None
            else:
                return rows[0]["cache_value"]
        except Exception as e:
            self.logging.log(traceback.format_exc())
            return None
