#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#!/usr/bin/env python
import requests
import sys
import time
import socket


def main(server_port, model_file_path, rows, batches, timeout, tls_enabled, admin_user, admin_password, sinks):

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    model_file_path_url_fromatted = model_file_path.replace("/", "%2F")
    headers = {"Accept":"*/*", "Content-Type": "multipart/form-data ; boundary=toto"}
    protocol = "http"
    if tls_enabled:
        protocol = "https"

    if len(sinks)==1:
        api_url = protocol + "://localhost:" + server_port + "/datagen/" + sinks[0] + \
            "?model=" + model_file_path_url_fromatted + "&rows=" + rows + \
            "&batches=" + batches
    else:
        sinks_string = ""
        for sink in range(len(sinks)):
            sinks_string += "sinks=" + sinks[sink] + "&"
        api_url = protocol + "://localhost:" + server_port + "/datagen/multiplesinks?" + sinks_string + \
                  "model=" + model_file_path_url_fromatted + "&rows=" + rows + \
                  "&batches=" + batches

    print("Will call API: " + api_url)

    start_time = time.time()

    response = requests.post(api_url, headers=headers, verify=False, auth=(admin_user, admin_password))

    if response.status_code != 200:
        print("Answer from API call is not 200")
        sys.exit(1)

    command_uuid = response.json().get("commandUuid")
    print("Received UUID for command: " + command_uuid)
    print("Follow command progression using a POST request: curl -X POST -H  \"accept: application/json\" \"" + protocol + "://" + socket.gethostname() + ":" + server_port + "/command/getCommandStatus?commandUuid=" + command_uuid +"\"")

    if command_uuid is None or command_uuid == "":
        print("UUID received for command is empty")
        sys.exit(1)

    status = ""
    while (time.time() - start_time) < float(timeout):
        api_status_request = requests.post(protocol + "://localhost:" + server_port + "/command/get?commandUuid=" + command_uuid,
                                           headers=headers, verify=False, auth=(admin_user, admin_password))
        status = api_status_request.json().get("status")
        print("Command is " + status)
        if status == "FINISHED" or status == "FAILED":
            break
        time.sleep(5)

    if (time.time() - start_time) > float(timeout):
        print("Timeout exceeded while waiting for command to finish")
        sys.exit(1)

    if status == "FAILED":
        print("Command finished in FAILED status")
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    if len(sys.argv) >= 5:
        server_port = sys.argv[1]
        model_file_path = sys.argv[2]
        rows = sys.argv[3]
        batches = sys.argv[4]
        timeout = sys.argv[5]
        tls_enabled = sys.argv[6]
        admin_user = sys.argv[7]
        admin_password = sys.argv[8]
        sinks = []
        for i in range(9, len(sys.argv)):
            sinks.append(sys.argv[i])
        main(server_port, model_file_path, rows, batches, timeout, tls_enabled, admin_user, admin_password, sinks)
    else:
        print("Missing arguments - please check call made")
