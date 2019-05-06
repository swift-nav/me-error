#!/bin/bash
cat ./data/00ec3270-7061-4d3d-a570-47b1eb753e04_PK40ac4c023b714c8aaaf2377242a2b1d3.sbp.json | json2json | jq '.data | select(.msg_type==74)' -rc > ./data/00ec3270-7061-4d3d-a570-47b1eb753e04_PK40ac4c023b714c8aaaf2377242a2b1d3_msg74decoded.sbp.json
