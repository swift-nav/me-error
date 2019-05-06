#!/bin/bash
aws s3 cp \
  s3://timon-capture/scott-test/00ec3270-7061-4d3d-a570-47b1eb753e04/PK40ac4c023b714c8aaaf2377242a2b1d3/serial-link.log.json \
  ./data/00ec3270-7061-4d3d-a570-47b1eb753e04_PK40ac4c023b714c8aaaf2377242a2b1d3.sbp.json
