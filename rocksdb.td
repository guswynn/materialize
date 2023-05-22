# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE CONNECTION conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> CREATE SOURCE upsert_source
  FROM KAFKA CONNECTION conn (TOPIC
  'test-small2'
  )
  KEY FORMAT TEXT
  VALUE FORMAT BYTES
  ENVELOPE UPSERT WITH (DISK, SIZE = '1')
