# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type import DataType
from materialize.output_consistency.data_type.data_type_category import DataTypeCategory
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.input_data.return_specs.string_return_spec import (
    StringReturnTypeSpec,
)
from materialize.output_consistency.operation.return_type_spec import ReturnTypeSpec

TEXT_TYPE_IDENTIFIER = "TEXT"


class StringDataType(DataType):
    def __init__(self, internal_identifier: str, type_name: str):
        super().__init__(
            internal_identifier,
            type_name,
            DataTypeCategory.STRING,
        )

    def resolve_return_type_spec(
        self, characteristics: set[ExpressionCharacteristics]
    ) -> ReturnTypeSpec:
        is_text_type = self.internal_identifier == TEXT_TYPE_IDENTIFIER
        return StringReturnTypeSpec(is_text_type=is_text_type)


TEXT_DATA_TYPE = StringDataType(TEXT_TYPE_IDENTIFIER, "TEXT")


STRING_DATA_TYPES: list[StringDataType] = []
STRING_DATA_TYPES.append(TEXT_DATA_TYPE)
