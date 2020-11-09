from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from typing import Text, List

# Keys
LABEL_KEY = 'sample_status'


def transformed_name(key: Text) -> Text:
    """Generate the name of the transformed feature from original name."""
    return key + '_xf'


def transformed_names(keys: List[Text]) -> List[Text]:
    """Transform multiple feature names at once."""
    return [transformed_name(key) for key in keys]
