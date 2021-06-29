from dbt_coves.utils.flags import DbtCovesFlags
from dbt_coves.core.main import parser


def test_dbt_coves_flags():
    flags = DbtCovesFlags(parser)
    assert flags.log_level == "info"